import decimal
import os
from typing import TypedDict, Annotated, List, Any, Optional, Union
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
import datetime
import re
import json
from .db_utils import cached_query_execution, analyze_query_complexity
from .config import llm, logger, DB_SCHEMA
from .db_utils import get_dynamic_schema_representation, is_safe_sql
from .prompts import sql_generation_prompt, sql_correction_prompt, final_answer_prompt
from .tools import execute_sql_query
from .config import EXPORT_API_URL
import requests

import ast

# --- NEW: Helper to get access token ---
def get_access_token():
    user = os.getenv("EXPORT_API_USER")
    pw = os.getenv("EXPORT_API_PASS")
    api_url = os.getenv("EXPORT_API_URL", "http://localhost:8000")
    if not user or not pw:
        logger.error("EXPORT_API_USER or EXPORT_API_PASS not set in environment.")
        return None
    try:
        resp = requests.post(
            f"{api_url}/auth/login",
            json={"": user, "password": pw},
            timeout=10
        )
        if resp.status_code == 200:
            return resp.json().get("access_token")
        else:
            logger.error(f"Login failed: {resp.status_code} {resp.text}")
            return None
    except Exception as e:
        logger.error(f"Error during login for export: {e}")
        return None

#  State Definition
class WorkflowState(TypedDict):
    messages: Annotated[List[Any], add_messages]
    user_input: str
    db_schema: str
    generated_sql: str
    db_result: str | None
    raw_db_result: Optional[Union[list, dict]]
    error_message: str | None
    correction_attempts: int

# Constants 
MAX_CORRECTION_ATTEMPTS = 3
MAX_ROWS_FOR_LLM = 20
MAX_COLS_FOR_LLM = 20
MAX_DB_RESULT_STRING_LENGTH = 6000
EXPORT_API_URL = os.getenv("EXPORT_API_URL", "http://localhost:8000")  # Configurable

def is_db_error(result: str | None) -> bool:
    if result is None:
        return False
    return isinstance(result, str) and result.strip().startswith("Error:")

def truncate_db_result_for_llm(db_result, state: WorkflowState):
    """Truncate results for LLM input - optimized for large datasets"""
    if not db_result:
        return db_result, ""
    
    # Handle string results directly
    if isinstance(db_result, str):
        if len(db_result) > MAX_DB_RESULT_STRING_LENGTH:
            return (
                db_result[:MAX_DB_RESULT_STRING_LENGTH] + "\n... (truncated)",
                "Results truncated to fit the LLM input limit."
            )
        return db_result, ""
    
    # Handle list/dict results
    try:
        if not isinstance(db_result, (list, dict)):
            db_result = [db_result]  # Wrap single results
        
        num_rows = len(db_result)
        truncated = db_result[:MAX_ROWS_FOR_LLM]
        
        # Truncate columns if needed
        if isinstance(truncated[0], dict):
            keys = list(truncated[0].keys())[:MAX_COLS_FOR_LLM]
            truncated = [{k: row.get(k) for k in keys} for row in truncated]
        
        result_str = json.dumps(truncated, default=str, ensure_ascii=False, indent=2)
        
        if len(result_str) > MAX_DB_RESULT_STRING_LENGTH:
            result_str = result_str[:MAX_DB_RESULT_STRING_LENGTH] + "\n... (truncated)"
            return result_str, f"Showing first {min(num_rows, MAX_ROWS_FOR_LLM)} of {num_rows} rows (truncated)"
        
        return result_str, f"Showing first {min(num_rows, MAX_ROWS_FOR_LLM)} of {num_rows} rows"
    
    except Exception:
        return str(db_result)[:MAX_DB_RESULT_STRING_LENGTH], "Results formatted for display"

# Graph Nodes 

def start_node(state: WorkflowState) -> WorkflowState:
    logger.info("Workflow started.")
    return {
        **state,
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "raw_db_result": None,
        "error_message": None,
        "correction_attempts": 0,
        "messages": [HumanMessage(content=state["user_input"])]
    }

def fetch_schema_node(state: WorkflowState) -> WorkflowState:
    logger.info("Fetching database schema...")
    try:
        schema = get_dynamic_schema_representation(target_schema=DB_SCHEMA)
        if schema.startswith("Error:"):
            raise Exception(schema)
        return {**state, "db_schema": schema}
    except Exception as e:
        logger.error(f"Failed to fetch schema: {e}")
        return {
            **state,
            "db_schema": "",
            "error_message": f"Schema Fetch Failed: {e}"
        }

def generate_sql_node(state: WorkflowState) -> WorkflowState:
    logger.info("Generating SQL query...")
    if not state["db_schema"] or state.get("error_message"):
        error = "Cannot generate SQL: " + (state.get("error_message") or "Database schema is missing")
        logger.error(error)
        return {**state, "error_message": error}

    try:
        prompt_value = sql_generation_prompt.invoke({
            "schema": state["db_schema"],
            "user_input": state["user_input"],
            "schema_name": DB_SCHEMA
        })
        llm_response = llm.invoke(prompt_value)
        sql_query = llm_response.content.strip()
        logger.info(f"Generated SQL: {sql_query}")
        return {
            **state,
            "generated_sql": sql_query,
            "messages": state["messages"] + [AIMessage(content=f"Generated SQL: {sql_query}")]
        }
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        return {**state, "error_message": f"SQL Generation Error: {e}"}

def execute_sql_node(state: WorkflowState) -> WorkflowState:
    sql_query = state["generated_sql"]
    
    # Validate SQL safety before execution
    if not sql_query or not is_safe_sql(sql_query):
        error_msg = "Blocked potentially dangerous SQL query"
        logger.critical(error_msg)
        return {
            **state,
            "db_result": error_msg,
            "error_message": error_msg,
            "messages": state["messages"] + [
                ToolMessage(content=error_msg, tool_call_id="sql_validation_block")
            ]
        }
    
    # NEW: Analyze query complexity and warn user
    complexity = analyze_query_complexity(sql_query)
    if complexity["warnings"]:
        logger.info(f"Query complexity warnings: {complexity['warnings']}")
    
    logger.info(f"Executing SQL: {sql_query}")
    try:
        tool_call_msg = AIMessage(content="", tool_calls=[{
            "name": execute_sql_query.name,
            "args": {"query": sql_query},
            "id": "tool_exec_sql_1"
        }])
        
        # Use cached execution instead of direct execution
        tool_result = cached_query_execution(sql_query)
        tool_response_msg = ToolMessage(content=str(tool_result), tool_call_id="tool_exec_sql_1")
        
        # Store raw result for export
        raw_result = tool_result if isinstance(tool_result, (list, dict)) else None
        
        return {
            **state,
            "db_result": str(tool_result),
            "raw_db_result": raw_result,
            "messages": state["messages"] + [tool_call_msg, tool_response_msg]
        }
    except Exception as e:
        error_msg = f"SQL execution failed: {e}"
        logger.error(error_msg)
        return {
            **state,
            "db_result": error_msg,
            "error_message": error_msg
        }

def format_final_answer_node(state: WorkflowState) -> WorkflowState:
    logger.info("Formatting final answer...")

    # Handle error cases first
    if is_db_error(state.get("db_result")) or state.get("error_message"):
        error = state.get("error_message", "Database operation failed")
        logger.warning(f"Cannot format answer due to error: {error}")
        content = f"Sorry, I encountered an error: {error}"
        return {
            **state,
            "messages": state["messages"] + [AIMessage(content=content)]
        }

    # Generate summary for LLM
    truncated_result, notice = truncate_db_result_for_llm(
        state.get("raw_db_result") or state.get("db_result"),
        state
    )

    prompt_input = {
        "user_input": state["user_input"],
        "db_result": f"{notice}\n{truncated_result}" if notice else truncated_result
    }

    try:
        llm_response   = llm.invoke(final_answer_prompt.invoke(prompt_input))
        summary_content = llm_response.content.strip()
        final_content   = summary_content
    except Exception as e:
        logger.error(f"Answer formatting failed: {e}")
        final_content = (
            f"Here are your results:\n{truncated_result}\n\n[Note: AI formatting failed]"
        )

    # Safe logging
    try:
        logger.info(f"Formatted Answer: {final_content}")
    except UnicodeEncodeError:
        logger.info("Formatted Answer: [Contains non-ASCII characters]")

    return {
        **state,
        "messages": state["messages"] + [AIMessage(content=final_content)]
    }

def correction_node(state: WorkflowState) -> WorkflowState:
    attempts = state["correction_attempts"] + 1
    logger.warning(f"Attempting SQL correction (Attempt {attempts}/{MAX_CORRECTION_ATTEMPTS})")
    
    try:
        prompt_value = sql_correction_prompt.invoke({
            "schema": state["db_schema"],
            "user_input": state["user_input"],
            "sql_query": state["generated_sql"],
            "db_error": state["db_result"]
        })
        llm_response = llm.invoke(prompt_value)
        corrected_sql = llm_response.content.strip()
        
        logger.info(f"Corrected SQL: {corrected_sql}")
        return {
            **state,
            "generated_sql": corrected_sql,
            "correction_attempts": attempts,
            "db_result": None,
            "error_message": None,
            "messages": state["messages"] + [
                AIMessage(content=f"Attempting corrected SQL (attempt {attempts}): {corrected_sql}")
            ]
        }
    except Exception as e:
        logger.error(f"Correction failed: {e}")
        return {
            **state,
            "error_message": f"Correction attempt failed: {e}",
            "correction_attempts": attempts
        }

def handle_error_node(state: WorkflowState) -> WorkflowState:
    error_msg = state.get("error_message", "An unknown error occurred")
    logger.error(f"Workflow ending with error: {error_msg}")
    
    if "Schema Fetch Failed" in error_msg:
        content = "Sorry, I couldn't access the database schema. Please try again later."
    elif state["correction_attempts"] >= MAX_CORRECTION_ATTEMPTS:
        last_error = state.get("db_result", "Database error")
        content = f"Sorry, I couldn't find a working solution after {MAX_CORRECTION_ATTEMPTS} attempts. Last error: {last_error}"
    else:
        content = f"Sorry, an unexpected error occurred: {error_msg}"
    
    return {
        **state,
        "messages": state["messages"] + [AIMessage(content=content)]
    }

def decide_after_schema_fetch(state: WorkflowState) -> str:
    if state.get("error_message") and "Schema Fetch Failed" in state["error_message"]:
        return "handle_error"
    return "generate_sql"

def decide_after_execution(state: WorkflowState) -> str:
    if is_db_error(state.get("db_result")):
        if state["correction_attempts"] < MAX_CORRECTION_ATTEMPTS:
            return "attempt_correction"
        state["error_message"] = f"Max corrections reached ({MAX_CORRECTION_ATTEMPTS})"
        return "handle_error"
    return "format_answer"

#Graph construction
workflow_graph = StateGraph(WorkflowState)

# Add nodes
workflow_graph.add_node("start", start_node)
workflow_graph.add_node("fetch_schema", fetch_schema_node)
workflow_graph.add_node("generate_sql", generate_sql_node)
workflow_graph.add_node("execute_sql", execute_sql_node)
workflow_graph.add_node("attempt_correction", correction_node)
workflow_graph.add_node("format_answer", format_final_answer_node)
workflow_graph.add_node("handle_error", handle_error_node)

# Define edges
workflow_graph.add_edge(START, "start")
workflow_graph.add_edge("start", "fetch_schema")

workflow_graph.add_conditional_edges(
    "fetch_schema",
    decide_after_schema_fetch,
    {
        "generate_sql": "generate_sql",
        "handle_error": "handle_error"
    }
)

workflow_graph.add_edge("generate_sql", "execute_sql")

workflow_graph.add_conditional_edges(
    "execute_sql",
    decide_after_execution,
    {
        "format_answer": "format_answer",
        "attempt_correction": "attempt_correction",
        "handle_error": "handle_error"
    }
)

workflow_graph.add_edge("attempt_correction", "execute_sql")
workflow_graph.add_edge("format_answer", END)
workflow_graph.add_edge("handle_error", END)

# Compile the workflow
app = workflow_graph.compile()
logger.info("LangGraph workflow compiled successfully.")