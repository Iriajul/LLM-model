import decimal
from typing import TypedDict, Annotated, List, Any, Optional, Union
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from pydantic import BaseModel, Field
import datetime
import re
import json

from config import llm, logger, DB_SCHEMA
from db_utils import get_dynamic_schema_representation
from prompts import sql_generation_prompt, sql_correction_prompt, final_answer_prompt
from tools import execute_sql_query

import ast
import requests

# --- State Definition ---
class WorkflowState(TypedDict):
    messages: Annotated[List[Any], add_messages]
    user_input: str
    db_schema: str
    generated_sql: str
    db_result: str | None
    raw_db_result: Optional[Union[list, dict]]
    error_message: str | None
    correction_attempts: int

# --- Constants ---
MAX_CORRECTION_ATTEMPTS = 3
MAX_ROWS_FOR_LLM = 20
MAX_COLS_FOR_LLM = 20
MAX_DB_RESULT_STRING_LENGTH = 6000

def is_complex_query(sql: str) -> bool:
    """Detects analytical/complex SQL features"""
    complex_patterns = [
        r'\bJOIN\b', r'\bGROUP BY\b', r'\bPARTITION BY\b',
        r'\bWITH\b.+?\bAS\b',  # CTE detection
        r'\bOVER\(\)', r'\bRANK\(\)', r'\bDENSE_RANK\(\)',
        r'\bCASE\b', r'\bUNION\b', r'\bHAVING\b',
        r'\bWINDOW\b', r'\bEXISTS\b', r'\bSUBQUERY\b'
    ]
    sql_upper = sql.upper()
    return any(re.search(pattern, sql_upper) for pattern in complex_patterns)

def is_db_error(result: str | None) -> bool:
    if result is None:
        return False
    return isinstance(result, str) and result.strip().startswith("Error:")

def truncate_db_result_for_llm(db_result, state: WorkflowState):
    """Truncate results for LLM input"""
    if not db_result:
        return db_result, ""
    
    rows = None
    was_truncated = False
    try:
        if isinstance(db_result, str) and db_result.strip().startswith("[") and db_result.strip().endswith("]"):
            rows = json.loads(db_result)
        else:
            rows = ast.literal_eval(db_result) if isinstance(db_result, str) else db_result
    except Exception:
        db_result_str = str(db_result)
        if len(db_result_str) > MAX_DB_RESULT_STRING_LENGTH:
            return (
                db_result_str[:MAX_DB_RESULT_STRING_LENGTH] + "\n... (truncated)",
                "Results truncated to fit the LLM input limit."
            )
        return db_result_str, ""
    
    # Auto-export trigger for large results
    if isinstance(rows, list) and len(rows) > MAX_ROWS_FOR_LLM:
        pass  # We always export now
    
    num_rows = len(rows)
    truncated_rows = rows[:MAX_ROWS_FOR_LLM]
    result_rows = []
    for r in truncated_rows:
        if isinstance(r, (list, tuple)):
            result_rows.append(list(r)[:MAX_COLS_FOR_LLM])
        elif isinstance(r, dict):
            result_rows.append({k: r[k] for k in list(r)[:MAX_COLS_FOR_LLM]})
        else:
            result_rows.append(r)
    
    result_str = json.dumps(result_rows, default=str, ensure_ascii=False, indent=2)
    if len(result_str) > MAX_DB_RESULT_STRING_LENGTH:
        was_truncated = True
        result_str = result_str[:MAX_DB_RESULT_STRING_LENGTH] + "\n... (truncated)"
    
    notice = ""
    if num_rows > MAX_ROWS_FOR_LLM or was_truncated:
        notice = (
            f"Showing first {min(num_rows, MAX_ROWS_FOR_LLM)} of {num_rows} rows, "
            f"first {MAX_COLS_FOR_LLM} columns. Download full results below."
        )
    return result_str, notice

# --- Graph Nodes ---

def start_node(state: WorkflowState) -> WorkflowState:
    logger.info("Workflow started.")
    return {
        **state,
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "error_message": None,
        "correction_attempts": 0,
        "messages": [HumanMessage(content=state["user_input"])]
    }

def fetch_schema_node(state: WorkflowState) -> WorkflowState:
    logger.info("Fetching database schema...")
    schema = get_dynamic_schema_representation(target_schema=DB_SCHEMA)
    if schema.startswith("Error:"):
        logger.error(f"Failed to fetch schema: {schema}")
        return {**state, "db_schema": "", "error_message": f"Schema Fetch Failed: {schema}"}
    logger.info("Schema fetched successfully.")
    return {**state, "db_schema": schema}

def generate_sql_node(state: WorkflowState) -> WorkflowState:
    logger.info("Generating SQL query...")
    if not state["db_schema"]:
        logger.error("Cannot generate SQL: Database schema is missing.")
        return {**state, "error_message": "Cannot generate SQL: Database schema is missing."}

    prompt_value = sql_generation_prompt.invoke({
        "schema": state["db_schema"],
        "user_input": state["user_input"],
        "schema_name": DB_SCHEMA
    })
    llm_response = llm.invoke(prompt_value)
    sql_query = llm_response.content.strip()
    logger.info(f"Generated SQL: {sql_query}")
    messages = state["messages"] + [AIMessage(content=f"Generated SQL: {sql_query}")]
    return {**state, "generated_sql": sql_query, "messages": messages}

def execute_sql_node(state: WorkflowState) -> WorkflowState:
    sql_query = state["generated_sql"]
    
    logger.info(f"Attempting to execute SQL: {sql_query}")
    if not sql_query:
        logger.warning("No SQL query to execute.")
        return {**state, "db_result": "Error: No SQL query generated.", "error_message": "No SQL query generated."}

    tool_call_msg = AIMessage(content="", tool_calls=[{
        "name": execute_sql_query.name,
        "args": {"query": sql_query},
        "id": "tool_exec_sql_1"
    }])
    
    tool_result = execute_sql_query.invoke({"query": sql_query})
    tool_response_msg = ToolMessage(content=str(tool_result), tool_call_id="tool_exec_sql_1")
    
    # ALWAYS store raw result if available
    state["raw_db_result"] = tool_result if isinstance(tool_result, (list, dict)) else None
    
    return {
        **state,
        "db_result": str(tool_result),
        "messages": state["messages"] + [tool_call_msg, tool_response_msg]
    }

def format_final_answer_node(state: WorkflowState) -> WorkflowState:
    logger.info("Formatting final answer...")
    db_result = state.get("db_result", "No result found.")
    
    if is_db_error(db_result):
        logger.warning("Cannot format final answer due to previous DB error.")
        final_answer_content = state.get("error_message", "Processing error occurred.")
    elif db_result is None:
        final_answer_content = "No database results found."
    else:
        download_links = ""
        export_attempted = False
        
        # ALWAYS attempt export if raw result exists
        if state.get("raw_db_result"):
            try:
                # Custom JSON serializer for datetime objects
                def json_serial(obj):
                    if isinstance(obj, (datetime.datetime, datetime.date)):
                        return obj.isoformat()
                    elif isinstance(obj, decimal.Decimal):
                        return float(obj)
                    raise TypeError(f"Type {type(obj)} not serializable")
                
                # Serialize data with datetime handling
                serialized_data = json.dumps(
                    {"data": state["raw_db_result"]},
                    default=json_serial
                )
                
                # Send to export API
                response = requests.post(
                    "http://localhost:8000/export",
                    data=serialized_data,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code == 200:
                    links = response.json()
                    download_links = (
                        "\n\nDownload Full Results:\n"
                        f"- CSV: http://localhost:8000{links['csv_url']}\n"
                        f"- Excel: http://localhost:8000{links['excel_url']}"
                    )
                    export_attempted = True
                else:
                    logger.error(f"Export API error: {response.status_code}")
            except Exception as e:
                logger.error(f"Export failed: {e}")
                # Still show download prompt if data exists
                if state.get("raw_db_result"):
                    download_links = (
                        "\n\n Full results available but export service failed. "
                        "Try refining your query or contact support."
                    )
        
        # Generate summary for all results
        truncated_result, notice = truncate_db_result_for_llm(db_result, state)
        prompt_input = {
            "user_input": state["user_input"],
            "db_result": f"{notice}\n{truncated_result}" if notice else truncated_result
        }
        llm_response = llm.invoke(final_answer_prompt.invoke(prompt_input))
        summary_content = llm_response.content.strip()
        
        # For non-exportable results, explain why
        if not export_attempted and not download_links:
            if state.get("raw_db_result") is None:
                download_links = "\n\n Full download not available (non-dataset result)"
            elif not state.get("raw_db_result"):
                download_links = "\n\n Full download not available (empty dataset)"
        
        # FINAL CONTENT (always include summary + download info)
        final_answer_content = f"{summary_content}{download_links}"

    logger.info(f"Formatted Answer: {final_answer_content}")
    return {
        **state,
        "messages": state["messages"] + [AIMessage(content=final_answer_content)]
    }

# ... [correction_node, handle_error_node, decision functions remain unchanged] ...

def correction_node(state: WorkflowState) -> WorkflowState:
    logger.warning(f"SQL execution failed. Attempting correction (Attempt {state['correction_attempts'] + 1}).")
    prompt_value = sql_correction_prompt.invoke({
        "schema": state["db_schema"],
        "user_input": state["user_input"],
        "sql_query": state["generated_sql"],
        "db_error": state["db_result"]
    })
    llm_response = llm.invoke(prompt_value)
    corrected_sql = llm_response.content.strip()
    logger.info(f"Corrected SQL: {corrected_sql}")
    messages = state["messages"] + [AIMessage(content=f"Attempting corrected SQL: {corrected_sql}")]
    return {
        **state,
        "generated_sql": corrected_sql,
        "correction_attempts": state["correction_attempts"] + 1,
        "db_result": None,
        "error_message": None,
        "messages": messages
    }

def handle_error_node(state: WorkflowState) -> WorkflowState:
    error_msg = state.get("error_message", "An unknown error occurred.")
    if "Schema Fetch Failed" in error_msg:
        logger.error(f"Workflow ending due to schema fetch failure: {error_msg}")
        final_content = f"Sorry, I could not process your request because I failed to retrieve the database schema information. Error: {error_msg}"
    elif state["correction_attempts"] >= MAX_CORRECTION_ATTEMPTS:
        logger.error(f"Workflow ending after {MAX_CORRECTION_ATTEMPTS} correction attempts.")
        last_error = state.get("db_result", "Unknown database error.")
        final_content = f"Sorry, I could not generate a working SQL query for your request after {MAX_CORRECTION_ATTEMPTS} attempts. Last error: {last_error}"
    else:
        logger.error(f"Workflow ending due to unhandled error: {error_msg}")
        final_content = f"Sorry, an unexpected error occurred: {error_msg}"
    messages = state["messages"] + [AIMessage(content=final_content)]
    return {**state, "messages": messages}

def decide_after_schema_fetch(state: WorkflowState) -> str:
    if state.get("error_message") and "Schema Fetch Failed" in state["error_message"]:
        logger.error("Routing to error handler due to schema fetch failure.")
        return "handle_error"
    logger.info("Schema fetched successfully, routing to SQL generation.")
    return "generate_sql"

def decide_after_execution(state: WorkflowState) -> str:
    db_result = state.get("db_result")
    if is_db_error(db_result):
        if state["correction_attempts"] < MAX_CORRECTION_ATTEMPTS:
            logger.warning("SQL error detected, routing to correction node.")
            return "attempt_correction"
        else:
            logger.error("SQL error detected and max correction attempts reached, routing to error handler.")
            state["error_message"] = f"Max correction attempts ({MAX_CORRECTION_ATTEMPTS}) reached."
            return "handle_error"
    logger.info("SQL execution successful, routing to format final answer.")
    return "format_answer"

workflow_graph = StateGraph(WorkflowState)
workflow_graph.add_node("start", start_node)
workflow_graph.add_node("fetch_schema", fetch_schema_node)
workflow_graph.add_node("generate_sql", generate_sql_node)
workflow_graph.add_node("execute_sql", execute_sql_node)
workflow_graph.add_node("attempt_correction", correction_node)
workflow_graph.add_node("format_answer", format_final_answer_node)
workflow_graph.add_node("handle_error", handle_error_node)
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
app = workflow_graph.compile()
logger.info("LangGraph workflow compiled successfully.")