from typing import TypedDict, Annotated, List, Any
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from pydantic import BaseModel, Field

from config import llm, logger, DB_SCHEMA
from db_utils import get_dynamic_schema_representation
from prompts import sql_generation_prompt, sql_correction_prompt, final_answer_prompt
from tools import execute_sql_query

import ast
import json

# --- State Definition ---
class WorkflowState(TypedDict):
    messages: Annotated[List[Any], add_messages]
    user_input: str
    db_schema: str
    generated_sql: str
    db_result: str | None
    error_message: str | None
    correction_attempts: int

# --- Constants ---
MAX_CORRECTION_ATTEMPTS = 2
MAX_ROWS_FOR_LLM = 20    # Lowered for safety
MAX_COLS_FOR_LLM = 20     # Lowered for safety
MAX_DB_RESULT_STRING_LENGTH = 6000  # Absolute safety net (tokens ~= chars for prompt)

def is_db_error(result: str | None) -> bool:
    if result is None:
        return False
    return isinstance(result, str) and result.strip().startswith("Error:")

def truncate_db_result_for_llm(db_result):
    """
    Truncate the DB result for the LLM, both by rows, columns, and string length.
    Always returns a reasonably small string and a notice if truncation occurred.
    """
    if not db_result:
        return db_result, ""
    rows = None
    was_truncated = False
    try:
        # Try JSON first
        if isinstance(db_result, str) and db_result.strip().startswith("[") and db_result.strip().endswith("]"):
            rows = json.loads(db_result)
        else:
            rows = ast.literal_eval(db_result) if isinstance(db_result, str) else db_result
    except Exception:
        # If cannot parse, treat as a single string (maybe summary, just string slice)
        db_result_str = str(db_result)
        if len(db_result_str) > MAX_DB_RESULT_STRING_LENGTH:
            was_truncated = True
            return (
                db_result_str[:MAX_DB_RESULT_STRING_LENGTH] + f"\n... (truncated, output too large)",
                "Results truncated to fit the LLM input limit."
            )
        return db_result_str, ""
    # Truncate rows
    num_rows = len(rows)
    truncated_rows = rows[:MAX_ROWS_FOR_LLM]
    # Truncate columns
    result_rows = []
    for r in truncated_rows:
        if isinstance(r, (list, tuple)):
            result_rows.append(list(r)[:MAX_COLS_FOR_LLM])
        elif isinstance(r, dict):
            # only take up to N key/value pairs
            result_rows.append({k: r[k] for k in list(r)[:MAX_COLS_FOR_LLM]})
        else:
            result_rows.append(r)
    result_str = json.dumps(result_rows, default=str, ensure_ascii=False, indent=2)
    # Absolute fallback: limit result_str length
    if len(result_str) > MAX_DB_RESULT_STRING_LENGTH:
        was_truncated = True
        result_str = result_str[:MAX_DB_RESULT_STRING_LENGTH] + "\n... (truncated, output too large)"
    notice = ""
    if num_rows > MAX_ROWS_FOR_LLM or was_truncated:
        notice = (
            f"Showing only the first {min(num_rows,MAX_ROWS_FOR_LLM)} out of {num_rows} rows, "
            f"and only the first {MAX_COLS_FOR_LLM} columns per row. "
            "Please refine your query for more specific results."
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
        return {**state, "db_result": "Error: No SQL query was generated.", "error_message": "No SQL query generated."}
    tool_call_msg = AIMessage(content="", tool_calls=[{
        "name": execute_sql_query.name,
        "args": {"query": sql_query},
        "id": "tool_exec_sql_1"
    }])
    tool_result = execute_sql_query.invoke({"query": sql_query})
    tool_response_msg = ToolMessage(content=str(tool_result), tool_call_id="tool_exec_sql_1")
    logger.info(f"SQL execution result: {tool_result}")
    messages = state["messages"] + [tool_call_msg, tool_response_msg]
    return {**state, "db_result": str(tool_result), "messages": messages}

def format_final_answer_node(state: WorkflowState) -> WorkflowState:
    logger.info("Formatting final answer...")
    db_result = state.get("db_result", "No result found.")
    if is_db_error(db_result):
        logger.warning("Cannot format final answer due to previous DB error.")
        final_answer_content = state.get("error_message", "An error occurred during processing.")
    elif db_result is None:
        final_answer_content = "No data was returned from the database."
    else:
        truncated_result, notice = truncate_db_result_for_llm(db_result)
        prompt_input = {
            "user_input": state["user_input"],
            "db_result": (notice + "\n" if notice else "") + truncated_result
        }
        prompt_value = final_answer_prompt.invoke(prompt_input)
        llm_response = llm.invoke(prompt_value)
        final_answer_content = llm_response.content.strip()
    logger.info(f"Formatted Answer: {final_answer_content}")
    messages = state["messages"] + [AIMessage(content=final_answer_content)]
    return {**state, "messages": messages}

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