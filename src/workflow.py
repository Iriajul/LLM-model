from typing import TypedDict, Annotated, List, Any
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from pydantic import BaseModel, Field

from config import llm, logger, DB_SCHEMA
from db_utils import get_dynamic_schema_representation
from prompts import sql_generation_prompt, sql_correction_prompt, final_answer_prompt # Optional: query_check_prompt
from tools import execute_sql_query

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

# --- Utility Functions ---
def is_db_error(result: str | None) -> bool:
    """Check if the database result string indicates an error."""
    if result is None:
        return False # No result yet is not an error state
    # Simple check, can be made more robust based on expected error patterns from safe_db_run
    return isinstance(result, str) and result.strip().startswith("Error:")

# --- Graph Nodes ---

def start_node(state: WorkflowState) -> WorkflowState:
    """Initializes the state with the user input."""
    # The initial state passed to invoke should contain the user_input
    logger.info("Workflow started.")
    # Initialize fields if they don't exist
    return {
        **state,
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "error_message": None,
        "correction_attempts": 0,
        "messages": [HumanMessage(content=state["user_input"])] # Start message history
    }

def fetch_schema_node(state: WorkflowState) -> WorkflowState:
    """Fetches the dynamic database schema."""
    logger.info("Fetching database schema...")
    schema = get_dynamic_schema_representation(target_schema=DB_SCHEMA)
    if schema.startswith("Error:"):
        logger.error(f"Failed to fetch schema: {schema}")
        # Decide how to handle schema fetch failure - perhaps end the graph?
        # For now, store the error and potentially end.
        return {**state, "db_schema": "", "error_message": f"Schema Fetch Failed: {schema}"}
    logger.info("Schema fetched successfully.")
    return {**state, "db_schema": schema}

def generate_sql_node(state: WorkflowState) -> WorkflowState:
    """Generates the SQL query using the LLM based on schema and user input."""
    logger.info("Generating SQL query...")
    if not state["db_schema"]:
         logger.error("Cannot generate SQL: Database schema is missing.")
         return {**state, "error_message": "Cannot generate SQL: Database schema is missing."}

    prompt_value = sql_generation_prompt.invoke({
        "schema": state["db_schema"],
        "user_input": state["user_input"],
        "schema_name": DB_SCHEMA # Pass schema name for explicit referencing
    })
    llm_response = llm.invoke(prompt_value)

    sql_query = llm_response.content.strip()
    logger.info(f"Generated SQL: {sql_query}")

    # Add LLM response to messages for context
    messages = state["messages"] + [AIMessage(content=f"Generated SQL: {sql_query}")]

    return {**state, "generated_sql": sql_query, "messages": messages}

def execute_sql_node(state: WorkflowState) -> WorkflowState:
    """Executes the generated SQL query using the tool."""
    sql_query = state["generated_sql"]
    logger.info(f"Attempting to execute SQL: {sql_query}")
    if not sql_query:
        logger.warning("No SQL query to execute.")
        return {**state, "db_result": "Error: No SQL query was generated.", "error_message": "No SQL query generated."}

    # Invoke the tool
    # We wrap the tool call in a message structure LangGraph expects
    tool_call_msg = AIMessage(content="", tool_calls=[{
        "name": execute_sql_query.name,
        "args": {"query": sql_query},
        "id": "tool_exec_sql_1" # Generate a unique ID if needed
    }])

    tool_result = execute_sql_query.invoke({"query": sql_query})

    # Create a ToolMessage with the result
    tool_response_msg = ToolMessage(content=str(tool_result), tool_call_id="tool_exec_sql_1")

    logger.info(f"SQL execution result: {tool_result}")

    # Update state and messages
    messages = state["messages"] + [tool_call_msg, tool_response_msg]

    return {**state, "db_result": str(tool_result), "messages": messages}

def format_final_answer_node(state: WorkflowState) -> WorkflowState:
    """Formats the final answer using the LLM based on the DB result."""
    logger.info("Formatting final answer...")
    db_result = state.get("db_result", "No result found.")

    if is_db_error(db_result):
        logger.warning("Cannot format final answer due to previous DB error.")
        # If there was an uncorrected error, use the error message
        final_answer_content = state.get("error_message", "An error occurred during processing.")
    elif db_result is None:
         final_answer_content = "No data was returned from the database."
    else:
        prompt_value = final_answer_prompt.invoke({
            "user_input": state["user_input"],
            "db_result": db_result
        })
        llm_response = llm.invoke(prompt_value)
        final_answer_content = llm_response.content.strip()

    logger.info(f"Formatted Answer: {final_answer_content}")
    messages = state["messages"] + [AIMessage(content=final_answer_content)]
    return {**state, "messages": messages}

def correction_node(state: WorkflowState) -> WorkflowState:
    """Attempts to correct the SQL query using the LLM."""
    logger.warning(f"SQL execution failed. Attempting correction (Attempt {state['correction_attempts'] + 1}).")

    prompt_value = sql_correction_prompt.invoke({
        "schema": state["db_schema"],
        "user_input": state["user_input"],
        "sql_query": state["generated_sql"],
        "db_error": state["db_result"] # The error message is stored in db_result
    })
    llm_response = llm.invoke(prompt_value)
    corrected_sql = llm_response.content.strip()

    logger.info(f"Corrected SQL: {corrected_sql}")
    messages = state["messages"] + [AIMessage(content=f"Attempting corrected SQL: {corrected_sql}")]

    return {
        **state,
        "generated_sql": corrected_sql, # Overwrite with corrected SQL
        "correction_attempts": state["correction_attempts"] + 1,
        "db_result": None, # Clear previous error result
        "error_message": None, # Clear previous error message
        "messages": messages
    }

def handle_error_node(state: WorkflowState) -> WorkflowState:
    """Handles cases where schema fetching fails or correction attempts are exhausted."""
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

# --- Conditional Edges ---
def decide_after_schema_fetch(state: WorkflowState) -> str:
    """Decide whether to proceed with SQL generation or handle schema fetch error."""
    if state.get("error_message") and "Schema Fetch Failed" in state["error_message"]:
        logger.error("Routing to error handler due to schema fetch failure.")
        return "handle_error"
    logger.info("Schema fetched successfully, routing to SQL generation.")
    return "generate_sql"

def decide_after_execution(state: WorkflowState) -> str:
    """Decide whether to format the answer, attempt correction, or handle max attempts."""
    db_result = state.get("db_result")
    if is_db_error(db_result):
        if state["correction_attempts"] < MAX_CORRECTION_ATTEMPTS:
            logger.warning("SQL error detected, routing to correction node.")
            return "attempt_correction"
        else:
            logger.error("SQL error detected and max correction attempts reached, routing to error handler.")
            # Update error message for the handler node
            state["error_message"] = f"Max correction attempts ({MAX_CORRECTION_ATTEMPTS}) reached."
            return "handle_error"
    logger.info("SQL execution successful, routing to format final answer.")
    return "format_answer"

# --- Build the Graph ---
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
workflow_graph.add_edge(START, "start") # Entry point
workflow_graph.add_edge("start", "fetch_schema")

# Conditional edge after fetching schema
workflow_graph.add_conditional_edges(
    "fetch_schema",
    decide_after_schema_fetch,
    {
        "generate_sql": "generate_sql",
        "handle_error": "handle_error"
    }
)

workflow_graph.add_edge("generate_sql", "execute_sql")

# Conditional edge after execution
workflow_graph.add_conditional_edges(
    "execute_sql",
    decide_after_execution,
    {
        "format_answer": "format_answer",
        "attempt_correction": "attempt_correction",
        "handle_error": "handle_error"
    }
)

# Edge for correction loop
workflow_graph.add_edge("attempt_correction", "execute_sql") # Retry execution after correction

# End nodes
workflow_graph.add_edge("format_answer", END)
workflow_graph.add_edge("handle_error", END)

# Compile the graph
app = workflow_graph.compile()

logger.info("LangGraph workflow compiled successfully.")

