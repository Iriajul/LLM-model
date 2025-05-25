from langchain_core.tools import tool
from db_utils import safe_db_run, logger

@tool
def execute_sql_query(query: str) -> str:
    """Executes a given PostgreSQL query against the database and returns the result or an error message.
    Use this tool to run the generated SQL query.
    Input must be a single, valid PostgreSQL query string.
    """
    logger.info(f"Tool 'execute_sql_query' invoked with query: {query}")
    # The actual execution and error handling happen within safe_db_run
    result = safe_db_run(query)
    return result

# You could add other tools here if needed, e.g., a tool to list tables
# directly if the LLM needs it explicitly, although fetching the schema
# representation beforehand is generally preferred.

# from config import db
# @tool
# def list_database_tables() -> str:
#     """Returns a list of table names available in the database schema."""
#     try:
#         table_names = db.get_usable_table_names()
#         logger.info(f"Tool 'list_database_tables' invoked. Found tables: {table_names}")
#         return f"Available tables: {', '.join(table_names)}"
#     except Exception as e:
#         logger.error(f"Error in list_database_tables tool: {e}", exc_info=True)
#         return f"Error: Could not list database tables. {e}"

