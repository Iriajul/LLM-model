from langchain_core.tools import tool
from db_utils import safe_db_run, logger
from typing import Union, List, Dict

@tool
def execute_sql_query(query: str) -> Union[List[Dict], str]:
    """Executes a PostgreSQL query and returns structured data or error message.
    
    Args:
        query: Valid PostgreSQL query string
        
    Returns:
        List of dictionaries (for SELECT queries) or execution message/error string
    """
    logger.info(f"Tool 'execute_sql_query' invoked with query: {query}")
    try:
        result = safe_db_run(query)
        return result
    except Exception as e:
        logger.error(f"Unexpected error in execute_sql_query: {e}")
        return f"Error: Failed to execute query - {str(e)}"
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

