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


