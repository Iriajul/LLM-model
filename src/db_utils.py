from sqlalchemy import text  # Add this import
from typing import Union, List, Dict
from config import db, logger, DB_SCHEMA, engine  # Ensure engine is imported from config

def get_dynamic_schema_representation(target_schema: str = DB_SCHEMA) -> str:
    """Fetches the schema representation for the specified schema using SQLDatabase methods.

    Args:
        target_schema: The database schema to fetch information for.

    Returns:
        A string containing the schema representation (e.g., CREATE TABLE statements).
    """
    logger.info(f"Fetching dynamic schema representation for schema: {target_schema}")
    try:
        table_names = db.get_usable_table_names()
        if not table_names:
            logger.warning(f"No usable tables found in schema {target_schema}")
            return f"-- No tables found in schema {target_schema}"

        schema_representation = db.get_table_info(table_names)
        logger.info(f"Successfully fetched schema for tables: {table_names}")
        return schema_representation
    except Exception as e:
        logger.error(f"Error fetching schema for schema {target_schema}: {e}", exc_info=True)
        return f"Error: Could not fetch schema information. {e}"

def safe_db_run(query: str) -> Union[List[Dict], str]:
    """Executes a SQL query safely and returns structured data or error message.

    Args:
        query: The SQL query string to execute.

    Returns:
        List of dictionaries (rows) for successful SELECT queries,
        execution message for other queries, or error string.
    """
    logger.info(f"Executing SQL query: {query}")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            
            # Return structured data for SELECT queries
            if query.strip().upper().startswith("SELECT"):
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result]
            
            # For non-SELECT queries, return execution confirmation
            conn.commit()
            return f"Query executed successfully: {result.rowcount} rows affected."
            
    except Exception as e:
        logger.error(f"Error executing query: {query}\nError: {e}", exc_info=True)
        return f"Error: {str(e)}"
    finally:
        # Ensure connection is closed
        if 'conn' in locals() and conn:  # Check if connection exists
            conn.close()
# Example usage (for testing purposes)
if __name__ == "__main__":
    print("--- Testing Dynamic Schema Fetching ---")
    schema_info = get_dynamic_schema_representation()
    print(schema_info)

    print("\n--- Testing Safe DB Run (Example Query) ---")
    test_query = f"SELECT * FROM {DB_SCHEMA}.customers LIMIT 2;"
    result = safe_db_run(test_query)
    print("Structured result:", result)

# Example usage (for testing purposes)
if __name__ == "__main__":
    print("--- Testing Dynamic Schema Fetching ---")
    schema_info = get_dynamic_schema_representation()
    print(schema_info)

    print("\n--- Testing Safe DB Run (Example Query) ---")
    # Replace with a valid query for your schema if needed for testing
    # example_query = f"SELECT COUNT(*) FROM {DB_SCHEMA}.customers;"
    # result = safe_db_run(example_query)
    # print(result)
    print("Example query execution skipped in direct run.")

