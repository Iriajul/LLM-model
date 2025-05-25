from config import db, logger, DB_SCHEMA

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

def safe_db_run(query: str) -> str:
    """Executes a SQL query safely using db.run_no_throw and logs the interaction.

    Args:
        query: The SQL query string to execute.

    Returns:
        The query result as a string, or an error message string if execution fails.
    """
    logger.info(f"Executing SQL query: {query}")
    try:
        result = db.run_no_throw(query)
        if result is None or result == "":
             # run_no_throw might return empty string or None on error/no result
             # Check for specific error patterns if possible, otherwise assume success with no data
             logger.warning(f"Query executed successfully but returned no result or an empty string: {query}")
             # Depending on how run_no_throw behaves with different errors, you might need more specific error checks here.
             # For now, return a standard message for empty results.
             return "Query executed successfully, but no data was returned."
        logger.info(f"Query result: {result}")
        return str(result) # Ensure result is always a string
    except Exception as e:
        # This catch block might be redundant if run_no_throw truly catches all exceptions
        # But it's good practice for unexpected issues.
        logger.error(f"Unexpected error during query execution: {query}\nError: {e}", exc_info=True)
        return f"Error: An unexpected error occurred during query execution. {e}"

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

