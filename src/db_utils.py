from sqlalchemy import text
from typing import Union, List, Dict
from config import db, logger, DB_SCHEMA, engine
import re

# Safe SQL patterns 
ALLOWED_SQL_PATTERNS = [
    r'^\s*SELECT\s+', 
    r'^\s*WITH\s+',    
    r'^\s*EXPLAIN\s+', 
]

# Blocked SQL patterns 
BLOCKED_SQL_PATTERNS = [
    r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b', 
    r'\bDROP\b', r'\bTRUNCATE\b', r'\bALTER\b',
    r'\bCREATE\b', r'\bGRANT\b', r'\bREVOKE\b',
    r'\bEXEC\b', r'\bEXECUTE\b', r'\bDECLARE\b',
    r'\b;\s*--', r'\b;\s*#',  
    r'\bSHUTDOWN\b', r'\bXP_\b', 
    r'\bFROM\s+PG_',  
    r'\bCOPY\s+',  
    r'\bUNION\s+ALL\s+SELECT',  
]

def is_safe_sql(query: str) -> bool:
    """Check if SQL query contains only allowed patterns and no dangerous commands"""
    if not query:
        return False
        
    normalized_query = query.upper().strip()
    
    # 1. Check for blocked patterns (more critical)
    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, normalized_query, re.IGNORECASE):
            logger.warning(f"Blocked SQL pattern detected: {pattern}")
            return False
    
    # 2. Enforce schema prefix
    required_schema_prefix = f"{DB_SCHEMA.upper()}."
    quoted_schema_prefix = f'"{DB_SCHEMA.upper()}".'
    if (required_schema_prefix not in normalized_query and 
        quoted_schema_prefix not in normalized_query):
        logger.warning(f"SQL blocked - missing required schema prefix '{DB_SCHEMA.upper()}.'")
        return False
    
    # 3. Check for allowed patterns
    return any(re.match(pattern, normalized_query, re.IGNORECASE) 
              for pattern in ALLOWED_SQL_PATTERNS)

def safe_db_run(query: str, params: Dict = None) -> Union[List[Dict], str]:
    """Executes SQL safely with parameter binding and validation"""
    logger.info(f"Validating SQL query: {query}")
    
    # Validate SQL safety
    if not is_safe_sql(query):
        logger.critical(f"Blocked potentially unsafe query: {query}")
        return "Error: Query blocked for security reasons"
    
    logger.info(f"Executing SQL query: {query}")
    try:
        with engine.connect() as conn:
            # Use SQLAlchemy's safe parameter binding
            stmt = text(query)
            result = conn.execute(stmt, params) if params else conn.execute(stmt)
            
            # Handle read only operations
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]
            
    except Exception as e:
        logger.error(f"Error executing query: {query}\nError: {e}", exc_info=True)
        return f"Error: {str(e)}"
    finally:
        # Ensure connection is closed
        if 'conn' in locals() and conn:
            conn.close()

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

# for testing purposes
if __name__ == "__main__":
    print("--- Testing Dynamic Schema Fetching ---")
    schema_info = get_dynamic_schema_representation()
    print(schema_info)

    print("\n--- Testing Safe DB Run (Example Query) ---")
    test_query = f"SELECT * FROM {DB_SCHEMA}.customers LIMIT 2;"
    result = safe_db_run(test_query)
    print("Structured result:", result)
    
    print("\n--- Testing Security Validation ---")
    malicious_queries = [
        "'); DROP TABLE customers; --",
        "SELECT * FROM users; UPDATE users SET admin=true;",
        "EXPLAIN; DELETE FROM orders"
    ]
    
    for query in malicious_queries:
        print(f"\nTesting: {query}")
        result = safe_db_run(query)
        print(f"Result: {result}")
    
    print("\n--- Testing Schema Enforcement ---")
    valid_queries = [
        f"SELECT * FROM {DB_SCHEMA}.customers",
        f'SELECT * FROM "{DB_SCHEMA.upper()}"."CUSTOMERS"',
        f"SELECT * FROM {DB_SCHEMA.upper()}.ORDERS"
    ]
    invalid_queries = [
        "SELECT * FROM customers", 
        "SELECT * FROM other_schema.users",  
        "SELECT * FROM \"WRONG_SCHEMA\".customers" 
    ]
    
    for query in valid_queries:
        print(f"\nTesting valid: {query}")
        result = safe_db_run(query)
        print(f"Result: {type(result)}")
    
    for query in invalid_queries:
        print(f"\nTesting invalid: {query}")
        result = safe_db_run(query)
        print(f"Result: {result}")