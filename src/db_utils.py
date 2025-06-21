
from sqlalchemy import text
from typing import Union, List, Dict
from src.config import db, logger, DB_SCHEMA, engine, redis_client
import os
import re
import time
import hashlib
import json
from functools import wraps
from contextlib import contextmanager

# ==============================
# User Management
# ==============================
def get_user_by_email(email: str):
    """Fetch user by email from info.users table."""
    with engine.connect() as conn:
        sql = text(f"SELECT id, email, username, hashed_password FROM {DB_SCHEMA}.users WHERE email = :email")
        row = conn.execute(sql, {"email": email}).first()
        if not row:
            return None
        return type("User", (), dict(id=row.id, email=row.email, username=row.username, hashed_password=row.hashed_password))

def get_user_by_username(username: str):
    """Fetch user by username from info.users table."""
    with engine.connect() as conn:
        sql = text(f"SELECT id, email, username, hashed_password FROM {DB_SCHEMA}.users WHERE username = :username")
        row = conn.execute(sql, {"username": username}).first()
        if not row:
            return None
        return type("User", (), dict(id=row.id, email=row.email, username=row.username, hashed_password=row.hashed_password))

def create_user(username: str, email: str, hashed_password: str):
    """Insert new user into info.users table."""
    with engine.connect() as conn:
        sql = text(f"INSERT INTO {DB_SCHEMA}.users (username, email, hashed_password) VALUES (:username, :email, :hpwd)")
        conn.execute(sql, {"username": username, "email": email, "hpwd": hashed_password})
        conn.commit()
        return get_user_by_email(email)

# ==============================
# Configuration
# ==============================
QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", "30"))
MAX_COMPLEXITY = int(os.getenv("MAX_QUERY_COMPLEXITY", "15"))

# ==============================
# SQL Safety Patterns
# ==============================
ALLOWED_SQL_PATTERNS = [
    r'^\s*SELECT\s+',
    r'^\s*WITH\s+',
    r'^\s*EXPLAIN\s+',
]

BLOCKED_SQL_PATTERNS = [
    r'\bINSERT\b', r'\bUPDATE\b', r'\bDELETE\b',
    r'\bDROP\b', r'\bTRUNCATE\b', r'\bALTER\b',
    r'\bCREATE\b', r'\bGRANT\b', r'\bREVOKE\b',
    r'\bEXEC\b', r'\bEXECUTE\b', r'\bDECLARE\b',
    r'\b;\s*--', r'\b;\s*#',
    r'\bSHUTDOWN\b', r'\bXP_\b',
    r'\bFROM\s+PG_', r'\bCOPY\s+',
    r'\bUNION\s+ALL\s+SELECT',
]

# ==============================
# Utility Functions
# ==============================
@contextmanager
def query_timer(query_type: str):
    start_time = time.time()
    try:
        yield
    finally:
        logger.info(f"Query execution time - {query_type}: {time.time() - start_time:.2f}s")

def get_query_hash(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()

# ==============================
# SQL Complexity Analyzer
# ==============================
def analyze_query_complexity(query: str) -> Dict:
    query_upper = query.upper()
    # strip out parenthetical sub-expressions to simplify join count
    query_upper = re.sub(r'\(.*?\)', '', query_upper)
    warnings = []

    complexity = {
        "is_expensive": False,
        "join_count": 0,
        "has_cross_join": False,
        "has_multiple_joins": False,
        "estimated_cost": "low",
        "warnings": warnings
    }

    # detect any CROSS JOIN
    if re.search(r'\bCROSS\s+JOIN\b', query_upper):
        complexity["has_cross_join"] = True
        complexity["is_expensive"] = True
        complexity["estimated_cost"] = "very_high"
        warnings.append("Cross join detected")

    # count all join keywords
    join_patterns = [
        r'\bINNER\s+JOIN\b', r'\bLEFT\s+JOIN\b',
        r'\bRIGHT\s+JOIN\b', r'\bFULL\s+JOIN\b', r'\bJOIN\b'
    ]
    total_joins = sum(len(re.findall(p, query_upper)) for p in join_patterns)
    complexity["join_count"] = total_joins

    # treat more than 3 joins as expensive
    MAX_JOINS_THRESHOLD = 8
    if total_joins > MAX_JOINS_THRESHOLD:
        complexity["has_multiple_joins"] = True
        complexity["is_expensive"] = True
        complexity["estimated_cost"] = "high"
        warnings.append(f"Multiple joins detected ({total_joins})")

    # detect expensive subquery patterns
    expensive_patterns = [
        r'SELECT\s+\*.*FROM.*WHERE.*IN\s*\(\s*SELECT',
        r'EXISTS\s*\(\s*SELECT.*FROM.*WHERE'
    ]
    for pattern in expensive_patterns:
        if re.search(pattern, query_upper):
            complexity["is_expensive"] = True
            if complexity["estimated_cost"] == "low":
                complexity["estimated_cost"] = "high"
            warnings.append("Expensive subquery pattern detected")

    return complexity

# ==============================
# Cache Decorator
# ==============================
def cache_schema(timeout=3600):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not redis_client:
                return func(*args, **kwargs)

            cache_key = f"schema:{DB_SCHEMA}"
            try:
                cached = redis_client.get(cache_key)
                if cached:
                    logger.info("Schema loaded from cache")
                    return cached
            except Exception as e:
                logger.warning(f"Cache read failed: {e}")

            result = func(*args, **kwargs)

            try:
                redis_client.setex(cache_key, timeout, result)
                logger.info("Schema cached successfully")
            except Exception as e:
                logger.warning(f"Cache write failed: {e}")

            return result
        return wrapper
    return decorator

# ==============================
# SQL Validation
# ==============================
def is_safe_sql(query: str) -> bool:
    if not query:
        return False

    normalized_query = query.upper().strip()

    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, normalized_query, re.IGNORECASE):
            logger.warning(f"Blocked SQL pattern detected: {pattern}")
            return False

    required_schema_prefix = f"{DB_SCHEMA.upper()}."
    quoted_schema_prefix = f'"{DB_SCHEMA.upper()}".'
    if required_schema_prefix not in normalized_query and quoted_schema_prefix not in normalized_query:
        logger.warning(f"SQL blocked - missing required schema prefix '{DB_SCHEMA.upper()}.'")
        return False

    if not any(re.match(p, normalized_query, re.IGNORECASE) for p in ALLOWED_SQL_PATTERNS):
        return False

    complexity = analyze_query_complexity(query)
    if complexity["is_expensive"]:
        logger.warning(f"Query blocked due to high complexity: {complexity}")
        return False

    return True

# ==============================
# Database Query Execution
# ==============================
def safe_db_run(query: str, params: Dict = None, timeout: int = QUERY_TIMEOUT) -> Union[List[Dict], str]:
    logger.info(f"Validating SQL query: {query}")
    if not is_safe_sql(query):
        logger.critical(f"Blocked potentially unsafe query: {query}")
        return "Error: Query blocked for security reasons"

    try:
        with engine.connect() as conn:
            stmt = text(query)
            result = conn.execute(stmt, params) if params else conn.execute(stmt)
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        logger.error(f"Error executing query: {query}\nError: {e}", exc_info=True)
        return f"Error: {str(e)}"

def cached_query_execution(query: str, cache_timeout: int = 300):
    if not redis_client:
        return safe_db_run(query)

    query_hash = get_query_hash(query)
    cache_key = f"query_result:{query_hash}"

    try:
        cached_result = redis_client.get(cache_key)
        if cached_result:
            logger.info("Query result loaded from cache")
            return json.loads(cached_result)
    except Exception as e:
        logger.warning(f"Cache read failed: {e}")

    result = safe_db_run(query)
    if not isinstance(result, str) or not result.startswith("Error:"):
        try:
            redis_client.setex(cache_key, cache_timeout, json.dumps(result, default=str))
            logger.info("Query result cached successfully")
        except Exception as e:
            logger.warning(f"Cache write failed: {e}")
    return result

# ==============================
# Schema Representation
# ==============================
@cache_schema()
def get_dynamic_schema_representation(target_schema: str = DB_SCHEMA) -> str:
    logger.info(f"Fetching dynamic schema representation for schema: {target_schema}")
    try:
        with query_timer("schema_fetch"):
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

# ==============================
# Manual Testing Block
# ==============================
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