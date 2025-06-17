from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# --- SQL Generation Prompt ---
# This prompt now takes the dynamically fetched schema representation.
sql_gen_system_prompt_template = """
You are an expert PostgreSQL query writer. Your ONLY task is to generate a single, syntactically correct PostgreSQL query based on the provided schema and the user's question.

DATABASE SCHEMA:
{schema}

USER QUESTION:
{user_input}

GUIDELINES:
- Output ONLY the SQL query. Do not include explanations, commentary, markdown, or any text other than the SQL query itself.
- Ensure the query is valid for PostgreSQL.
- If the user asks for a specific number of results, use that number. 
- Order results by a relevant column to provide meaningful output (e.g., ORDER BY total_amount DESC, ORDER BY order_date ASC).
- Never SELECT *. Only select the columns necessary to answer the question.
- If the question cannot be answered with the given schema, generate a query that returns an empty result set (e.g., SELECT NULL WHERE 1=0), but DO NOT explain why.
- CRITICAL: NEVER generate Data Manipulation Language (DML) statements (INSERT, UPDATE, DELETE, DROP, etc.). Only SELECT queries are allowed.
- Pay close attention to table and column names provided in the schema.
- Use the specified schema name (e.g., {schema_name}.table_name) when referencing tables.
"""

sql_generation_prompt = ChatPromptTemplate.from_messages([
    ("system", sql_gen_system_prompt_template),
    # No human message needed here as the user input is part of the system prompt template
])

# --- SQL Correction Prompt ---
# Takes the failed SQL, error message, schema, and original question.
sql_correction_system_template = """
You are a PostgreSQL expert specializing in debugging SQL queries.
The previous query you generated failed.

DATABASE SCHEMA:
{schema}

ORIGINAL USER QUESTION:
{user_input}

FAILED SQL QUERY:
{sql_query}

DATABASE ERROR MESSAGE:
{db_error}

Please analyze the error message and the original query in the context of the schema and the user's question.
Rewrite the SQL query to fix the error.

GUIDELINES:
- Output ONLY the corrected PostgreSQL query.
- Do not include explanations, apologies, markdown, or any text other than the SQL query.
- Ensure the corrected query is valid for PostgreSQL and addresses the specific error.
- Adhere to all the original query generation guidelines (SELECT only, LIMIT 5 default, etc.).
"""

sql_correction_prompt = ChatPromptTemplate.from_messages([
    ("system", sql_correction_system_template),
])

# --- Final Answer Formatting Prompt ---
# Takes the original question and the successful database query result.
final_answer_system_template = """
You are a helpful assistant. The user asked the following question:
'{user_input}'

We executed a query and received the following result from the database:
{db_result}

Based ONLY on the provided database result, formulate a concise and clear natural language answer to the user's original question.
If the database result indicates no data was found or is empty, state that clearly.
Do not add any information not present in the database result.
Do not mention the SQL query that was run.
"""

final_answer_prompt = ChatPromptTemplate.from_messages([
    ("system", final_answer_system_template),
])

# --- Query Check Prompt (Optional but Recommended) ---
# This can be used for an LLM-based check before execution, similar to the original project.
query_check_system = """You are a PostgreSQL expert. Review the following PostgreSQL query for potential syntax errors or common mistakes.

QUERY:
{query_to_check}

If the query appears syntactically correct for PostgreSQL, output the query exactly as it is.
If you find a syntax error, attempt to correct it and output ONLY the corrected query.
Do not add explanations or commentary.
"""

query_check_prompt = ChatPromptTemplate.from_messages([
    ("system", query_check_system),
])
