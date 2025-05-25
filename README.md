# NL2SQL Project with Dynamic Schema and Llama-3-70B

This project implements a Natural Language to SQL (NL2SQL) system using LangChain, LangGraph, Groq (for Llama-3-70B access), and PostgreSQL. It dynamically fetches database schema information to guide the LLM in generating SQL queries.

## Project Structure

```
/
├── .env                  # Environment variables (API key, DB credentials) - **CONFIGURE THIS**
├── requirements.txt      # Project dependencies
├── README.md             # This file
├── logs/                 # Directory for log files (created automatically)
└── src/                  # Source code directory
    ├── __init__.py
    ├── config.py         # Configuration loading, LLM/DB initialization, logging setup
    ├── db_utils.py       # Dynamic schema fetching, safe query execution
    ├── prompts.py        # LLM prompt templates
    ├── tools.py          # LangChain tool definitions
    ├── workflow.py       # LangGraph state machine definition
    └── main.py           # Example entry point for running the workflow
```

## Setup

1.  **Prerequisites:**
    *   Python 3.9+
    *   Access to a PostgreSQL database with the target schema (e.g., `info`).
    *   A Groq API key (https://console.groq.com/keys).

2.  **Clone/Download:** Obtain the project files.

3.  **Configure Environment:**
    *   Rename or copy the `.env` file.
    *   Edit the `.env` file and replace the placeholder values with your actual Groq API key and PostgreSQL database connection details (host, port, user, password, database name, and the specific schema name you want to query, e.g., `info`).
    ```dotenv
    # Groq API Key
    GROQ_API_KEY="YOUR_GROQ_API_KEY"

    # PostgreSQL Database Configuration
    DB_HOST="your_db_host"
    DB_PORT="5432"
    DB_USER="your_db_user"
    DB_PASSWORD="your_db_password"
    DB_NAME="your_db_name"
    DB_SCHEMA="info" # <-- Ensure this matches your target schema
    ```

4.  **Install Dependencies:**
    *   Navigate to the project's root directory (where `requirements.txt` is located) in your terminal.
    *   Create and activate a virtual environment (recommended):
        ```bash
        python -m venv venv
        source venv/bin/activate  # On Windows use `venv\Scripts\activate`
        ```
    *   Install the required packages:
        ```bash
        pip install -r requirements.txt
        ```

## Running the Example

1.  Ensure your PostgreSQL database server is running and accessible with the credentials provided in `.env`.
2.  Make sure your virtual environment is activated.
3.  Run the main script from the project's root directory:
    ```bash
    python src/main.py
    ```
4.  The script will execute the default example query ("List products with stock level below 50") defined in `src/main.py`. You can modify this query in the `main.py` file to ask different questions.
5.  The application will log its progress to both the console and the `logs/nl2sql_app.log` file.
6.  The final natural language answer generated from the database results will be printed to the console.

## How it Works

1.  The `main.py` script initiates the LangGraph workflow defined in `workflow.py`.
2.  The workflow starts by fetching the database schema dynamically using functions in `db_utils.py` and configuration from `config.py`.
3.  It uses the Llama-3-70B model (via Groq) and prompts from `prompts.py` to generate a PostgreSQL query based on the user's question and the fetched schema.
4.  The generated query is executed against the database using the tool defined in `tools.py`.
5.  If the query fails, a correction loop attempts to fix the SQL using the LLM and the error message.
6.  Once a query executes successfully, the results are passed to the LLM again with a formatting prompt to generate a user-friendly natural language answer.

