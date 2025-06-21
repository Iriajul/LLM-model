# NL2SQL Project with Dynamic Schema and Llama-3-70B

A modern, secure, and modular **Natural Language to SQL** (NL2SQL) application with export and download capabilities, powered by FastAPI, LangGraph, Streamlit, and PostgreSQL.

---

## Features

- **Natural Language to SQL**: Ask questions in plain English and get SQL queries and results.
- **Secure Export API**: Export query results as CSV or Excel files via authenticated FastAPI endpoints.
- **JWT Authentication**: Secure login, access token, and refresh token support.
- **Streamlit Frontend**: User-friendly web interface for querying, exporting, and downloading data.
- **Database & Redis Integration**: PostgreSQL for data, Redis for caching and token/session management.
- **LLM Integration**: Uses LLM (e.g., Llama 3) for SQL generation and answer formatting.
- **Security**: SQL injection protection, query complexity checks, and schema enforcement.
- **Extensible**: Modular codebase for easy customization and extension.
---

## Project Structure

```
/
├── .env                  # Environment variables (API key, DB credentials) - **CONFIGURE THIS**
├── requirements.txt      # Project dependencies
├── README.md             # This file
├── logs/                 # Directory for log files (created automatically)
└── src/                  # Source code directory
    ├── __init__.py
    ├── config.py         # Configuration loading, LLM/DB/Redis initialization, logging setup
    ├── db_utils.py       # Dynamic schema fetching, safe query execution, SQL validation
    ├── prompts.py        # LLM prompt templates
    ├── tools.py          # LangChain tool definitions
    ├── workflow.py       # LangGraph state machine definition
    ├── main.py           # Example entry point for running the workflow and export
    ├── monitoring.py     # Monitoring and health checks
    ├── redis_check.py    # Redis connection check utility
    ├── test_security.py  # Security and complexity test scripts
    ├── streamlit_app.py  # Streamlit web UI for interactive querying and export
    └── export_api/       # FastAPI export and authentication API
        ├── __init__.py
        ├── main.py           # FastAPI app and router registration
        ├── config.py         # Export API-specific config (JWT, export dir)
        ├── auth.py           # Auth endpoints (login, refresh, logout), JWT logic
        ├── models.py         # Pydantic models for API requests/responses
        ├── utils.py          # Utility functions (file cleanup, path safety)
        └── routers/
            ├── export.py     # /export endpoint logic
            └── download.py   # /download endpoint logic
```
## Quick Start

### 1. **Install Dependencies**

```bash
pip install -r requirements.txt
```

### 2. **Configure Environment**

- Edit `.env` and `.streamlit/secrets.toml` with your database, Redis, and API credentials.

### 3. **Run the FastAPI Export API**

```bash
uvicorn src.export_api.main:app --reload --host 127.0.0.1 --port 8000
```

### 4. **Run the Streamlit App**

```bash
streamlit run src/streamlit_app.py
```

---

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


## API Endpoints

| Endpoint                              | Method | Purpose                                      | Auth Required? |
|---------------------------------------|--------|----------------------------------------------|----------------|
| `/auth/login`                         | POST   | User login, returns access (JWT) token       | No             |
| `/auth/refresh`                       | POST   | Get a new access token using refresh token   | Yes (cookie)   |
| `/auth/logout`                        | POST   | Log out, deletes refresh token               | Yes (cookie)   |
| `/export`                             | POST   | Export data (CSV/Excel), returns download URI| Yes (JWT)      |
| `/download/{file_id}.{format}`        | GET    | Download exported file (CSV/Excel)           | Yes (JWT)      |
| `/health`                             | GET    | Health check/status                          | No             |
| `/docs`                               | GET    | Swagger/OpenAPI docs                         | No             |

---

## Authentication Flow

1. **Login**:  
   `POST /auth/login` with username/email and password.  
   Returns JWT access token and sets a refresh token cookie.

2. **Export**:  
   `POST /export` with JWT in the `Authorization` header.  
   Returns download URLs for CSV/Excel.

3. **Download**:  
   `GET /download/{file_id}.{format}` with JWT in the `Authorization` header.

4. **Refresh Token**:  
   `POST /auth/refresh` with refresh token cookie to get a new access token.

5. **Logout**:  
   `POST /auth/logout` to invalidate the refresh token.

---

## Security

- **JWT** for all protected endpoints.
- **Refresh tokens** stored securely in Redis and as HTTP-only cookies.
- **SQL validation**: Only allows safe, schema-qualified SELECT queries.
- **Query complexity analysis**: Blocks expensive or dangerous queries.
- **Password hashing**: All user passwords are hashed with bcrypt.

---

## Customization

- **Add new tables**: Update your database and schema.
- **Change LLM model**: Edit `.env` and `prompts.py` as needed.
- **Extend API**: Add new endpoints in `export_api/routers/`.

---

## Development & Testing

- **Unit and security tests**: See `test_security.py`.
- **Manual DB and schema tests**: Run `db_utils.py` as a script.
- **Monitoring**: See `monitoring.py` for performance tracking.

---

## Credits

- Built with [FastAPI](https://fastapi.tiangolo.com/), [Streamlit](https://streamlit.io/), [SQLAlchemy](https://www.sqlalchemy.org/), [LangGraph](https://github.com/langchain-ai/langgraph), and [PostgreSQL](https://www.postgresql.org/).
- LLM integration via [Groq](https://groq.com/) or your configured provider.

---

## License

MIT License

---


