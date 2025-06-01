import os
import logging
import warnings
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq
from sqlalchemy import create_engine

load_dotenv()

# Environment Variable Validation
def get_env_variable(var_name: str) -> str:
    """Get an environment variable or raise an error if it's not set."""
    value = os.getenv(var_name)
    if value is None:
        raise EnvironmentError(f"Error: Environment variable '{var_name}' not set. Please check your .env file.")
    return value

# Logging Configuration
LOG_DIR = "/home/ubuntu/nl2sql_project/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "nl2sql_app.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE), 
        logging.StreamHandler()        
    ]
)

logger = logging.getLogger(__name__)


# Database Configuration
DB_HOST = get_env_variable("DB_HOST")
DB_PORT = get_env_variable("DB_PORT")
DB_USER = get_env_variable("DB_USER")
DB_PASSWORD = get_env_variable("DB_PASSWORD")
DB_NAME = get_env_variable("DB_NAME")
DB_SCHEMA = get_env_variable("DB_SCHEMA") # Target schema for NL2SQL
EXPORT_API_URL = os.getenv("EXPORT_API_URL", "http://localhost:8000")

# Construct the database URL securely
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logger.info(f"Connecting to database: postgresql+psycopg2://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")

try:
    engine = create_engine(DATABASE_URL)

    # Initialize LangChain SQLDatabase
    db = SQLDatabase(engine=engine, schema=DB_SCHEMA, sample_rows_in_table_info=0) 
    logger.info(f"Successfully connected to database and initialized SQLDatabase for schema '{DB_SCHEMA}'.")
    # Test connection - This will raise an exception if connection fails
    # db.get_usable_table_names()
    # logger.info(f"Available tables in schema '{DB_SCHEMA}': {db.get_usable_table_names()}")

except Exception as e:
    logger.error(f"Failed to connect to the database or initialize SQLDatabase: {e}", exc_info=True)
    raise EnvironmentError(f"Database connection failed. Please check your DB credentials and connection: {e}")

# --- LLM Configuration ---
GROQ_API_KEY = get_env_variable("GROQ_API_KEY")
LLM_MODEL_NAME = "llama3-70b-8192"

logger.info(f"Initializing LLM: {LLM_MODEL_NAME}")
try:
    llm = ChatGroq(model=LLM_MODEL_NAME, temperature=0.0, groq_api_key=GROQ_API_KEY)
    logger.info("LLM initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize the LLM: {e}", exc_info=True)
    raise EnvironmentError(f"LLM initialization failed: {e}")

