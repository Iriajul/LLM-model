import os
import logging
import warnings

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq
import redis

# Load environment variables
load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────────────────────────────────────

def get_env_variable(var_name: str) -> str:
    """Fetch a required environment variable or raise an error."""
    value = os.getenv(var_name)
    if value is None:
        raise EnvironmentError(f"Environment variable '{var_name}' is not set.")
    return value

# ──────────────────────────────────────────────────────────────────────────────
# Logging Configuration
# ──────────────────────────────────────────────────────────────────────────────

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

# ──────────────────────────────────────────────────────────────────────────────
# Environment & Database Configuration
# ──────────────────────────────────────────────────────────────────────────────

DB_HOST     = get_env_variable("DB_HOST")
DB_PORT     = get_env_variable("DB_PORT")
DB_USER     = get_env_variable("DB_USER")
DB_PASSWORD = get_env_variable("DB_PASSWORD")
DB_NAME     = get_env_variable("DB_NAME")
DB_SCHEMA   = get_env_variable("DB_SCHEMA")

EXPORT_API_URL = os.getenv("EXPORT_API_URL", "http://localhost:8000")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
logger.info(f"Connecting to database: postgresql+psycopg2://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")

try:
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )
    db = SQLDatabase(engine=engine, schema=DB_SCHEMA, sample_rows_in_table_info=0)
    logger.info(f"Successfully connected to database and initialized SQLDatabase for schema '{DB_SCHEMA}'.")

except Exception as e:
    logger.error("Database connection or SQLDatabase initialization failed.", exc_info=True)
    raise EnvironmentError(f"Database setup failed: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# LLM Configuration
# ──────────────────────────────────────────────────────────────────────────────

GROQ_API_KEY = get_env_variable("GROQ_API_KEY")
LLM_MODEL_NAME = "llama3-70b-8192"

logger.info(f"Initializing LLM: {LLM_MODEL_NAME}")

try:
    llm = ChatGroq(
        model=LLM_MODEL_NAME,
        temperature=0.0,
        groq_api_key=GROQ_API_KEY
    )
    logger.info("LLM initialized successfully.")
except Exception as e:
    logger.error("LLM initialization failed.", exc_info=True)
    raise EnvironmentError(f"LLM initialization failed: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Redis Configuration
# ──────────────────────────────────────────────────────────────────────────────

REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

logger.info(f"Connecting to Redis: {REDIS_HOST}:{REDIS_PORT}")

try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5
    )
    redis_client.ping()
    logger.info("Redis connection established successfully.")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Caching will be disabled.")
    redis_client = None