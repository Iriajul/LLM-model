import os
from datetime import timedelta
from src.config import redis_client   # reuse the same Redis client

EXPORT_DIR = "exports"

# match what auth.py expects and your .env
JWT_SECRET_KEY   = os.getenv("JWT_SECRET_KEY", os.getenv("JWT_SECRET", "supersecretkey"))
JWT_ALGORITHM    = "HS256"
ACCESS_TOKEN_EXPIRE = timedelta(minutes=int(os.getenv("JWT_EXPIRE_MINUTES", 60)))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(BASE_DIR, "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)
