import os
from datetime import timedelta
from src.config import redis_client   # reuse the same Redis client

EXPORT_DIR = "exports"

# match what auth.py expects and your .env
JWT_SECRET_KEY   = os.getenv("JWT_SECRET_KEY", os.getenv("JWT_SECRET", "supersecretkey"))
JWT_ALGORITHM    = "HS256"
ACCESS_TOKEN_EXPIRE = timedelta(minutes=int(os.getenv("JWT_EXPIRE_MINUTES", 60)))

os.makedirs(EXPORT_DIR, exist_ok=True)

