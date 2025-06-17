import redis
import os

r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    ssl=os.getenv("REDIS_SSL") == "True"
)

try:
    pong = r.ping()
    print("✅ Redis is working! PONG:", pong)
    r.set("nl2sql_test_key", "hello", ex=10)
    val = r.get("nl2sql_test_key")
    print("✅ Redis SET/GET worked:", val)
except Exception as e:
    print("❌ Redis check failed:", str(e))
