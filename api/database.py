import os
import mysql.connector
from mysql.connector import Error
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════
# REDIS (Upstash dùng rediss:// — SSL tự động)
# ══════════════════════════════════════════════
REDIS_URL = os.getenv("REDIS_URL")

if not REDIS_URL:
    raise ValueError("REDIS_URL is missing!")

async_redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)

# ══════════════════════════════════════════════
# MYSQL / TiDB Cloud 
# ══════════════════════════════════════════════
def get_db_connection():
    ssl_disabled = os.getenv("DB_SSL_DISABLED", "false").lower() == "true"
    try:
        config = {
            "host":     os.getenv("DB_HOST", "localhost"),
            "port":     int(os.getenv("DB_PORT", 3306)),
            "user":     os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "cryptopulse"),
        }
        if not ssl_disabled:
            config["ssl_disabled"] = False
            config["ssl_verify_cert"] = False
            config["ssl_verify_identity"] = False

        return mysql.connector.connect(**config)
    except Error as e:
        print(f"[!] Lỗi kết nối DB: {e}")
        raise e