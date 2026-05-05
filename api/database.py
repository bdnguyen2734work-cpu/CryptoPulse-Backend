import os
import mysql.connector
from mysql.connector import Error
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════
# REDIS (Upstash dùng rediss:// — SSL tự động)
# ══════════════════════════════════════════════
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
import ssl

async_redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    ssl_certfile=None,
    ssl_keyfile=None,
    ssl_cert_reqs=ssl.CERT_NONE if REDIS_URL.startswith("rediss://") else None,
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