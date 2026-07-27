import os
import mysql.connector
from mysql.connector import pooling
from mysql.connector import Error
import redis.asyncio as redis
from sqlalchemy import create_engine
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
# MYSQL / TiDB Cloud Connection Pooling
# ══════════════════════════════════════════════
db_pool = None

def init_db_pool():
    global db_pool
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

        db_pool = pooling.MySQLConnectionPool(
            pool_name="cryptopulse_pool",
            pool_size=15,  # Tăng kích thước pool để đáp ứng tải
            pool_reset_mode='session',
            **config
        )
        print("[DB] Khởi tạo Connection Pool thành công (pool_size=15).")
    except Error as e:
        print(f"[!] Lỗi khởi tạo DB Pool: {e}")
        raise e

def get_db_connection():
    global db_pool
    if db_pool is None:
        init_db_pool()
    return db_pool.get_connection()

# ══════════════════════════════════════════════
# SQLALCHEMY ENGINE (Dùng chung cho Analysis & Dashboard)
# ══════════════════════════════════════════════
_engine = None

def get_sqlalchemy_engine():
    global _engine
    if _engine is None:
        host     = os.getenv("DB_HOST", "localhost")
        port     = os.getenv("DB_PORT", "4000")
        user     = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASSWORD", "")
        database = os.getenv("DB_NAME", "cryptopulse")
        url = (
            f"mysql+pymysql://{user}:{password}"
            f"@{host}:{port}/{database}"
            f"?ssl_verify_cert=false&ssl_verify_identity=false"
        )
        _engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_recycle=1800,
            pool_pre_ping=True
        )
        print("[DB] Khởi tạo SQLAlchemy Engine thành công.")
    return _engine