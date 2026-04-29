import os
import mysql.connector
from mysql.connector import Error
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# REDIS
# ==========================================
REDIS_URL = os.getenv("REDIS_URL", f"redis://{os.getenv('REDIS_HOST', 'redis')}:6379/0")

async_redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ==========================================
# MYSQL
# ==========================================
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST",     os.getenv("MYSQL_HOST",     "mysql")),
            user=os.getenv("DB_USER",     "root"),
            password=os.getenv("DB_PASSWORD", os.getenv("MYSQL_PASSWORD", "rootpassword")),
            database=os.getenv("DB_NAME", "cryptopulse")
        )
        return conn
    except Error as e:
        print(f"[!] Lỗi kết nối MySQL: {e}")
        raise e