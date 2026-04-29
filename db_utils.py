"""
db_utils.py – Helper kết nối MySQL dùng chung cho tất cả script.
Dùng context manager để đảm bảo connection luôn được đóng.
"""
import mysql.connector
from contextlib import contextmanager
from config import DB_CONFIG


@contextmanager
def get_db():
    """
    Usage:
        with get_db() as (conn, cursor):
            cursor.execute(...)
            conn.commit()
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def ensure_kline_table(table_name: str):
    """Tạo bảng kline nếu chưa tồn tại (dùng cho backfill)."""
    with get_db() as (conn, cursor):
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol      VARCHAR(20) NOT NULL,
                open_time   BIGINT      NOT NULL,
                open_price  DOUBLE      NOT NULL,
                high_price  DOUBLE      NOT NULL,
                low_price   DOUBLE      NOT NULL,
                close_price DOUBLE      NOT NULL,
                volume      DOUBLE      NOT NULL,
                PRIMARY KEY (symbol, open_time),
                INDEX idx_symbol (symbol),
                INDEX idx_time   (open_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
