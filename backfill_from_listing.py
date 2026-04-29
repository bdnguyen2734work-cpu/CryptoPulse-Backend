"""
backfill_from_listing.py – Tải toàn bộ lịch sử 1D & 1W từ ngày niêm yết.

Chạy 1 lần trước khi khởi động pipeline.
"""

import requests
import time
from datetime import datetime

from config import SYMBOLS
from db_utils import get_db, ensure_kline_table

TIMEFRAMES = ["1d", "1w"]
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"


def fetch_genesis_klines(symbol: str, tf: str):
    table_name = f"kline_{tf}"
    ensure_kline_table(table_name)

    print(f"\n[{tf.upper()}] {symbol} – từ ngày niêm yết...")

    current_start = 0
    total = 0

    while True:
        try:
            resp = requests.get(
                BINANCE_KLINES,
                params={"symbol": symbol, "interval": tf,
                        "limit": 1000, "startTime": current_start},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                break

            rows = [
                (symbol,
                 int(r[0] / 1000),   # open_time → seconds
                 float(r[1]),         # open
                 float(r[2]),         # high
                 float(r[3]),         # low
                 float(r[4]),         # close
                 float(r[5]))         # volume
                for r in data
            ]

            with get_db() as (conn, cursor):
                cursor.executemany(f"""
                    INSERT IGNORE INTO cryptopulse.{table_name}
                        (symbol, open_time, open_price, high_price,
                         low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, rows)

            total += len(rows)
            current_start = data[-1][6] + 1   # close_time + 1ms

            date_str = datetime.fromtimestamp(data[-1][0] / 1000).strftime("%Y-%m-%d")
            print(f"   → đến {date_str} | tổng: {total:,}")

            if len(rows) < 1000:
                break

            time.sleep(0.12)   # ~8 req/s – an toàn với Binance weight limit

        except requests.HTTPError as e:
            print(f"[HTTP {e.response.status_code}] {symbol} {tf} – dừng")
            break
        except Exception as exc:
            print(f"[ERROR] {symbol} {tf}: {exc} – retry 3s")
            time.sleep(3)
            break

    print(f"✅ {symbol} [{tf}] – {total:,} nến")


if __name__ == "__main__":
    t0 = time.time()
    print("=== BACKFILL LỊCH SỬ (1D & 1W) ===")

    for tf in TIMEFRAMES:
        print(f"\n{'='*40}\nKHUNG: {tf.upper()}\n{'='*40}")
        for coin in SYMBOLS:
            fetch_genesis_klines(coin, tf)

    mins = (time.time() - t0) / 60
    print(f"\n🏁 Xong! Tổng thời gian: {mins:.1f} phút")
