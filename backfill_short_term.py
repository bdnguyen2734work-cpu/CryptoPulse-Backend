import requests
import time
from datetime import datetime, timedelta

from config import SYMBOLS, BACKFILL_SHORT_DAYS
from db_utils import get_db, ensure_kline_table

SHORT_TIMEFRAMES = ["1h", "15m", "5m", "1m"]
BINANCE_KLINES   = "https://api.binance.com/api/v3/klines"

start_dt = datetime.now() - timedelta(days=BACKFILL_SHORT_DAYS)
START_MS  = int(start_dt.timestamp() * 1000)


def fetch_recent_klines(symbol: str, tf: str, start_ms: int):
    table_name = f"kline_{tf}"
    ensure_kline_table(table_name)

    current_start = start_ms
    total = 0

    print(f"{symbol} | {tf} | từ {start_dt.strftime('%d/%m/%Y')}")

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

            if isinstance(data, dict):
                print(f"[Binance API Error] {data}")
                break
            if not data:
                break

            rows = [
                (symbol,
                 int(r[0] / 1000),
                 float(r[1]), float(r[2]),
                 float(r[3]), float(r[4]),
                 float(r[5]))
                for r in data
            ]

            with get_db() as (conn, cursor):
                cursor.executemany(f"""
                    INSERT IGNORE INTO {table_name}
                        (symbol, open_time, open_price, high_price,
                         low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, rows)

            total += len(rows)
            current_start = data[-1][6] + 1

            if len(rows) < 1000:
                break

            time.sleep(0.12)

        except requests.HTTPError as e:
            print(f"[HTTP {e.response.status_code}] {symbol} {tf}")
            break
        except Exception as exc:
            print(f"[ERROR] {symbol} {tf}: {exc}")
            time.sleep(3)

    print(f"✅ {symbol} {tf}: {total:,} records")


if __name__ == "__main__":
    print(f"\n=== BACKFILL {BACKFILL_SHORT_DAYS} NGÀY GẦN NHẤT ===\n")

    for tf in SHORT_TIMEFRAMES:
        print(f"\n{'='*40}\nTIMEFRAME: {tf.upper()}\n{'='*40}")
        for coin in SYMBOLS:
            fetch_recent_klines(coin, tf, START_MS)

    print("\n🏁 HOÀN TẤT!")
