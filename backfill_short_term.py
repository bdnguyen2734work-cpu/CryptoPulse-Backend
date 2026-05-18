"""
backfill_short_term.py – Tải dữ liệu lịch sử ngắn hạn từ Binance về TiDB Cloud.

Chạy 1 lần để lấy dữ liệu {BACKFILL_SHORT_DAYS} ngày gần nhất.
Mặc định: 1m, 5m, 15m, 1h, 4h — có thể chỉnh SHORT_TIMEFRAMES bên dưới.
"""

import requests
import time
from datetime import datetime, timedelta

from config import SYMBOLS, BACKFILL_SHORT_DAYS
from db_utils import get_db, ensure_kline_table

SHORT_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h"]


BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

start_dt = datetime.now() - timedelta(days=BACKFILL_SHORT_DAYS)
START_MS  = int(start_dt.timestamp() * 1000)


def fetch_recent_klines(symbol: str, tf: str, start_ms: int):
    table_name = f"kline_{tf}"
    ensure_kline_table(table_name)

    current_start = start_ms
    total         = 0

    print(f"  {symbol} | {tf} | từ {start_dt.strftime('%d/%m/%Y')}")

    while True:
        try:
            resp = requests.get(
                BINANCE_KLINES,
                params={
                    "symbol":    symbol,
                    "interval":  tf,
                    "limit":     1000,
                    "startTime": current_start,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict):
                print(f"  [Binance Error] {data.get('msg', data)}")
                break
            if not data:
                break

            rows = [
                (
                    symbol,
                    int(r[0] / 1000),   # open_time → seconds
                    float(r[1]),         # open
                    float(r[2]),         # high
                    float(r[3]),         # low
                    float(r[4]),         # close
                    float(r[5]),         # volume
                )
                for r in data
            ]

            with get_db() as (conn, cursor):
                cursor.executemany(
                    f"""
                    INSERT IGNORE INTO {table_name}
                        (symbol, open_time, open_price, high_price,
                         low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

            total         += len(rows)
            current_start  = data[-1][6] + 1   # close_time + 1ms

            last_date = datetime.fromtimestamp(data[-1][0] / 1000).strftime("%d/%m/%Y")
            print(f"    → đến {last_date} | tổng: {total:,}")

            if len(rows) < 1000:
                break

            time.sleep(0.12)   # ~8 req/s — an toàn với Binance weight limit

        except requests.HTTPError as e:
            print(f"  [HTTP {e.response.status_code}] {symbol} {tf} — bỏ qua")
            break
        except Exception as exc:
            print(f"  [ERROR] {symbol} {tf}: {exc} — thử lại sau 5s")
            time.sleep(5)
            break

    print(f"  {symbol} [{tf}]: {total:,} records\n")


if __name__ == "__main__":
    t0 = time.time()
    print(f"\n{'='*50}")
    print(f"  BACKFILL {BACKFILL_SHORT_DAYS} NGÀY GẦN NHẤT")
    print(f"  Timeframes: {', '.join(SHORT_TIMEFRAMES)}")
    print(f"  Từ ngày:    {start_dt.strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*50}\n")

    for tf in SHORT_TIMEFRAMES:
        print(f"\n{'─'*40}")
        print(f"  TIMEFRAME: {tf.upper()}")
        print(f"{'─'*40}")
        for coin in SYMBOLS:
            fetch_recent_klines(coin, tf, START_MS)

    mins = (time.time() - t0) / 60
    print(f"\n{'='*50}")
    print(f"  🏁 HOÀN TẤT! Thời gian: {mins:.1f} phút")
    print(f"{'='*50}\n")