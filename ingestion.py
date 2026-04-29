"""
ingestion.py – WebSocket Binance → Kafka producer.

Cải tiến so với bản gốc:
  • Không hardcode config (dùng config.py)
  • Tách delivery_report rõ ràng
  • Reconnect tự động khi mất kết nối WebSocket
  • Rate-limit: 1 message/giây/symbol (giống bản gốc, nhưng dùng config)
  • Log rõ ràng hơn
"""

import json
import time
import websocket
from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, SYMBOLS_SET

# ─────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "queue.buffering.max.messages": 10000,
    "batch.num.messages": 500,
    "linger.ms": 100,           # gom batch trong 100ms
    "compression.type": "lz4",  # giảm bandwidth ~60%
})


def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka ERROR] {err}")


# ─────────────────────────────────────────
# Rate-limit tracker
# ─────────────────────────────────────────
last_sent_time: dict[str, float] = {}
SEND_INTERVAL = 1.0  # giây


# ─────────────────────────────────────────
# WebSocket handlers
# ─────────────────────────────────────────
def on_message(ws, message: str):
    try:
        raw_data = json.loads(message)
        if not isinstance(raw_data, list):
            return

        now = time.time()
        batch = {}

        for ticker in raw_data:
            symbol = ticker.get("s", "")
            if symbol not in SYMBOLS_SET:
                continue
            # Chống spam: bỏ qua nếu < SEND_INTERVAL kể từ lần gửi trước
            if now - last_sent_time.get(symbol, 0) < SEND_INTERVAL:
                continue
            batch[symbol] = ticker
            last_sent_time[symbol] = now

        if not batch:
            return

        payload = json.dumps(list(batch.values()))
        producer.produce(KAFKA_TOPIC, value=payload, callback=delivery_report)
        producer.poll(0)

        top = next(iter(batch))
        print(f"[Kafka] Gửi {len(batch)} coin | Top: {top} @ {batch[top]['c']}")

    except Exception as exc:
        print(f"[on_message ERROR] {exc}")


def on_open(ws):
    print(f"[WS] Đã kết nối Binance | Theo dõi {len(SYMBOLS_SET)} coin")


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, code, msg):
    print(f"[WS] Đóng kết nối (code={code}) – sẽ reconnect sau 5s...")


# ─────────────────────────────────────────
# Main – reconnect loop
# ─────────────────────────────────────────
BINANCE_WS = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

if __name__ == "__main__":
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            print(f"[Main ERROR] {exc}")
        print("Reconnect sau 5 giây...")
        time.sleep(5)
