import json
import asyncio
import websockets
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, SYMBOLS_SET

load_dotenv()

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


# Bộ đệm tích lũy ticker mới nhất cho mỗi symbol trong vòng 1 giây
ticker_buffer = {}
buffer_lock = asyncio.Lock()


async def kafka_flush_loop():
    """Gửi dữ liệu aggregated từ buffer sang Kafka định kỳ mỗi 1 giây."""
    while True:
        await asyncio.sleep(1.0)
        async with buffer_lock:
            if not ticker_buffer:
                continue
            batch = list(ticker_buffer.values())
            ticker_buffer.clear()

        try:
            payload = json.dumps(batch)
            producer.produce(KAFKA_TOPIC, value=payload, callback=delivery_report)
            producer.poll(0)

            top = batch[0]["s"]
            print(f"[Kafka] Gửi batch {len(batch)} coins | Top: {top} @ {batch[0]['c']}")
        except Exception as e:
            print(f"[Kafka Send Error] {e}")


async def binance_ws_listener():
    """Lắng nghe kết nối WebSocket từ Binance MiniTicker stream."""
    binance_ws_url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    retry_delay = 5

    while True:
        try:
            print(f"[WS] Kết nối tới Binance MiniTicker stream...")
            async with websockets.connect(binance_ws_url, ping_interval=30, ping_timeout=10) as ws:
                print(f"[WS] Kết nối thành công! Đang lắng nghe {len(SYMBOLS_SET)} coins.")
                retry_delay = 5

                async for message in ws:
                    try:
                        raw_data = json.loads(message)
                        if not isinstance(raw_data, list):
                            continue

                        async with buffer_lock:
                            for ticker in raw_data:
                                symbol = ticker.get("s", "")
                                if symbol in SYMBOLS_SET:
                                    # Tích lũy: chỉ lưu bản tin mới nhất của symbol trong giây đó
                                    ticker_buffer[symbol] = ticker
                    except Exception as exc:
                        print(f"[WS Parser Error] {exc}")

        except asyncio.CancelledError:
            print("[WS] Đang dừng Ingestion listener...")
            raise
        except Exception as e:
            print(f"[WS Error] Kết nối lỗi: {e} - thử lại sau {retry_delay}s")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)


async def main():
    await asyncio.gather(
        binance_ws_listener(),
        kafka_flush_loop()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[Main] Đã dừng bởi người dùng.")
