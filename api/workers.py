"""
workers.py – Background workers cho CryptoPulse.
  • Binance WebSocket → live price cache
  • Fear & Greed Index (tự tính từ Binance + CoinGecko)
  • Metadata 24h (change%, volume)
  • Flush Redis mỗi 45s → top_20_coins_stats + live_prices
  • Price Alert → Firebase FCM push notification
  • Data Retention → xóa dữ liệu cũ lúc 3AM
"""

try:
    from database import async_redis_client, get_db_connection
except ModuleNotFoundError:
    from api.database import async_redis_client, get_db_connection

import asyncio
import httpx
import math
import hashlib
import websockets
from datetime import datetime, timezone, timedelta

try:
    import orjson as _json
    def dumps(x): return _json.dumps(x)
    def loads(x): return _json.loads(x)
except ImportError:
    import json as _json
    def dumps(x): return _json.dumps(x, ensure_ascii=False).encode()
    def loads(x): return _json.loads(x)

# Firebase FCM
try:
    import firebase_admin
    from firebase_admin import messaging
    _firebase_ready = bool(firebase_admin._apps)
except Exception:
    _firebase_ready = False


# ══════════════════════════════════════════════════════════════════
# CONFIG — 24 COINS
# ══════════════════════════════════════════════════════════════════
TRACKED_COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT",
    "LINKUSDT", "TRXUSDT", "POLUSDT", "LTCUSDT", "BCHUSDT",
    "UNIUSDT", "NEARUSDT", "ATOMUSDT", "ICPUSDT", "APTUSDT",
    "SUIUSDT", "INJUSDT", "OPUSDT", "ARBUSDT",
]

NAME_MAP = {
    "BTCUSDT":  "Bitcoin",
    "ETHUSDT":  "Ethereum",
    "BNBUSDT":  "BNB",
    "SOLUSDT":  "Solana",
    "XRPUSDT":  "XRP",
    "ADAUSDT":  "Cardano",
    "DOGEUSDT": "Dogecoin",
    "AVAXUSDT": "Avalanche",
    "SHIBUSDT": "Shiba Inu",
    "DOTUSDT":  "Polkadot",
    "LINKUSDT": "Chainlink",
    "TRXUSDT":  "TRON",
    "POLUSDT":  "Polygon",
    "LTCUSDT":  "Litecoin",
    "BCHUSDT":  "Bitcoin Cash",
    "UNIUSDT":  "Uniswap",
    "NEARUSDT": "NEAR",
    "ATOMUSDT": "Cosmos",
    "ICPUSDT":  "Internet Computer",
    "APTUSDT":  "Aptos",
    "SUIUSDT":  "Sui",
    "INJUSDT":  "Injective",
    "OPUSDT":   "Optimism",
    "ARBUSDT":  "Arbitrum",
}

SYMBOL_MAP = {s: s.replace("USDT", "") for s in TRACKED_COINS}

WS_STREAMS = "/".join(f"{s.lower()}@kline_1m" for s in TRACKED_COINS)
WS_URI     = f"wss://stream.binance.com:9443/stream?streams={WS_STREAMS}"

FLUSH_INTERVAL = 45    # giây — flush Redis
META_INTERVAL  = 3600  # giây — refresh metadata
FNG_INTERVAL   = 3600  # giây — refresh Fear & Greed

# Redis keys — đồng bộ với main.py
REDIS_KEY_TOP_COINS = "top_20_coins_stats"
REDIS_KEY_LIVE      = "live_prices"
REDIS_KEY_FNG       = "market_fear_greed"
REDIS_KEY_META      = "top_coins_meta"

# In-memory cache
_live_price_cache: dict = {}
_meta_cache:       dict = {}
_last_hash:        str  = ""


# ══════════════════════════════════════════════════════════════════
# UTILS
# ══════════════════════════════════════════════════════════════════
def _std(data: list) -> float:
    if len(data) < 2:
        return 0.0
    mean = sum(data) / len(data)
    return math.sqrt(sum((x - mean) ** 2 for x in data) / (len(data) - 1))


def _clamp(val: int, lo: int = 0, hi: int = 100) -> int:
    return max(lo, min(hi, val))


# ══════════════════════════════════════════════════════════════════
# FEAR & GREED
# ══════════════════════════════════════════════════════════════════
async def _calculate_fear_greed(client: httpx.AsyncClient) -> dict:
    scores = {}

    try:
        resp    = await client.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1d", "limit": 90},
        )
        klines  = resp.json()
        closes  = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
        vol_30  = _std(returns[-30:])
        vol_90  = _std(returns)

        scores["volatility"] = _clamp(int(100 - (vol_30 / vol_90 - 0.5) * 80) if vol_90 else 50)
        scores["momentum"]   = _clamp(int(50 + (closes[-1] - closes[-30]) / closes[-30] * 200))
        scores["volume"]     = _clamp(int((sum(volumes[-7:]) / 7) / (sum(volumes[-30:]) / 30) * 50))
    except Exception as e:
        print(f"[F&G] Binance klines lỗi: {e}")
        scores.update({"volatility": 50, "momentum": 50, "volume": 50})

    # ── SỬA: thêm alternative.me + dùng .get() cho CoinGecko ──────
    fng_external = None
    try:
        resp = await client.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=10,
        )
        if resp.status_code == 200:
            item         = resp.json().get("data", [{}])[0]
            fng_external = int(item.get("value", 50))
            print(f"[F&G] alternative.me: {fng_external}")
    except Exception as e:
        print(f"[F&G] alternative.me lỗi: {e}")

    try:
        resp = await client.get("https://api.coingecko.com/api/v3/global")
        if resp.status_code == 200:
            raw  = resp.json()
            data = raw.get("data")  # ← dùng .get() thay vì ["data"]
            if data and isinstance(data, dict):
                btc_dom = data.get("market_cap_percentage", {}).get("btc", 50)
                mkt_ch  = data.get("market_cap_change_percentage_24h_usd", 0)
                scores["dominance"]  = _clamp(int(100 - (btc_dom - 40) * 2))
                scores["market_cap"] = _clamp(int(50 + mkt_ch * 3))
                print(f"[F&G] CoinGecko OK — BTC dom: {btc_dom:.1f}%")
            else:
                print(f"[F&G] CoinGecko: thiếu 'data' — dùng mặc định")
                scores.update({"dominance": 50, "market_cap": 50})
        else:
            print(f"[F&G] CoinGecko HTTP {resp.status_code}")
            scores.update({"dominance": 50, "market_cap": 50})
    except Exception as e:
        print(f"[F&G] CoinGecko lỗi: {e}")
        scores.update({"dominance": 50, "market_cap": 50})
    # ───────────────────────────────────────────────────────────────

    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": "BTCUSDT"},
        )
        pct = float(resp.json()["priceChangePercent"])
        scores["social"] = _clamp(int(50 + pct * 4))
    except Exception as e:
        print(f"[F&G] Social lỗi: {e}")
        scores["social"] = 50

    weights = {
        "volatility": 0.25, "momentum": 0.25, "volume": 0.15,
        "dominance":  0.10, "market_cap": 0.15, "social": 0.10,
    }
    calculated = _clamp(int(sum(scores[k] * weights[k] for k in weights)))

    # Blend với alternative.me nếu có
    if fng_external is not None:
        final = _clamp(int(calculated * 0.5 + fng_external * 0.5))
    else:
        final = calculated

    print(f"[F&G] calc={calculated} | ext={fng_external} | final={final}")

    return {
        "value": final,
        "classification": (
            "Extreme Greed" if final >= 75 else
            "Greed"         if final >= 56 else
            "Neutral"       if final >= 45 else
            "Fear"          if final >= 25 else
            "Extreme Fear"
        ),
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
    }


async def fetch_fear_and_greed():
    while True:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                data = await _calculate_fear_greed(client)
            await async_redis_client.set(REDIS_KEY_FNG, dumps(data))
            print(f"[F&G] {data['value']} – {data['classification']}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[F&G] Lỗi: {e}")
        await asyncio.sleep(FNG_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# METADATA — 24h change & volume cho 24 coins
# ══════════════════════════════════════════════════════════════════
async def fetch_coin_metadata():
    while True:
        try:
            import json as _stdlib_json
            symbols_json = _stdlib_json.dumps(TRACKED_COINS, separators=(",", ":"))
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://api.binance.com/api/v3/ticker/24hr",
                    params={"symbols": symbols_json},
                )
            raw = resp.json()
            if isinstance(raw, dict):
                print(f"[Meta] Binance lỗi: {raw}")
                raise ValueError(f"Unexpected: {raw}")

            meta = {}
            for item in raw:
                sym = item["symbol"]
                meta[sym] = {
                    "coinName":           NAME_MAP.get(sym, SYMBOL_MAP.get(sym, sym)),
                    "coinSymbol":         SYMBOL_MAP.get(sym, sym.replace("USDT", "")),
                    "priceChangePercent": round(float(item["priceChangePercent"]), 2),
                    "quoteVolume":        round(float(item["quoteVolume"]), 0),
                    "highPrice":          float(item["highPrice"]),
                    "lowPrice":           float(item["lowPrice"]),
                    "lastPrice":          float(item["lastPrice"]),
                }
            _meta_cache.clear()
            _meta_cache.update(meta)
            await async_redis_client.set(REDIS_KEY_META, dumps(meta))
            print(f"[Meta] Cập nhật {len(meta)} coins")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Meta] Lỗi: {e}")
        await asyncio.sleep(META_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# PRICE ALERT — FCM Push Notification
# ══════════════════════════════════════════════════════════════════
async def _send_fcm(fcm_token: str, title: str, body: str):
    """Gửi push notification qua Firebase FCM."""
    if not _firebase_ready:
        print(f"[Alert] Firebase chưa init — bỏ qua FCM")
        return
    try:
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default",
                    channel_id="price_alerts",
                ),
            ),
        )
        await asyncio.get_event_loop().run_in_executor(
            None, messaging.send, message
        )
        print(f"[Alert] FCM gửi thành công → {fcm_token[:20]}...")
    except Exception as e:
        print(f"[Alert] FCM lỗi: {e}")


async def check_price_alerts(snapshot: dict, fng_value: int):
    """Kiểm tra tất cả alert đang active, gọi sau mỗi flush."""
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            "SELECT id, user_id, symbol, `condition`, target, fcm_token"
            " FROM price_alerts WHERE is_active=1 AND triggered=0"
        )
        alerts = cursor.fetchall()

        if not alerts:
            return

        triggered_ids = []

        for alert in alerts:
            sym       = alert["symbol"]
            condition = alert["condition"]
            target    = float(alert["target"])
            fired     = False
            title     = ""
            body      = ""

            if condition in ("above", "below") and sym in snapshot:
                price = float(snapshot[sym].get("price", 0))
                name  = snapshot[sym].get("name", sym)

                if condition == "above" and price >= target:
                    fired = True
                    title = f"🚀 {name} vượt ngưỡng!"
                    body  = f"{name} đang ở ${price:,.2f} — vượt mức ${target:,.2f} bạn đặt"

                elif condition == "below" and price <= target:
                    fired = True
                    title = f"📉 {name} xuống ngưỡng!"
                    body  = f"{name} đang ở ${price:,.2f} — dưới mức ${target:,.2f} bạn đặt"

            elif condition == "fng_above" and fng_value >= int(target):
                fired = True
                title = "😱 Thị trường Tham lam cực độ!"
                body  = f"Fear & Greed Index = {fng_value} — vượt ngưỡng {int(target)} bạn đặt"

            elif condition == "fng_below" and fng_value <= int(target):
                fired = True
                title = "😨 Thị trường Sợ hãi cực độ!"
                body  = f"Fear & Greed Index = {fng_value} — dưới ngưỡng {int(target)} bạn đặt"

            if fired:
                triggered_ids.append(alert["id"])
                await _send_fcm(alert["fcm_token"], title, body)
                print(f"[Alert] Fired #{alert['id']} — {title}")

        if triggered_ids:
            placeholders = ",".join(["%s"] * len(triggered_ids))
            cursor.execute(
                f"UPDATE price_alerts SET triggered=1, is_active=0"
                f" WHERE id IN ({placeholders})",
                triggered_ids,
            )
            conn.commit()

    except Exception as e:
        print(f"[Alert] check_price_alerts lỗi: {e}")
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ══════════════════════════════════════════════════════════════════
# FLUSH — ghi Redis mỗi 45 giây
# ══════════════════════════════════════════════════════════════════
async def _flush_live_prices():
    global _last_hash

    while True:
        await asyncio.sleep(FLUSH_INTERVAL)

        if not _live_price_cache:
            continue

        snapshot = {}
        for sym, live in _live_price_cache.items():
            entry = {
                "symbol":     sym,
                "name":       NAME_MAP.get(sym, sym.replace("USDT", "")),
                "coinSymbol": SYMBOL_MAP.get(sym, sym.replace("USDT", "")),
                "price":      live.get("price", 0),
                "time":       live.get("time", 0),
            }
            if sym in _meta_cache:
                m = _meta_cache[sym]
                entry["change"]  = m.get("priceChangePercent", 0)
                entry["volume"]  = m.get("quoteVolume", 0)
                entry["high24h"] = m.get("highPrice", 0)
                entry["low24h"]  = m.get("lowPrice", 0)
            snapshot[sym] = entry

        top_24_list = [
            snapshot[sym]
            for sym in TRACKED_COINS
            if sym in snapshot
        ]

        payload_dict = dumps(snapshot)
        payload_list = dumps(top_24_list)

        new_hash = hashlib.md5(payload_list).hexdigest()
        if new_hash == _last_hash:
            continue
        _last_hash = new_hash

        try:
            await async_redis_client.set(REDIS_KEY_LIVE,      payload_dict)
            await async_redis_client.set(REDIS_KEY_TOP_COINS, payload_list)
            print(f"[Flush] {len(top_24_list)} coins → Redis")

            fng_raw = await async_redis_client.get(REDIS_KEY_FNG)
            fng_val = 50
            if fng_raw:
                try:
                    fng_val = loads(fng_raw).get("value", 50)
                except Exception:
                    pass
            await check_price_alerts(snapshot, fng_val)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Flush] Lỗi: {e}")


# ══════════════════════════════════════════════════════════════════
# WEBSOCKET — Binance kline_1m stream
# ══════════════════════════════════════════════════════════════════
async def binance_ws():
    retry_delay = 5

    while True:
        try:
            async with websockets.connect(
                WS_URI,
                ping_interval=30,
                ping_timeout=10,
            ) as ws:
                print(f"[WS] Kết nối Binance — {len(TRACKED_COINS)} coins")
                retry_delay = 5

                while True:
                    raw = await ws.recv()
                    msg = loads(raw)
                    if "data" in msg:
                        msg = msg["data"]
                    if "k" not in msg:
                        continue
                    k   = msg["k"]
                    sym = msg["s"]
                    _live_price_cache[sym] = {
                        "price": float(k["c"]),
                        "time":  int(k["t"]),
                    }
        except asyncio.CancelledError:
            print("[WS] Đang tắt WebSocket connection...")
            raise
        except Exception as e:
            print(f"[WS] Mất kết nối: {e} — thử lại sau {retry_delay}s")

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


# ══════════════════════════════════════════════════════════════════
# DATA RETENTION — tự động xóa dữ liệu cũ lúc 3AM
# ══════════════════════════════════════════════════════════════════
RETENTION_POLICY = {
    "kline_1m":  7,   # giữ 7 ngày
    "kline_5m":  30,  # giữ 30 ngày
    "kline_15m": 90,  # giữ 90 ngày
}

async def data_retention_worker():
    import time as _time

    while True:
        now    = datetime.now()
        target = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)

        wait_seconds = (target - now).total_seconds()
        print(f"[Retention] Sẽ chạy lúc 3AM — còn {wait_seconds/3600:.1f}h")
        await asyncio.sleep(wait_seconds)

        print("[Retention] Bắt đầu dọn dữ liệu cũ...")
        total_deleted = 0

        for table, days in RETENTION_POLICY.items():
            conn = cursor = None
            try:
                cutoff  = int(_time.time()) - (days * 86400)
                conn    = get_db_connection()
                cursor  = conn.cursor()
                deleted = 0

                while True:
                    cursor.execute(
                        f"DELETE FROM {table} WHERE open_time < %s LIMIT 10000",
                        (cutoff,)
                    )
                    conn.commit()
                    rows     = cursor.rowcount
                    deleted += rows
                    if rows < 10000:
                        break
                    await asyncio.sleep(0.5)

                total_deleted += deleted
                print(f"[Retention] {table}: xóa {deleted:,} rows (cũ hơn {days} ngày)")

            except asyncio.CancelledError:
                if conn: conn.rollback()
                raise
            except Exception as e:
                print(f"[Retention] Lỗi {table}: {e}")
                if conn: conn.rollback()
            finally:
                if cursor: cursor.close()
                if conn:   conn.close()

        print(f"[Retention] Hoàn tất — tổng {total_deleted:,} rows đã xóa")
        await asyncio.sleep(60)


# ══════════════════════════════════════════════════════════════════
# START — gọi từ lifespan FastAPI
# ══════════════════════════════════════════════════════════════════
async def start():
    print("[Workers] Khởi động tất cả background tasks...")
    await asyncio.gather(
        fetch_fear_and_greed(),
        fetch_coin_metadata(),
        binance_ws(),
        _flush_live_prices(),
        data_retention_worker(),
    )