try:
    from database import async_redis_client
except ModuleNotFoundError:
    from api.database import async_redis_client

import asyncio
import httpx
import math
import websockets
import hashlib
from datetime import datetime, timezone

try:
    import orjson as json
    def dumps(x): return json.dumps(x)
    def loads(x): return json.loads(x)
except:
    import json
    def dumps(x): return json.dumps(x).encode()
    def loads(x): return json.loads(x)


# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
TRACKED_COINS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","LINKUSDT",
    "POLUSDT","UNIUSDT","ATOMUSDT","LTCUSDT","NEARUSDT",
    "APTUSDT","ARBUSDT","OPUSDT","INJUSDT","SUIUSDT",
    "TRXUSDT","SHIBUSDT","BCHUSDT","ICPUSDT",
]

NAME_MAP = {
    "BTCUSDT":"Bitcoin","ETHUSDT":"Ethereum","BNBUSDT":"BNB",
    "SOLUSDT":"Solana","XRPUSDT":"XRP","ADAUSDT":"Cardano",
    "DOGEUSDT":"Dogecoin","AVAXUSDT":"Avalanche","DOTUSDT":"Polkadot",
    "LINKUSDT":"Chainlink","POLUSDT":"Polygon","UNIUSDT":"Uniswap",
    "ATOMUSDT":"Cosmos","LTCUSDT":"Litecoin","NEARUSDT":"NEAR",
    "APTUSDT":"Aptos","ARBUSDT":"Arbitrum","OPUSDT":"Optimism",
    "INJUSDT":"Injective","SUIUSDT":"Sui","TRXUSDT":"TRON",
    "SHIBUSDT":"Shiba Inu","BCHUSDT":"Bitcoin Cash","ICPUSDT":"Internet Computer",
}
WS_STREAMS = "/".join(f"{s.lower()}@kline_1m" for s in TRACKED_COINS)
WS_URI = f"wss://stream.binance.com:9443/stream?streams={WS_STREAMS}"

LIVE_PRICE_FLUSH_INTERVAL = 15
META_FETCH_INTERVAL = 3600

_live_price_cache = {}
_meta_cache = {}
_last_hash = None


# ══════════════════════════════════════════════════════════════════
# UTILS
# ══════════════════════════════════════════════════════════════════
def _std(data):
    if len(data) < 2:
        return 0.0
    mean = sum(data) / len(data)
    return math.sqrt(sum((x - mean) ** 2 for x in data) / (len(data) - 1))


# ══════════════════════════════════════════════════════════════════
# FEAR & GREED
# ══════════════════════════════════════════════════════════════════
async def calculate_fear_greed(client):
    scores = {}

    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1d", "limit": 90}
        )
        klines = resp.json()
        closes = [float(k[4]) for k in klines]

        returns = [(closes[i]-closes[i-1])/closes[i-1] for i in range(1,len(closes))]

        vol_30 = _std(returns[-30:])
        vol_90 = _std(returns)

        scores["volatility"] = int(100 - (vol_30/vol_90 - 0.5)*80) if vol_90 else 50
        scores["momentum"] = int(50 + (closes[-1]-closes[-30])/closes[-30]*200)

        volumes = [float(k[5]) for k in klines]
        scores["volume"] = int((sum(volumes[-7:])/7)/(sum(volumes[-30:])/30)*50)

    except:
        scores.update({"volatility":50,"momentum":50,"volume":50})

    try:
        resp = await client.get("https://api.coingecko.com/api/v3/global")
        d = resp.json()["data"]

        btc_dom = d["market_cap_percentage"]["btc"]
        total_ch = d["market_cap_change_percentage_24h_usd"]

        scores["dominance"] = int(100-(btc_dom-40)*2)
        scores["market_cap"] = int(50+total_ch*3)
    except:
        scores.update({"dominance":50,"market_cap":50})

    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol":"BTCUSDT"}
        )
        pct = float(resp.json()["priceChangePercent"])
        scores["social"] = int(50+pct*4)
    except:
        scores["social"] = 50

    weights = {
        "volatility":0.25,"momentum":0.25,"volume":0.15,
        "dominance":0.10,"market_cap":0.15,"social":0.10
    }

    final = int(sum(scores[k]*weights[k] for k in weights))

    return {
        "value": final,
        "classification":
            "Extreme Greed" if final>=75 else
            "Greed" if final>=56 else
            "Neutral" if final>=45 else
            "Fear" if final>=25 else "Extreme Fear",
        "timestamp": int(datetime.now(timezone.utc).timestamp())
    }


async def fetch_fear_and_greed():
    while True:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                data = await calculate_fear_greed(client)

            await async_redis_client.set("market_fear_greed", dumps(data))
            print("[F&G]", data["value"], data["classification"])

        except Exception as e:
            print("[F&G] lỗi:", e)

        await asyncio.sleep(3600)


# ══════════════════════════════════════════════════════════════════
# METADATA
# ══════════════════════════════════════════════════════════════════
async def fetch_coin_metadata():
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://api.binance.com/api/v3/ticker/24hr",
                    params={"symbols": dumps(TRACKED_COINS).decode()}
                )

            raw = resp.json()
            meta = {}

            for item in raw:
                sym = item["symbol"]
                meta[sym] = {
                    "coinName": NAME_MAP.get(sym, sym.replace("USDT","")),
                    "priceChangePercent": float(item["priceChangePercent"]),
                    "quoteVolume": float(item["quoteVolume"])
                }

            _meta_cache.clear()
            _meta_cache.update(meta)

            await async_redis_client.set("top_coins_meta", dumps(meta))
            print("[Meta] updated")

        except Exception as e:
            print("[Meta] lỗi:", e)

        await asyncio.sleep(META_FETCH_INTERVAL)


# ══════════════════════════════════════════════════════════════════
# FLUSH
# ══════════════════════════════════════════════════════════════════
async def _flush_live_prices():
    global _last_hash

    while True:
        await asyncio.sleep(LIVE_PRICE_FLUSH_INTERVAL)

        if not _live_price_cache:
            continue

        snapshot_live = dict(_live_price_cache)

        snapshot = {}
        for sym, live in snapshot_live.items():
            entry = dict(live)
            if sym in _meta_cache:
                entry.update(_meta_cache[sym])
            snapshot[sym] = entry

        payload = dumps(snapshot)
        new_hash = hashlib.md5(payload).hexdigest()

        if new_hash == _last_hash:
            continue

        _last_hash = new_hash

        try:
            await async_redis_client.set("live_prices", payload)
            print("[WS] flush", len(snapshot))
        except Exception as e:
            print("[WS] flush lỗi:", e)


# ══════════════════════════════════════════════════════════════════
# WEBSOCKET
# ══════════════════════════════════════════════════════════════════
async def binance_ws():
    retry = 5

    while True:
        try:
            async with websockets.connect(WS_URI) as ws:
                print("[WS] connected")
                retry = 5

                while True:
                    raw = await ws.recv()
                    msg = loads(raw)
                    if "data" in msg:
                        msg = msg["data"]

                    if "k" not in msg:
                        continue

                    k = msg["k"]
                    sym = msg["s"]

                    _live_price_cache[sym] = {
                        "price": float(k["c"]),
                        "time": int(k["t"])
                    }

        except Exception as e:
            print("[WS] lỗi:", e)

        await asyncio.sleep(retry)
        retry = min(retry*2, 60)


# ══════════════════════════════════════════════════════════════════
# START
# ══════════════════════════════════════════════════════════════════
async def start():
    await asyncio.gather(
        fetch_fear_and_greed(),
        fetch_coin_metadata(),
        binance_ws(),
        _flush_live_prices(),
    )