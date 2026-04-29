import asyncio
import httpx
import json
import websockets
import math
from datetime import datetime, timezone
from database import async_redis_client

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════
TOP_COINS = [
    "BTCUSDT",  "ETHUSDT",  "BNBUSDT",  "SOLUSDT",  "XRPUSDT",
    "ADAUSDT",  "DOGEUSDT", "AVAXUSDT", "DOTUSDT",  "LINKUSDT",
    "POLUSDT",  "UNIUSDT",  "ATOMUSDT", "LTCUSDT",  "NEARUSDT",
    "APTUSDT",  "ARBUSDT",  "OPUSDT",   "INJUSDT",  "SUIUSDT",
    "TRXUSDT",  "SHIBUSDT", "BCHUSDT",  "ICPUSDT",
]

WS_COINS = [
    "btcusdt", "ethusdt", "bnbusdt",  "solusdt",
    "xrpusdt", "adausdt", "dogeusdt", "avaxusdt",
]
WS_URI = (
    "wss://stream.binance.com:9443/ws/"
    + "/".join(f"{s}@kline_1m" for s in WS_COINS)
)


# ══════════════════════════════════════════════════════════════════
#  FEAR & GREED — tự tính theo thuật toán chuẩn
#
#  Công thức giống alternative.me / CoinMarketCap:
#  1. Volatility        25% — so sánh std dev giá BTC 30d vs 90d
#  2. Market Momentum   25% — volume 30d so với trung bình + price change
#  3. Social Media      15% — Reddit + Twitter mentions (approx)
#  4. Dominance         10% — BTC dominance (cao = Fear)
#  5. Trends            10% — Google Trends BTC (approx từ price action)
#  6. Market Cap Change 15% — total market cap 24h change
# ══════════════════════════════════════════════════════════════════
async def calculate_fear_greed(client: httpx.AsyncClient) -> dict:
    scores = {}

    # ── 1. BTC Price & Volatility (25%) ──────────────────────────
    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1d", "limit": 90}
        )
        if resp.status_code == 200:
            klines  = resp.json()
            closes  = [float(k[4]) for k in klines]
            returns = [
                (closes[i] - closes[i-1]) / closes[i-1]
                for i in range(1, len(closes))
            ]

            # Volatility 30d vs 90d
            vol_30 = _std(returns[-30:]) if len(returns) >= 30 else 0
            vol_90 = _std(returns)       if len(returns) >= 60 else vol_30

            # Volatility thấp hơn trung bình → Greed, cao hơn → Fear
            if vol_90 > 0:
                vol_ratio = vol_30 / vol_90
                # vol_ratio < 1 → bình thường/greed, > 1 → fear
                vol_score = max(0, min(100, int(100 - (vol_ratio - 0.5) * 80)))
            else:
                vol_score = 50

            # Price momentum 30d
            if len(closes) >= 30:
                price_change_30d = (closes[-1] - closes[-30]) / closes[-30]
                momentum_score   = max(0, min(100, int(50 + price_change_30d * 200)))
            else:
                momentum_score = 50

            scores["volatility"] = vol_score
            scores["momentum"]   = momentum_score

            # Volume momentum (so sánh 7d vs 30d)
            volumes  = [float(k[5]) for k in klines]
            vol7d    = sum(volumes[-7:])  / 7  if len(volumes) >= 7  else 0
            vol30d   = sum(volumes[-30:]) / 30 if len(volumes) >= 30 else vol7d
            if vol30d > 0:
                vol_momentum = vol7d / vol30d
                scores["volume"] = max(0, min(100, int(vol_momentum * 50)))
            else:
                scores["volume"] = 50

    except Exception as e:
        print(f"[F&G] BTC klines lỗi: {e}")
        scores["volatility"] = 50
        scores["momentum"]   = 50
        scores["volume"]     = 50

    # ── 2. BTC Dominance (10%) ────────────────────────────────────
    try:
        resp = await client.get("https://api.coingecko.com/api/v3/global")
        if resp.status_code == 200:
            d        = resp.json().get("data", {})
            btc_dom  = float(d.get("market_cap_percentage", {}).get("btc", 50))
            total_ch = float(d.get("market_cap_change_percentage_24h_usd", 0))

            # BTC dom > 55% → thị trường sợ hãi (chạy về BTC)
            # BTC dom < 40% → altcoin season → greed
            dom_score = max(0, min(100, int(100 - (btc_dom - 40) * 2)))
            scores["dominance"] = dom_score

            # Total market cap change (15%)
            mktcap_score = max(0, min(100, int(50 + total_ch * 3)))
            scores["market_cap"] = mktcap_score

    except Exception as e:
        print(f"[F&G] CoinGecko global lỗi: {e}")
        scores["dominance"]  = 50
        scores["market_cap"] = 50

    # ── 3. Social / Trends approx (15%) ──────────────────────────
    # Dùng price action ngắn hạn làm proxy cho social sentiment
    try:
        resp = await client.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": "BTCUSDT"}
        )
        if resp.status_code == 200:
            t        = resp.json()
            pct_24h  = float(t.get("priceChangePercent", 0))
            # Giá tăng → social positive → greed
            social_score = max(0, min(100, int(50 + pct_24h * 4)))
            scores["social"] = social_score
    except Exception as e:
        print(f"[F&G] Social proxy lỗi: {e}")
        scores["social"] = 50

    # ── Tổng hợp có trọng số ──────────────────────────────────────
    #  Volatility  25%, Momentum  25%, Volume  15%
    #  Dominance   10%, MarketCap 15%, Social  10%
    weights = {
        "volatility": 0.25,
        "momentum":   0.25,
        "volume":     0.15,
        "dominance":  0.10,
        "market_cap": 0.15,
        "social":     0.10,
    }
    final = sum(scores.get(k, 50) * w for k, w in weights.items())
    final = max(0, min(100, int(round(final))))

    if final >= 75:   label = "Extreme Greed"
    elif final >= 56: label = "Greed"
    elif final >= 45: label = "Neutral"
    elif final >= 25: label = "Fear"
    else:             label = "Extreme Fear"

    return {
        "value":          final,
        "classification": label,
        "source":         "self-calculated",
        "components":     scores,
        "timestamp":      int(datetime.now(timezone.utc).timestamp()),
        "note": (
            "Volatility 25% + Momentum 25% + Volume 15% + "
            "Dominance 10% + MarketCap 15% + Social 10%"
        ),
    }


def _std(data: list) -> float:
    """Standard deviation."""
    if len(data) < 2:
        return 0.0
    n    = len(data)
    mean = sum(data) / n
    return math.sqrt(sum((x - mean) ** 2 for x in data) / (n - 1))


# ══════════════════════════════════════════════════════════════════
#  WORKER 1: FEAR & GREED
# ══════════════════════════════════════════════════════════════════
async def fetch_fear_and_greed():
    """
    Tự tính Fear & Greed theo thuật toán chuẩn.
    Fallback về alternative.me nếu tính lỗi.
    Cập nhật mỗi 1 giờ.
    """
    while True:
        success = False
        try:
            async with httpx.AsyncClient(
                timeout=20,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as client:
                payload = await calculate_fear_greed(client)

            await async_redis_client.set(
                "market_fear_greed", json.dumps(payload)
            )
            print(
                f"[F&G] Tự tính → {payload['value']} "
                f"({payload['classification']}) | "
                f"components: {payload['components']}"
            )
            success = True

        except Exception as e:
            print(f"[F&G] Tự tính lỗi: {e}")

        # ── Fallback: alternative.me ──────────────────────────────
        if not success:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        "https://api.alternative.me/fng/?limit=1&format=json"
                    )
                if resp.status_code == 200:
                    item  = resp.json()["data"][0]
                    value = int(item["value"])
                    label = item["value_classification"]
                    payload = {
                        "value":          value,
                        "classification": label,
                        "source":         "alternative.me (fallback)",
                        "timestamp":      int(item.get("timestamp", 0)),
                    }
                    await async_redis_client.set(
                        "market_fear_greed", json.dumps(payload)
                    )
                    print(f"[F&G] Fallback alternative.me → {value} ({label})")
            except Exception as e:
                print(f"[F&G] Fallback lỗi: {e}")

        await asyncio.sleep(3600)  # 1 giờ


# ══════════════════════════════════════════════════════════════════
#  WORKER 2: TOP 24 COINS
# ══════════════════════════════════════════════════════════════════
async def fetch_top_coins():
    """
    Ticker 24h của 24 coin từ Binance.
    Cập nhật mỗi 5 phút.
    """
    symbols_json = json.dumps(TOP_COINS)
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbols={symbols_json}"

    name_map = {
        "BTCUSDT": "Bitcoin",          "ETHUSDT": "Ethereum",
        "BNBUSDT": "BNB",              "SOLUSDT": "Solana",
        "XRPUSDT": "XRP",             "ADAUSDT": "Cardano",
        "DOGEUSDT": "Dogecoin",        "AVAXUSDT": "Avalanche",
        "DOTUSDT": "Polkadot",         "LINKUSDT": "Chainlink",
        "POLUSDT": "Polygon",          "UNIUSDT": "Uniswap",
        "ATOMUSDT": "Cosmos",          "LTCUSDT": "Litecoin",
        "NEARUSDT": "NEAR",            "APTUSDT": "Aptos",
        "ARBUSDT": "Arbitrum",         "OPUSDT": "Optimism",
        "INJUSDT": "Injective",        "SUIUSDT": "Sui",
        "TRXUSDT": "TRON",             "SHIBUSDT": "Shiba Inu",
        "BCHUSDT": "Bitcoin Cash",     "ICPUSDT": "Internet Computer",
    }

    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url)

            if resp.status_code == 200:
                data = resp.json()
                data.sort(
                    key=lambda x: float(x.get("quoteVolume", 0)),
                    reverse=True
                )
                for item in data:
                    sym = item.get("symbol", "")
                    item["coinName"]      = name_map.get(sym, sym.replace("USDT", ""))
                    item["displaySymbol"] = "POL" if sym in ("MATICUSDT", "POLUSDT") \
                                           else sym.replace("USDT", "")

                await async_redis_client.set(
                    "top_20_coins_stats", json.dumps(data)
                )
                print(f"[Coins] Cập nhật {len(data)} coins ✓")
            else:
                print(f"[Coins] Binance HTTP {resp.status_code}")

        except Exception as e:
            print(f"[Coins] Lỗi: {e}")

        await asyncio.sleep(300)


# ══════════════════════════════════════════════════════════════════
#  WORKER 3: LIVE PRICE WebSocket
# ══════════════════════════════════════════════════════════════════
async def binance_live_price_worker():
    """
    Stream realtime 8 coin qua Binance WebSocket kline_1m.
    Exponential backoff khi mất kết nối.
    """
    retry_delay = 5

    while True:
        try:
            async with websockets.connect(
                WS_URI,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                print(f"[WS] Kết nối Binance WebSocket ✓ ({len(WS_COINS)} streams)")
                retry_delay = 5

                while True:
                    raw  = await ws.recv()
                    data = json.loads(raw)
                    if "k" not in data:
                        continue

                    symbol = data["s"].upper()
                    kline  = data["k"]

                    await async_redis_client.hset(
                        f"kline:latest:{symbol}",
                        mapping={
                            "price":  str(float(kline["c"])),
                            "open":   str(float(kline["o"])),
                            "high":   str(float(kline["h"])),
                            "low":    str(float(kline["l"])),
                            "volume": str(float(kline["v"])),
                            "time":   str(int(kline["t"])),
                            "closed": str(kline["x"]),
                        }
                    )

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WS] Mất kết nối: {e}. Reconnect sau {retry_delay}s...")
        except Exception as e:
            print(f"[WS] Lỗi: {e}. Reconnect sau {retry_delay}s...")

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)


# ══════════════════════════════════════════════════════════════════
#  KHỞI ĐỘNG
# ══════════════════════════════════════════════════════════════════
async def start_all_workers():
    print("[Workers] Khởi động 3 workers...")
    await asyncio.gather(
        fetch_fear_and_greed(),
        fetch_top_coins(),
        binance_live_price_worker(),
        return_exceptions=True,
    )