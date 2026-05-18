import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# 1. Database (TiDB Cloud)
# ─────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 4000)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "cryptopulse"),
    # TiDB Cloud bắt buộc SSL
    "ssl_verify_cert":     False,
    "ssl_verify_identity": False,
}

JDBC_URL    = f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

# ─────────────────────────────────────────
# 2. Kafka
# ─────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = "market_tick_data"
KAFKA_GROUP_ID  = "cryptopulse_group"

# ─────────────────────────────────────────
# 3. Redis (Upstash)
# ─────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db":   0,
}

# ─────────────────────────────────────────
# 4. Symbols
# ─────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT",
    "LINKUSDT", "TRXUSDT", "POLUSDT", "LTCUSDT", "BCHUSDT",
    "UNIUSDT", "NEARUSDT", "ATOMUSDT", "ICPUSDT", "APTUSDT",
    "SUIUSDT", "INJUSDT",  "OPUSDT",  "ARBUSDT",
]
SYMBOLS_SET = set(SYMBOLS)

# ─────────────────────────────────────────
# 5. Timeframes
# ─────────────────────────────────────────
TIMEFRAMES = {
    "1m":  "1 minute",
    "5m":  "5 minutes",
    "15m": "15 minutes",
    "1h":  "1 hour",
    "4h":  "4 hours",
    "1d":  "1 day",
    "1w":  "7 days",
}

BACKFILL_SHORT_DAYS = int(os.getenv("BACKFILL_SHORT_DAYS", 30))

# ─────────────────────────────────────────
# 6. Spark
# ─────────────────────────────────────────
SPARK_CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    "/opt/spark/checkpoints/clean_v2"
)

# ─────────────────────────────────────────
# 7. Coin map (Android app)
# ─────────────────────────────────────────
APP_COIN_MAP = {
    "BTCUSDT":  {"name": "Bitcoin",           "symbol": "BTC"},
    "ETHUSDT":  {"name": "Ethereum",          "symbol": "ETH"},
    "BNBUSDT":  {"name": "BNB",               "symbol": "BNB"},
    "SOLUSDT":  {"name": "Solana",            "symbol": "SOL"},
    "XRPUSDT":  {"name": "XRP",               "symbol": "XRP"},
    "ADAUSDT":  {"name": "Cardano",           "symbol": "ADA"},
    "DOGEUSDT": {"name": "Dogecoin",          "symbol": "DOGE"},
    "AVAXUSDT": {"name": "Avalanche",         "symbol": "AVAX"},
    "SHIBUSDT": {"name": "Shiba Inu",         "symbol": "SHIB"},
    "DOTUSDT":  {"name": "Polkadot",          "symbol": "DOT"},
    "LINKUSDT": {"name": "Chainlink",         "symbol": "LINK"},
    "TRXUSDT":  {"name": "TRON",              "symbol": "TRX"},
    "POLUSDT":  {"name": "Polygon",           "symbol": "POL"},
    "LTCUSDT":  {"name": "Litecoin",          "symbol": "LTC"},
    "BCHUSDT":  {"name": "Bitcoin Cash",      "symbol": "BCH"},
    "UNIUSDT":  {"name": "Uniswap",           "symbol": "UNI"},
    "NEARUSDT": {"name": "NEAR",              "symbol": "NEAR"},
    "ATOMUSDT": {"name": "Cosmos",            "symbol": "ATOM"},
    "ICPUSDT":  {"name": "Internet Computer", "symbol": "ICP"},
    "APTUSDT":  {"name": "Aptos",             "symbol": "APT"},
    "SUIUSDT":  {"name": "Sui",               "symbol": "SUI"},
    "INJUSDT":  {"name": "Injective",         "symbol": "INJ"},
    "OPUSDT":   {"name": "Optimism",          "symbol": "OP"},
    "ARBUSDT":  {"name": "Arbitrum",          "symbol": "ARB"},
}