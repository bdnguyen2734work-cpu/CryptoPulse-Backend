import os
import socket

# Hàm kiểm tra xem có đang chạy trong Docker hay không
def is_docker():
    return os.path.exists('/.dockerenv')

DB_CONFIG = {
    # Nếu trong Docker thì host='mysql', nếu ở ngoài (VS Code) thì host='localhost'
    "host": "mysql" if is_docker() else "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "cryptopulse",
    "port": 3306,
}

# Chuỗi JDBC dùng cho Spark kết nối Database
JDBC_URL    = f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

# ─────────────────────────────────────────
# 2. Cấu hình Kafka (Truyền tải dữ liệu Real-time)
# ─────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC      = "market_tick_data"
KAFKA_GROUP_ID   = "cryptopulse_group"

# ─────────────────────────────────────────
# 3. Cấu hình Redis (Lưu trữ Cache & Live Price)
# ─────────────────────────────────────────
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "redis"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db":   0,
}

# ─────────────────────────────────────────
# 4. Danh sách 25 Symbols chuẩn (Nguồn dữ liệu)
# ─────────────────────────────────────────
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT",
    "LINKUSDT", "TRXUSDT", "POLUSDT", "LTCUSDT", "BCHUSDT",
    "UNIUSDT", "NEARUSDT", "ATOMUSDT", "ICPUSDT", "APTUSDT",
    "SUIUSDT", "INJUSDT",  "OPUSDT",  "ARBUSDT"]
SYMBOLS_SET = set(SYMBOLS)

# Cấu hình khung thời gian cho Spark Streaming
TIMEFRAMES = {
    "1m":  "1 minute",
    "5m":  "5 minutes",
    "15m": "15 minutes",
    "1h":  "1 hour",
    "1d":  "1 day",
    "1w":  "7 days",
}

# Số ngày tối đa để cào dữ liệu lịch sử (Backfill)
BACKFILL_SHORT_DAYS = 30

# ─────────────────────────────────────────
# 5. Cấu hình Spark
# ─────────────────────────────────────────
SPARK_CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    "/opt/spark/checkpoints/clean_v2"
)

# ─────────────────────────────────────────
# 6. Mapping cho App Android (Hiển thị Tên & Mã)
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