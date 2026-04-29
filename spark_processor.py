import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window,
    min as spark_min, max as spark_max,
    first, last, sum as spark_sum, explode,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
    ArrayType, LongType,
)

from config import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC,
    SPARK_CHECKPOINT_DIR, TIMEFRAMES,
    REDIS_CONFIG,
)

# ─────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CryptoPulse_Processor") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "mysql:mysql-connector-java:8.0.28",
    ) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# Schema 
# ─────────────────────────────────────────
ticker_schema = StructType([
    StructField("s", StringType()),   # symbol
    StructField("c", StringType()),   # close price
    StructField("o", StringType()),   # open price
    StructField("h", StringType()),   # high
    StructField("l", StringType()),   # low
    StructField("v", StringType()),   # volume
    StructField("E", LongType()),     # event time ms
])

# ─────────────────────────────────────────
# Đọc Kafka stream
# ─────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

# ─────────────────────────────────────────
# Parse + chuẩn hóa
# ─────────────────────────────────────────
parsed_df = (
    raw_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), ArrayType(ticker_schema)).alias("arr"))
    .select(explode(col("arr")).alias("d"))
    .select("d.*")
    # Đổi tên tránh trùng keyword Python/SQL
    .withColumn("price",     col("c").cast(DoubleType()))
    .withColumn("open_p",    col("o").cast(DoubleType()))
    .withColumn("high_p",    col("h").cast(DoubleType()))
    .withColumn("low_p",     col("l").cast(DoubleType()))
    .withColumn("volume_p",  col("v").cast(DoubleType()))
    .withColumn("timestamp", (col("E") / 1000).cast(TimestampType()))
    .withWatermark("timestamp", "15 seconds")
)


# ─────────────────────────────────────────
# Upsert MySQL (chạy per-partition)
# ─────────────────────────────────────────
def upsert_to_mysql(partition, table_name: str):
    """Chạy bên trong executor – import cục bộ."""
    import mysql.connector, os

    cfg = {
        "host":     os.getenv("MYSQL_HOST", "mysql"),
        "user":     "root",
        "password": os.getenv("MYSQL_ROOT_PASSWORD", "rootpassword"),
        "database": "cryptopulse",
    }
    rows = [
        (r.symbol, r.open_time, r.open_p, r.high_p, r.low_p, r.close_p, r.volume_p)
        for r in partition
    ]
    if not rows:
        return

    try:
        conn   = mysql.connector.connect(**cfg)
        cursor = conn.cursor()
        cursor.executemany(f"""
            INSERT INTO {table_name}
                (symbol, open_time, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                high_price  = IF(VALUES(high_price)  > high_price,  VALUES(high_price),  high_price),
                low_price   = IF(VALUES(low_price)   < low_price,   VALUES(low_price),   low_price),
                close_price = VALUES(close_price),
                volume      = VALUES(volume)
        """, rows)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as exc:
        print(f"[MySQL ERROR @ {table_name}] {exc}")


# ─────────────────────────────────────────
# Cập nhật Redis ticker_latest
# ─────────────────────────────────────────
def update_redis_ticker(partition):
    """Lưu giá mới nhất vào Redis để app Android poll nhanh."""
    from redis import Redis
    import os

    r = Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=0,
        decode_responses=True,
    )
    pipe = r.pipeline()
    for row in partition:
        pipe.hset(f"kline:latest:{row.symbol}", mapping={
            "price":     str(row.close_p),
            "open":      str(row.open_p),
            "high":      str(row.high_p),
            "low":       str(row.low_p),
            "volume":    str(row.volume_p),
            "open_time": str(row.open_time),
        })
        # TTL 10 phút – tự dọn nếu coin ngừng stream
        pipe.expire(f"kline:latest:{row.symbol}", 600)
    pipe.execute()


# ─────────────────────────────────────────
# Xử lý từng micro-batch
# ─────────────────────────────────────────
def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    print(f"[Spark] Batch #{batch_id} – đang xử lý...")
    batch_df.persist()

    for tf_label, tf_duration in TIMEFRAMES.items():
        tf_df = (
            batch_df
            .groupBy(window(col("timestamp"), tf_duration), col("s").alias("symbol"))
            .agg(
                first("open_p").alias("open_p"),
                spark_max("high_p").alias("high_p"),
                spark_min("low_p").alias("low_p"),
                last("price").alias("close_p"),
                spark_sum("volume_p").alias("volume_p"),
            )
            .withColumn("open_time", col("window.start").cast("long"))
            .drop("window")
        )

        # Cập nhật Redis cho nến 1m (realtime ticker)
        if tf_label == "1m":
            tf_df.foreachPartition(update_redis_ticker)

        # Ghi vào MySQL
        tf_df.foreachPartition(
            lambda p, tbl=f"kline_{tf_label}": upsert_to_mysql(p, tbl)
        )

    batch_df.unpersist()
    print(f"[Spark] Batch #{batch_id} hoàn tất ✓")


# ─────────────────────────────────────────
# Start streaming query
# ─────────────────────────────────────────
query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", SPARK_CHECKPOINT_DIR)
    .trigger(processingTime="5 seconds")
    .start()
)

print("CryptoPulse Spark Processor running...")
query.awaitTermination()
