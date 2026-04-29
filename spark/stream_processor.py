import os
import redis
import psycopg2
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    from_json,
    to_timestamp,
    current_timestamp,
    window,
    avg,
    min,
    max,
    sum,
    count,
    first,
    last,
)


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
)

from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import execute_batch


load_dotenv()

APP_NAME = os.getenv("SPARK_APP_NAME", "crypto-stream-processor")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def json_default(obj):
    if isinstance (obj,Decimal):
        return float(obj)
    if isinstance (obj,datetime):
        return obj.isoformat()
    return str(obj)


def write_latest_ohlcv_to_redis(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty batch, skipping Redis write")
        return

    r =redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    latest_df =(
        batch_df
        .orderBy("symbol",batch_df.window_start.desc())
        .dropDuplicates(["symbol"])
    )

    rows = latest_df.collect()

    for row in rows:
        data=row.asDict(recursive=True)
        key = f"latest_candle:{data['symbol']}"
        r.set(key, json.dumps(data, default=json_default))
        print(f"Batch {batch_id}: updated Redis key {key}")

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.jars", "/tmp/postgresql.jar")
        .getOrCreate()
    )

def write_raw_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/crypto")
        .option("dbtable", "raw.trades")
        .option("user", "crypto_user")
        .option("password", "crypto_pass")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

def write_ohlcv_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()

    if not rows:
        return

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="crypto",
        user="crypto_user",
        password="crypto_pass",
    )

    cursor = conn.cursor()

    upsert_sql = """
    INSERT INTO marts.ohlcv_10s (
        symbol,
        window_start,
        window_end,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        trade_count,
        created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (symbol, window_start, window_end)
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count,
        created_at = NOW();
    """

    for row in rows:
        cursor.execute(
            upsert_sql,
            (
                row["symbol"],
                row["window_start"],
                row["window_end"],
                row["open_price"],
                row["high_price"],
                row["low_price"],
                row["close_price"],
                row["volume"],
                row["trade_count"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()
    write_latest_ohlcv_to_redis(batch_df, batch_id)

def get_trade_schema() -> StructType:
    return StructType([
        StructField("event_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_trade_schema()

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("trade_time", to_timestamp("trade_time"))
        .withColumn("event_time", to_timestamp("event_time"))
        .withColumn("ingested_at", current_timestamp())
    )

    ohlcv_10s_df = (
        parsed_df
        .withWatermark("trade_time", "10 seconds")
        .groupBy(
            window(col("trade_time"), "10 seconds"),
            col("symbol")
        )
        .agg(
            first("price").alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price").alias("close_price"),
            sum("quantity").alias("volume"),
            count("*").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("volume"),
            col("trade_count"),
            current_timestamp().alias("created_at"),
        )
    )
            

    query = (
        parsed_df.writeStream
        .foreachBatch(write_raw_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/raw_trades_to_postgres_v2")
        .start()
    )

    ohlcv_query = (
        ohlcv_10s_df.writeStream
        .foreachBatch(write_ohlcv_to_postgres)
        .outputMode("update")
        .trigger(processingTime="2 seconds")
        .option("checkpointLocation", "/app/checkpoints/ohlcv_10s_v2")
        .start()
        )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()