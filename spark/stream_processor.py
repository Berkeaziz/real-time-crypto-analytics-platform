import os
import redis
import psycopg2
import json

from pyspark.sql.window import Window
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
    row_number,
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
from psycopg2.extras import execute_values


load_dotenv()

APP_NAME = os.getenv("SPARK_APP_NAME")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER= os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")


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

    w = Window.partitionBy("symbol").orderBy(col("window_start").desc())

    latest_df = (
        batch_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    latest_df.foreachPartition(write_latest_redis_partition)

    print(f"Batch {batch_id}: Redis latest candles updated")

def write_latest_redis_partition(rows_iter):

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    for row in rows_iter:
        data = row.asDict(recursive=True)
        key = f"latest_candle:{data['symbol']}"
        r.set(key, json.dumps(data, default=json_default))

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.jars", "/tmp/postgresql.jar")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

def write_raw_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    (
        batch_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
def write_ohlcv_partition_to_postgres(rows_iter):
    values = [
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
        )
        for row in rows_iter
    ]

    if not values:
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    insert_sql = """
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
        created_at,
        updated_at
    )
    VALUES %s
    ON CONFLICT (symbol, window_start, window_end)
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count,
        updated_at = NOW();
    """

    try:
        with conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_sql,
                    values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())",
                    page_size=1000,
                )
    finally:
        conn.close()

def write_ohlcv_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.foreachPartition(write_ohlcv_partition_to_postgres)

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
        .withWatermark("trade_time", "5 seconds")
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
        .option("checkpointLocation", "/tmp/checkpoints/raw_trades_to_postgres_v3")
        .start()
    )

    ohlcv_query = (
        ohlcv_10s_df.writeStream
        .foreachBatch(write_ohlcv_to_postgres)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "/app/checkpoints/ohlcv_10s_v3")
        .start()
        )
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()