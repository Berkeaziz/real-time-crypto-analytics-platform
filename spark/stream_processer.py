import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,to_timestamp,to_utc_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
)

from dotenv import load_dotenv


load_dotenv()

APP_NAME = os.getenv("SPARK_APP_NAME", "crypto-stream-processor")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.jars", "/tmp/postgresql.jar")
        .getOrCreate()
    )


def write_to_postgres(batch_df, batch_id):
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
        .withColumn("ingested_at", to_timestamp("ingested_at"))
        .withColumn("event_time", to_utc_timestamp("event_time", "UTC"))
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()