import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import
(
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
)

from dotenv import load_dotenv

load_dotenv()

APP_NAME = os.getenv("SPARK_APP_NAME","crypto-stream-processor")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL","KAFKA:29092")
KAFKA_TOPIC = os.getenv(("KAFKA_TOPIC","raw_trades"))

def build_spark_session() -> SparkSession:
    return(
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.shuffle.partitions","2")
        .getOrCreate()
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
        .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",KAFKA_TOPIC)
        .option("startingOffsets","latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"),schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate",False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()