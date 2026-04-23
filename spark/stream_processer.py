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