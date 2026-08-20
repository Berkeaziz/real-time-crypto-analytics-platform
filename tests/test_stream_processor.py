import pytest
from pyspark.sql import SparkSession

import spark.stream_processor as stream_module


@pytest.fixture(scope="module")
def spark_session():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("stream-processor-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.warehouse.dir",
            "/tmp/spark-warehouse-tests",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def test_splits_malformed_json_into_dlq(spark_session):
    raw_df = spark_session.createDataFrame(
        [
            (b"{invalid-json",),
        ],
        ["value"],
    )

    valid_df, dlq_df = stream_module.split_trade_records(
        raw_df,
        stream_module.get_trade_schema(),
    )

    assert valid_df.count() == 0

    dlq_rows = dlq_df.collect()

    assert len(dlq_rows) == 1

    dlq_record = dlq_rows[0]

    assert dlq_record["schema_version"] == 1
    assert dlq_record["stage"] == "spark"
    assert dlq_record["source"] == "binance"
    assert dlq_record["error_type"] == "invalid_json"
    assert dlq_record["original_message"] == "{invalid-json"
    assert dlq_record["error_reason"]
    assert dlq_record["failed_at"] is not None