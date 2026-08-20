import pytest
from pyspark.sql import SparkSession

import spark.stream_processor as stream_module
import json

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

def test_routes_missing_required_field_to_dlq(spark_session):
    message = json.dumps({
        "event_type": "trade",
        "trade_id": 123456,
        "price": 60000.50,
        "quantity": 0.001,
        "trade_time": "2026-08-20T19:00:00+00:00",
        "event_time": "2026-08-20T19:00:00+00:00",
        "is_buyer_maker": False,
        "source": "binance",
    })

    raw_df = spark_session.createDataFrame(
        [
            (message.encode("utf-8"),),
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

    assert dlq_record["stage"] == "spark"
    assert dlq_record["error_type"] == "missing_required_fields"
    assert "symbol" in dlq_record["error_reason"]
    assert dlq_record["original_message"] == message



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

def test_routes_valid_trade_to_valid_dataframe(spark_session):
    message = json.dumps({
        "event_type": "trade",
        "symbol": "BTCUSDT",
        "trade_id": 123456,
        "price": 60000.50,
        "quantity": 0.001,
        "trade_time": "2026-08-20T19:00:00+00:00",
        "event_time": "2026-08-20T19:00:00+00:00",
        "is_buyer_maker": False,
        "source": "binance",
    })

    raw_df = spark_session.createDataFrame(
        [
            (message.encode("utf-8"),),
        ],
        ["value"],
    )

    valid_df, dlq_df = stream_module.split_trade_records(
        raw_df,
        stream_module.get_trade_schema(),
    )

    assert dlq_df.count() == 0

    valid_rows = valid_df.collect()

    assert len(valid_rows) == 1

    trade = valid_rows[0]

    assert trade["symbol"] == "BTCUSDT"
    assert trade["trade_id"] == 123456
    assert trade["price"] == 60000.50
    assert trade["quantity"] == 0.001
    assert trade["trade_time"] is not None
    assert trade["event_time"] is not None
    assert trade["ingested_at"] is not None    