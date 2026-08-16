from datetime import datetime
import producer.binance_producer as producer_module
from producer.binance_producer import normalize_trade_message

def test_builds_dlq_record_for_invalid_json():
    result = producer_module.build_dlq_record(
        original_message="{invalid-json",
        error_type="invalid_json",
        error_reason="JSON could not be decoded",
    )

    assert result["schema_version"] == 1
    assert result["stage"] == "producer"
    assert result["source"] == "binance"
    assert result["error_type"] == "invalid_json"
    assert result["error_reason"] == "JSON could not be decoded"
    assert result["original_message"] == "{invalid-json"

    failed_at = datetime.fromisoformat(result["failed_at"])
    assert failed_at.tzinfo is not None

def valid_message():
    return {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1755259200000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "95000.50",
            "q": "0.001",
            "T": 1755259200000,
            "m": False,
        },
    }


def test_normalizes_valid_message():
    result = normalize_trade_message(valid_message())

    assert result is not None
    assert result["symbol"] == "BTCUSDT"
    assert result["price"] == 95000.50
    assert result["quantity"] == 0.001
    assert result["source"] == "binance"


def test_rejects_message_with_missing_field():
    message = valid_message()
    del message["data"]["p"]

    result = normalize_trade_message(message)

    assert result is None


def test_rejects_message_with_invalid_price():
    message = valid_message()
    message["data"]["p"] = "bozuk-fiyat"

    result = normalize_trade_message(message)

    assert result is None