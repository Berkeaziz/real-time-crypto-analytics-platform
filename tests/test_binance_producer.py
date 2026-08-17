import json
from datetime import datetime
from datetime import datetime
import producer.binance_producer as producer_module
from producer.binance_producer import normalize_trade_message

def test_routes_message_with_invalid_price_to_dlq(monkeypatch):
    class FakeProducer:
        def __init__(self):
            self.produced_messages = []

        def produce(self, **kwargs):
            self.produced_messages.append(kwargs)

        def poll(self, timeout):
            pass

    fake_producer = FakeProducer()

    monkeypatch.setattr(
        producer_module,
        "KAFKA_DLQ_TOPIC",
        "raw_trades_dlq",
    )

    message = json.dumps({
        "data": {
            "e": "trade",
            "E": 1755338400000,
            "s": "BTCUSDT",
            "t": 123456,
            "p": "not-a-number",
            "q": "0.001",
            "T": 1755338400000,
            "m": False,
        }
    })

    producer_module.process_trade_message(
        fake_producer,
        message,
    )

    assert len(fake_producer.produced_messages) == 1

    produced_message = fake_producer.produced_messages[0]
    dlq_record = json.loads(produced_message["value"])

    assert produced_message["topic"] == "raw_trades_dlq"
    assert dlq_record["error_type"] == "invalid_field_value"
    assert dlq_record["original_message"] == message
    assert dlq_record["error_reason"]

def test_routes_invalid_json_to_dlq(monkeypatch):
    class FakeProducer:
        def __init__(self):
            self.produced_messages = []

        def produce(self, **kwargs):
            self.produced_messages.append(kwargs)

        def poll(self, timeout):
            pass

    fake_producer = FakeProducer()

    monkeypatch.setattr(
        producer_module,
        "KAFKA_DLQ_TOPIC",
        "raw_trades_dlq",
    )

    producer_module.process_trade_message(
        fake_producer,
        "{invalid-json",
    )

    assert len(fake_producer.produced_messages) == 1

    produced_message = fake_producer.produced_messages[0]
    dlq_record = json.loads(produced_message["value"])

    assert produced_message["topic"] == "raw_trades_dlq"
    assert dlq_record["stage"] == "producer"
    assert dlq_record["error_type"] == "invalid_json"
    assert dlq_record["original_message"] == "{invalid-json"
    
def test_publishes_record_to_dlq_topic(monkeypatch):
    class FakeProducer:
        def __init__(self):
            self.produced_messages = []
            self.poll_calls = []

        def produce(self, **kwargs):
            self.produced_messages.append(kwargs)

        def poll(self, timeout):
            self.poll_calls.append(timeout)

    fake_producer = FakeProducer()

    dlq_record = {
        "schema_version": 1,
        "failed_at": "2026-08-16T10:00:00+00:00",
        "stage": "producer",
        "source": "binance",
        "error_type": "invalid_json",
        "error_reason": "JSON could not be decoded",
        "original_message": "{invalid-json",
    }

    monkeypatch.setattr(
        producer_module,
        "KAFKA_DLQ_TOPIC",
        "raw_trades_dlq",
        raising=False,
    )

    producer_module.publish_dlq_record(
        fake_producer,
        dlq_record,
    )

    assert len(fake_producer.produced_messages) == 1

    produced_message = fake_producer.produced_messages[0]

    assert produced_message["topic"] == "raw_trades_dlq"
    assert json.loads(produced_message["value"]) == dlq_record
    assert produced_message["callback"] is producer_module.delivery_report
    assert fake_producer.poll_calls == [0]


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