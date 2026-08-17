import os
import json
import asyncio
import time
import websockets

from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_DLQ_TOPIC = os.getenv(
    "KAFKA_DLQ_TOPIC",
    "raw_trades_dlq",
)
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL")

def process_trade_message(
    producer: Producer,
    message: str,
) -> None:
    try:
        raw_message = json.loads(message)

    except json.JSONDecodeError as e:
        print(f"[Message Rejected] Invalid JSON: {e}")

        dlq_record = build_dlq_record(
            original_message=message,
            error_type="invalid_json",
            error_reason=str(e),
        )

        publish_dlq_record(
            producer,
            dlq_record,
        )
        return

    try:
        normalized = normalize_trade_message(
            raw_message,
            raise_on_error=True,
        )

    except TradeValidationError as e:
        dlq_record = build_dlq_record(
            original_message=message,
            error_type=e.error_type,
            error_reason=e.error_reason,
        )

        publish_dlq_record(
            producer,
            dlq_record,
        )
        return

    if normalized is None:
        return

    producer.produce(
        topic=KAFKA_TOPIC,
        key=normalized["symbol"],
        value=json.dumps(normalized),
        callback=delivery_report,
    )
    producer.poll(0)

def build_dlq_record(
    original_message: str,
    error_type: str,
    error_reason: str,
) -> dict:
    return {
        "schema_version": 1,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "stage": "producer",
        "source": "binance",
        "error_type": error_type,
        "error_reason": error_reason,
        "original_message": original_message,
    }

class TradeValidationError(Exception):
    def __init__(
        self,
        error_type: str,
        error_reason: str,
    ):
        super().__init__(error_reason)
        self.error_type = error_type
        self.error_reason = error_reason


def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka Delivery Error] {err}")
    else:
        print(
            f"[Kafka Delivered] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )

def publish_dlq_record(
    producer: Producer,
    dlq_record: dict,
) -> None:
    producer.produce(
        topic=KAFKA_DLQ_TOPIC,
        value=json.dumps(dlq_record),
        callback=delivery_report,
    )

    producer.poll(0)

def create_kafka_producer() -> Producer:
    """Create and return a configured Kafka producer."""

    config = {
        # Initial broker addresses used to discover the Kafka cluster.
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

        # Identifies this producer in Kafka logs and monitoring tools.
        "client.id": "binance-trade-producer",

        # Prevents duplicate records caused by producer retries.
        "enable.idempotence": True,

        # Waits for acknowledgments from all required in-sync replicas.
        "acks": "all",

        # Retries delivery up to 10 times for retriable errors.
        "retries": 10,

        # Waits 500 milliseconds between retry attempts.
        "retry.backoff.ms": 500,

        # Fails delivery if it cannot complete within 120 seconds.
        "delivery.timeout.ms": 120000,
    }

    while True:
        try:
            producer = Producer(config)
            producer.list_topics(timeout=5)

            print("Kafka connected")
            return producer

        except Exception as e:
            print(f"Kafka not ready: {e}")
            print("retrying in 5 seconds...")
            time.sleep(5)


def normalize_trade_message(raw_message:dict,*,raise_on_error:bool = False,) -> dict | None:
    if not isinstance(raw_message, dict):
        print("[Message Rejected] Message is not a JSON object")
        return None

    data = raw_message.get("data")

    if not isinstance(data, dict):
        print("[Message Rejected] Missing or invalid data field")
        return None

    required_fields = ("e", "E", "s", "t", "p", "q", "T", "m")

    missing_fields = [
        field
        for field in required_fields
        if data.get(field) is None
    ]

    if missing_fields:
        error_reason = f"Missing fields: {missing_fields}"

        if raise_on_error:
            raise TradeValidationError(
                error_type="missing_fields",
                error_reason=error_reason,
            )

        print(f"[Message Rejected] {error_reason}")
        return None

    try:
        normalized = {
            "event_type": data["e"],
            "symbol": data["s"],
            "trade_id": data["t"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "trade_time": datetime.fromtimestamp(
                data["T"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "event_time": datetime.fromtimestamp(
                data["E"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
            "is_buyer_maker": data["m"],
            "source": "binance",
        }

    except (TypeError, ValueError, OverflowError) as e:
        error_reason = f"{type(e).__name__}:{e}"

        if raise_on_error:
            raise TradeValidationError(
                error_type="invalid_field_value",
                error_reason=error_reason,
            )from e

        print(
            f"[Message Rejected] Invalid field value"
        )
        return None

    return normalized

async def stream_binance_trades(producer):
    retry_delay = 1
    max_retry_delay = 60

    while True:
        try:
            print(f"[WS] Connecting to {BINANCE_WS_URL}")

            async with websockets.connect(
                BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                print("[WS] Connected to Binance stream.")

                async for message in ws:
                    retry_delay = 1

                    process_trade_message(
                        producer,
                        message,
                    )

        except Exception as e:
            print(f"[WS] Error: {type(e).__name__}: {e}")
            print(
                f"[WS] Reconnecting in "
                f"{retry_delay} seconds..."
            )

            await asyncio.sleep(retry_delay)

            retry_delay = min(
                retry_delay * 2,
                max_retry_delay,
            )


if __name__ == "__main__":
    producer = create_kafka_producer()

    try:
        asyncio.run(stream_binance_trades(producer))

    except KeyboardInterrupt:
        print("[Producer] Stopped by user.")

    finally:
        print("[Kafka] Flushing pending messages...")

        remaining_messages = producer.flush(10)

        if remaining_messages == 0:
            print("[Kafka] Flush completed.")
        else:
            print(
                f"[Kafka] Flush timeout: "
                f"{remaining_messages} message(s) still pending."
            )
