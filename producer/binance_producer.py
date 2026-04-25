import os
import json
import asyncio
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")
BINANCE_WS_URL = os.getenv(
    "BINANCE_WS_URL",
    "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade",
)


def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka Delivery Error] {err}")
    else:
        print(
            f"[Kafka Delivered] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def create_kafka_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "binance-trade-producer",
    }
    return Producer(config)


def normalize_trade_message(raw_message: dict) -> dict | None:
    """
    Binance combined stream message example:
    {
      "stream": "btcusdt@trade",
      "data": {...}
    }
    """
    data = raw_message.get("data")
    if not data:
        return None

    event_time_ms = data.get("E")
    trade_time_ms = data.get("T")

    normalized = {
        "event_type": data.get("e"),
        "symbol": data.get("s"),
        "trade_id": data.get("t"),
        "price": float(data.get("p")),
        "quantity": float(data.get("q")),
        "trade_time": (
            datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).isoformat()
            if trade_time_ms
            else None
        ),
        "event_time": (
            datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat()
            if event_time_ms
            else None
        ),
        "is_buyer_maker": data.get("m"),
        "source": "binance",
    }
    return normalized


async def stream_binance_trades(producer):
    while True:
        try:
            print(f"[WS] Connecting to {BINANCE_WS_URL}")
            async with websockets.connect(BINANCE_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print("[WS] Connected to Binance stream.")

                async for message in ws:
                    raw_message = json.loads(message)
                    normalized = normalize_trade_message(raw_message)

                    if normalized is None:
                        continue

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=normalized["symbol"],
                        value=json.dumps(normalized),
                    )
                    producer.poll(0)

                    print(f"[produced] {normalized}")

        except Exception as e:
            print("[WS] Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ =="__main__":
    producer = create_kafka_producer()

    try:
        asyncio.run(stream_binance_trades(producer))

    except KeyboardInterrupt:
        print("Producer stopped by user")

    finally:
        print("Flushing Kafka producer...")
        producer.flush()