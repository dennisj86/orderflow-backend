import logging

logger = logging.getLogger(__name__)

from datetime import datetime, timezone

import json
from aiokafka import AIOKafkaProducer


async def produce_trade(producer: AIOKafkaProducer, trade: dict, topic: str, symbol: str, exchange: str):
    """
    Sendet einen Trade-Datensatz an Kafka.

    :param producer: AIOKafkaProducer-Instanz
    :param trade: Dictionary mit Preis, Volumen, Seite etc.
    :param topic: Kafka-Topic
    :param symbol: Symbol (z.B. BTC-USDT)
    :param exchange: Exchange-Name (z.B. binance)
    """
    try:
        message = {
            "type": "trade",
            "exchange": exchange,
            "symbol": symbol,
            "data": trade
        }
        key = f"{exchange}-{symbol}"
        await produce(producer, topic, message, key=symbol)
    except Exception as e:
        print(f"[ERROR] Trade-Message konnte nicht gesendet werden: {e}")

async def produce_order_book(producer, snapshot, topic: str, symbol: str = None, exchange: str = None, level: str = "l2"):
    message = {
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "bids": snapshot["bids"],
        "asks": snapshot["asks"],
        "exchange": exchange,
        "symbol": symbol,
        "level": level
    }

    await produce(producer=producer, topic=topic, message=message)

async def produce(producer: AIOKafkaProducer, topic: str, message: dict, key: str = None):
    try:
        await producer.send_and_wait(
            topic=topic,
            value=json.dumps(message).encode("utf-8"),
            key=key.encode("utf-8") if key else None)
        print(f"Producing to topic: {topic} | Message: {message}")
        logger.debug(f"Message sent to {topic}: {message}")
    except Exception as e:
        logger.error(f"Kafka produced error on topic {topic}: {e}")
