import asyncio
import os
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import TRADES, L2_BOOK

from .utils import produce_order_book, produce_trade

# ENV-Variablen laden
load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SYMBOL = os.getenv("SYMBOL", "BTC-USDT")

# 🎯 Kafka Topics
TOPIC_TRADES = "binance-trades-spot"
TOPIC_ORDERBOOK = "binance-orderbook-spot"

# Hauptfunktion
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    # FeedHandler initialisieren
    fh = FeedHandler()

    try:
        async def handle_trade(feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp, **kwargs):
            trade = {
                "price": price,
                "amount": amount,
                "side": side,
                "timestamp": int(receipt_timestamp * 1000)
            }
            await produce_trade(producer, trade, topic=TOPIC_TRADES, symbol=symbol, exchange="binance")

        async def handle_order_book(feed, symbol, book, timestamp, receipt_timestamp):
            snapshot = {
                "bids": list(book.book.bids.items()),
                "asks": list(book.book.asks.items())
            }
            await produce_order_book(producer, snapshot, topic=TOPIC_ORDERBOOK, symbol=symbol, exchange="binance", level="l2")

        fh.add_feed(Binance(
            symbols=[SYMBOL],
            channels=[TRADES, L2_BOOK],
            callbacks={
                TRADES: handle_trade,
                L2_BOOK: handle_order_book
            }
        ))

        fh.run(start_loop=False)
        await asyncio.Event().wait()
    finally:
        fh.stop()
        await producer.stop()

# Scriptstart
if __name__ == "__main__":
    asyncio.run(main())
