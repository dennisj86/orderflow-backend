import asyncio
import os
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import TRADES, L2_BOOK

from services.utils.utils import produce_order_book, produce_trade

# ENV-Variablen laden
load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SYMBOL = os.getenv("SYMBOL", "BTC-USDT")

# Kafka Topics
TOPIC_TRADES = "binance-trades-spot"
TOPIC_ORDERBOOK = "binance-orderbook-spot"

# Hauptfunktion
async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER
    )
    await producer.start()
    # FeedHandler initialisieren
    fh = FeedHandler()

    try:
        async def handle_trade(trade, receipt_timestamp, **kwargs):
            print(f"Trade:")
            message = {
                "price": float(trade.price),
                "amount": float(trade.amount),
                "side": trade.side,
                "timestamp": int(receipt_timestamp * 1000)
            }
            await produce_trade(producer, message, topic=TOPIC_TRADES, symbol=trade.symbol, exchange="binance")

        async def handle_order_book(book, receipt_timestamp):
            print(f"Orderbook: {book}")
            snapshot = {
                "bids": [(float(price), float(book.book.bids[price])) for price in book.book.bids],
                "asks": [(float(price), float(book.book.asks[price])) for price in book.book.asks]
            }
            await produce_order_book(producer, snapshot, topic=TOPIC_ORDERBOOK, symbol=book.symbol, exchange="binance", level="l2")

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
