from aiokafka import AIOKafkaConsumer
import asyncio
import json

async def consume_trades(topic, bootstrap_servers="localhost:9092"):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"{msg.value}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_trades("binance-trades-spot"))
