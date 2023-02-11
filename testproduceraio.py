import asyncio
from kafkaproxy import ProducerProxy, KafkaType, AioProducerProtocol
from typing import cast

kafkap = ProducerProxy()


async def run() -> None:
    p = cast(AioProducerProtocol, kafkap)
    async with p.mount() as cli:
        for i in range(10):
            await cli.publish("topic1", f"send {i}")
            print("send ok")
            await asyncio.sleep(0.1)

# kafkac.initialize_from_addresses("localhost:9094", "topic1", group_id="test2", kafka_type=KafkaType.Kafka)


async def main() -> None:
    kafkap.initialize_from_addresses("localhost:9094", kafka_type=KafkaType.AioKafka, acks="all")
    await run()


try:
    print("start watching")
    asyncio.run(main())
finally:
    print("stoped")
