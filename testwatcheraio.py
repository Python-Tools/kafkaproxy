import asyncio
from kafkaproxy import ConsumerProxy, KafkaAutoOffsetReset, KafkaType, AioConsumerProtocol
from typing import cast

kafkac = ConsumerProxy()


async def run() -> None:
    c = cast(AioConsumerProtocol, kafkac)
    async with c.watch() as g:
        async for record in g:
            print("*****")
            print(record.value)


async def main() -> None:
    kafkac.initialize_from_addresses("localhost:9094", "topic1", group_id="test2", kafka_type=KafkaType.AioKafka, auto_offset_reset=KafkaAutoOffsetReset.earliest)
    await run()


try:
    print("start watching")
    asyncio.run(main())
finally:
    print("stoped")
