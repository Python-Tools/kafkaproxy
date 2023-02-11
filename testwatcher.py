from kafkaproxy import ConsumerProxy, KafkaType, ConsumerProtocol
from typing import cast

kafkac = ConsumerProxy()


def run() -> None:
    c = cast(ConsumerProtocol, kafkac)
    with c.watch() as g:
        print("_______")
        for record in g:
            print("*****")
            print(record.value)


# kafkac.initialize_from_addresses("localhost:9094", "topic1", group_id="test2", kafka_type=KafkaType.Kafka)
kafkac.initialize_from_addresses("localhost:9094", "topic1", group_id="test2", kafka_type=KafkaType.ConfluentKafka)
try:
    print("start watching")
    run()
finally:
    print("stoped")
