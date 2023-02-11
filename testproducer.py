from kafkaproxy import ProducerProxy, KafkaType, ProducerProtocol
from typing import cast
import time
kafkap = ProducerProxy()


def run() -> None:
    p = cast(ProducerProtocol, kafkap)
    with p.mount() as cli:
        for i in range(10):
            cli.publish("topic1", f"send {i}")
            print("send ok")
            time.sleep(0.1)


# kafkap.initialize_from_addresses("localhost:9094", kafka_type=KafkaType.ConfluentKafka, acks="all")
kafkap.initialize_from_addresses("localhost:9094", kafka_type=KafkaType.Kafka, acks="all")
try:
    print("start publishing")
    run()
finally:
    print("stoped")
