from __future__ import annotations

import json
import os

from confluent_kafka import Consumer, KafkaError


def build_consumer(topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "group.id": os.environ.get("CONSUMER_GROUP", "comments-monitor"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def run(topic: str = "comments.cleaned") -> None:
    consumer = build_consumer(topic)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))
            print(
                f"comment_id={payload['comment_id']} "
                f"language={payload.get('language', 'und')} "
                f"text_len={len(payload.get('text', ''))}"
            )
            consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    run(os.environ.get("TOPIC", "comments.cleaned"))
