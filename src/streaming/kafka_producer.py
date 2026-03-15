from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.common.schemas import CommentEvent


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed for key={msg.key()}: {err}")


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": os.environ.get("KAFKA_COMPRESSION", "lz4"),
            "linger.ms": 25,
            "batch.size": 524288,
        }
    )


def run(topic: str = "comments.raw", rate_per_sec: int = 2000) -> None:
    producer = build_producer()
    i = 1
    while True:
        event = CommentEvent(
            comment_id=i,
            user_id=random.randint(1, 1_000_000),
            text=f"Synthetic comment number {i}",
            language="en",
            created_at=datetime.now(timezone.utc).isoformat(),
            metadata={"source": "load-test"},
        )
        producer.produce(
            topic,
            key=str(event.comment_id),
            value=json.dumps(event.to_dict()).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        i += 1
        time.sleep(1 / max(rate_per_sec, 1))


if __name__ == "__main__":
    run(
        topic=os.environ.get("RAW_TOPIC", "comments.raw"),
        rate_per_sec=int(os.environ.get("RATE_PER_SEC", "2000")),
    )
