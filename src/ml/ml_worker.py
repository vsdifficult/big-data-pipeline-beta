from __future__ import annotations

import json
import os
from typing import List

import numpy as np
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI
from pydantic import BaseModel

from src.common.schemas import MlResult


MODEL_VERSION = os.environ.get("MODEL_VERSION", "v1.0.0")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "64"))

app = FastAPI(title="ml-worker", version=MODEL_VERSION)


class InferenceItem(BaseModel):
    comment_id: int
    text: str


class InferenceBatch(BaseModel):
    items: List[InferenceItem]


def score_sentiment(text: str) -> float:
    positive_words = {"good", "great", "love", "excellent", "happy"}
    tokens = set(text.lower().split())
    return min(1.0, len(tokens.intersection(positive_words)) / 3.0)


def score_toxicity(text: str) -> float:
    toxic_words = {"hate", "stupid", "idiot", "worst"}
    tokens = set(text.lower().split())
    return min(1.0, len(tokens.intersection(toxic_words)) / 2.0)


def embedding(text: str) -> list[float]:
    vec = np.zeros((EMBED_DIM,), dtype=np.float32)
    for i, b in enumerate(text.encode("utf-8")):
        vec[i % EMBED_DIM] += (b % 13) / 13.0
    norm = np.linalg.norm(vec) + 1e-6
    return (vec / norm).tolist()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.post("/predict")
def predict(batch: InferenceBatch) -> list[dict]:
    output = []
    for item in batch.items:
        result = MlResult(
            comment_id=item.comment_id,
            sentiment=score_sentiment(item.text),
            toxicity=score_toxicity(item.text),
            embedding=embedding(item.text),
            processed_at=MlResult.now_ts(),
            model_version=MODEL_VERSION,
        )
        output.append(result.to_dict())
    return output


def run_kafka_worker() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "group.id": os.environ.get("ML_CONSUMER_GROUP", "ml-workers"),
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1048576,
        }
    )
    consumer.subscribe([os.environ.get("ML_TASKS_TOPIC", "comments.ml_tasks")])

    producer = Producer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
        }
    )

    batch_size = int(os.environ.get("INFER_BATCH_SIZE", "256"))

    while True:
        records = []
        msgs = []
        for _ in range(batch_size):
            msg = consumer.poll(0.2)
            if msg is None:
                break
            if msg.error():
                continue
            payload = json.loads(msg.value().decode("utf-8"))
            records.append(InferenceItem(comment_id=payload["comment_id"], text=payload["text"]))
            msgs.append(msg)

        if not records:
            continue

        predictions = predict(InferenceBatch(items=records))
        for item in predictions:
            producer.produce(
                os.environ.get("ML_RESULTS_TOPIC", "comments.ml_results"),
                key=str(item["comment_id"]),
                value=json.dumps(item).encode("utf-8"),
            )
        producer.flush()
        for msg in msgs:
            consumer.commit(msg)


if __name__ == "__main__":
    run_kafka_worker()
