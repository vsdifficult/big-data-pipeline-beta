from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import psycopg2
from confluent_kafka import Producer

from src.common.schemas import CommentEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cdc_poller")


@dataclass
class Cursor:
    last_created_at: datetime
    last_id: int


class CursorStore:
    """Simple file-based cursor store. Replace with Redis/S3/etcd in production."""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> Cursor:
        if not os.path.exists(self.path):
            return Cursor(last_created_at=datetime(1970, 1, 1), last_id=0)
        with open(self.path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return Cursor(
            last_created_at=datetime.fromisoformat(data["last_created_at"]),
            last_id=int(data["last_id"]),
        )

    def save(self, cursor: Cursor) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_created_at": cursor.last_created_at.isoformat(),
                    "last_id": cursor.last_id,
                },
                f,
            )


class IncrementalCommentExtractor:
    def __init__(self, pg_dsn: str, kafka_bootstrap: str, topic: str, cursor_path: str) -> None:
        self.pg_dsn = pg_dsn
        self.topic = topic
        self.cursor_store = CursorStore(cursor_path)
        self.producer = Producer(
            {
                "bootstrap.servers": kafka_bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 20,
                "batch.size": 512000,
            }
        )

    def fetch_batch(self, cursor: Cursor, batch_size: int) -> Iterable[CommentEvent]:
        query = """
            SELECT id, user_id, text, created_at, language, metadata
            FROM comments
            WHERE (created_at > %s)
               OR (created_at = %s AND id > %s)
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """
        with psycopg2.connect(self.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (cursor.last_created_at, cursor.last_created_at, cursor.last_id, batch_size))
                for row in cur.fetchall():
                    yield CommentEvent(
                        comment_id=row[0],
                        user_id=row[1],
                        text=row[2] or "",
                        created_at=row[3].isoformat(),
                        language=row[4] or "und",
                        metadata=row[5] or {},
                    )

    def produce_events(self, events: list[CommentEvent]) -> None:
        for event in events:
            self.producer.produce(
                self.topic,
                key=str(event.comment_id),
                value=json.dumps(event.to_dict()).encode("utf-8"),
            )
        self.producer.flush()

    def run_forever(self, batch_size: int = 10000, sleep_seconds: float = 1.0) -> None:
        while True:
            cursor = self.cursor_store.load()
            events = list(self.fetch_batch(cursor, batch_size))
            if not events:
                time.sleep(sleep_seconds)
                continue

            self.produce_events(events)
            last = events[-1]
            updated_cursor = Cursor(
                last_created_at=datetime.fromisoformat(last.created_at),
                last_id=last.comment_id,
            )
            self.cursor_store.save(updated_cursor)
            logger.info("published=%d last_id=%d", len(events), last.comment_id)


if __name__ == "__main__":
    extractor = IncrementalCommentExtractor(
        pg_dsn=os.environ["PG_DSN"],
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        topic=os.environ.get("RAW_TOPIC", "comments.raw"),
        cursor_path=os.environ.get("CURSOR_PATH", "/tmp/comment_cursor.json"),
    )
    extractor.run_forever(
        batch_size=int(os.environ.get("BATCH_SIZE", "10000")),
        sleep_seconds=float(os.environ.get("POLL_INTERVAL", "1.0")),
    )
