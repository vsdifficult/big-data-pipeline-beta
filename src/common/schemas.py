from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass(frozen=True)
class CommentEvent:
    comment_id: int
    user_id: int
    text: str
    language: str
    created_at: str
    metadata: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["metadata"] = payload.get("metadata") or {}
        return payload


@dataclass(frozen=True)
class MlResult:
    comment_id: int
    sentiment: float
    toxicity: float
    embedding: list[float]
    processed_at: str
    model_version: str

    @staticmethod
    def now_ts() -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
