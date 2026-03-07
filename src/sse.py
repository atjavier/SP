import json
from dataclasses import dataclass
from datetime import datetime, timezone


def now_iso8601() -> str:
    return datetime.now(timezone.utc).isoformat()


def format_sse_event(event: str, payload: dict) -> str:
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return f"event: {event}\n" f"data: {data}\n\n"


def format_sse_retry(ms: int) -> str:
    return f"retry: {int(ms)}\n\n"


def format_sse_comment(text: str) -> str:
    return f": {text}\n\n"


@dataclass(frozen=True)
class SseEnvelope:
    run_id: str
    event_at: str
    data: dict

    def to_dict(self) -> dict:
        return {"run_id": self.run_id, "event_at": self.event_at, "data": self.data}

