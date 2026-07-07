from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone


def doc_id(event: dict) -> str:
    """Deterministic id from event content so a redelivered event overwrites its prior copy
    (effectively-once) instead of creating a duplicate. The timestamp is set once at ingest
    and carried through Kafka, so a redelivered event hashes to the same id."""
    payload = json.dumps(
        [event.get("tenant"), event.get("timestamp"), event.get("service"),
         event.get("level"), event.get("message"), event.get("attrs")],
        sort_keys=True, default=str,
    )
    return hashlib.sha1(payload.encode()).hexdigest()


def parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
