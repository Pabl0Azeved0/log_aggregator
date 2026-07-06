from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    buffer_backend: str = field(default_factory=lambda: os.getenv("BUFFER_BACKEND", "kafka"))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "logs"))
    store_backend: str = field(default_factory=lambda: os.getenv("STORE_BACKEND", "opensearch"))
    opensearch_url: str = field(default_factory=lambda: os.getenv("OPENSEARCH_URL", "http://localhost:9200"))
    retention_days: int = field(default_factory=lambda: int(os.getenv("RETENTION_DAYS", "7")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("INDEXER_BATCH_SIZE", "500")))
    batch_timeout_s: float = field(default_factory=lambda: float(os.getenv("INDEXER_BATCH_TIMEOUT_S", "1.0")))
    memory_queue_max: int = field(default_factory=lambda: int(os.getenv("MEMORY_QUEUE_MAX", "100000")))
    dead_letter_path: str = field(default_factory=lambda: os.getenv("DEAD_LETTER_PATH", "dead_letter/failed.jsonl"))


@lru_cache
def get_settings() -> Settings:
    return Settings()
