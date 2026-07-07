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
    # retention archival — when enabled, an expiring index is exported to object storage
    # (S3/MinIO) as gzipped JSONL before it is deleted, instead of delete-only.
    archive_enabled: bool = field(default_factory=lambda: os.getenv("ARCHIVE_ENABLED", "false").lower() == "true")
    archive_bucket: str = field(default_factory=lambda: os.getenv("ARCHIVE_BUCKET", "log-archive"))
    s3_endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", "minioadmin"))


@lru_cache
def get_settings() -> Settings:
    return Settings()
