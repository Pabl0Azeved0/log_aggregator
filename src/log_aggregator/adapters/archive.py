from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ArchiveConfig:
    """Where expiring indices are exported before deletion (S3-compatible object store)."""

    enabled: bool
    bucket: str
    endpoint: str
    access_key: str
    secret_key: str


class S3Archive:
    """boto3-backed S3/MinIO object store for retention archival. The client is created
    lazily (and the bucket ensured on first write) so importing needs no boto3/network."""

    def __init__(self, cfg: ArchiveConfig) -> None:
        self._cfg = cfg
        self._client = None

    def _s3(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self._cfg.endpoint,
                aws_access_key_id=self._cfg.access_key,
                aws_secret_access_key=self._cfg.secret_key,
                region_name="us-east-1",
            )
        return self._client

    def put(self, key: str, data: bytes) -> None:
        s3 = self._s3()
        try:
            s3.head_bucket(Bucket=self._cfg.bucket)
        except Exception:
            s3.create_bucket(Bucket=self._cfg.bucket)
        s3.put_object(Bucket=self._cfg.bucket, Key=key, Body=data)

    def fetch(self, key: str) -> bytes:
        return self._s3().get_object(Bucket=self._cfg.bucket, Key=key)["Body"].read()
