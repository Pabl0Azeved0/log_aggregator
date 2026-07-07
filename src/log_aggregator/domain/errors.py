from __future__ import annotations


class BufferFull(Exception):
    """Raised when the buffer cannot accept more events (backpressure signal)."""


class PartialIndexError(Exception):
    """Some documents in a batch were rejected by the store (a per-document error, not a
    transport failure). Carries the count successfully indexed and the rejected events so
    the caller can dead-letter only the failures instead of the whole batch."""

    def __init__(self, indexed: int, failed: list[dict]) -> None:
        super().__init__(f"{len(failed)} of {indexed + len(failed)} documents rejected")
        self.indexed = indexed
        self.failed = failed
