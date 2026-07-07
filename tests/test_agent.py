"""Shipper agent: the batch ship must never drop on backpressure (429) or transport error —
it retries the same batch with backoff until the ingest API accepts it."""

from __future__ import annotations

from types import SimpleNamespace

from log_aggregator.workers import agent


class _FakeClient:
    """Returns a scripted sequence of status codes; raises on a `None` entry (transport error)."""

    def __init__(self, script):
        self._script = list(script)
        self.bodies: list[str] = []

    def post(self, url, params=None, content=None, headers=None):
        self.bodies.append(content)
        code = self._script.pop(0)
        if code is None:
            raise ConnectionError("ingest unreachable")
        return SimpleNamespace(status_code=code)


def test_ship_retries_through_429_and_errors_then_succeeds():
    client = _FakeClient([429, None, 500, 202])  # backpressure, outage, error, then accepted
    agent.ship(client, "http://ingest:8000", "demo", ["a", "b"], {}, sleep=lambda _s: None)

    assert len(client.bodies) == 4          # retried until accepted — nothing dropped
    assert client.bodies[-1] == "a\nb"      # same batch each time, correct payload


def test_ship_returns_immediately_on_202():
    client = _FakeClient([202])
    agent.ship(client, "http://ingest:8000", "demo", ["only"], {}, sleep=lambda _s: None)
    assert client.bodies == ["only"]
