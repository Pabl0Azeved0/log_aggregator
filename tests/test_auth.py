"""Multi-tenant auth: credential enforcement, tenant isolation, and the api-key→JWT flow."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from log_aggregator.workers import indexer
from log_aggregator.adapters.memory_buffer import MemoryBuffer
from log_aggregator.config import Settings
from log_aggregator.api.ingest import create_app as create_ingest_app
from log_aggregator.api.query import create_app as create_query_app
from log_aggregator.api.security import mint_jwt, parse_api_keys, verify_jwt
from log_aggregator.adapters.memory_store import MemoryStore


def _auth_settings():
    return Settings(auth_enabled=True, api_keys="acmekey:acme,globexkey:globex",
                    jwt_secret="test-secret-at-least-thirty-two-bytes-long", jwt_ttl_s=3600, batch_timeout_s=0.05)


def test_api_key_parsing_and_jwt_roundtrip():
    assert parse_api_keys("a:1, b:2 ,junk,:x,y:") == {"a": "1", "b": "2"}
    secret = "unit-test-secret-at-least-thirty-two-bytes"
    token = mint_jwt("acme", secret, 60)
    assert verify_jwt(token, secret) == "acme"
    assert verify_jwt(token, "another-secret-of-thirty-two-plus-bytes!!") is None
    assert verify_jwt("not-a-token", secret) is None


def test_boot_fails_closed_on_weak_jwt_secret():
    for weak in ("dev-only-jwt-secret-change-me-in-production",
                 "change-me-to-a-32-plus-byte-random-secret", "", "short"):
        s = Settings(auth_enabled=True, api_keys="k:acme", jwt_secret=weak)
        with pytest.raises(RuntimeError):
            create_query_app(store=MemoryStore(), settings=s)
        with pytest.raises(RuntimeError):
            create_ingest_app(buffer=MemoryBuffer(maxsize=1), settings=s)
    # a real 32+ byte secret boots fine; auth-off never checks
    create_query_app(store=MemoryStore(), settings=Settings(auth_enabled=True, api_keys="k:acme", jwt_secret="x" * 32))
    create_query_app(store=MemoryStore(), settings=Settings(auth_enabled=False))


def test_search_requires_credential_when_auth_enabled():
    query = TestClient(create_query_app(store=MemoryStore(), settings=_auth_settings()))
    assert query.get("/search").status_code == 401
    assert query.get("/search", headers={"Authorization": "Bearer nope"}).status_code == 401


def test_wildcard_tenant_credential_rejected():
    # an api key that maps to a wildcard tenant must not resolve (would widen logs-*-*)
    s = Settings(auth_enabled=True, api_keys="wild:*", jwt_secret="test-secret-at-least-thirty-two-bytes-long")
    query = TestClient(create_query_app(store=MemoryStore(), settings=s))
    assert query.get("/search", headers={"Authorization": "Bearer wild"}).status_code == 401


def test_dashboard_sends_security_headers():
    query = TestClient(create_query_app(store=MemoryStore()))  # auth off → dashboard open
    r = query.get("/")
    assert r.status_code == 200
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]


def test_disabled_auth_is_open():
    query = TestClient(create_query_app(store=MemoryStore(), settings=Settings(auth_enabled=False)))
    assert query.get("/search").status_code == 200


def test_tenant_isolation_end_to_end():
    settings = _auth_settings()
    buf = MemoryBuffer(maxsize=1000)
    store = MemoryStore()
    ingest = TestClient(create_ingest_app(buffer=buf, settings=settings))
    query = TestClient(create_query_app(store=store, settings=settings))

    acme = {"Authorization": "Bearer acmekey"}
    globex = {"Authorization": "Bearer globexkey"}
    assert ingest.post("/logs", json={"service": "a", "message": "acme-secret"}, headers=acme).status_code == 202
    assert ingest.post("/logs", json={"service": "b", "message": "globex-secret"}, headers=globex).status_code == 202

    asyncio.run(indexer.run(buf, store, settings, once=True))

    assert [e["message"] for e in query.get("/search", headers=acme).json()] == ["acme-secret"]
    assert [e["message"] for e in query.get("/search", headers=globex).json()] == ["globex-secret"]
    assert query.get("/stats", headers=acme).json()["total"] == 1


def test_auth_token_is_rate_limited():
    query = TestClient(create_query_app(store=MemoryStore(), settings=_auth_settings()))
    codes = [query.post("/auth/token", headers={"X-API-Key": "acmekey"}).status_code for _ in range(15)]
    assert codes[0] == 200 and 429 in codes  # first succeed, then throttled per IP


def test_api_key_exchanged_for_jwt():
    settings = _auth_settings()
    query = TestClient(create_query_app(store=MemoryStore(), settings=settings))

    bad = query.post("/auth/token", headers={"X-API-Key": "nope"})
    assert bad.status_code == 401

    good = query.post("/auth/token", headers={"X-API-Key": "acmekey"})
    assert good.status_code == 200 and good.json()["tenant"] == "acme"
    token = good.json()["token"]
    assert query.get("/search", headers={"Authorization": f"Bearer {token}"}).status_code == 200
