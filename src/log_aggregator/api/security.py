"""Multi-tenant auth: resolve a request to a tenant from an API key or a short-lived JWT.

When `auth_enabled` is false the whole system runs single-tenant as "default" with open APIs
(local dev / loadgen / offline tests). When enabled, every ingest/query request must carry a
bearer credential — either a configured API key or a JWT minted from one via /auth/token.
"""

from __future__ import annotations

import re
import time

import jwt
from fastapi import Header, HTTPException

from log_aggregator.config import Settings

DEFAULT_TENANT = "default"

# Tenant flows straight into the OpenSearch index pattern `logs-<tenant>-*`; restrict it so a
# crafted value can't inject `*` / `,` (multi-index) and reach other tenants' indices.
_TENANT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

# Secrets that ship in this repo (code + k8s placeholder). Refusing them means an
# AUTH_ENABLED=true deployment that forgot to set a real JWT_SECRET fails closed.
_WEAK_JWT_SECRETS = {
    "",
    "dev-only-jwt-secret-change-me-in-production",
    "change-me-to-a-32-plus-byte-random-secret",
}


def validate_auth_config(settings: Settings) -> None:
    """Fail closed at boot: if auth is on, JWT_SECRET must be a real 32+ byte value, never a
    default/placeholder — otherwise anyone who read the repo could forge tokens."""
    if not settings.auth_enabled:
        return
    secret = settings.jwt_secret
    if secret in _WEAK_JWT_SECRETS or len(secret.encode()) < 32:
        raise RuntimeError(
            "AUTH_ENABLED=true requires JWT_SECRET set to a strong, non-default value "
            "(>= 32 bytes). Refusing to start with a placeholder/weak secret."
        )


def parse_api_keys(raw: str) -> dict[str, str]:
    """"k1:acme,k2:globex" -> {"k1": "acme", "k2": "globex"}."""
    out: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            key, tenant = pair.split(":", 1)
            if key.strip() and tenant.strip():
                out[key.strip()] = tenant.strip()
    return out


def mint_jwt(tenant: str, secret: str, ttl_s: int) -> str:
    now = int(time.time())
    return jwt.encode({"sub": tenant, "iat": now, "exp": now + ttl_s}, secret, algorithm="HS256")


def verify_jwt(token: str, secret: str) -> str | None:
    try:
        return jwt.decode(token, secret, algorithms=["HS256"]).get("sub")
    except Exception:
        return None


def _bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def make_require_tenant(settings: Settings):
    """Build a FastAPI dependency that resolves the caller's tenant (or 401)."""

    def require_tenant(authorization: str | None = Header(default=None)) -> str:
        if not settings.auth_enabled:
            return DEFAULT_TENANT
        token = _bearer(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="missing bearer credential")
        tenant = parse_api_keys(settings.api_keys).get(token) or verify_jwt(token, settings.jwt_secret)
        if not tenant or not _TENANT_RE.match(tenant):
            raise HTTPException(status_code=401, detail="invalid credential")
        return tenant

    return require_tenant
