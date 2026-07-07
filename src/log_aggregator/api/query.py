from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse

from log_aggregator.composition import make_store
from log_aggregator.config import Settings, get_settings
from log_aggregator.ports.store import Store
from log_aggregator.api.security import make_require_tenant, mint_jwt, parse_api_keys

_DASHBOARD = Path(__file__).parent / "static" / "dashboard.html"


def create_app(store: Store | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    require_tenant = make_require_tenant(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        if app.state.store is not None:
            await app.state.store.close()

    app = FastAPI(title="log-aggregator query", lifespan=lifespan)
    app.state.store = store

    def _store() -> Store:
        if app.state.store is None:
            app.state.store = make_store(get_settings())
        return app.state.store

    @app.post("/auth/token")
    async def auth_token(x_api_key: str = Header(...)):
        tenant = parse_api_keys(settings.api_keys).get(x_api_key)
        if not tenant:
            raise HTTPException(status_code=401, detail="invalid api key")
        return {"token": mint_jwt(tenant, settings.jwt_secret, settings.jwt_ttl_s), "tenant": tenant}

    @app.get("/search")
    async def search(tenant: str = Depends(require_tenant), q: str = "", level: str = "", service: str = "", limit: int = 100):
        return await _store().search(tenant=tenant, q=q, level=level, service=service, limit=min(limit, 1000))

    @app.get("/stats")
    async def stats(tenant: str = Depends(require_tenant)):
        return await _store().stats(tenant)

    @app.get("/alerts")
    async def alerts(tenant: str = Depends(require_tenant)):
        return await _store().recent_alerts(tenant)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/")
    async def dashboard():
        return FileResponse(_DASHBOARD)

    return app


app = create_app()
