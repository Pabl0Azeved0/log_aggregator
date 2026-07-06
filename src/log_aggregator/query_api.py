from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from log_aggregator.config import get_settings
from log_aggregator.store import Store, make_store

_DASHBOARD = Path(__file__).parent / "static" / "dashboard.html"


def create_app(store: Store | None = None) -> FastAPI:
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

    @app.get("/search")
    async def search(q: str = "", level: str = "", service: str = "", limit: int = 100):
        return await _store().search(q=q, level=level, service=service, limit=min(limit, 1000))

    @app.get("/stats")
    async def stats():
        return await _store().stats()

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/")
    async def dashboard():
        return FileResponse(_DASHBOARD)

    return app


app = create_app()
