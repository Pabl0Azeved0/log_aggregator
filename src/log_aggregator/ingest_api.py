from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from log_aggregator.buffer import Buffer, BufferFull, make_buffer
from log_aggregator.config import Settings, get_settings
from log_aggregator.models import LogEvent, parse_line
from log_aggregator.security import make_require_tenant

INGESTED = Counter("ingested_events_total", "Events accepted into the buffer")
REJECTED = Counter("rejected_events_total", "Events rejected by backpressure")


def create_app(buffer: Buffer | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    require_tenant = make_require_tenant(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        # flush the producer's linger window on SIGTERM instead of dropping it
        if app.state.buffer is not None:
            await app.state.buffer.close()

    app = FastAPI(title="log-aggregator ingest", lifespan=lifespan)
    app.state.buffer = buffer

    def _buffer() -> Buffer:
        if app.state.buffer is None:
            app.state.buffer = make_buffer(get_settings())
        return app.state.buffer

    async def _accept(events: list[dict]):
        try:
            await _buffer().publish(events)
        except BufferFull:
            REJECTED.inc(len(events))
            return JSONResponse(
                status_code=429,
                content={"detail": "buffer at capacity — retry with backoff"},
            )
        INGESTED.inc(len(events))
        return {"accepted": len(events)}

    @app.post("/logs", status_code=202)
    async def ingest(payload: LogEvent | list[LogEvent], tenant: str = Depends(require_tenant)):
        events = [payload] if isinstance(payload, LogEvent) else payload
        dumped = [e.model_dump(mode="json") for e in events]
        for e in dumped:
            e["tenant"] = tenant  # authoritative: the caller's credential, not client-supplied
        return await _accept(dumped)

    @app.post("/logs/raw", status_code=202)
    async def ingest_raw(request: Request, service: str = Query(default="unknown"), tenant: str = Depends(require_tenant)):
        body = (await request.body()).decode(errors="replace")
        events = [parse_line(line, service).model_dump(mode="json") for line in body.splitlines() if line.strip()]
        if not events:
            return {"accepted": 0}
        for e in events:
            e["tenant"] = tenant
        return await _accept(events)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> Response:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
