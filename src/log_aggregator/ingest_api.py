from __future__ import annotations

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from log_aggregator.buffer import Buffer, BufferFull, make_buffer
from log_aggregator.config import get_settings
from log_aggregator.models import LogEvent, parse_line

INGESTED = Counter("ingested_events_total", "Events accepted into the buffer")
REJECTED = Counter("rejected_events_total", "Events rejected by backpressure")


def create_app(buffer: Buffer | None = None) -> FastAPI:
    app = FastAPI(title="log-aggregator ingest")
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
    async def ingest(payload: LogEvent | list[LogEvent]):
        events = [payload] if isinstance(payload, LogEvent) else payload
        return await _accept([e.model_dump(mode="json") for e in events])

    @app.post("/logs/raw", status_code=202)
    async def ingest_raw(request: Request, service: str = Query(default="unknown")):
        body = (await request.body()).decode(errors="replace")
        events = [parse_line(line, service).model_dump(mode="json") for line in body.splitlines() if line.strip()]
        if not events:
            return {"accepted": 0}
        return await _accept(events)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> Response:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
