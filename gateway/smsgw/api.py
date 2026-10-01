"""REST API: accepts messages, queues them, reports status."""
import json
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from . import config as config_loader
from . import encoding, metrics
from .routing import Router
from .store import Store


class SendRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    to: str = Field(pattern=r"^\+?[1-9]\d{7,14}$", description="Recipient in E.164 format")
    sender: str = Field(alias="from", pattern=r"^([A-Za-z0-9 ]{1,11}|\+?\d{3,15})$",
                        description="Alphanumeric sender ID (max 11) or number")
    text: str = Field(min_length=1, max_length=1600)


def create_app(store: Store = None, router: Router = None, api_keys=None) -> FastAPI:
    if api_keys is None:
        api_keys = {k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()}

    @asynccontextmanager
    async def lifespan(app):
        if app.state.store is None:
            import redis.asyncio as redis

            app.state.store = Store(redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0")))
        if app.state.router is None:
            app.state.config = config_loader.load()
            app.state.router = Router(app.state.config)
        await app.state.store.ensure_group()
        yield

    app = FastAPI(title="SMS Gateway", version="1.0.0", lifespan=lifespan)
    app.state.store, app.state.router = store, router

    def authenticate(x_api_key: str = Header(default=None)):
        if api_keys and x_api_key not in api_keys:
            raise HTTPException(401, "invalid or missing X-API-Key")

    @app.post("/api/v1/messages", status_code=202, dependencies=[Depends(authenticate)])
    async def send(req: SendRequest):
        route = app.state.router.match(req.to)
        if route is None:
            metrics.API_MESSAGES.labels("no_route").inc()
            raise HTTPException(422, f"no route for {req.to}")
        info = encoding.analyse(req.text)
        message = await app.state.store.enqueue(req.to, req.sender, req.text, info["encoding"],
                                                info["segments"], route.name)
        metrics.API_MESSAGES.labels("accepted").inc()
        return {k: message[k] for k in ("id", "status", "route", "encoding", "segments")}

    @app.get("/api/v1/messages/{message_id}", dependencies=[Depends(authenticate)])
    async def status(message_id: str):
        message = await app.state.store.get(message_id)
        if message is None:
            raise HTTPException(404, "message not found")
        return message

    @app.get("/api/v1/routes", dependencies=[Depends(authenticate)])
    async def routes():
        states = {k: json.loads(v) for k, v in (await app.state.store.carrier_states()).items()}
        grouped = {}
        for r in app.state.router.routes:
            entry = grouped.setdefault(r.name, {
                "name": r.name,
                "prefixes": [],
                "carriers": [{"carrier": o.carrier, "cost": o.cost, **states.get(o.carrier, {})}
                             for o in r.options],
            })
            entry["prefixes"].append(r.prefix)
        return list(grouped.values())

    @app.get("/health")
    async def health():
        try:
            await app.state.store.redis.ping()
        except Exception as exc:
            raise HTTPException(503, f"redis unavailable: {exc}") from exc
        return {"status": "ok"}

    @app.get("/metrics")
    def prometheus_metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
