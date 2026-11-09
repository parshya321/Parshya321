"""
main.py - FastAPI service for the Anime VoD Platform.

Endpoints
---------
GET  /healthz                         Liveness/readiness (Mongo must be up; Redis is optional).
GET  /api/anime/catalog               Catalog of scraped anime titles and thumbnails.
GET  /api/anime/resolve               Resolves live HLS (.m3u8) streaming link from ravok.buzz with Redis caching.
POST /api/anime/sync-catalog          Admin endpoint: Triggers scraping to sync catalog into MongoDB.
POST /internal/cache/invalidate       Admin-only: purge cached keys by prefix.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pymongo.errors import PyMongoError

from database import mongo
from redis_cache import cache
from scraper import fetch_anime_catalog, resolve_streaming_link

try:
    import orjson
    from fastapi.responses import ORJSONResponse as DefaultResponse
except ImportError:
    DefaultResponse = JSONResponse

log = logging.getLogger("vod.api")


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
class Settings:
    def __init__(self) -> None:
        self.jwt_key = os.getenv("JWT_KEY", "")
        self.jwt_algorithms = [a.strip() for a in os.getenv("JWT_ALGORITHMS", "HS256").split(",")]
        self.jwt_audience = os.getenv("JWT_AUDIENCE") or None
        self.jwt_issuer = os.getenv("JWT_ISSUER") or None
        self.cors_origins = [
            o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",") if o.strip()
        ]

    def validate(self) -> None:
        if not self.jwt_key:
            raise RuntimeError("JWT_KEY must be set")


settings = Settings()

PUBLIC_CACHE_HEADER = "public, max-age=30, stale-while-revalidate=120"


# --------------------------------------------------------------------------- #
# App lifecycle
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.validate()
    await mongo.connect()      # Fail fast: database is mandatory
    await cache.connect()      # Redis cache for ephemeral stream links
    try:
        yield
    finally:
        await cache.close()
        await mongo.close()


app = FastAPI(title="Anime VoD Streaming Platform API", lifespan=lifespan, default_response_class=DefaultResponse)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    max_age=600,
)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    log.error("Database error: %r", exc)
    return JSONResponse({"detail": "Service temporarily unavailable"}, status_code=503)


@app.exception_handler(asyncio.TimeoutError)
async def timeout_handler(_: Request, exc: asyncio.TimeoutError):
    log.error("Upstream timeout while loading data")
    return JSONResponse({"detail": "Service temporarily unavailable"}, status_code=503)


# --------------------------------------------------------------------------- #
# Authentication & Admin Checks
# --------------------------------------------------------------------------- #
bearer = HTTPBearer(auto_error=False)


async def current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> dict:
    unauthorized = {"WWW-Authenticate": "Bearer"}
    if creds is None:
        raise HTTPException(401, "Missing bearer token", headers=unauthorized)
    try:
        payload = jwt.decode(
            creds.credentials,
            settings.jwt_key,
            algorithms=settings.jwt_algorithms,
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={"require": ["exp", "sub"]},
        )
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid or expired token", headers=unauthorized)
    return {"id": str(payload["sub"]), "roles": payload.get("roles", [])}


def require_admin(user: dict = Depends(current_user)) -> dict:
    if "admin" not in user["roles"]:
        raise HTTPException(403, "Admin role required")
    return user


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/healthz")
async def healthz():
    db_ok = await mongo.ping()
    return JSONResponse(
        {"mongo": db_ok, "cache": await cache.health()},
        status_code=200 if db_ok else 503,
    )


@app.get("/api/anime/catalog")
async def get_anime_catalog(response: Response):
    """
    Fetches the saved anime catalog from MongoDB (or triggers fallback cache).
    """
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    
    # Try fetching from cache/database first
    cached_catalog = await cache.get_or_set(
        "anime:catalog:all",
        lambda: mongo.animes.find().to_list(length=100) if hasattr(mongo, 'animes') else fetch_anime_catalog(),
        fresh_ttl=60,
        stale_ttl=300
    )
    return {"status": "success", "items": cached_catalog or []}


@app.post("/api/anime/sync-catalog")
async def sync_anime_catalog(_: dict = Depends(require_admin)):
    """
    Admin-only: Scrapes animesalt.cx and updates MongoDB with the latest anime list and thumbnails.
    """
    items = await fetch_anime_catalog()
    if not items:
        raise HTTPException(status_code=500, detail="Scraper failed to extract anime catalog.")
    
    # Save/Update into MongoDB if collection is configured
    if hasattr(mongo, 'animes'):
        for item in items:
            await mongo.animes.update_one(
                {"url": item["url"]},
                {"$set": item},
                upsert=True
            )
            
    # Invalidate cache so users get the fresh list immediately
    await cache.invalidate_prefix("anime:catalog")
    
    return {"status": "success", "synced_count": len(items)}


@app.get("/api/anime/resolve")
async def resolve_anime_stream(
    url: str = Query(..., description="Target episode page URL from animesalt.cx"),
    _: dict = Depends(current_user),
):
    """
    Resolves the live HLS (.m3u8) streaming link hosted on ravok.buzz.
    Uses Redis caching with TTL to prevent hammering the source and handle link expirations.
    """
    # 1. Check Redis cache first to avoid real-time overhead and link expiry loops
    cache_key = f"stream:hls:{hash(url)}"
    cached_stream = await cache.get(cache_key)
    
    if cached_stream:
        return {"source": "redis_cache", "stream_url": cached_stream, "type": "hls"}

    # 2. If missing or expired, resolve live via the scraper module
    stream_data = await resolve_streaming_link(url)
    stream_url = stream_data.get("stream_url")
    
    if not stream_url:
        raise HTTPException(status_code=404, detail="Could not resolve live streaming link.")

    # 3. Cache the fresh stream URL in Redis for 2 hours (7200 seconds)
    await cache.set(cache_key, stream_url, ttl=7200)

    return {"source": "live_scraper", "stream_url": stream_url, "type": stream_data.get("type", "hls")}


@app.post("/internal/cache/invalidate")
async def invalidate_cache(
    prefix: str = Query(..., min_length=3, max_length=100),
    _: dict = Depends(require_admin),
):
    """Purge cached keys, e.g. ?prefix=anime:catalog after syncing."""
    return {"removed": await cache.invalidate_prefix(prefix)}
