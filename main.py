"""
main.py - FastAPI service for the VoD training platform.

Endpoints
---------
GET  /healthz                         Liveness/readiness (Mongo must be up; Redis is optional).
GET  /api/courses                     Keyset-paginated catalog of published courses.
GET  /api/courses/{course_id}         One course with its lesson outline.
GET  /api/search?q=                   Weighted full-text search (title > tags > description).
GET  /api/assets/{item_id}/manifest   Auth-protected: origin nodes with signed, expiring URLs.
POST /internal/cache/invalidate       Admin-only: purge cached keys by prefix.

Auth model
----------
* Callers present a JWT (``Authorization: Bearer ...``) issued by your identity provider.
  ``sub`` is the user id; an optional ``roles`` claim may contain "admin".
* Asset manifests require the user to be enrolled in the asset's course (or be an admin).
* Each origin URL is signed with an HMAC over (asset path prefix | expiry). Your CDN/origin
  edge verifies it with the same ASSET_SIGNING_KEY (see ``verify_signature`` below), so the
  HLS manifest AND every segment under that prefix are protected, and links expire.

Required environment
--------------------
JWT_KEY, ASSET_SIGNING_KEY            Secrets (the app refuses to start without them).
Optional: JWT_ALGORITHMS, JWT_AUDIENCE, JWT_ISSUER, ASSET_URL_TTL_SECONDS, CORS_ORIGINS,
          MONGO_URI, MONGO_DB, REDIS_URL  (see database.py / redis_cache.py)

Run:  uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import jwt
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pymongo.errors import PyMongoError

from database import decode_cursor, mongo
from admin import build_admin_router
from origin_security import signature
from redis_cache import cache

try:  # faster JSON serialisation when orjson is installed
    import orjson  # noqa: F401
    from fastapi.responses import ORJSONResponse as DefaultResponse
except ImportError:  # pragma: no cover
    DefaultResponse = JSONResponse

log = logging.getLogger("vod.api")


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #
class Settings:
    def __init__(self) -> None:
        self.jwt_key = os.getenv("JWT_KEY", "")
        self.jwt_algorithms = [a.strip() for a in os.getenv("JWT_ALGORITHMS", "HS256").split(",")]
        # If your tokens carry an "aud" claim you MUST set JWT_AUDIENCE, or PyJWT rejects them.
        self.jwt_audience = os.getenv("JWT_AUDIENCE") or None
        self.jwt_issuer = os.getenv("JWT_ISSUER") or None
        self.signing_key = os.getenv("ASSET_SIGNING_KEY", "")
        self.signed_url_ttl = int(os.getenv("ASSET_URL_TTL_SECONDS", str(6 * 3600)))
        self.cors_origins = [
            o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",") if o.strip()
        ]

    def validate(self) -> None:
        if not self.jwt_key or not self.signing_key:
            raise RuntimeError("JWT_KEY and ASSET_SIGNING_KEY must be set")


settings = Settings()

PUBLIC_CACHE_HEADER = "public, max-age=30, stale-while-revalidate=120"
TAG_PATTERN = re.compile(r"^[\w\- ]{1,50}$")


# --------------------------------------------------------------------------- #
# App lifecycle
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.validate()
    await mongo.connect()      # fail fast: no point serving without the database
    await cache.connect()      # degrades gracefully if Redis is down
    try:
        yield
    finally:
        await cache.close()
        await mongo.close()


app = FastAPI(title="VoD Training Platform API", lifespan=lifespan, default_response_class=DefaultResponse)
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
# Authentication & entitlement
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


async def is_entitled(user: dict, course_id: str) -> bool:
    if "admin" in user["roles"]:
        return True
    # Short cache: enrollment checks run on every playback start.
    return await cache.get_or_set(
        cache.key("enrolled", user["id"], course_id),
        lambda: mongo.users.is_enrolled(user["id"], course_id),
        fresh_ttl=30,
        stale_ttl=0,
    )


# --------------------------------------------------------------------------- #
# Signed URLs (verified at your CDN / origin edge)
# --------------------------------------------------------------------------- #
def _signature(path_prefix: str, expires: int) -> str:
    return signature(settings.signing_key, path_prefix, expires)  # shared with health_check.py


def sign_prefix(path_prefix: str, expires: int) -> str:
    """Signature covering every object under ``path_prefix`` until ``expires`` (epoch seconds)."""
    return _signature(path_prefix, expires)


def verify_signature(request_path: str, path_prefix: str, expires: int, sig: str) -> bool:
    """
    Reference check for the edge (reimplement in nginx/Lua, Cloudflare Worker, etc.):
    the request must sit under the signed prefix, be unexpired, and carry a matching HMAC.
    """
    return (
        request_path.startswith(path_prefix)
        and expires >= time.time()
        and hmac.compare_digest(sig, _signature(path_prefix, expires))
    )


# --------------------------------------------------------------------------- #
# Cached accessors
# --------------------------------------------------------------------------- #
async def cached_course(course_id: str) -> Optional[dict]:
    return await cache.get_or_set(
        cache.key("course", course_id),
        lambda: mongo.courses.get(course_id),
        fresh_ttl=60,
        stale_ttl=600,
    )


async def cached_asset(item_id: str) -> Optional[dict]:
    # Short TTL so origin health changes propagate within ~30s.
    return await cache.get_or_set(
        cache.key("asset", item_id),
        lambda: mongo.assets.get(item_id),
        fresh_ttl=30,
        stale_ttl=120,
    )


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/healthz")
async def healthz():
    db_ok = await mongo.ping()
    return JSONResponse(
        {"mongo": db_ok, "cache": await cache.health()},
        status_code=200 if db_ok else 503,  # Redis being down must not fail readiness
    )


@app.get("/api/courses")
async def list_courses(
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None, max_length=512),
    tag: Optional[str] = Query(None),
):
    if tag is not None and not TAG_PATTERN.match(tag):
        raise HTTPException(400, "Invalid tag")
    if cursor:
        try:
            decode_cursor(cursor)  # reject junk before it can become a cache key
        except ValueError:
            raise HTTPException(400, "Invalid cursor")

    page = await cache.get_or_set(
        cache.key("catalog", tag or "-", limit, cursor or "first"),
        lambda: mongo.courses.list_published(limit, cursor, tag),
        fresh_ttl=30,
        stale_ttl=300,
    )
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    return page


@app.get("/api/courses/{course_id}")
async def get_course(response: Response, course_id: str = Path(..., min_length=1, max_length=64)):
    course = await cached_course(course_id)
    if not course or course.get("status") != "published":
        raise HTTPException(404, "Course not found")
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    return course


@app.get("/api/search")
async def search_courses(
    response: Response,
    q: str = Query(..., min_length=2, max_length=100),
    limit: int = Query(20, ge=1, le=50),
):
    normalized = " ".join(q.lower().split())
    items = await cache.get_or_set(
        cache.key("search", limit, normalized),
        lambda: mongo.courses.search(normalized, limit),
        fresh_ttl=30,
        stale_ttl=120,
    )
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    return {"query": q, "items": items}


@app.get("/api/assets/{item_id}/manifest")
async def asset_manifest(
    response: Response,
    item_id: str = Path(..., min_length=1, max_length=64),
    user: dict = Depends(current_user),
):
    """
    Returns the asset's origin nodes in failover order (healthy first, then priority),
    each with a signed manifest URL plus the bare ``query`` the player appends to
    segment/playlist requests on that origin.
    """
    asset = await cached_asset(item_id)
    if not asset:
        raise HTTPException(404, "Asset not found")

    course = await cached_course(asset["course_id"])
    if not course or course.get("status") != "published":
        raise HTTPException(404, "Asset not found")

    if not await is_entitled(user, asset["course_id"]):
        raise HTTPException(403, "You are not enrolled in this course")

    expires = int(time.time()) + settings.signed_url_ttl
    origins = []
    for o in asset.get("origins", []):
        base = o["base_url"].rstrip("/") + "/"
        prefix = urlparse(base).path                      # signature covers everything below it
        query = f"exp={expires}&sig={sign_prefix(prefix, expires)}"
        origins.append(
            {
                "name": o["name"],
                "priority": o.get("priority", 99),
                "healthy": o.get("healthy", True),
                "url": f"{base}{o['manifest_path'].lstrip('/')}?{query}",
                "query": query,
            }
        )
    if not origins:
        raise HTTPException(503, "No origin nodes configured for this asset")

    response.headers["Cache-Control"] = "private, no-store"   # per-user, signed, expiring
    return {
        "item_id": asset["id"],
        "course_id": asset["course_id"],
        "title": asset.get("title", ""),
        "duration_s": asset.get("duration_s"),
        "expires_at": datetime.fromtimestamp(expires, timezone.utc).isoformat(),
        "origins": origins,
    }


@app.post("/internal/cache/invalidate")
async def invalidate_cache(
    prefix: str = Query(..., min_length=3, max_length=100),
    _: dict = Depends(require_admin),
):
    """Purge cached keys, e.g. ?prefix=catalog: after publishing a course."""
    return {"removed": await cache.invalidate_prefix(prefix)}


# Admin API (courses, asset manifests, enrollments) - every route requires the admin role.
app.include_router(build_admin_router(require_admin))
