"""
main.py - Single-file Anime VoD Platform API (No external scraper dependency)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import httpx
from bs4 import BeautifulSoup
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

try:
    import orjson
    from fastapi.responses import ORJSONResponse as DefaultResponse
except ImportError:
    DefaultResponse = JSONResponse

log = logging.getLogger("vod.api")

# --------------------------------------------------------------------------- #
# Embedded Scraper Functions (animesalt.cx & ravok.buzz resolver)
# --------------------------------------------------------------------------- #
BASE_URL = "https://animesalt.cx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://animesalt.cx/"
}

async def fetch_anime_catalog():
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15.0) as client:
        try:
            response = await client.get(BASE_URL)
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            anime_list = []
            
            for item in soup.select('.anime-card, .item, .film-poster'):
                title_elem = item.find(['a', 'h3', 'div'], class_=re.compile('title|name', re.I))
                img_elem = item.find('img')
                link_elem = item.find('a', href=True)
                
                if title_elem and link_elem:
                    title = title_elem.get_text(strip=True)
                    url = link_elem['href']
                    if not url.startswith('http'):
                        url = BASE_URL + url
                    
                    thumbnail = img_elem.get('data-src') or img_elem.get('src') if img_elem else ""
                    anime_list.append({"title": title, "url": url, "thumbnail": thumbnail})
            
            return anime_list
        except Exception as e:
            print(f"Error scraping catalog: {e}")
            return []

async def resolve_streaming_link(episode_url: str):
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15.0) as client:
        try:
            response = await client.get(episode_url)
            if response.status_code != 200:
                raise HTTPException(status_code=400, detail="Failed to fetch episode page.")
            
            html_content = response.text
            m3u8_pattern = r'https?://[^\s<>"]+?\.m3u8[^\s<>"]*'
            found_links = re.findall(m3u8_pattern, html_content)
            
            if found_links:
                return {"stream_url": found_links[0], "type": "hls"}
            
            soup = BeautifulSoup(html_content, 'html.parser')
            iframe = soup.find('iframe', src=True)
            if iframe:
                return {"stream_url": iframe['src'], "type": "iframe"}
            
            raise HTTPException(status_code=404, detail="Streaming source (.m3u8) could not be resolved.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------------- #
# Settings & App Setup
# --------------------------------------------------------------------------- #
class Settings:
    def __init__(self) -> None:
        self.jwt_key = os.getenv("JWT_KEY", "")
        self.jwt_algorithms = [a.strip() for a in os.getenv("JWT_ALGORITHMS", "HS256").split(",")]
        self.jwt_audience = os.getenv("JWT_AUDIENCE") or None
        self.jwt_issuer = os.getenv("JWT_ISSUER") or None
        self.cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",") if o.strip()]

    def validate(self) -> None:
        if not self.jwt_key:
            raise RuntimeError("JWT_KEY must be set")

settings = Settings()
PUBLIC_CACHE_HEADER = "public, max-age=30, stale-while-revalidate=120"


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.validate()
    await mongo.connect()
    await cache.connect()
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


bearer = HTTPBearer(auto_error=False)

async def current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> dict:
    unauthorized = {"WWW-Authenticate": "Bearer"}
    if creds is None:
        raise HTTPException(401, "Missing bearer token", headers=unauthorized)
    try:
        payload = jwt.decode(creds.credentials, settings.jwt_key, algorithms=settings.jwt_algorithms, options={"require": ["exp", "sub"]})
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
    return JSONResponse({"mongo": db_ok, "cache": await cache.health()}, status_code=200 if db_ok else 503)


@app.get("/api/anime/catalog")
async def get_anime_catalog(response: Response):
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    cached_catalog = await cache.get_or_set(
        "anime:catalog:all",
        lambda: mongo.animes.find().to_list(length=100) if hasattr(mongo, 'animes') else fetch_anime_catalog(),
        fresh_ttl=60,
        stale_ttl=300
    )
    return {"status": "success", "items": cached_catalog or []}


@app.post("/api/anime/sync-catalog")
async def sync_anime_catalog(_: dict = Depends(require_admin)):
    items = await fetch_anime_catalog()
    if not items:
        raise HTTPException(status_code=500, detail="Scraper failed to extract anime catalog.")
    
    if hasattr(mongo, 'animes'):
        for item in items:
            await mongo.animes.update_one({"url": item["url"]}, {"$set": item}, upsert=True)
            
    await cache.invalidate_prefix("anime:catalog")
    return {"status": "success", "synced_count": len(items)}


@app.get("/api/anime/resolve")
async def resolve_anime_stream(url: str = Query(..., description="Target episode page URL from animesalt.cx"), _: dict = Depends(current_user)):
    cache_key = f"stream:hls:{hash(url)}"
    cached_stream = await cache.get(cache_key)
    if cached_stream:
        return {"source": "redis_cache", "stream_url": cached_stream, "type": "hls"}

    stream_data = await resolve_streaming_link(url)
    stream_url = stream_data.get("stream_url")
    if not stream_url:
        raise HTTPException(status_code=404, detail="Could not resolve live streaming link.")

    await cache.set(cache_key, stream_url, ttl=7200)
    return {"source": "live_scraper", "stream_url": stream_url, "type": stream_data.get("type", "hls")}


@app.post("/internal/cache/invalidate")
async def invalidate_cache(prefix: str = Query(..., min_length=3, max_length=100), _: dict = Depends(require_admin)):
    return {"removed": await cache.invalidate_prefix(prefix)}
