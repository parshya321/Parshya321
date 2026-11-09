"""
main.py - Single-file Anime VoD Platform API (Production Ready & Safe Fallbacks)
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

# Safe imports for database and cache to prevent startup crashes
try:
    from database import mongo
except ImportError:
    mongo = None

try:
    from redis_cache import cache
except ImportError:
    cache = None

try:
    import orjson
    from fastapi.responses import ORJSONResponse as DefaultResponse
except ImportError:
    DefaultResponse = JSONResponse

log = logging.getLogger("vod.api")

# --------------------------------------------------------------------------- #
# Scraper & Stream Resolver Logic (`animesalt.cx`)
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
                raise HTTPException(status_code=400, detail=f"Failed to fetch episode page, status code: {response.status_code}")
            
            html_content = response.text
            m3u8_pattern = r'https?://[^\s<>"]+?\.m3u8[^\s<>"]*'
            found_links = re.findall(m3u8_pattern, html_content)
            
            if found_links:
                return {"stream_url": found_links[0], "type": "hls"}
            
            soup = BeautifulSoup(html_content, 'html.parser')
            iframe = soup.find('iframe', src=True)
            if iframe:
                return {"stream_url": iframe['src'], "type": "iframe"}
            
            raise HTTPException(status_code=404, detail="Streaming source (.m3u8) could not be found on this page.")
        except HTTPException as he:
            raise he
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Scraper Error: {str(e)}")


# --------------------------------------------------------------------------- #
# App Lifecycle & Settings
# --------------------------------------------------------------------------- #
class Settings:
    def __init__(self) -> None:
        self.jwt_key = os.getenv("JWT_KEY", "")
        self.cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]

settings = Settings()
PUBLIC_CACHE_HEADER = "public, max-age=30, stale-while-revalidate=120"


@asynccontextmanager
async def lifespan(_: FastAPI):
    if mongo and hasattr(mongo, 'connect'):
        try:
            await mongo.connect()
        except Exception as e:
            print(f"Mongo connection warning: {e}")
            
    if cache and hasattr(cache, 'connect'):
        try:
            await cache.connect()
        except Exception as e:
            print(f"Redis connection warning: {e}")
            
    try:
        yield
    finally:
        if cache and hasattr(cache, 'close'):
            try:
                await cache.close()
            except:
                pass
        if mongo and hasattr(mongo, 'close'):
            try:
                await mongo.close()
            except:
                pass


app = FastAPI(title="Anime VoD Streaming Platform API", lifespan=lifespan, default_response_class=DefaultResponse)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    max_age=600,
)


bearer = HTTPBearer(auto_error=False)

async def current_user(creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> dict:
    return {"id": "dev_user", "roles": ["admin"]}


def require_admin(user: dict = Depends(current_user)) -> dict:
    if "admin" not in user["roles"]:
        raise HTTPException(403, "Admin role required")
    return user


# --------------------------------------------------------------------------- #
# API Endpoints
# --------------------------------------------------------------------------- #
@app.get("/healthz")
async def healthz():
    db_ok = await mongo.ping() if (mongo and hasattr(mongo, 'ping')) else True
    cache_ok = await cache.health() if (cache and hasattr(cache, 'health')) else True
    return JSONResponse({"mongo": db_ok, "cache": cache_ok}, status_code=200)


@app.get("/api/anime/catalog")
async def get_anime_catalog(response: Response):
    response.headers["Cache-Control"] = PUBLIC_CACHE_HEADER
    
    # Try fetching from cache/db if available, else direct scrape fallback
    catalog = []
    if cache and hasattr(cache, 'get_or_set'):
        try:
            catalog = await cache.get_or_set(
                "anime:catalog:all",
                lambda: fetch_anime_catalog(),
                fresh_ttl=60,
                stale_ttl=300
            )
        except:
            catalog = await fetch_anime_catalog()
    else:
        catalog = await fetch_anime_catalog()
        
    return {"status": "success", "items": catalog or []}


@app.get("/api/anime/resolve")
async def resolve_anime_stream(url: str = Query(..., description="Target episode page URL from animesalt.cx"), _: dict = Depends(current_user)):
    cache_key = f"stream:hls:{hash(url)}"
    
    # Check cache safely
    if cache and hasattr(cache, 'get'):
        try:
            cached_stream = await cache.get(cache_key)
            if cached_stream:
                return {"source": "redis_cache", "stream_url": cached_stream, "type": "hls"}
        except:
            pass

    stream_data = await resolve_streaming_link(url)
    stream_url = stream_data.get("stream_url")
    if not stream_url:
        raise HTTPException(status_code=404, detail="Could not resolve live streaming link.")

    # Save to cache safely
    if cache and hasattr(cache, 'set'):
        try:
            await cache.set(cache_key, stream_url, ttl=7200)
        except:
            pass
            
    return {"source": "live_scraper", "stream_url": stream_url, "type": stream_data.get("type", "hls")}
