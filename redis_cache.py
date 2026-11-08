"""
redis_cache.py - Async cache with stampede protection and stale-while-revalidate.

Read path (fastest first)
-------------------------
1. L1: tiny in-process LRU (~microseconds, bounded to a couple of seconds of age).
2. L2: Redis (~1 ms on a local network).
3. Loader: your DB query, only when both layers miss.

Every cached value carries two deadlines:

    now < fresh_until                  -> FRESH: return it.
    fresh_until <= now < stale_until   -> STALE: return it immediately AND refresh
                                          once in the background (SWR).
    now >= stale_until                 -> MISS: load, with stampede protection.

Stampede protection (many requests, one cold key)
-------------------------------------------------
* In-process: concurrent callers for the same key share one asyncio task.
* Cross-process: a Redis lock (SET NX PX + token-checked release) elects a single
  loader across all workers; the others poll Redis briefly for its result.
* Fail-open: if Redis is down, the lock is stuck, or the peer is too slow, callers
  just load from the source. The cache must never take the API down with it.

Usage (FastAPI lifespan):
    await cache.connect()
    data = await cache.get_or_set("course:abc", lambda: repo.get("abc"), fresh_ttl=60)
    ...
    await cache.close()
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

try:  # orjson is ~3-5x faster; fall back to stdlib if it isn't installed
    import orjson

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)

    _loads = orjson.loads
except ImportError:  # pragma: no cover

    def _dumps(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":"), default=str).encode()

    _loads = json.loads

log = logging.getLogger("vod.cache")

Loader = Callable[[], Awaitable[Any]]

# Release a lock only if we still own it (prevents deleting a peer's newer lock).
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)

_SKIP = object()      # background refresh skipped: a peer is already doing it
_NO_LOCK = ""         # token meaning "proceed without a distributed lock"


# --------------------------------------------------------------------------- #
# Configuration & entry type
# --------------------------------------------------------------------------- #
@dataclass
class CacheConfig:
    url: str = os.getenv("REDIS_URL", "redis://localhost:6379/2")
    namespace: str = os.getenv("CACHE_NAMESPACE", "vod")
    fresh_ttl: float = float(os.getenv("CACHE_FRESH_TTL", "60"))     # seconds
    stale_ttl: float = float(os.getenv("CACHE_STALE_TTL", "600"))    # extra seconds servable
    jitter: float = float(os.getenv("CACHE_JITTER", "0.1"))          # +/-10% TTL spread
    lock_ttl_ms: int = int(os.getenv("CACHE_LOCK_TTL_MS", "10000"))
    peer_wait: float = float(os.getenv("CACHE_PEER_WAIT", "2.0"))    # max wait for a peer's load
    loader_timeout: float = float(os.getenv("CACHE_LOADER_TIMEOUT", "5.0"))
    l1_max_items: int = int(os.getenv("CACHE_L1_MAX_ITEMS", "2048"))
    l1_ttl: float = float(os.getenv("CACHE_L1_TTL", "2.0"))          # bounds cross-worker skew
    redis_cooldown: float = float(os.getenv("CACHE_REDIS_COOLDOWN", "5.0"))
    refresh_backoff: float = float(os.getenv("CACHE_REFRESH_BACKOFF", "5.0"))
    socket_timeout: float = float(os.getenv("CACHE_SOCKET_TIMEOUT", "0.25"))


@dataclass
class Entry:
    data: Any
    fresh_until: float   # epoch seconds
    stale_until: float


# --------------------------------------------------------------------------- #
# Cache manager
# --------------------------------------------------------------------------- #
class CacheManager:
    def __init__(self, config: Optional[CacheConfig] = None, client: Any = None):
        self.cfg = config or CacheConfig()
        self._redis = client                      # injectable for tests
        self._l1: OrderedDict[str, tuple[Entry, float]] = OrderedDict()
        self._inflight: dict[str, asyncio.Task] = {}
        self._backoff: dict[str, float] = {}      # key -> monotonic time when refresh may retry
        self._down_until = 0.0                    # monotonic; Redis circuit-breaker

    # ---- lifecycle -------------------------------------------------------- #
    async def connect(self) -> None:
        if self._redis is None:
            import redis.asyncio as aioredis      # imported lazily

            self._redis = aioredis.from_url(
                self.cfg.url,
                socket_timeout=self.cfg.socket_timeout,
                socket_connect_timeout=self.cfg.socket_timeout,
                health_check_interval=30,
                max_connections=100,
            )
        try:
            await self._redis.ping()
            log.info("Redis cache connected")
        except Exception as exc:  # start degraded rather than refuse to boot
            self._trip(exc)

    async def close(self) -> None:
        pending = list(self._inflight.values())
        if pending:
            await asyncio.wait(pending, timeout=2)
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass

    async def health(self) -> dict:
        try:
            t0 = time.perf_counter()
            await self._redis.ping()
            return {"redis": "ok", "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}
        except Exception as exc:
            return {"redis": "down", "error": str(exc)}

    # ---- public API ------------------------------------------------------- #
    def key(self, *parts: Any) -> str:
        return ":".join(str(p) for p in parts)

    async def get_or_set(
        self,
        key: str,
        loader: Loader,
        *,
        fresh_ttl: Optional[float] = None,
        stale_ttl: Optional[float] = None,
    ) -> Any:
        fresh = self.cfg.fresh_ttl if fresh_ttl is None else fresh_ttl
        stale = self.cfg.stale_ttl if stale_ttl is None else stale_ttl

        entry = self._l1_get(key)
        if entry is None:
            entry = await self._redis_get(key)
            if entry is not None:
                self._l1_put(key, entry)

        now = time.time()
        if entry is not None:
            if now < entry.fresh_until:
                return entry.data                                   # FRESH
            if now < entry.stale_until:
                self._refresh_in_background(key, loader, fresh, stale)
                return entry.data                                   # STALE (SWR)

        return await self._load_single_flight(key, loader, fresh, stale, have_stale=False)

    async def get(self, key: str) -> Any:
        """Return cached data (fresh or stale) or None. Never calls a loader."""
        entry = self._l1_get(key) or await self._redis_get(key)
        if entry is not None and time.time() < entry.stale_until:
            return entry.data
        return None

    async def set(self, key: str, value: Any, *, fresh_ttl: Optional[float] = None,
                  stale_ttl: Optional[float] = None) -> None:
        fresh = self.cfg.fresh_ttl if fresh_ttl is None else fresh_ttl
        stale = self.cfg.stale_ttl if stale_ttl is None else stale_ttl
        await self._store(key, value, fresh, stale)

    async def delete(self, *keys: str) -> None:
        for k in keys:
            self._l1.pop(k, None)
        if self._redis_available() and keys:
            try:
                await self._redis.delete(*[self._k(k) for k in keys])
            except Exception as exc:
                self._trip(exc)

    async def invalidate_prefix(self, prefix: str) -> int:
        """Delete every key starting with ``prefix`` (SCAN-based, non-blocking)."""
        for k in [k for k in self._l1 if k.startswith(prefix)]:
            self._l1.pop(k, None)
        removed = 0
        if not self._redis_available():
            return removed
        try:
            batch: list[Any] = []
            async for rk in self._redis.scan_iter(match=f"{self._k(prefix)}*", count=500):
                batch.append(rk)
                if len(batch) >= 500:
                    removed += await self._redis.delete(*batch)
                    batch = []
            if batch:
                removed += await self._redis.delete(*batch)
        except Exception as exc:
            self._trip(exc)
        return removed

    # ---- single-flight + distributed lock --------------------------------- #
    def _start_task(self, key, loader, fresh, stale, have_stale) -> asyncio.Task:
        task = asyncio.create_task(self._load_with_lock(key, loader, fresh, stale, have_stale))
        self._inflight[key] = task

        def _done(t: asyncio.Task, key=key, have_stale=have_stale) -> None:
            self._inflight.pop(key, None)
            if t.cancelled():
                return
            exc = t.exception()  # also marks it retrieved
            if exc is not None and have_stale:
                # Background refresh failed: keep serving stale data, retry later.
                self._backoff[key] = time.monotonic() + self.cfg.refresh_backoff
                log.warning("Background refresh of %s failed: %r", key, exc)
            elif exc is None:
                self._backoff.pop(key, None)

        task.add_done_callback(_done)
        return task

    async def _load_single_flight(self, key, loader, fresh, stale, have_stale) -> Any:
        task = self._inflight.get(key) or self._start_task(key, loader, fresh, stale, have_stale)
        # shield(): one caller being cancelled must not cancel the shared load.
        return await asyncio.shield(task)

    def _refresh_in_background(self, key, loader, fresh, stale) -> None:
        if key in self._inflight or self._backoff.get(key, 0) > time.monotonic():
            return
        self._start_task(key, loader, fresh, stale, have_stale=True)

    async def _load_with_lock(self, key, loader, fresh, stale, have_stale) -> Any:
        token = await self._acquire_lock(key)
        if token is None:                       # a peer process is loading this key
            if have_stale:
                return _SKIP                    # callers already have stale data
            entry = await self._wait_for_peer(key)
            if entry is not None:
                self._l1_put(key, entry)
                return entry.data
            log.warning("Peer load of %s timed out; loading locally (fail-open)", key)
            token = _NO_LOCK
        try:
            return await self._compute_and_store(key, loader, fresh, stale)
        finally:
            if token != _NO_LOCK:
                await self._release_lock(key, token)

    async def _compute_and_store(self, key, loader, fresh, stale) -> Any:
        value = await asyncio.wait_for(loader(), timeout=self.cfg.loader_timeout)
        await self._store(key, value, fresh, stale)
        return value

    async def _wait_for_peer(self, key: str) -> Optional[Entry]:
        deadline = time.monotonic() + self.cfg.peer_wait
        while time.monotonic() < deadline:
            await asyncio.sleep(0.025)
            entry = await self._redis_get(key)
            if entry is not None and time.time() < entry.stale_until:
                return entry
        return None

    async def _acquire_lock(self, key: str) -> Optional[str]:
        """Token if acquired, None if a peer holds it, _NO_LOCK if Redis is unavailable."""
        if not self._redis_available():
            return _NO_LOCK
        token = uuid.uuid4().hex
        try:
            ok = await self._redis.set(self._lock_k(key), token, nx=True, px=self.cfg.lock_ttl_ms)
        except Exception as exc:
            self._trip(exc)
            return _NO_LOCK
        return token if ok else None

    async def _release_lock(self, key: str, token: str) -> None:
        try:
            await self._redis.eval(_RELEASE_LUA, 1, self._lock_k(key), token)
        except Exception as exc:
            self._trip(exc)                     # lock expires on its own via PX

    # ---- storage ---------------------------------------------------------- #
    async def _store(self, key: str, value: Any, fresh: float, stale: float) -> None:
        spread = 1 + random.uniform(-self.cfg.jitter, self.cfg.jitter)  # de-sync expirations
        now = time.time()
        entry = Entry(value, now + fresh * spread, now + fresh * spread + stale)
        self._l1_put(key, entry)
        if not self._redis_available():
            return
        try:
            payload = _dumps({"d": value, "f": entry.fresh_until, "s": entry.stale_until})
            ttl = max(1, int(entry.stale_until - now) + 1)
            await self._redis.set(self._k(key), payload, ex=ttl)
        except Exception as exc:
            self._trip(exc)

    async def _redis_get(self, key: str) -> Optional[Entry]:
        if not self._redis_available():
            return None
        try:
            raw = await self._redis.get(self._k(key))
        except Exception as exc:
            self._trip(exc)
            return None
        if raw is None:
            return None
        try:
            obj = _loads(raw)
            return Entry(obj["d"], float(obj["f"]), float(obj["s"]))
        except Exception:
            log.warning("Corrupt cache entry for %s; ignoring", key)
            return None

    # ---- L1 --------------------------------------------------------------- #
    def _l1_get(self, key: str) -> Optional[Entry]:
        item = self._l1.get(key)
        if item is None:
            return None
        entry, expires = item
        if time.monotonic() >= expires or time.time() >= entry.stale_until:
            self._l1.pop(key, None)
            return None
        self._l1.move_to_end(key)
        return entry

    def _l1_put(self, key: str, entry: Entry) -> None:
        self._l1[key] = (entry, time.monotonic() + self.cfg.l1_ttl)
        self._l1.move_to_end(key)
        while len(self._l1) > self.cfg.l1_max_items:
            self._l1.popitem(last=False)

    # ---- misc ------------------------------------------------------------- #
    def _k(self, key: str) -> str:
        return f"{self.cfg.namespace}:{key}"

    def _lock_k(self, key: str) -> str:
        return f"{self.cfg.namespace}:lock:{key}"

    def _redis_available(self) -> bool:
        return self._redis is not None and time.monotonic() >= self._down_until

    def _trip(self, exc: Exception) -> None:
        """Simple circuit breaker: skip Redis for a few seconds after an error."""
        self._down_until = time.monotonic() + self.cfg.redis_cooldown
        log.warning("Redis unavailable (%r); bypassing cache for %.0fs", exc, self.cfg.redis_cooldown)


# Process-wide singleton, imported by main.py
cache = CacheManager()
