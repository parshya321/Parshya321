"""
health_check.py - Background monitor that keeps origin-node `healthy` flags accurate.

What it does each cycle
-----------------------
1. Finds every distinct origin node (base URL) referenced by asset manifests.
2. Probes each node ONCE (not once per asset): fetches a real canary manifest with a
   freshly signed URL, then (deep mode) requests the first segment/playlist it references.
   This catches nodes that answer playlists but hang on media, or that reject signatures.
3. Applies hysteresis so one blip doesn't flap traffic:
       healthy -> unhealthy after HC_FAIL_THRESHOLD   consecutive failures (default 3)
       unhealthy -> healthy after HC_RECOVER_THRESHOLD consecutive successes (default 2)
4. On a transition, flips `healthy` on every asset that uses that node (one update_many)
   and purges the API's cached asset records so players see it within seconds.
5. Persists per-node state in the `origin_health` collection (GET /api/admin/origins/health).

Safety valves
-------------
* If EVERY node fails in the same cycle, the problem is far more likely to be this checker's
  own network than all your CDN nodes dying at once, so flags are left alone and a CRITICAL
  line is logged. (Players still fail over on their own either way.)
* Origins outside ALLOWED_ORIGIN_HOSTS are never contacted.
* A failing cycle is logged and retried; the process never exits on its own.

Run
---
    python health_check.py            # loop forever (systemd / Kubernetes Deployment)
    python health_check.py --once     # single pass (cron / Kubernetes CronJob)

Environment: ASSET_SIGNING_KEY (required), ALLOWED_ORIGIN_HOSTS, MONGO_URI/MONGO_DB, REDIS_URL,
HC_INTERVAL=30  HC_TIMEOUT=5  HC_FAIL_THRESHOLD=3  HC_RECOVER_THRESHOLD=2  HC_CONCURRENCY=10  HC_DEEP=1
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import signal
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

from database import mongo, utcnow
from origin_security import host_allowed, signature
from redis_cache import cache

log = logging.getLogger("vod.health")

RANGE_HEADER = {"Range": "bytes=0-2047"}  # we only need the first bytes of anything


@dataclass
class HCConfig:
    interval: float = float(os.getenv("HC_INTERVAL", "30"))
    timeout: float = float(os.getenv("HC_TIMEOUT", "5"))
    fail_threshold: int = int(os.getenv("HC_FAIL_THRESHOLD", "3"))
    recover_threshold: int = int(os.getenv("HC_RECOVER_THRESHOLD", "2"))
    concurrency: int = int(os.getenv("HC_CONCURRENCY", "10"))
    deep: bool = os.getenv("HC_DEEP", "1") == "1"


@dataclass
class ProbeResult:
    ok: bool
    latency_ms: float
    error: Optional[str] = None


# --------------------------------------------------------------------------- #
# Probing
# --------------------------------------------------------------------------- #
def normalize(base_url: str) -> str:
    return base_url.rstrip("/") + "/"


async def probe(client: httpx.AsyncClient, base: str, manifest_path: str, deep: bool) -> ProbeResult:
    key = os.environ["ASSET_SIGNING_KEY"]
    prefix = urlparse(base).path
    exp = int(time.time()) + 300
    query = f"exp={exp}&sig={signature(key, prefix, exp)}"
    manifest_url = f"{base}{manifest_path.lstrip('/')}"
    t0 = time.perf_counter()

    def elapsed() -> float:
        return round((time.perf_counter() - t0) * 1000, 1)

    try:
        r = await client.get(f"{manifest_url}?{query}", headers=RANGE_HEADER)
        if r.status_code not in (200, 206):
            return ProbeResult(False, elapsed(), f"manifest HTTP {r.status_code}")
        text = r.text
        if not text.lstrip().startswith("#EXTM3U"):
            return ProbeResult(False, elapsed(), "manifest is not valid HLS")

        if deep:
            first = next((ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")), None)
            if first:
                target = urljoin(manifest_url, first)
                # Only follow references on the same node (never let a manifest steer us elsewhere).
                if urlparse(target).hostname == urlparse(base).hostname:
                    sep = "&" if "?" in target else "?"
                    async with client.stream("GET", f"{target}{sep}{query}", headers=RANGE_HEADER) as r2:
                        if r2.status_code not in (200, 206):
                            return ProbeResult(False, elapsed(), f"media HTTP {r2.status_code}")
                        async for _ in r2.aiter_bytes(1024):  # first chunk proves bytes flow
                            break
        return ProbeResult(True, elapsed())
    except httpx.TimeoutException:
        return ProbeResult(False, elapsed(), "timeout")
    except httpx.HTTPError as exc:
        return ProbeResult(False, elapsed(), type(exc).__name__)


# --------------------------------------------------------------------------- #
# State machine (pure function, easy to unit test)
# --------------------------------------------------------------------------- #
def advance(cfg: HCConfig, state: dict, ok: bool) -> tuple[dict, Optional[bool]]:
    """Return (new_state, transition) where transition is True/False when the flag must change."""
    s = dict(state)
    if ok:
        s["oks"], s["fails"] = s.get("oks", 0) + 1, 0
    else:
        s["fails"], s["oks"] = s.get("fails", 0) + 1, 0

    current = s.get("healthy")
    transition: Optional[bool] = None
    if current is None:                              # first sighting: sync flags to reality
        s["healthy"] = transition = ok
    elif current and s["fails"] >= cfg.fail_threshold:
        s["healthy"] = transition = False
    elif not current and s["oks"] >= cfg.recover_threshold:
        s["healthy"] = transition = True
    return s, transition


# --------------------------------------------------------------------------- #
# One monitoring cycle
# --------------------------------------------------------------------------- #
async def run_cycle(client: httpx.AsyncClient, cfg: HCConfig) -> dict:
    assets = mongo.assets.col
    health = mongo.db["origin_health"]

    # 1) Discover distinct nodes (group raw spellings of the same base URL).
    groups: dict[str, list[str]] = {}
    for raw in await assets.distinct("origins.base_url"):
        groups.setdefault(normalize(raw), []).append(raw)
    if not groups:
        return {"nodes": 0}

    sem = asyncio.Semaphore(cfg.concurrency)

    async def check(base: str, raws: list[str]) -> tuple[str, Optional[ProbeResult]]:
        if not host_allowed(urlparse(base).hostname):
            log.warning("Skipping %s: host not in ALLOWED_ORIGIN_HOSTS", base)
            return base, None
        canary = await assets.find_one({"origins.base_url": {"$in": raws}}, sort=[("updated_at", -1)])
        entry = next((o for o in (canary or {}).get("origins", []) if o.get("base_url") in raws), None)
        if entry is None:
            return base, None
        async with sem:
            return base, await probe(client, base, entry.get("manifest_path", "master.m3u8"), cfg.deep)

    results = [(b, r) for b, r in await asyncio.gather(*[check(b, rs) for b, rs in groups.items()]) if r]
    if not results:
        return {"nodes": len(groups), "probed": 0}

    # 2) Safety valve: everything failing at once points at the checker, not the nodes.
    all_down = len(results) > 1 and all(not r.ok for _, r in results)
    if all_down:
        log.critical("ALL %d origin nodes failed this cycle - suspecting checker-side outage; "
                     "leaving health flags unchanged", len(results))

    states = {d["_id"]: d async for d in health.find({"_id": {"$in": [b for b, _ in results]}})}
    changed: list[tuple[str, bool]] = []

    for base, res in results:
        now = utcnow()
        diag = {"last_checked": now, "last_ok": res.ok, "latency_ms": res.latency_ms, "last_error": res.error}
        if all_down:
            await health.update_one({"_id": base}, {"$set": diag}, upsert=True)
            continue

        state, transition = advance(cfg, states.get(base, {}), res.ok)
        await health.update_one(
            {"_id": base},
            {"$set": {**diag, "healthy": state["healthy"], "fails": state["fails"], "oks": state["oks"]}},
            upsert=True,
        )
        if transition is not None:
            raws = groups[base]
            await assets.update_many(
                {"origins.base_url": {"$in": raws}},
                {"$set": {"origins.$[o].healthy": transition, "updated_at": now}},
                array_filters=[{"o.base_url": {"$in": raws}}],
            )
            changed.append((base, transition))
            level = logging.INFO if transition else logging.ERROR
            log.log(level, "Origin %s -> %s (%s)", base, "HEALTHY" if transition else "UNHEALTHY", res.error or "ok")

    # 3) Make players see the change quickly instead of waiting out the cache TTL.
    if changed:
        await cache.invalidate_prefix("asset:")

    return {
        "nodes": len(groups),
        "probed": len(results),
        "failing": sum(1 for _, r in results if not r.ok),
        "changed": [{"base": b, "healthy": h} for b, h in changed],
    }


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
async def main_loop(cfg: HCConfig, once: bool) -> None:
    if not os.getenv("ASSET_SIGNING_KEY"):
        raise RuntimeError("ASSET_SIGNING_KEY must be set")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # e.g. Windows
            pass

    await mongo.connect()
    await cache.connect()
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(cfg.timeout),
            limits=httpx.Limits(max_connections=cfg.concurrency * 2),
            follow_redirects=False,
            headers={"User-Agent": "vod-origin-healthcheck/1.0"},
        ) as client:
            while not stop.is_set():
                started = time.monotonic()
                try:
                    log.info("cycle: %s", await run_cycle(client, cfg))
                except Exception:  # never let one bad cycle kill the monitor
                    log.exception("health cycle failed")
                if once:
                    break
                delay = max(1.0, cfg.interval - (time.monotonic() - started)) + random.uniform(0, 2)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
    finally:
        await cache.close()
        await mongo.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Origin node health monitor")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--interval", type=float, help="seconds between cycles (overrides HC_INTERVAL)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = HCConfig()
    if args.interval:
        config.interval = args.interval
    asyncio.run(main_loop(config, args.once))
