"""
database.py - Async MongoDB layer (Motor) for the VoD training platform.

Collections
-----------
courses          Course metadata + lesson outline (one doc per course).
users            Learner / instructor profiles (no credentials: auth lives in your IdP).
asset_manifests  One doc per playable asset (lesson video) listing the origin
                 nodes that host its HLS manifest, in failover priority order.

Design notes
------------
* One shared AsyncIOMotorClient per process (it owns the connection pool).
* Every method returns JSON-safe dicts (``id`` instead of ``_id``, ISO-8601
  strings instead of datetimes) so results can go straight into Redis / FastAPI.
* Listing uses keyset pagination (stable and O(log n) at any depth, unlike skip()).
* Indexes are created idempotently on startup.

Usage (FastAPI lifespan):
    await mongo.connect()
    ...
    await mongo.close()
"""
from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.errors import DuplicateKeyError

log = logging.getLogger("vod.db")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class DBConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name: str = os.getenv("MONGO_DB", "vod_platform")
    max_pool: int = int(os.getenv("MONGO_MAX_POOL", "100"))
    min_pool: int = int(os.getenv("MONGO_MIN_POOL", "5"))
    server_selection_ms: int = int(os.getenv("MONGO_SELECT_TIMEOUT_MS", "3000"))
    connect_ms: int = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "3000"))
    socket_ms: int = int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "10000"))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return uuid.uuid4().hex


def to_json_safe(value: Any) -> Any:
    """Recursively convert datetimes to ISO strings so documents are JSON-ready."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat() + "Z"
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    return value


def out(doc: Optional[dict]) -> Optional[dict]:
    """Mongo document -> API-friendly dict (``_id`` becomes ``id``)."""
    if doc is None:
        return None
    doc = dict(doc)
    if "_id" in doc:
        doc["id"] = doc.pop("_id")
    return to_json_safe(doc)


def encode_cursor(published_at: datetime, doc_id: str) -> str:
    raw = json.dumps({"t": published_at.isoformat(), "i": doc_id}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        obj = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(obj["t"]), obj["i"]
    except Exception as exc:
        raise ValueError("invalid cursor") from exc


# --------------------------------------------------------------------------- #
# Connection manager
# --------------------------------------------------------------------------- #
class Mongo:
    """Owns the Motor client and exposes repositories."""

    def __init__(self, config: Optional[DBConfig] = None):
        self.cfg = config or DBConfig()
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self.courses: "CourseRepository"
        self.users: "UserRepository"
        self.assets: "AssetRepository"

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(
            self.cfg.uri,
            maxPoolSize=self.cfg.max_pool,
            minPoolSize=self.cfg.min_pool,
            serverSelectionTimeoutMS=self.cfg.server_selection_ms,
            connectTimeoutMS=self.cfg.connect_ms,
            socketTimeoutMS=self.cfg.socket_ms,
            retryWrites=True,
            retryReads=True,
            appname="vod-platform",
            tz_aware=True,
        )
        self.db = self.client[self.cfg.db_name]
        await self.client.admin.command("ping")  # fail fast if unreachable
        self.courses = CourseRepository(self.db)
        self.users = UserRepository(self.db)
        self.assets = AssetRepository(self.db)
        await self.ensure_indexes()
        log.info("MongoDB connected (%s)", self.cfg.db_name)

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    async def ping(self) -> bool:
        """Cheap liveness probe for /healthz."""
        try:
            await self.client.admin.command("ping")
            return True
        except Exception as exc:
            log.warning("Mongo ping failed: %s", exc)
            return False

    async def ensure_indexes(self) -> None:
        await self.courses.col.create_indexes(
            [
                IndexModel([("slug", ASCENDING)], unique=True, name="uq_slug"),
                # Serves the main listing query + keyset pagination.
                IndexModel(
                    [("status", ASCENDING), ("published_at", DESCENDING), ("_id", DESCENDING)],
                    name="ix_listing",
                ),
                IndexModel(
                    [("status", ASCENDING), ("tags", ASCENDING), ("published_at", DESCENDING)],
                    name="ix_tag_listing",
                ),
                # Full-text search. For large catalogs consider Atlas Search instead.
                IndexModel(
                    [("title", "text"), ("description", "text"), ("tags", "text")],
                    weights={"title": 10, "tags": 5, "description": 1},
                    name="tx_search",
                ),
            ]
        )
        await self.users.col.create_indexes(
            [IndexModel([("email", ASCENDING)], unique=True, name="uq_email")]
        )
        await self.assets.col.create_indexes(
            [
                IndexModel([("course_id", ASCENDING), ("lesson_id", ASCENDING)], name="ix_course_lesson"),
                # Lets the health checker find a canary asset per origin node quickly.
                IndexModel([("origins.base_url", ASCENDING)], name="ix_origin_base"),
            ]
        )


# --------------------------------------------------------------------------- #
# Repositories
# --------------------------------------------------------------------------- #
class CourseRepository:
    # Heavy / internal fields are excluded from list views.
    LIST_PROJECTION = {"lessons": 0}

    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["courses"]

    async def create(self, data: dict) -> dict:
        now = utcnow()
        doc = {
            "_id": data.get("id") or new_id(),
            "slug": data["slug"],
            "title": data["title"],
            "description": data.get("description", ""),
            "instructor": data.get("instructor", ""),
            "tags": data.get("tags", []),
            "level": data.get("level", "beginner"),
            "thumbnail_url": data.get("thumbnail_url"),
            "lessons": data.get("lessons", []),  # [{lesson_id, title, order, duration_s}]
            "lesson_count": len(data.get("lessons", [])),
            "status": data.get("status", "draft"),
            "published_at": now if data.get("status") == "published" else None,
            "created_at": now,
            "updated_at": now,
        }
        try:
            await self.col.insert_one(doc)
        except DuplicateKeyError as exc:
            raise ValueError(f"slug '{doc['slug']}' already exists") from exc
        return out(doc)

    async def get(self, course_id: str) -> Optional[dict]:
        return out(await self.col.find_one({"_id": course_id}))

    async def get_by_slug(self, slug: str) -> Optional[dict]:
        return out(await self.col.find_one({"slug": slug}))

    async def list_published(
        self, limit: int = 20, cursor: Optional[str] = None, tag: Optional[str] = None
    ) -> dict:
        """Keyset-paginated listing, newest first. Returns {items, next_cursor}."""
        limit = max(1, min(limit, 100))
        query: dict[str, Any] = {"status": "published"}
        if tag:
            query["tags"] = tag
        if cursor:
            ts, last_id = decode_cursor(cursor)
            query["$or"] = [
                {"published_at": {"$lt": ts}},
                {"published_at": ts, "_id": {"$lt": last_id}},
            ]
        docs = (
            await self.col.find(query, self.LIST_PROJECTION)
            .sort([("published_at", DESCENDING), ("_id", DESCENDING)])
            .limit(limit + 1)  # one extra row tells us whether another page exists
            .to_list(length=limit + 1)
        )
        next_cursor = None
        if len(docs) > limit:
            docs = docs[:limit]
            last = docs[-1]
            next_cursor = encode_cursor(last["published_at"], last["_id"])
        return {"items": [out(d) for d in docs], "next_cursor": next_cursor}

    async def search(self, text: str, limit: int = 20) -> list[dict]:
        """Relevance-ranked full-text search over published courses."""
        limit = max(1, min(limit, 50))
        docs = (
            await self.col.find(
                {"$text": {"$search": text}, "status": "published"},
                {"score": {"$meta": "textScore"}, **self.LIST_PROJECTION},
            )
            .sort([("score", {"$meta": "textScore"})])
            .limit(limit)
            .to_list(length=limit)
        )
        return [out(d) for d in docs]

    async def update(self, course_id: str, changes: dict) -> Optional[dict]:
        allowed = {"title", "description", "instructor", "tags", "level", "thumbnail_url", "lessons", "status"}
        patch = {k: v for k, v in changes.items() if k in allowed}
        if "lessons" in patch:
            patch["lesson_count"] = len(patch["lessons"])
        patch["updated_at"] = utcnow()
        if patch.get("status") == "published":
            # Only stamp published_at the first time.
            await self.col.update_one(
                {"_id": course_id, "published_at": None}, {"$set": {"published_at": utcnow()}}
            )
        doc = await self.col.find_one_and_update(
            {"_id": course_id}, {"$set": patch}, return_document=ReturnDocument.AFTER
        )
        return out(doc)


class UserRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["users"]

    async def upsert_profile(self, email: str, profile: dict) -> dict:
        """Create or update a profile keyed by lower-cased email."""
        email = email.strip().lower()
        allowed = {"display_name", "role", "avatar_url", "locale"}
        fields = {k: v for k, v in profile.items() if k in allowed}
        now = utcnow()
        doc = await self.col.find_one_and_update(
            {"email": email},
            {
                "$set": {**fields, "updated_at": now},
                "$setOnInsert": {"_id": new_id(), "email": email, "created_at": now,
                                 "enrolled_course_ids": []},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return out(doc)

    async def get(self, user_id: str) -> Optional[dict]:
        return out(await self.col.find_one({"_id": user_id}))

    async def enroll(self, user_id: str, course_id: str) -> None:
        await self.col.update_one(
            {"_id": user_id}, {"$addToSet": {"enrolled_course_ids": course_id}}
        )

    async def is_enrolled(self, user_id: str, course_id: str) -> bool:
        return await self.col.count_documents(
            {"_id": user_id, "enrolled_course_ids": course_id}, limit=1
        ) > 0


class AssetRepository:
    """
    Asset manifest record shape:
        {
          "_id": "<item_id>", "course_id": "...", "lesson_id": "...",
          "title": "...", "duration_s": 1234,
          "origins": [
              {"name": "origin-1", "base_url": "https://o1.cdn.example.com/vod/abc/",
               "manifest_path": "master.m3u8", "priority": 1, "healthy": true},
              {"name": "origin-2", ...}, {"name": "origin-3", ...}
          ]
        }
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        self.col = db["asset_manifests"]

    async def upsert(self, item_id: str, data: dict) -> dict:
        now = utcnow()
        doc = await self.col.find_one_and_update(
            {"_id": item_id},
            {
                "$set": {
                    "course_id": data["course_id"],
                    "lesson_id": data["lesson_id"],
                    "title": data.get("title", ""),
                    "duration_s": data.get("duration_s"),
                    "origins": data["origins"],
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return out(doc)

    async def get(self, item_id: str) -> Optional[dict]:
        doc = await self.col.find_one({"_id": item_id})
        if doc:
            # Healthy origins first, then by configured priority.
            doc["origins"] = sorted(
                doc.get("origins", []),
                key=lambda o: (not o.get("healthy", True), o.get("priority", 99)),
            )
        return out(doc)

    async def set_origin_health(self, item_id: str, origin_name: str, healthy: bool) -> bool:
        """Called by a health-check job to demote / restore an origin node."""
        res = await self.col.update_one(
            {"_id": item_id},
            {"$set": {"origins.$[o].healthy": healthy, "updated_at": utcnow()}},
            array_filters=[{"o.name": origin_name}],
        )
        return res.modified_count > 0


# Process-wide singleton, imported by main.py
mongo = Mongo()
