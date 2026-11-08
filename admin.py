"""
admin.py - Admin API: manage courses, register asset manifests, manage enrollments.

Mounted by main.py with the app's own ``require_admin`` dependency, so every route
below needs a valid JWT whose ``roles`` claim contains "admin":

    app.include_router(build_admin_router(require_admin))

Routes (all under /api/admin)
-----------------------------
Courses
  POST   /courses                         create (draft by default)
  GET    /courses?status=&limit=          list, including drafts/archived
  GET    /courses/{id}                    full record
  PATCH  /courses/{id}                    partial update (slug is immutable)
  POST   /courses/{id}/publish            publish (gate: every lesson needs an asset; ?force=true skips)
  POST   /courses/{id}/unpublish          back to draft
  DELETE /courses/{id}                    soft-delete: archive (never destroys data)
Assets
  PUT    /assets/{item_id}                register / replace a manifest and its origin nodes
  GET    /assets/{item_id}                raw record incl. health flags
  PATCH  /assets/{item_id}/origins/{name}/health   manual override (health job re-evaluates next cycle)
  DELETE /assets/{item_id}
Users & enrollment
  POST   /users                           upsert a profile by email
  POST   /enrollments                     enroll {user_id, course_id}
  DELETE /enrollments?user_id=&course_id= remove enrollment
Ops
  GET    /origins/health                  state written by health_check.py

Every mutation purges the affected cache keys so changes show up immediately
(other API workers' tiny in-process caches converge within ~2 seconds).
"""
import logging
import re
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field, field_validator

from database import mongo, out, utcnow
from origin_security import validate_manifest_path, validate_origin_base_url
from redis_cache import cache

log = logging.getLogger("vod.admin")

SLUG_RE = r"^[a-z0-9]+(?:-[a-z0-9]+)*$"
ID_RE = r"^[A-Za-z0-9_\-]{1,64}$"
TAG_RE = re.compile(r"^[\w\- ]{1,50}$")


# --------------------------------------------------------------------------- #
# Request models
# --------------------------------------------------------------------------- #
def _check_tags(v):
    if v is not None:
        for t in v:
            if not TAG_RE.match(t):
                raise ValueError(f"invalid tag '{t}'")
    return v


def _check_https(v):
    if v and not v.startswith("https://"):
        raise ValueError("must be an https URL")
    return v


def _check_unique_lessons(v):
    if v is not None:
        ids = [lesson.lesson_id for lesson in v]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate lesson_id in lessons")
    return v


class LessonIn(BaseModel):
    lesson_id: str = Field(pattern=ID_RE)
    title: str = Field(min_length=1, max_length=200)
    order: int = Field(ge=0)
    duration_s: Optional[int] = Field(None, ge=0)


class CourseCreate(BaseModel):
    slug: str = Field(pattern=SLUG_RE, max_length=100)
    title: str = Field(min_length=1, max_length=200)
    description: str = Field("", max_length=5000)
    instructor: str = Field("", max_length=100)
    tags: list[str] = Field(default_factory=list, max_length=20)
    level: Literal["beginner", "intermediate", "advanced"] = "beginner"
    thumbnail_url: Optional[str] = Field(None, max_length=500)
    lessons: list[LessonIn] = Field(default_factory=list, max_length=500)
    status: Literal["draft", "published"] = "draft"

    _tags = field_validator("tags")(_check_tags)
    _thumb = field_validator("thumbnail_url")(_check_https)
    _lessons = field_validator("lessons")(_check_unique_lessons)


class CourseUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=5000)
    instructor: Optional[str] = Field(None, max_length=100)
    tags: Optional[list[str]] = Field(None, max_length=20)
    level: Optional[Literal["beginner", "intermediate", "advanced"]] = None
    thumbnail_url: Optional[str] = Field(None, max_length=500)
    lessons: Optional[list[LessonIn]] = Field(None, max_length=500)

    _tags = field_validator("tags")(_check_tags)
    _thumb = field_validator("thumbnail_url")(_check_https)
    _lessons = field_validator("lessons")(_check_unique_lessons)


class OriginIn(BaseModel):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9\-]{0,31}$")
    base_url: str = Field(max_length=500)
    manifest_path: str = Field("master.m3u8", max_length=200)
    priority: int = Field(1, ge=1, le=99)
    healthy: bool = True


class AssetIn(BaseModel):
    course_id: str = Field(pattern=ID_RE)
    lesson_id: str = Field(pattern=ID_RE)
    title: str = Field("", max_length=200)
    duration_s: Optional[int] = Field(None, ge=0)
    origins: list[OriginIn] = Field(min_length=1, max_length=10)

    @field_validator("origins")
    @classmethod
    def _unique_names(cls, v):
        names = [o.name for o in v]
        if len(set(names)) != len(names):
            raise ValueError("origin names must be unique")
        return v


class HealthOverride(BaseModel):
    healthy: bool


class UserIn(BaseModel):
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=254)
    display_name: Optional[str] = Field(None, max_length=100)
    role: Optional[Literal["learner", "instructor"]] = None
    locale: Optional[str] = Field(None, max_length=20)


class EnrollmentIn(BaseModel):
    user_id: str = Field(pattern=ID_RE)
    course_id: str = Field(pattern=ID_RE)


# --------------------------------------------------------------------------- #
# Cache invalidation
# --------------------------------------------------------------------------- #
async def _bust(course_id: Optional[str] = None, item_id: Optional[str] = None) -> None:
    keys = []
    if course_id:
        keys.append(cache.key("course", course_id))
    if item_id:
        keys.append(cache.key("asset", item_id))
    if keys:
        await cache.delete(*keys)
    await cache.invalidate_prefix("catalog:")
    await cache.invalidate_prefix("search:")


# --------------------------------------------------------------------------- #
# Router factory
# --------------------------------------------------------------------------- #
def build_admin_router(require_admin) -> APIRouter:
    router = APIRouter(prefix="/api/admin", tags=["admin"], dependencies=[Depends(require_admin)])

    # ---- courses ---------------------------------------------------------- #
    @router.post("/courses", status_code=201)
    async def create_course(body: CourseCreate):
        try:
            course = await mongo.courses.create(body.model_dump())
        except ValueError as exc:
            raise HTTPException(409, str(exc))
        await _bust(course["id"])
        return course

    @router.get("/courses")
    async def list_courses(
        status: Optional[Literal["draft", "published", "archived"]] = None,
        limit: int = Query(50, ge=1, le=100),
    ):
        query = {"status": status} if status else {}
        docs = (
            await mongo.courses.col.find(query, {"lessons": 0})
            .sort([("updated_at", -1)])
            .limit(limit)
            .to_list(length=limit)
        )
        return {"items": [out(d) for d in docs]}

    @router.get("/courses/{course_id}")
    async def get_course(course_id: str = Path(pattern=ID_RE)):
        course = await mongo.courses.get(course_id)
        if not course:
            raise HTTPException(404, "Course not found")
        return course

    @router.patch("/courses/{course_id}")
    async def update_course(body: CourseUpdate, course_id: str = Path(pattern=ID_RE)):
        changes = body.model_dump(exclude_unset=True)
        if not changes:
            raise HTTPException(400, "No fields to update")
        course = await mongo.courses.update(course_id, changes)
        if not course:
            raise HTTPException(404, "Course not found")
        await _bust(course_id)
        return course

    @router.post("/courses/{course_id}/publish")
    async def publish_course(
        course_id: str = Path(pattern=ID_RE),
        force: bool = Query(False, description="Skip the 'every lesson has an asset' check"),
    ):
        course = await mongo.courses.get(course_id)
        if not course:
            raise HTTPException(404, "Course not found")
        if not force:
            lessons = course.get("lessons", [])
            if not lessons:
                raise HTTPException(422, "Add at least one lesson before publishing")
            have = set(await mongo.assets.col.distinct("lesson_id", {"course_id": course_id}))
            missing = [l["lesson_id"] for l in lessons if l["lesson_id"] not in have]
            if missing:
                raise HTTPException(
                    422,
                    {"message": "Some lessons have no registered asset", "missing_lesson_ids": missing[:20]},
                )
        updated = await mongo.courses.update(course_id, {"status": "published"})
        await _bust(course_id)
        return updated

    @router.post("/courses/{course_id}/unpublish")
    async def unpublish_course(course_id: str = Path(pattern=ID_RE)):
        updated = await mongo.courses.update(course_id, {"status": "draft"})
        if not updated:
            raise HTTPException(404, "Course not found")
        await _bust(course_id)
        return updated

    @router.delete("/courses/{course_id}")
    async def archive_course(course_id: str = Path(pattern=ID_RE)):
        updated = await mongo.courses.update(course_id, {"status": "archived"})
        if not updated:
            raise HTTPException(404, "Course not found")
        await _bust(course_id)
        return {"id": course_id, "status": "archived"}

    # ---- assets ----------------------------------------------------------- #
    @router.put("/assets/{item_id}")
    async def register_asset(body: AssetIn, item_id: str = Path(pattern=ID_RE)):
        course = await mongo.courses.get(body.course_id)
        if not course:
            raise HTTPException(404, "course_id does not exist")
        lesson_ids = {l["lesson_id"] for l in course.get("lessons", [])}
        if lesson_ids and body.lesson_id not in lesson_ids:
            raise HTTPException(422, "lesson_id is not part of that course")

        origins = []
        for o in body.origins:
            try:
                origins.append(
                    {
                        "name": o.name,
                        "base_url": validate_origin_base_url(o.base_url),
                        "manifest_path": validate_manifest_path(o.manifest_path),
                        "priority": o.priority,
                        "healthy": o.healthy,
                    }
                )
            except ValueError as exc:
                raise HTTPException(422, f"origin '{o.name}': {exc}")

        asset = await mongo.assets.upsert(
            item_id,
            {
                "course_id": body.course_id,
                "lesson_id": body.lesson_id,
                "title": body.title,
                "duration_s": body.duration_s,
                "origins": origins,
            },
        )
        await _bust(body.course_id, item_id)
        return asset

    @router.get("/assets/{item_id}")
    async def get_asset(item_id: str = Path(pattern=ID_RE)):
        asset = out(await mongo.assets.col.find_one({"_id": item_id}))
        if not asset:
            raise HTTPException(404, "Asset not found")
        return asset

    @router.patch("/assets/{item_id}/origins/{name}/health")
    async def override_health(
        body: HealthOverride,
        item_id: str = Path(pattern=ID_RE),
        name: str = Path(pattern=r"^[a-z0-9][a-z0-9\-]{0,31}$"),
    ):
        if not await mongo.assets.set_origin_health(item_id, name, body.healthy):
            raise HTTPException(404, "Asset or origin not found (or flag already set)")
        await cache.delete(cache.key("asset", item_id))
        return {"item_id": item_id, "origin": name, "healthy": body.healthy}

    @router.delete("/assets/{item_id}")
    async def delete_asset(item_id: str = Path(pattern=ID_RE)):
        res = await mongo.assets.col.delete_one({"_id": item_id})
        if res.deleted_count == 0:
            raise HTTPException(404, "Asset not found")
        await cache.delete(cache.key("asset", item_id))
        return {"deleted": item_id}

    # ---- users & enrollment ---------------------------------------------- #
    @router.post("/users")
    async def upsert_user(body: UserIn):
        profile = body.model_dump(exclude={"email"}, exclude_none=True)
        return await mongo.users.upsert_profile(body.email, profile)

    @router.post("/enrollments", status_code=201)
    async def enroll(body: EnrollmentIn):
        if not await mongo.users.get(body.user_id):
            raise HTTPException(404, "User not found")
        if not await mongo.courses.get(body.course_id):
            raise HTTPException(404, "Course not found")
        await mongo.users.enroll(body.user_id, body.course_id)
        await cache.delete(cache.key("enrolled", body.user_id, body.course_id))
        return {"user_id": body.user_id, "course_id": body.course_id, "enrolled": True}

    @router.delete("/enrollments")
    async def unenroll(user_id: str = Query(pattern=ID_RE), course_id: str = Query(pattern=ID_RE)):
        res = await mongo.users.col.update_one(
            {"_id": user_id}, {"$pull": {"enrolled_course_ids": course_id}}
        )
        if res.matched_count == 0:
            raise HTTPException(404, "User not found")
        await cache.delete(cache.key("enrolled", user_id, course_id))
        return {"user_id": user_id, "course_id": course_id, "enrolled": False}

    # ---- ops -------------------------------------------------------------- #
    @router.get("/origins/health")
    async def origins_health():
        docs = await mongo.db["origin_health"].find({}).sort("_id", 1).to_list(length=200)
        return {"items": [out(d) for d in docs]}

    return router
