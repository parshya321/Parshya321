"""
origin_security.py - Shared helpers for origin URLs and signed-URL signatures.

Used by main.py (issuing signed URLs), admin.py (validating registered origins) and
health_check.py (signing probe requests), so the three can never disagree.

Environment
-----------
ALLOWED_ORIGIN_HOSTS   Comma-separated host suffixes your origin nodes live under,
                       e.g. "cdn.example.com,storage.example.net". Origins outside this
                       list are rejected. Unset = every registration is refused (fail closed),
                       which stops an admin account (or a stolen admin token) from pointing
                       the health checker at internal addresses (SSRF).
"""
from __future__ import annotations

import hashlib
import hmac
import os
from urllib.parse import urlparse


def signature(key: str, path_prefix: str, expires: int) -> str:
    """HMAC-SHA256 over '<path_prefix>|<expires>' - the scheme your CDN edge verifies."""
    msg = f"{path_prefix}|{expires}".encode()
    return hmac.new(key.encode(), msg, hashlib.sha256).hexdigest()


def allowed_origin_suffixes() -> list[str]:
    return [s.strip().lower() for s in os.getenv("ALLOWED_ORIGIN_HOSTS", "").split(",") if s.strip()]


def host_allowed(host: str | None) -> bool:
    host = (host or "").lower()
    return any(host == s or host.endswith("." + s) for s in allowed_origin_suffixes())


def validate_origin_base_url(base_url: str) -> str:
    """Return the normalised base URL (trailing slash) or raise ValueError."""
    u = urlparse((base_url or "").strip())
    if u.scheme != "https":
        raise ValueError("base_url must use https")
    if not u.hostname:
        raise ValueError("base_url has no host")
    if u.username or u.password or u.query or u.fragment:
        raise ValueError("base_url must not contain credentials, a query string or a fragment")
    if not allowed_origin_suffixes():
        raise ValueError("ALLOWED_ORIGIN_HOSTS is not configured on the server")
    if not host_allowed(u.hostname):
        raise ValueError(f"host '{u.hostname}' is not in ALLOWED_ORIGIN_HOSTS")
    return base_url.strip().rstrip("/") + "/"


def validate_manifest_path(path: str) -> str:
    p = (path or "").strip().lstrip("/")
    if not p.endswith(".m3u8"):
        raise ValueError("manifest_path must end with .m3u8")
    if ".." in p.split("/") or "://" in p or "?" in p or "#" in p:
        raise ValueError("manifest_path must be a plain relative path")
    return p
