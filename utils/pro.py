from __future__ import annotations

import hashlib
import os
from typing import Tuple


def _stored_key_path() -> str:
    # Store under instance/ to avoid source control; ensure folder exists
    base = os.path.join(os.getcwd(), "instance")
    try:
        os.makedirs(base, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base, "license.key")


def get_license_key() -> str:
    # Priority: ENV -> file
    key = os.environ.get("LICENSE_KEY", "").strip()
    if key:
        return key
    try:
        with open(_stored_key_path(), "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def set_license_key(key: str) -> None:
    try:
        with open(_stored_key_path(), "w", encoding="utf-8") as f:
            f.write((key or "").strip())
    except Exception:
        # Fallback to env var in memory (non-persistent)
        os.environ["LICENSE_KEY"] = key or ""


def is_pro_enabled(app=None) -> bool:
    # Extremely light validation: key starts with prefix and checksum matches pattern
    key = (getattr(app, "config", {}).get("LICENSE_KEY") if app else None) or get_license_key()
    key = (key or "").strip()
    if not key:
        return False
    if not key.upper().startswith("CS-PRO-"):
        return False
    # Expected suffix: 6 hex chars based on simple hash of prefix body
    body = key.split("CS-PRO-", 1)[-1].split("-", 1)[0]
    h = hashlib.sha1(body.encode("utf-8")).hexdigest()[:6].upper()
    return h in key.upper()


def upgrade_url(app=None) -> str:
    if app and hasattr(app, "config"):
        return app.config.get("BILLING_UPGRADE_URL") or "https://example.com/upgrade"
    return os.environ.get("BILLING_UPGRADE_URL", "https://example.com/upgrade")

