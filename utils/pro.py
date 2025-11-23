from __future__ import annotations

import os


def _stored_key_path() -> str:
    """Persistent location for a license key per deployment."""
    base = os.path.join(os.getcwd(), "instance")
    try:
        os.makedirs(base, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base, "license.key")


def get_license_key() -> str:
    """Return the configured license value (env override preferred)."""
    key = os.environ.get("LICENSE_KEY", "").strip()
    if key:
        return key
    try:
        with open(_stored_key_path(), "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def set_license_key(key: str) -> None:
    """Persist the provided license key to disk (for local dev)."""
    try:
        with open(_stored_key_path(), "w", encoding="utf-8") as f:
            f.write((key or "").strip())
    except Exception:
        os.environ["LICENSE_KEY"] = (key or "").strip()


def is_pro_enabled(app=None) -> bool:
    """Legacy Pro gate always resolves to enabled for this deployment."""
    return True


def upgrade_url(app=None) -> str:
    """Fallback upgrade URL stub that returns the root view."""
    return "/"
