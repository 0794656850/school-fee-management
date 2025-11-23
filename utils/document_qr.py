from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Mapping

from flask import current_app


def build_document_qr(kind: str, payload: Mapping[str, Any], secret: str | None = None) -> str:
    """Build a signed QR payload that documents can embed for authenticity checks."""
    try:
        secret_key = (
            secret
            or current_app.secret_key
            or current_app.config.get("SECRET_KEY")
            or "secret123"
        )
        data = dict(payload or {})
        data["t"] = kind
        canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        sig = hmac.new(secret_key.encode("utf-8"), canon, hashlib.sha256).hexdigest()[:20]
        data["sig"] = sig
        return json.dumps(data, separators=(",", ":"))
    except Exception:
        return ""
