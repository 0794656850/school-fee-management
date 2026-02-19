from __future__ import annotations

import os
import time
from typing import Any, Dict

import requests
from requests.exceptions import RequestException


def _rasa_url() -> str:
    raw = os.environ.get("RASA_URL", "http://localhost:5005").rstrip("/")
    return raw or "http://localhost:5005"


def _timeout() -> float:
    try:
        return float(os.environ.get("RASA_TIMEOUT_SECONDS", "2.5"))
    except ValueError:
        return 2.5


_STATUS_CACHE: Dict[str, float | bool] = {"at": 0.0, "ok": False}


def rasa_is_available() -> bool:
    ttl = 45.0
    try:
        ttl = float(os.environ.get("RASA_STATUS_CACHE_SECONDS", "45"))
    except ValueError:
        ttl = 45.0
    now = time.monotonic()
    cached_at = float(_STATUS_CACHE.get("at") or 0.0)
    if now - cached_at <= ttl:
        return bool(_STATUS_CACHE.get("ok"))
    url = f"{_rasa_url()}/status"
    try:
        resp = requests.get(url, timeout=_timeout())
        ok = bool(resp.ok)
        _STATUS_CACHE["ok"] = ok
        _STATUS_CACHE["at"] = now
        return ok
    except RequestException:
        _STATUS_CACHE["ok"] = False
        _STATUS_CACHE["at"] = now
        return False


def rasa_parse(question: str, sender_id: str | None = None) -> Dict[str, Any]:
    payload = {"text": question, "sender": sender_id or "guardian_portal"}
    resp = requests.post(f"{_rasa_url()}/model/parse", json=payload, timeout=_timeout())
    resp.raise_for_status()
    return resp.json()
