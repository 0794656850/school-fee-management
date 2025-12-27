from __future__ import annotations

import os
from typing import Any, Dict

import requests
from requests.exceptions import RequestException


def _rasa_url() -> str:
    raw = os.environ.get("RASA_URL", "http://localhost:5005").rstrip("/")
    return raw or "http://localhost:5005"


def _timeout() -> float:
    try:
        return float(os.environ.get("RASA_TIMEOUT_SECONDS", "6"))
    except ValueError:
        return 6.0


def rasa_is_available() -> bool:
    url = f"{_rasa_url()}/status"
    try:
        resp = requests.get(url, timeout=_timeout())
        return resp.ok
    except RequestException:
        return False


def rasa_parse(question: str, sender_id: str | None = None) -> Dict[str, Any]:
    payload = {"text": question, "sender": sender_id or "guardian_portal"}
    resp = requests.post(f"{_rasa_url()}/model/parse", json=payload, timeout=_timeout())
    resp.raise_for_status()
    return resp.json()
