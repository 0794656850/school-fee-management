from __future__ import annotations

from typing import Iterable, Tuple, Dict, Any

import requests
from flask import current_app


def _cfg(key: str, default: str = "") -> str:
    try:
        return (current_app.config.get(key) or default).strip()
    except Exception:
        return default


def ujumbe_sms_configured() -> bool:
    return bool(_cfg("UJUMBE_SMS_API_KEY") and _cfg("UJUMBE_EMAIL"))


def _normalize_numbers(numbers: str | Iterable[str]) -> str:
    if isinstance(numbers, (list, tuple, set)):
        cleaned = [str(n).strip() for n in numbers if str(n).strip()]
        return ",".join(cleaned)
    return str(numbers).strip()


def send_ujumbe_sms(numbers: str | Iterable[str], message: str, sender: str | None = None) -> Tuple[bool, Dict[str, Any]]:
    api_key = _cfg("UJUMBE_SMS_API_KEY")
    email = _cfg("UJUMBE_EMAIL")
    url = _cfg("UJUMBE_API_URL", "https://ujumbesms.co.ke/api/messaging")
    sender_id = (sender or _cfg("UJUMBE_SENDER_ID", "UjumbeSMS")).strip()
    if not (api_key and email and url and message):
        return False, {"error": "Missing UjumbeSMS configuration or empty message."}

    numbers_str = _normalize_numbers(numbers)
    if not numbers_str:
        return False, {"error": "No recipient numbers."}

    payload = {
        "data": [
            {
                "message_bag": {
                    "numbers": numbers_str,
                    "message": message,
                    "sender": sender_id,
                }
            }
        ]
    }
    headers = {
        "X-Authorization": api_key,
        "Email": email,
        "Content-Type": "application/json",
    }
    try:
        timeout = int(_cfg("UJUMBE_TIMEOUT", "15") or "15")
    except Exception:
        timeout = 15
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except Exception:
        return False, {"error": "Network error while contacting UjumbeSMS."}
    if not resp.ok:
        return False, {"error": f"HTTP {resp.status_code}"}
    try:
        data = resp.json() or {}
        status = data.get("status") or {}
        status_type = str(status.get("type") or "").lower()
        ok = status_type == "success" or resp.ok
        meta = data.get("meta") or {}
        info = {
            "type": status_type,
            "code": status.get("code"),
            "description": status.get("description"),
            "recipients": meta.get("recipients"),
            "credits_deducted": meta.get("credits_deducted"),
            "available_credits": meta.get("available_credits"),
            "user": meta.get("user"),
        }
        return ok, info
    except Exception:
        return True, {"description": "Queued (unparsed response)"}
