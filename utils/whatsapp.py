from __future__ import annotations

from typing import Tuple, List, Dict, Any

import requests
from flask import current_app

from utils.settings import get_setting


def _wa_config() -> Tuple[str | None, str | None]:
    cfg = current_app.config
    # Prefer DB-backed settings then fall back to in-memory config
    token = get_setting("WHATSAPP_ACCESS_TOKEN") or cfg.get("WHATSAPP_ACCESS_TOKEN")
    phone_id = get_setting("WHATSAPP_PHONE_NUMBER_ID") or cfg.get("WHATSAPP_PHONE_NUMBER_ID")
    return token, phone_id


def whatsapp_is_configured() -> Tuple[bool, str | None]:
    token, phone_number_id = _wa_config()
    if not token or not phone_number_id:
        return (
            False,
            "WhatsApp Cloud API is not configured. Set WHATSAPP_ACCESS_TOKEN and WHATSAPP_PHONE_NUMBER_ID.",
        )
    # Basic sanity check
    if (not token.startswith("EAAG")) and len(token) < 10:
        return False, "WhatsApp token appears invalid. Provide a valid access token."
    return True, None


def _endpoint() -> str:
    _, phone_number_id = _wa_config()
    return f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"


def _headers() -> Dict[str, str]:
    token, _ = _wa_config()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _digits_only(number: str) -> str:
    return "".join(ch for ch in str(number) if ch.isdigit())


def send_whatsapp_text(to: str, body: str) -> Tuple[bool, str | None]:
    ok, reason = whatsapp_is_configured()
    if not ok:
        return False, reason
    payload = {
        "messaging_product": "whatsapp",
        "to": _digits_only(to),
        "type": "text",
        "text": {"body": body},
    }
    try:
        r = requests.post(_endpoint(), headers=_headers(), json=payload, timeout=20)
        if 200 <= r.status_code < 300:
            return True, None
        return False, f"HTTP {r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)


def send_whatsapp_template(
    to: str,
    template_name: str,
    language: str = "en_US",
    body_parameters: List[str] | None = None,
) -> Tuple[bool, str | None]:
    ok, reason = whatsapp_is_configured()
    if not ok:
        return False, reason

    components: List[Dict[str, Any]] = []
    if body_parameters:
        components.append(
            {
                "type": "body",
                "parameters": [{"type": "text", "text": str(v)} for v in body_parameters],
            }
        )

    payload = {
        "messaging_product": "whatsapp",
        "to": _digits_only(to),
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
            "components": components,
        },
    }
    try:
        r = requests.post(_endpoint(), headers=_headers(), json=payload, timeout=20)
        if 200 <= r.status_code < 300:
            return True, None
        return False, f"HTTP {r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)
