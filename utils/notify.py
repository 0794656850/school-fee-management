from __future__ import annotations

from typing import Tuple
from flask import current_app


def normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    phone = str(raw).strip()
    if phone.startswith("+"):
        return phone
    cc = current_app.config.get("DEFAULT_COUNTRY_CODE", "+254")
    if phone.startswith("0"):
        return f"{cc}{phone[1:]}"
    digits = "".join(ch for ch in phone if ch.isdigit())
    if 9 <= len(digits) <= 10:
        return f"{cc}{digits[-9:]}"
    return phone


"""
Utility helpers for notifications.

Currently only provides `normalize_phone`. Twilio SMS support has been
removed; WhatsApp Cloud API is used via utils.whatsapp.
"""

# Note: Twilio-related helpers were intentionally removed to decouple the
# system from SMS until WhatsApp credentials are provided.
