from __future__ import annotations

from typing import Tuple
from flask import current_app


def normalize_phone(raw: str | None) -> str | None:
    if not raw:
        return None
    phone = str(raw).strip()
    cc = str(current_app.config.get("DEFAULT_COUNTRY_CODE", "254") or "254").strip()
    cc_digits = "".join(ch for ch in cc if ch.isdigit()) or "254"
    if phone.startswith("+"):
        phone = phone[1:]
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        return None
    if digits.startswith(cc_digits):
        return digits
    if phone.startswith("0"):
        return f"{cc_digits}{phone[1:]}"
    if 9 <= len(digits) <= 10:
        return f"{cc_digits}{digits[-9:]}"
    return digits


"""
Utility helpers for notifications.

Currently only provides `normalize_phone`. Twilio SMS support has been
removed; WhatsApp Cloud API is used via utils.whatsapp.
"""

# Note: Twilio-related helpers were intentionally removed to decouple the
# system from SMS until WhatsApp credentials are provided.
