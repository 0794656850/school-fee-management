from __future__ import annotations

import hashlib
import secrets
from datetime import datetime
from typing import Iterable

from utils.gmail_api import send_email


def generate_otp(digits: int = 6) -> str:
    """Random numeric OTP used for sensitive approvals."""
    alphabet = "0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(digits))


def hash_otp(code: str) -> str:
    return hashlib.sha256((code or "").strip().encode("utf-8")).hexdigest()


def send_otp_email(to: str, otp: str) -> bool:
    if not to or not otp:
        return False
    subject = "Your approval request OTP"
    body = (
        f"Your one-time code to confirm the approval request is {otp}. "
        "This code expires in 10 minutes and is required before the request appears for review."
    )
    return send_email(to, subject, body)


def send_alert_email(subject: str, body: str, recipients: Iterable[str]) -> dict[str, bool]:
    successes: dict[str, bool] = {}
    for recipient in recipients:
        if not recipient:
            continue
        try:
            sent = send_email(recipient, subject, body)
        except Exception:
            sent = False
        successes[recipient] = sent
    return successes
