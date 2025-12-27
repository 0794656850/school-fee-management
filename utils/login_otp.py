from __future__ import annotations

import secrets
from datetime import datetime
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from flask import current_app
from typing import Optional

try:
    from utils.gmail_api import send_email, send_email_html
except Exception:
    def send_email(*args, **kwargs):  # type: ignore
        return False

    def send_email_html(*args, **kwargs):  # type: ignore
        return False


OTP_DIGITS = "0123456789"
OTP_LENGTH = 6


def generate_login_otp(length: int = OTP_LENGTH) -> str:
    """Return a secure numeric OTP of the requested length."""
    digits = []
    for _ in range(length):
        digits.append(secrets.choice(OTP_DIGITS))
    return "".join(digits)


def mask_email(email: Optional[str]) -> str:
    """Obfuscate the local part of an email for display."""
    if not email or "@" not in email:
        return email or ""
    local, domain = email.split("@", 1)
    if len(local) <= 2:
        local_masked = local[0] + "*"
    else:
        local_masked = local[0] + "*" * (len(local) - 2) + local[-1]
    return f"{local_masked}@{domain}"


def _smtp_send_one_time_code(to_email: str, subject: str, text_body: str, html_body: str | None) -> bool:
    cfg = current_app.config
    server = cfg.get("MAIL_SERVER")
    if not server:
        return False
    port = cfg.get("MAIL_PORT", 587)
    use_tls = cfg.get("MAIL_USE_TLS", True)
    use_ssl = cfg.get("MAIL_USE_SSL", False)
    username = cfg.get("MAIL_USERNAME")
    password = cfg.get("MAIL_PASSWORD")
    sender = cfg.get("MAIL_SENDER") or cfg.get("MAIL_DEFAULT_SENDER") or username
    if not sender:
        return False

    parts = []
    parts.append(MIMEText(text_body or "", "plain", "utf-8"))
    if html_body:
        parts.append(MIMEText(html_body, "html", "utf-8"))

    try:
        if use_ssl:
            smtp = smtplib.SMTP_SSL(server, port or 465, context=ssl.create_default_context(), timeout=20)
        else:
            smtp = smtplib.SMTP(server, port or 587, timeout=20)
            if use_tls:
                smtp.starttls(context=ssl.create_default_context())
        if username and password:
            smtp.login(username, password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject or ""
        msg["From"] = sender
        msg["To"] = to_email
        for part in parts:
            msg.attach(part)

        smtp.sendmail(sender, [to_email], msg.as_string())
        smtp.quit()
        return True
    except Exception:
        return False


def send_portal_login_otp(
    target_email: str,
    recipient_label: str,
    portal_name: str,
    otp_code: str,
    expires_minutes: int = 20,
) -> bool:
    """Send a branded OTP message to the recipient."""
    if not target_email:
        return False
    subject = f"{portal_name} login code"
    html = f"""
<p>Hello {recipient_label or 'guardian'},</p>
<p>Your secure code for <strong>{portal_name}</strong> is <strong>{otp_code}</strong>.</p>
<p>This code expires in {expires_minutes} minutes and can only be used once.</p>
<p>If you didn't request access, ignore this message.</p>
<p class="text-xs text-gray-400" style="font-size:.75rem;">Sent on {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}</p>
"""
    text = (
        f"Hello {recipient_label or 'guardian'},\n"
        f"Your {portal_name} login code is {otp_code}. "
        f"It expires in {expires_minutes} minutes."
    )
    sent = send_email_html(target_email, subject, html)
    if not sent:
        sent = send_email(target_email, subject, text)
    if not sent:
        sent = _smtp_send_one_time_code(target_email, subject, text, html)
    return bool(sent)
