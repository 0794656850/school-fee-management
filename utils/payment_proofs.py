from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import re
from datetime import datetime as _dt
from flask import current_app, url_for
from werkzeug.utils import secure_filename

from routes.term_routes import (
    ensure_discounts_table,
    ensure_invoices_tables,
    ensure_student_fee_items_table,
    ensure_term_fees_table,
)
from utils.gmail_api import send_email as gmail_send_email, send_email_html as gmail_send_email_html
from utils.notify import normalize_phone

ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".pdf"}
STATUS_LABELS = {
    "pending": "Pending Review",
    "in_review": "In Review",
    "verified": "Verified",
    "accepted": "Verified",
    "rejected": "Rejected",
}


def allowed_proof_file(filename: str) -> bool:
    if not filename:
        return False
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def _proof_upload_paths(school_id: int) -> tuple[Path, str]:
    relative = current_app.config.get("PAYMENT_PROOF_UPLOADS_DIR") or current_app.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR") or "uploads/payment_proofs"
    root = Path(current_app.root_path) / "static" / relative
    target = root / str(school_id)
    target.mkdir(parents=True, exist_ok=True)
    return target, relative


def save_payment_proof_file(file, school_id: int) -> str:
    if not allowed_proof_file(file.filename or ""):
        raise ValueError("Unsupported file type")
    target, relative = _proof_upload_paths(school_id)
    ext = Path(file.filename or "").suffix.lower()
    if not ext:
        ext = ".bin"
    filename = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}{ext}"
    safe_name = secure_filename(filename)
    path = target / safe_name
    file.save(path)
    rel_path = Path(relative) / str(school_id) / safe_name
    return rel_path.as_posix()


def format_status_label(status: str | None) -> str:
    if not status:
        return "Pending Review"
    return STATUS_LABELS.get(status.lower(), status.replace("_", " ").title())


def notify_parent_about_proof(
    *,
    proof: dict,
    student_name: str,
    status: str,
    reason: str | None = None,
) -> None:
    email = (proof.get("guardian_email") or "").strip()
    phone = (proof.get("guardian_phone") or "").strip()
    parent_name = proof.get("guardian_name") or "Parent"
    label = format_status_label(status)
    amount = proof.get("amount")
    amount_str = f"KES {float(amount):,.2f}" if amount not in (None, "") else "an amount"
    subject = f"Payment proof {label} for {student_name}"
    portal_url = url_for("guardian.guardian_login", _external=True)
    message_body = f"""
    Hi {parent_name},

    Your payment proof for {student_name} ({amount_str}) is now {label}.
    """
    if reason:
        message_body += f"\n\nReason: {reason}\n"
    message_body += f"\nView the status in the Parent Portal: {portal_url}\n"
    plain_body = "\n".join(line.strip() for line in message_body.splitlines() if line.strip())
    html_body = (
        f"<p>Hi {parent_name},</p>"
        f"<p>Your payment proof for <strong>{student_name}</strong> ({amount_str}) is now <strong>{label}</strong>.</p>"
    )
    if reason:
        html_body += f"<p><strong>Reason:</strong> {reason}</p>"
    html_body += f"<p>Visit the <a href=\"{portal_url}\">Parent Portal</a> to view the current status.</p>"
    try:
        if email:
            if not gmail_send_email_html(email, subject, html_body):
                gmail_send_email(email, subject, plain_body)
    except Exception:
        pass

    def _send_sms(dest: str) -> None:
        normalized = normalize_phone(dest)
        if not normalized:
            return
        body = f"{student_name} payment proof is now {label}. {reason or ''} View {portal_url}"
        send_twilio_sms(normalized, body)

    if phone:
        _send_sms(phone)


def send_twilio_sms(to_number: str, message: str) -> bool:
    sid = current_app.config.get("TWILIO_ACCOUNT_SID")
    token = current_app.config.get("TWILIO_AUTH_TOKEN")
    sender = current_app.config.get("TWILIO_PHONE_NUMBER")
    if not (sid and token and sender and to_number):
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
    try:
        resp = requests.post(
            url,
            data={"To": to_number, "From": sender, "Body": message},
            auth=(sid, token),
            timeout=15,
        )
        return resp.ok
    except Exception:
        return False


def calculate_expected_invoice_total(db, student_id: int, year: int, term: int) -> float:
    ensure_student_fee_items_table(db)
    ensure_term_fees_table(db)
    ensure_discounts_table(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT COALESCE(SUM(amount),0) AS total FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s",
        (student_id, year, term),
    )
    row = cur.fetchone() or {}
    total = float(row.get("total") or 0)
    if total <= 0:
        cur.execute(
            "SELECT COALESCE(SUM(fee_amount),0) AS total FROM term_fees WHERE student_id=%s AND year=%s AND term=%s",
            (student_id, year, term),
        )
        row = cur.fetchone() or {}
        total = float(row.get("total") or 0)
    if total <= 0:
        return 0.0
    cur.execute(
        "SELECT kind, value FROM discounts WHERE student_id=%s AND year=%s AND term=%s LIMIT 1",
        (student_id, year, term),
    )
    disc = cur.fetchone() or {}
    if disc:
        kind = (disc.get("kind") or "").lower()
        value = float(disc.get("value") or 0)
        if kind == "percent":
            total = max(total - round(total * (value / 100.0), 2), 0.0)
        else:
            total = max(total - value, 0.0)
    return total


def _look_for_amount(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(r"\b(?:KES|KES\.|Ksh|Ksh\.|Shs|Sh)\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if matches:
        return matches[0]
    matches = re.findall(r"\b([\d,]{3,}(?:\.\d+)?)\b", text)
    return matches[0] if matches else None


def _look_for_date(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})", text)
    if matches:
        return matches[0]
    matches = re.findall(r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})", text, re.IGNORECASE)
    return matches[0] if matches else None


def _look_for_bank(text: str) -> str | None:
    if not text:
        return None
    keywords = ["mpesa", "equity", "kcb", "dt", "fahari", "barclays", "stanbic", "co-operative", "bank"]
    for word in keywords:
        if word.lower() in text.lower():
            return word.title()
    return None


def extract_proof_metadata(file_path: str, fallback_text: str | None = None) -> dict:
    text_chunks = []
    if fallback_text:
        text_chunks.append(fallback_text)
    if file_path:
        abs_path = os.path.join(current_app.static_folder or os.path.join(current_app.root_path, "static"), file_path)
        try:
            suffix = Path(file_path).suffix.lower()
            if suffix in {".png", ".jpg", ".jpeg"}:
                try:
                    from PIL import Image
                    import pytesseract

                    img = Image.open(abs_path)
                    text_chunks.append(pytesseract.image_to_string(img, lang="eng"))
                except Exception:
                    pass
            elif suffix == ".pdf":
                try:
                    import fitz

                    doc = fitz.open(abs_path)
                    for page in doc:
                        text_chunks.append(page.get_text())
                except Exception:
                    pass
        except Exception:
            pass
    text_blob = "\n".join(c for c in text_chunks if c)
    amount = _look_for_amount(text_blob)
    date = _look_for_date(text_blob)
    bank = _look_for_bank(text_blob or fallback_text or "")
    return {
        "raw": text_blob,
        "amount": amount,
        "date": date,
        "bank": bank,
    }


def ensure_student_invoice(
    db,
    student_id: int,
    year: int,
    term: int,
    fallback_amount: float | None = None,
) -> Optional[int]:
    ensure_invoices_tables(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT id FROM invoices WHERE student_id=%s AND year=%s AND term=%s",
        (student_id, year, term),
    )
    row = cur.fetchone()
    if row:
        return int(row.get("id"))
    total = calculate_expected_invoice_total(db, student_id, year, term)
    if total <= 0 and fallback_amount:
        total = fallback_amount
    total = float(total or 0.0)
    ins = db.cursor()
    ins.execute(
        "INSERT INTO invoices (student_id, year, term, status, total) VALUES (%s,%s,%s,%s,%s)",
        (student_id, year, term, "draft", total),
    )
    return ins.lastrowid


def set_invoice_status(db, invoice_id: int, status: str) -> None:
    cur = db.cursor()
    cur.execute("UPDATE invoices SET status=%s WHERE id=%s", (status, invoice_id))
