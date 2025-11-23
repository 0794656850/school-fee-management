#!/usr/bin/env python3
# ADD TO APP: app.register_blueprint(billing_bp)

from __future__ import annotations

import os
import hmac
import base64
import uuid
import hashlib
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

from flask import (
    Blueprint,
    request,
    jsonify,
    current_app,
    url_for,
    render_template_string,
)
from werkzeug.utils import secure_filename

# Optional import of school settings for emailing school as well
try:  # avoid hard dependency if utils not available
    from utils.settings import get_setting as _get_setting  # type: ignore
except Exception:  # pragma: no cover
    def _get_setting(key: str, default: Optional[str] = None):  # type: ignore
        return default

try:
    from extensions import db  # type: ignore
except Exception:  # pragma: no cover
    from flask_sqlalchemy import SQLAlchemy  # type: ignore

    db = SQLAlchemy()


# -----------------------------
# Blueprint
# -----------------------------

billing_bp = Blueprint("billing", __name__, url_prefix="/billing")


# -----------------------------
# Config / Constants
# -----------------------------

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads", "receipts")
ALLOWED_MIME = {"image/png", "image/jpeg", "image/jpg", "application/pdf"}
ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".pdf"}
MAX_SIZE_BYTES = 5 * 1024 * 1024  # 5MB


def _require_secret() -> str:
    secret = os.getenv("LICENSE_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "LICENSE_SECRET is not set. Please set LICENSE_SECRET to a long random string in your environment."
        )
    return secret


def _base_url() -> str:
    env_base = (os.getenv("BASE_URL") or "").strip().rstrip("/")
    if env_base:
        return env_base
    try:
        return request.host_url.strip().rstrip("/")
    except Exception:
        return "http://localhost:5000"


def _admin_email() -> str:
    return os.getenv("ADMIN_EMAIL", "").strip()


def _school_email() -> str:
    # Prefer configured SCHOOL_EMAIL setting, fallback to ADMIN_EMAIL
    try:
        val = (_get_setting("SCHOOL_EMAIL") or "").strip()
        if val:
            return val
    except Exception:
        pass
    return _admin_email()


def _smtp_config() -> Dict[str, Any]:
    return {
        "host": os.getenv("SMTP_HOST", "").strip(),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER", "").strip(),
        "pass": os.getenv("SMTP_PASS", "").strip(),
        "from": os.getenv("FROM_EMAIL", os.getenv("SMTP_USER", "")).strip(),
        "use_tls": (os.getenv("SMTP_USE_TLS", "1").strip().lower() not in ("0", "false", "no")),
    }


def _inbound_secret() -> str:
    return os.getenv("EMAIL_INBOUND_SECRET", "").strip()


def _log(*args):
    print("[billing]", *args)


# -----------------------------
# Models (add to your migrations)
# -----------------------------

# TODO: Add these models to your migrations. If you maintain a central models.py, move them there and import here.


class LicenseRequest(db.Model):
    __tablename__ = "license_requests"

    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_name = db.Column(db.String(120), nullable=False)
    user_email = db.Column(db.String(255), nullable=False, index=True)
    receipt_filename = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    message = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(20), nullable=False, default="PENDING")  # PENDING/VERIFIED/REJECTED/ACTIVATED
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    verify_token = db.Column(db.String(36), unique=True, index=True)
    verify_expires = db.Column(db.DateTime, nullable=False)
    admin_note = db.Column(db.Text, nullable=True)
    admin_verified_by = db.Column(db.String(120), nullable=True)


class LicenseKey(db.Model):
    __tablename__ = "license_keys"

    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    request_id = db.Column(db.String(36), db.ForeignKey("license_requests.id"), nullable=False, index=True)
    user_email = db.Column(db.String(255), nullable=False, index=True)
    license_key = db.Column(db.String(255), nullable=False, unique=True, index=True)
    issued_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=True)  # NULL == lifetime
    signature = db.Column(db.String(64), nullable=False)  # hex signature
    active = db.Column(db.Boolean, default=False, nullable=False)


def _issue_license_for_request(req: "LicenseRequest", admin_name: str, admin_note: Optional[str], expires_at: Optional[datetime]) -> "LicenseKey":
    """Issue a license for a request, email the user (not activated yet). Returns LicenseKey."""
    secret = _require_secret()
    license_key, signature, payload = generate_license_key(req.user_email, expires_at, secret)
    lic = LicenseKey(
        request_id=req.id,
        user_email=req.user_email,
        license_key=license_key,
        issued_at=datetime.utcnow(),
        expires_at=expires_at,
        signature=signature,
        active=False,
    )
    db.session.add(lic)

    req.status = "VERIFIED"
    req.admin_note = admin_note
    req.admin_verified_by = admin_name
    req.verify_token = None
    req.verify_expires = datetime.utcnow() - timedelta(seconds=1)
    db.session.commit()

    base = _base_url()
    subject_user = "Your SmartEduPay Premium License"
    expires_label = "Lifetime" if not expires_at else expires_at.strftime("%Y-%m-%d")
    body_user = f"""
    <p>Thank you {req.user_name},</p>
    <p>Your payment was verified. Here are your license details:</p>
    <p><strong>License Key:</strong> <code>{license_key}</code><br/>
    <strong>Expires:</strong> {expires_label}</p>
    <p>To activate:</p>
    <ol>
      <li>Go to <a href="{base}/admin/billing">{base}/admin/billing</a> and paste the key, or</li>
      <li>Open <a href="{base}/activate">{base}/activate</a> and follow the steps below.</li>
    </ol>
    <p>Keep this key private.</p>
    """
    _send_email(req.user_email, subject_user, body_user)

    # Also notify the school email with the license details
    try:
        school_email = _school_email()
        if school_email:
            _send_email(
                school_email,
                f"[SmartEduPay] License issued for {req.user_email}",
                f"<p>Issued license for {req.user_email}.<br><strong>Key:</strong> {license_key}<br><strong>Expires:</strong> {expires_label}</p>",
            )
    except Exception:
        pass
    return lic


def _activate_license_for_user(email: str, lic: "LicenseKey") -> None:
    """Mark license active and set user's premium flag if model exists."""
    lic.active = True
    try:
        from models import User  # type: ignore
    except Exception:
        try:
            from app.models import User  # type: ignore
        except Exception:
            User = None  # type: ignore

    if User is not None:
        user_obj = User.query.filter_by(email=email).first()
        if user_obj is not None and hasattr(user_obj, "is_premium"):
            setattr(user_obj, "is_premium", True)
    # Also mark original request as ACTIVATED if present
    try:
        req = LicenseRequest.query.filter_by(id=lic.request_id).first()
        if req:
            req.status = "ACTIVATED"
    except Exception:
        pass
    db.session.commit()


# -----------------------------
# Helpers: license key generate/verify
# -----------------------------


def _to_base36(n: int) -> str:
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if n == 0:
        return "0"
    digits = []
    x = abs(n)
    while x:
        x, rem = divmod(x, 36)
        digits.append(chars[rem])
    return "".join(reversed(digits))


def _email_hash_segment(email: str) -> str:
    h = hashlib.sha1((email or "").lower().encode("utf-8")).hexdigest()
    return _to_base36(int(h[:12], 16)).upper()[:6]


def _rand_nonce_seg() -> str:
    r = int.from_bytes(os.urandom(5), "big")  # 40 bits
    s = _to_base36(r).upper()
    return s.zfill(8)[-8:]


def _expiry_segment(expires_at: Optional[datetime]) -> str:
    if not expires_at:
        return "LIFE"
    return expires_at.strftime("%Y%m%d")


def generate_license_key(user_email: str, expires_at: Optional[datetime], secret: str) -> Tuple[str, str, str]:
    """
    Returns (license_key, signature_hex, payload_used_for_signature).
    Payload: f"{user_email}|{issued_at_iso}|{expires_or_LIFETIME}|{nonce}"
    Key: "{rand8}-{hash6}-{EXPSEG}-{sig16}"
    """
    issued_at = datetime.utcnow()
    nonce = _rand_nonce_seg()
    exp_str = "LIFETIME" if not expires_at else expires_at.isoformat()
    payload = f"{user_email.lower()}|{issued_at.isoformat()}|{exp_str}|{nonce}"
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    key = f"{nonce}-{_email_hash_segment(user_email)}-{_expiry_segment(expires_at)}-{sig[:16].upper()}"
    return key, sig, payload


def verify_license_key(license_key: str, secret: str, issued_at: datetime, expires_at: Optional[datetime], user_email: str) -> Dict[str, Any]:
    """
    Verify signature using DB-known issued_at and expires_at.
    Returns dict with parsed fields on success; raises ValueError on failure.
    """
    if not license_key or "-" not in license_key:
        raise ValueError("Malformed license key.")
    parts = license_key.strip().split("-")
    if len(parts) != 4:
        raise ValueError("Invalid license key format.")
    nonce, hash6, expseg, sig16 = parts
    exp_str = "LIFETIME" if not expires_at else expires_at.isoformat()
    payload = f"{user_email.lower()}|{issued_at.isoformat()}|{exp_str}|{nonce}"
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if sig[:16].upper() != sig16.upper():
        raise ValueError("Invalid license signature.")
    if _email_hash_segment(user_email).upper() != hash6.upper():
        raise ValueError("License key does not match email.")
    return {"nonce": nonce, "email_hash": hash6, "expires_segment": expseg, "signature_prefix": sig16, "payload": payload, "signature": sig}


# -----------------------------
# Email
# -----------------------------


def _send_email(to_email: str, subject: str, html_body: str, inline_image: Optional[bytes] = None) -> None:
    cfg = _smtp_config()
    if not (cfg["host"] and cfg["from"] and to_email):
        _log("SMTP not fully configured or missing recipient. Skipping email.", cfg)
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = to_email
    msg.set_content("HTML email is required to view this message.")
    msg.add_alternative(html_body, subtype="html")
    if inline_image:
        img_cid = "preview@inline"
        msg.get_payload()[1].add_related(inline_image, maintype="image", subtype="png", cid=f"<{img_cid}>")
    try:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
            if cfg["use_tls"]:
                server.starttls()
            if cfg["user"] and cfg["pass"]:
                server.login(cfg["user"], cfg["pass"])
            server.send_message(msg)
        _log("Email sent:", subject, "->", to_email)
    except Exception as e:  # pragma: no cover
        _log("Failed sending email:", e)


def _strip_quoted_reply(text: str) -> str:
    """Return the top portion of an email reply, ignoring quoted history.
    Removes lines starting with '>' and content after common reply separators.
    """
    if not text:
        return ""
    lines = []
    for raw in text.splitlines():
        line = raw.strip("\r\n")
        # Stop at common separators
        lower = line.lower()
        if lower.startswith("on ") and lower.endswith("wrote:"):
            break
        if lower.startswith("from:") or lower.startswith("sent:") or lower.startswith("subject:"):
            # likely header section in reply
            break
        if line.startswith(">"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _parse_simple_yes_no(text: str) -> Optional[str]:
    """Parse a simple yes/no decision from the top of a reply body.
    Returns 'yes', 'no', or None if undecided.
    """
    if not text:
        return None
    cleaned = _strip_quoted_reply(text)
    if not cleaned:
        return None
    # Look at the first non-empty word
    for token in cleaned.replace("\r", "\n").split():
        t = token.strip().strip(".,!?:;()[]{}\"' ")
        if not t:
            continue
        tl = t.lower()
        if tl in ("y", "yes", "approve", "approved", "accept"):
            return "yes"
        if tl in ("n", "no", "reject", "rejected", "decline"):
            return "no"
        # Only consider the first token for simplicity
        break
    return None


# -----------------------------
# Minimal HTML Templates (inline)
# -----------------------------


ADMIN_VERIFY_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Verify Payment · Premium</title>
  <style>
    body { font-family: system-ui, Arial, sans-serif; margin: 2rem; }
    .card { border: 1px solid #e5e7eb; border-radius: 12px; padding: 1.5rem; max-width: 800px; }
    .row { display: flex; gap: 2rem; align-items: flex-start; }
    .col { flex: 1; }
    .muted { color: #6b7280; font-size: 0.9rem; }
    .btn { padding: 0.6rem 1rem; border-radius: 8px; border: 1px solid #d1d5db; background: #111827; color: #fff; cursor: pointer; }
    .btn.secondary { background: #fff; color: #111827; }
    .grid { display: grid; grid-template-columns: 180px 1fr; gap: 8px 16px; }
    input, select, textarea { width: 100%; padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 8px; }
    img { max-width: 100%; border: 1px solid #d1d5db; border-radius: 10px; }
    .label { color: #374151; font-weight: 600; }
  </style>
  <script>
    function rejectNoteToggle(){
      const sel = document.querySelector('select[name="action"]').value;
      const lbl = document.getElementById('lblNote');
      lbl.textContent = sel === 'reject' ? 'Reject Note' : 'Admin Note';
    }
  </script>
  </head>
<body>
  <div class="card">
    <h2>Verify Payment Receipt</h2>
    {% if invalid %}
      <p class="muted">This verification link is invalid or has expired.</p>
      <p><a href="{{ base_url }}/admin" class="btn secondary">Go to Admin Dashboard</a></p>
    {% else %}
      <div class="row">
        <div class="col">
          <h3>Request Details</h3>
          <div class="grid">
            <div class="label">Name</div><div>{{ req.user_name }}</div>
            <div class="label">Email</div><div>{{ req.user_email }}</div>
            <div class="label">Amount</div><div>{{ req.amount }}</div>
            <div class="label">Message</div><div>{{ req.message or '-' }}</div>
            <div class="label">Uploaded</div><div>{{ req.created_at }}</div>
            <div class="label">Filename</div><div>{{ req.receipt_filename }}</div>
            <div class="label">Status</div><div>{{ req.status }}</div>
          </div>
          <p class="muted" style="margin-top: 0.5rem;">
            File stored at: <code>{{ storage_path }}</code><br/>
            Public link (ensure you serve uploads): <a target="_blank" href="{{ base_url }}/uploads/receipts/{{ req.receipt_filename }}">{{ base_url }}/uploads/receipts/{{ req.receipt_filename }}</a>
          </p>
        </div>
        <div class="col">
          <h3>Receipt Preview</h3>
          {% if is_image and preview_b64 %}
            <img alt="Receipt Preview" src="data:image/png;base64,{{ preview_b64 }}" />
          {% else %}
            <p class="muted">Preview not available. This file is likely a PDF. Use the public link above to view.</p>
          {% endif %}
        </div>
      </div>

      <hr style="margin:1.5rem 0;"/>

      <form method="post" action="{{ action_url }}" oninput="rejectNoteToggle()">
        <h3>Take Action</h3>
        <div class="grid">
          <div class="label">Admin Name</div><div><input required name="admin_name" placeholder="Your name" /></div>
          <div class="label">Action</div>
            <div>
              <select name="action">
                <option value="verify">Verify & Issue License</option>
                <option value="reject">Reject</option>
              </select>
            </div>
          <div class="label" id="lblNote">Admin Note</div><div><textarea name="admin_note" rows="3" placeholder="Optional note..."></textarea></div>
          <div class="label">Expiry</div>
            <div>
              <label><input type="radio" name="expiry_mode" value="days" checked /> Expires in days</label>
              <input type="number" name="expires_in_days" min="1" max="3650" value="365" />
              <br/>
              <label><input type="radio" name="expiry_mode" value="date" /> Expires on date</label>
              <input type="date" name="expires_date" />
              <br/>
              <label><input type="radio" name="expiry_mode" value="lifetime" /> Lifetime</label>
            </div>
        </div>
        <div style="margin-top: 1rem; display: flex; gap: 1rem;">
          <button class="btn" type="submit">Submit</button>
          <a class="btn secondary" href="{{ base_url }}/admin">Cancel</a>
        </div>
      </form>
    {% endif %}
  </div>
</body>
</html>
"""


ACTIVATION_RESULT_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>License Activation</title>
  <style>
    body { font-family: system-ui, Arial, sans-serif; margin: 2rem; }
    .card { border: 1px solid #e5e7eb; border-radius: 12px; padding: 1.5rem; max-width: 680px; }
    .ok { color: #065f46; background: #ecfdf5; padding: 0.75rem 1rem; border-radius: 8px; }
    .err { color: #991b1b; background: #fee2e2; padding: 0.75rem 1rem; border-radius: 8px; }
  </style>
  </head>
<body>
  <div class="card">
    <h2>License Activation</h2>
    {% if success %}
      <div class="ok">Your license is now active. Enjoy premium features!</div>
      <p>Email: <strong>{{ email }}</strong></p>
      <p>License Key: <code>{{ license_key }}</code></p>
      <p>Expires: <strong>{{ expires }}</strong></p>
      <p><a href="{{ base_url }}/admin/billing">Continue to billing</a></p>
    {% else %}
      <div class="err">Activation failed: {{ error }}</div>
      <p>Please double-check your email and license key. If the problem persists, contact support.</p>
      <p><a href="{{ base_url }}/activate">Try Again</a></p>
    {% endif %}
  </div>
</body>
</html>
"""


# -----------------------------
# Internal utils
# -----------------------------


def _ensure_upload_dir() -> None:
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
    except Exception as e:
        _log("Failed to create upload directory:", e)
        raise


def _read_file_bytes(file_storage) -> bytes:
    data = file_storage.read()
    file_storage.stream.seek(0)
    if len(data) > MAX_SIZE_BYTES:
        raise ValueError("File too large (max 5MB).")
    return data


def _validate_upload(file_storage) -> Tuple[str, str, bytes]:
    if not file_storage:
        raise ValueError("Missing receipt file.")
    filename = secure_filename(file_storage.filename or "")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXT:
        raise ValueError("Unsupported file type. Allowed: png, jpg, jpeg, pdf.")
    mimetype = (file_storage.mimetype or "").lower()
    if mimetype not in ALLOWED_MIME:
        raise ValueError("Unsupported content type.")
    data = _read_file_bytes(file_storage)
    return filename, mimetype, data


def _save_receipt(file_storage) -> str:
    _ensure_upload_dir()
    filename, mimetype, data = _validate_upload(file_storage)
    stored_name = f"{uuid.uuid4().hex[:12]}_{filename}"
    path = os.path.join(UPLOAD_DIR, stored_name)
    with open(path, "wb") as f:
        f.write(data)
    _log("Saved receipt:", path)
    return stored_name


def _img_preview_if_image(filename: str) -> Optional[str]:
    ext = os.path.splitext(filename)[1].lower()
    if ext not in {".png", ".jpg", ".jpeg"}:
        return None
    path = os.path.join(UPLOAD_DIR, filename)
    try:
        with open(path, "rb") as f:
            b = f.read()
        return base64.b64encode(b).decode("utf-8")
    except Exception:
        return None


# -----------------------------
# Endpoints
# -----------------------------


@billing_bp.route("/request", methods=["POST"])
def submit_request():
    try:
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        amount = (request.form.get("amount") or "").strip()
        message = (request.form.get("message") or "").strip() or None
        receipt = request.files.get("receipt")

        if not (name and email and amount and receipt):
            return jsonify({"error": "name, email, amount, and receipt are required"}), 400

        try:
            amt_val = float(amount)
        except Exception:
            return jsonify({"error": "Invalid amount"}), 400

        stored_file = _save_receipt(receipt)
        token = str(uuid.uuid4())
        req = LicenseRequest(
            user_name=name,
            user_email=email,
            receipt_filename=stored_file,
            amount=amt_val,
            message=message,
            status="PENDING",
            verify_token=token,
            verify_expires=datetime.utcnow() + timedelta(days=7),
        )
        db.session.add(req)
        db.session.commit()

        # Email admin
        admin = _admin_email()
        base = _base_url()
        verify_link = f"{base}{url_for('billing.verify_page', verify_token=token)}"
        subject = f"[SmartEduPay] New Premium Request from {name} ({email}) [REQ:{token}]"
        preview_b64 = _img_preview_if_image(stored_file)
        preview_html = (
            f'<p><img alt="preview" src="data:image/png;base64,{preview_b64}" style="max-width:420px;border:1px solid #eee;border-radius:8px;"/></p>'
            if preview_b64
            else ""
        )
        body = f"""
        <h3>New Premium Request</h3>
        <p><strong>Name:</strong> {name}<br>
        <strong>Email:</strong> {email}<br>
        <strong>Amount:</strong> {amt_val}<br>
        <strong>Message:</strong> {message or '-'}<br>
        <strong>Uploaded:</strong> {req.created_at}</p>
        {preview_html}
        <p>Receipt file: {stored_file}</p>
        <p><a href="{verify_link}">Verify request</a>: {verify_link}</p>
        <hr/>
        <p><strong>Quick Reply:</strong> You can simply reply to this email with <strong>YES</strong> to approve and auto-activate the license (the key will be emailed to the school email), or <strong>NO</strong> to reject. Keep the subject unchanged so the system can match this request. Token: REQ:{token}</p>
        """
        if admin:
            _send_email(admin, subject, body)
        else:
            _log("ADMIN_EMAIL not set; skipping admin email.")

        return jsonify({"status": "pending", "request_id": req.id})
    except Exception as e:  # pragma: no cover
        _log("Error in /billing/request:", e)
        return jsonify({"error": "Failed to submit request"}), 500


@billing_bp.route("/verify/<verify_token>", methods=["GET"])
def verify_page(verify_token: str):
    base = _base_url()
    req = None
    invalid = False
    try:
        req = LicenseRequest.query.filter_by(verify_token=verify_token).first()
        if not req or not req.verify_expires or req.verify_expires < datetime.utcnow() or req.status not in (
            "PENDING",
            "VERIFIED",
        ):
            invalid = True
    except Exception:
        invalid = True

    preview_b64 = None
    is_image = False
    if not invalid and req:
        preview_b64 = _img_preview_if_image(req.receipt_filename)
        is_image = bool(preview_b64)

    html = render_template_string(
        ADMIN_VERIFY_PAGE,
        invalid=invalid,
        req=req,
        base_url=base,
        storage_path=os.path.join(UPLOAD_DIR, req.receipt_filename) if req else "",
        is_image=is_image,
        preview_b64=preview_b64,
        action_url=f"{base}{url_for('billing.verify_action', verify_token=verify_token)}",
    )
    return html


@billing_bp.route("/verify/<verify_token>/action", methods=["POST"])
def verify_action(verify_token: str):
    try:
        req = LicenseRequest.query.filter_by(verify_token=verify_token).first()
        if not req:
            return render_template_string(
                ACTIVATION_RESULT_PAGE, success=False, error="Invalid verification token.", base_url=_base_url()
            )
        if not req.verify_expires or req.verify_expires < datetime.utcnow():
            return render_template_string(
                ADMIN_VERIFY_PAGE,
                invalid=True,
                req=None,
                base_url=_base_url(),
                storage_path="",
                is_image=False,
                preview_b64=None,
                action_url="#",
            )

        action = (request.form.get("action") or "").lower()
        admin_name = (request.form.get("admin_name") or "").strip()
        admin_note = (request.form.get("admin_note") or "").strip() or None
        expiry_mode = (request.form.get("expiry_mode") or "days").lower()
        expires_in_days = request.form.get("expires_in_days")
        expires_date = request.form.get("expires_date")

        if not admin_name:
            return render_template_string(
                ACTIVATION_RESULT_PAGE, success=False, error="Admin name is required.", base_url=_base_url()
            )

        if action == "reject":
            req.status = "REJECTED"
            req.admin_note = admin_note
            req.admin_verified_by = admin_name
            req.verify_token = None
            req.verify_expires = datetime.utcnow() - timedelta(seconds=1)
            db.session.commit()

            if req.user_email:
                subject = "SmartEduPay Premium Request - Update"
                body = f"""
                <p>Hello {req.user_name},</p>
                <p>Your premium request has been reviewed and could not be verified at this time.</p>
                <p>Note: {admin_note or 'No additional details provided.'}</p>
                <p>Please reply to this email if you think this is a mistake.</p>
                """
                _send_email(req.user_email, subject, body)

            return render_template_string(
                ACTIVATION_RESULT_PAGE, success=False, error="Request marked as rejected.", base_url=_base_url()
            )

        if action != "verify":
            return render_template_string(
                ACTIVATION_RESULT_PAGE, success=False, error="Invalid action.", base_url=_base_url()
            )

        # Determine expiry
        expires_at: Optional[datetime] = None
        if expiry_mode == "days":
            try:
                days = int(expires_in_days or "365")
                expires_at = datetime.utcnow() + timedelta(days=days)
            except Exception:
                expires_at = datetime.utcnow() + timedelta(days=365)
        elif expiry_mode == "date":
            try:
                expires_at = datetime.strptime((expires_date or "").strip(), "%Y-%m-%d")
            except Exception:
                return render_template_string(
                    ACTIVATION_RESULT_PAGE, success=False, error="Invalid expiry date.", base_url=_base_url()
                )
        elif expiry_mode == "lifetime":
            expires_at = None
        else:
            expires_at = datetime.utcnow() + timedelta(days=365)

        secret = _require_secret()
        license_key, signature, payload = generate_license_key(req.user_email, expires_at, secret)
        lic = LicenseKey(
            request_id=req.id,
            user_email=req.user_email,
            license_key=license_key,
            issued_at=datetime.utcnow(),
            expires_at=expires_at,
            signature=signature,
            active=False,
        )
        db.session.add(lic)

        req.status = "VERIFIED"
        req.admin_note = admin_note
        req.admin_verified_by = admin_name
        req.verify_token = None
        req.verify_expires = datetime.utcnow() - timedelta(seconds=1)
        db.session.commit()

        base = _base_url()
        subject_user = "Your SmartEduPay Premium License"
        expires_label = "Lifetime" if not expires_at else expires_at.strftime("%Y-%m-%d")
        body_user = f"""
        <p>Thank you {req.user_name},</p>
        <p>Your payment was verified. Here are your license details:</p>
        <p><strong>License Key:</strong> <code>{license_key}</code><br/>
        <strong>Expires:</strong> {expires_label}</p>
        <p>To activate:</p>
        <ol>
      <li>Go to <a href="{base}/admin/billing">{base}/admin/billing</a> and paste the key, or</li>
          <li>Open <a href="{base}/activate">{base}/activate</a> and follow the steps below.</li>
        </ol>
        <p>Keep this key private.</p>
        """
        _send_email(req.user_email, subject_user, body_user)

        # Also email school copy of the license details
        try:
            school_email = _school_email()
            if school_email:
                _send_email(
                    school_email,
                    f"[SmartEduPay] License issued for {req.user_email}",
                    f"<p>Issued license for {req.user_email}.<br><strong>Key:</strong> {license_key}<br><strong>Expires:</strong> {expires_label}</p>",
                )
        except Exception:
            pass

        return render_template_string(
            ACTIVATION_RESULT_PAGE,
            success=True,
            email=req.user_email,
            license_key=license_key,
            expires=expires_label,
            base_url=base,
        )
    except Exception as e:  # pragma: no cover
        _log("Error verifying request:", e)
        return render_template_string(
            ACTIVATION_RESULT_PAGE, success=False, error="Internal error processing verification.", base_url=_base_url()
        )


@billing_bp.route("/activate", methods=["POST"])
def activate_license():
    try:
        if request.is_json:
            email = (request.json.get("email") or "").strip().lower()
            license_key = (request.json.get("license_key") or "").strip()
        else:
            email = (request.form.get("email") or "").strip().lower()
            license_key = (request.form.get("license_key") or "").strip()

        if not (email and license_key):
            return jsonify({"ok": False, "error": "Email and license_key are required."}), 400

        lic = LicenseKey.query.filter_by(user_email=email, license_key=license_key).first()
        if not lic:
            return jsonify({"ok": False, "error": "License not found."}), 404

        if lic.expires_at and datetime.utcnow() > lic.expires_at:
            return jsonify({"ok": False, "error": "License expired."}), 400

        secret = _require_secret()
        verify_license_key(lic.license_key, secret, lic.issued_at, lic.expires_at, lic.user_email)

        lic.active = True

        req = LicenseRequest.query.filter_by(id=lic.request_id).first()
        if req:
            req.status = "ACTIVATED"

        # TODO: Replace with your actual user model import and field update.
        try:
            from models import User  # type: ignore
        except Exception:
            try:
                from app.models import User  # type: ignore
            except Exception:
                User = None  # type: ignore

        if User is not None:
            user_obj = User.query.filter_by(email=email).first()
            if user_obj is not None and hasattr(user_obj, "is_premium"):
                setattr(user_obj, "is_premium", True)

        db.session.commit()

        expires_label = "Lifetime" if not lic.expires_at else lic.expires_at.strftime("%Y-%m-%d")
        return jsonify({"ok": True, "email": email, "license_key": license_key, "expires": expires_label})
    except Exception as e:  # pragma: no cover
        _log("Activation error:", e)
        return jsonify({"ok": False, "error": "Activation failed."}), 500


# -----------------------------
# Minimal activation frontend (paste into a page served at /activate)
# -----------------------------


ACTIVATION_SNIPPET_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Activate Premium</title>
  <style>
    body { font-family: system-ui, Arial, sans-serif; margin: 2rem; }
    .card { border: 1px solid #e5e7eb; border-radius: 12px; padding: 1.5rem; max-width: 560px; }
    input { width: 100%; padding: 0.6rem 0.75rem; border: 1px solid #d1d5db; border-radius: 8px; margin-bottom: 0.8rem; }
    button { padding: 0.6rem 1rem; border-radius: 8px; border: 1px solid #d1d5db; background: #111827; color: #fff; cursor: pointer; }
    .ok { color: #065f46; background: #ecfdf5; padding: 0.75rem 1rem; border-radius: 8px; margin-top: 1rem; }
    .err { color: #991b1b; background: #fee2e2; padding: 0.75rem 1rem; border-radius: 8px; margin-top: 1rem; }
  </style>
  </head>
<body>
  <div class="card">
    <h2>Activate Premium License</h2>
    <input id="email" type="email" placeholder="Your email" required />
    <input id="key" type="text" placeholder="License key" required />
    <button id="btn">Activate</button>
    <div id="result"></div>
  </div>
  <script>
    const btn = document.getElementById('btn');
    const res = document.getElementById('result');
    btn.addEventListener('click', async () => {
      const email = document.getElementById('email').value.trim().toLowerCase();
      const license_key = document.getElementById('key').value.trim();
      res.innerHTML = '';
      try {
        const r = await fetch('/billing/activate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, license_key })
        });
        const data = await r.json();
        if (data.ok) {
          res.innerHTML = '<div class="ok">Activated! Expires: ' + data.expires + '</div>';
        } else {
          res.innerHTML = '<div class="err">Error: ' + (data.error || 'Unknown error') + '</div>';
        }
      } catch (e) {
        res.innerHTML = '<div class="err">Network error.</div>';
      }
    });
  </script>
 </body>
</html>
"""


@billing_bp.route("/activate-page", methods=["GET"])
def activation_page():
    return ACTIVATION_SNIPPET_HTML


@billing_bp.route("/inbound-email", methods=["POST"])
def inbound_email():
    """Inbound email webhook to approve/reject by replying YES/NO.
    Secure this by setting EMAIL_INBOUND_SECRET and passing it via
    header X-Email-Secret, form field 'secret', or query param ?secret=.
    Supported payloads: JSON or form-encoded from common providers (SendGrid, Mailgun).
    Expects subject to contain token pattern 'REQ:<uuid>' included in admin emails.
    """
    try:
        # Secret check
        expected = _inbound_secret()
        if expected:
            provided = (
                request.headers.get("X-Email-Secret")
                or request.args.get("secret")
                or (request.form.get("secret") if request.form else None)
                or (request.json.get("secret") if request.is_json and isinstance(request.json, dict) else None)
            )
            if not provided or provided.strip() != expected:
                return jsonify({"ok": False, "error": "Forbidden"}), 403

        # Extract subject and body text from various providers
        subject = None
        text = None
        if request.is_json and isinstance(request.json, dict):
            data = request.json
            subject = (data.get("subject") or data.get("Subject") or "").strip()
            text = (
                data.get("text")
                or data.get("stripped_text")
                or data.get("body")
                or data.get("body-plain")
                or ""
            )
        else:
            # multipart/form-data or application/x-www-form-urlencoded
            subject = (request.form.get("subject") or request.form.get("Subject") or "").strip()
            text = (
                request.form.get("stripped-text")
                or request.form.get("body-plain")
                or request.form.get("text")
                or request.form.get("html")
                or ""
            )

        # Fallback body: raw data
        if not text:
            try:
                text = request.get_data(as_text=True) or ""
            except Exception:
                text = ""

        # Find token in subject or body
        import re

        token = None
        pat = re.compile(r"REQ:([0-9a-fA-F\-]{8,36})")
        if subject:
            m = pat.search(subject)
            if m:
                token = m.group(1)
        if not token and text:
            m = pat.search(text)
            if m:
                token = m.group(1)
        if not token:
            return jsonify({"ok": False, "error": "Request token not found in subject/body."}), 400

        req = LicenseRequest.query.filter_by(verify_token=token).first()
        if not req:
            return jsonify({"ok": False, "error": "Request not found or already processed."}), 404
        if not req.verify_expires or req.verify_expires < datetime.utcnow() or req.status not in ("PENDING", "VERIFIED"):
            return jsonify({"ok": False, "error": "Verification expired or invalid status."}), 400

        decision = _parse_simple_yes_no(text)
        if decision not in ("yes", "no"):
            return jsonify({"ok": False, "error": "Could not parse YES/NO from reply."}), 400

        admin_email = _admin_email()
        base = _base_url()

        if decision == "no":
            req.status = "REJECTED"
            req.admin_note = "Rejected via email reply"
            req.admin_verified_by = "Email Reply"
            req.verify_token = None
            req.verify_expires = datetime.utcnow() - timedelta(seconds=1)
            db.session.commit()

            if req.user_email:
                subject_user = "SmartEduPay Premium Request - Update"
                body_user = f"""
                <p>Hello {req.user_name},</p>
                <p>Your premium request has been reviewed and could not be verified at this time.</p>
                <p>Note: Rejected via email reply.</p>
                <p>Please reply if you think this is a mistake.</p>
                """
                _send_email(req.user_email, subject_user, body_user)

            if admin_email:
                _send_email(
                    admin_email,
                    f"[SmartEduPay] Processed NO for request {req.id}",
                    f"<p>Request {req.id} for {req.user_email} marked REJECTED.</p><p><a href='{base}/admin'>Admin</a></p>",
                )
            return jsonify({"ok": True, "status": "REJECTED"})

        # YES path: issue license, email it, then auto-activate
        # default expiry 365 days
        expires_at = datetime.utcnow() + timedelta(days=365)
        lic = _issue_license_for_request(req, admin_name="Email Reply", admin_note="Approved via email reply", expires_at=expires_at)
        _activate_license_for_user(req.user_email, lic)

        if admin_email:
            expires_label = "Lifetime" if not lic.expires_at else lic.expires_at.strftime("%Y-%m-%d")
            _send_email(
                admin_email,
                f"[SmartEduPay] Processed YES for request {req.id}",
                f"<p>Issued and activated license for {req.user_email}.<br><strong>Key:</strong> {lic.license_key}<br><strong>Expires:</strong> {expires_label}</p>",
            )

        # Email school with license details as well
        try:
            school_email = _school_email()
            if school_email:
                expires_label = "Lifetime" if not lic.expires_at else lic.expires_at.strftime("%Y-%m-%d")
                _send_email(
                    school_email,
                    f"[SmartEduPay] License issued for {req.user_email}",
                    f"<p>Issued and activated license for {req.user_email}.<br><strong>Key:</strong> {lic.license_key}<br><strong>Expires:</strong> {expires_label}</p>",
                )
        except Exception:
            pass

        return jsonify({"ok": True, "status": "ACTIVATED", "email": req.user_email, "license_key": lic.license_key})
    except Exception as e:  # pragma: no cover
        _log("Inbound email error:", e)
        return jsonify({"ok": False, "error": "Inbound processing failed."}), 500
