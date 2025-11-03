from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
from datetime import datetime, timedelta
import mysql.connector

from utils.licensing import verify_key
from utils.licensing import generate_key
from utils.settings import get_setting
from utils.gmail_api import send_email
import hmac, time, base64
import difflib

# --- M-PESA message parsing helpers ---
import re
import hashlib

def _normalize_mpesa_message(msg: str) -> str:
    s = (msg or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\u200b", "").replace("\u200c", "")
    return s.upper()

def _extract_ref_code(msg_norm: str) -> str | None:
    cand = re.findall(r"\b[A-Z0-9]{8,15}\b", msg_norm or "")
    if not cand:
        return None
    cand.sort(key=lambda x: (not x[0].isalpha(), -len(x)))
    return cand[0]

def _extract_amount(msg_norm: str) -> float | None:
    m = re.search(r"KES\s*([0-9,]+(?:\.[0-9]{1,2})?)", msg_norm)
    if not m:
        m = re.search(r"([0-9,]+(?:\.[0-9]{1,2})?)\s*KES", msg_norm)
    if not m:
        m = re.search(r"\b([0-9]{3,6}(?:\.[0-9]{1,2})?)\b", msg_norm)
    try:
        raw = (m.group(1) if m else None)
        if not raw:
            return None
        return float(raw.replace(",", ""))
    except Exception:
        return None

def _fingerprint_message(msg_norm: str) -> str:
    return hashlib.sha1((msg_norm or "").encode("utf-8")).hexdigest()


monetization_bp = Blueprint("monetization", __name__, url_prefix="/admin/monetization")


def _db():
    cfg = current_app.config
    from urllib.parse import urlparse
    host = "localhost"; user = "root"; password = ""; database = "school_fee_db"
    uri = cfg.get("SQLALCHEMY_DATABASE_URI", "")
    if uri and uri.startswith("mysql"):
        try:
            parsed = urlparse(uri)
            host = parsed.hostname or host
            user = parsed.username or user
            password = parsed.password or password
            if parsed.path and len(parsed.path) > 1:
                database = parsed.path.lstrip("/")
        except Exception:
            pass
    import os
    host = os.environ.get("DB_HOST", host)
    user = os.environ.get("DB_USER", user)
    password = os.environ.get("DB_PASSWORD", password)
    database = os.environ.get("DB_NAME", database)
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _require_admin():
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return redirect(url_for("admin.login"))


def ensure_monetization_tables(db):
    cur = db.cursor()
    # school_plans
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_plans (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            plan_code VARCHAR(20) NOT NULL DEFAULT 'FREE',
            expires_at DATETIME NULL,
            grace_days INT NOT NULL DEFAULT 7,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            activated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_school_plans_school_id (school_id),
            CONSTRAINT fk_school_plans_school FOREIGN KEY (school_id)
                REFERENCES schools(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # school_features
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            feature_code VARCHAR(50) NOT NULL,
            is_enabled TINYINT(1) NOT NULL DEFAULT 0,
            UNIQUE KEY uq_school_feature (school_id, feature_code),
            INDEX idx_school_features_school_id (school_id),
            CONSTRAINT fk_school_features_school FOREIGN KEY (school_id)
                REFERENCES schools(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # license_events
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS license_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            event_type VARCHAR(30) NOT NULL,
            details TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_license_events_school_id (school_id),
            CONSTRAINT fk_license_events_school FOREIGN KEY (school_id)
                REFERENCES schools(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()

    # manual_payments: stores M-PESA message submissions for manual review
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_payments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            plan_choice VARCHAR(20) NOT NULL, -- MONTHLY | YEARLY | LIFETIME
            amount DECIMAL(12,2) NULL,
            payer_phone VARCHAR(32) NULL,
            payer_name VARCHAR(128) NULL,
            school_email VARCHAR(255) NULL,
            mpesa_message TEXT NOT NULL,
            msg_normalized TEXT NULL,
            message_fingerprint VARCHAR(64) NULL,
            ref_code VARCHAR(32) NULL,
            amount_extracted DECIMAL(12,2) NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            verified_at DATETIME NULL,
            INDEX idx_manual_payments_school_id (school_id),
            CONSTRAINT fk_manual_payments_school FOREIGN KEY (school_id)
                REFERENCES schools(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()

    # Add columns and indexes idempotently (compatible with older MySQL/MariaDB)
    def _col_exists(name: str) -> bool:
        try:
            cur.execute("SHOW COLUMNS FROM manual_payments LIKE %s", (name,))
            return cur.fetchone() is not None
        except Exception:
            return False

    def _ensure_col(name: str, ddl: str) -> None:
        try:
            if not _col_exists(name):
                cur.execute(f"ALTER TABLE manual_payments ADD COLUMN {ddl}")
        except Exception:
            # ignore if already exists or insufficient privileges
            pass

    def _index_exists(name: str) -> bool:
        try:
            cur.execute("SHOW INDEX FROM manual_payments WHERE Key_name=%s", (name,))
            return cur.fetchone() is not None
        except Exception:
            return False

    def _ensure_index(name: str, ddl_sql: str) -> None:
        try:
            if not _index_exists(name):
                cur.execute(ddl_sql)
        except Exception:
            pass

    _ensure_col("message_fingerprint", "message_fingerprint VARCHAR(64) NULL")
    _ensure_col("ref_code", "ref_code VARCHAR(32) NULL")
    _ensure_col("msg_normalized", "msg_normalized TEXT NULL")
    _ensure_col("amount_extracted", "amount_extracted DECIMAL(12,2) NULL")

    _ensure_index("uq_manual_ref_code", "CREATE UNIQUE INDEX uq_manual_ref_code ON manual_payments(ref_code)")
    _ensure_index("uq_manual_msg_fp", "CREATE UNIQUE INDEX uq_manual_msg_fp ON manual_payments(message_fingerprint)")
    _ensure_index("idx_manual_created_at", "CREATE INDEX idx_manual_created_at ON manual_payments(created_at)")
    db.commit()


def _get_school_code(db, school_id: int) -> str | None:
    cur = db.cursor()
    cur.execute("SELECT code FROM schools WHERE id=%s", (school_id,))
    row = cur.fetchone()
    if not row:
        return None
    return row[0] if not isinstance(row, dict) else row.get("code")


# --- Secure one-click approval links (email-based) ---
def _signing_secret() -> bytes:
    key = (current_app.config.get("APP_SIGNING_SECRET") or current_app.config.get("SECRET_KEY") or "dev-secret").encode(
        "utf-8"
    )
    return key


def _make_action_token(payment_id: int, school_id: int, plan_choice: str, ttl_seconds: int = 7 * 24 * 3600) -> str:
    exp = int(time.time()) + int(ttl_seconds)
    msg = f"{payment_id}|{school_id}|{plan_choice}|{exp}".encode("utf-8")
    import hashlib as _hl
    sig = hmac.new(_signing_secret(), msg, _hl.sha256).digest()
    token = base64.urlsafe_b64encode(msg + b"." + sig).decode("ascii")
    return token


def _check_action_token(token: str, payment_id: int, school_id: int, plan_choice: str) -> bool:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        parts = raw.split(b".")
        if len(parts) != 2:
            return False
        msg, sig = parts[0], parts[1]
        import hashlib as _hl
        expected = hmac.new(_signing_secret(), msg, _hl.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            return False
        msg_str = msg.decode("utf-8")
        pid_s, sid_s, plan_s, exp_s = msg_str.split("|")
        if int(pid_s) != int(payment_id) or int(sid_s) != int(school_id) or (plan_s or "").upper() != (plan_choice or "").upper():
            return False
        if time.time() > float(exp_s):
            return False
        return True
    except Exception:
        return False


def _plan_status(db, school_id: int) -> dict:
    ensure_monetization_tables(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM school_plans WHERE school_id=%s AND is_active=1 ORDER BY id DESC LIMIT 1",
        (school_id,),
    )
    plan = cur.fetchone()
    if not plan:
        return {"active": False, "expired": True, "plan_code": "FREE", "expires_at": None}
    expired = False
    in_grace = False
    expires_at = plan.get("expires_at")
    if expires_at:
        expired = datetime.utcnow() > (expires_at if isinstance(expires_at, datetime) else expires_at)
        if expired:
            grace = (expires_at if isinstance(expires_at, datetime) else expires_at) + timedelta(days=int(plan.get("grace_days", 7)))
            in_grace = datetime.utcnow() <= grace
    return {
        "active": (not expired) or in_grace,
        "expired": expired,
        "in_grace": in_grace,
        "plan_code": plan.get("plan_code", "FREE"),
        "expires_at": expires_at,
    }


@monetization_bp.route("/")
def index():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("admin.dashboard"))
    db = _db()
    try:
        status = _plan_status(db, sid)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM license_events WHERE school_id=%s ORDER BY created_at DESC LIMIT 5",
            (sid,),
        )
        events = cur.fetchall() or []
    finally:
        db.close()
    # Default school email for convenience in the submission form
    school_email = get_setting("SCHOOL_EMAIL") or ""
    # Fixed pricing as requested
    pricing = {"MONTHLY": 1500, "YEARLY": 15000, "LIFETIME": 50000}
    return render_template(
        "admin/monetization.html",
        status=status,
        events=events,
        school_email=school_email,
        pricing=pricing,
    )


@monetization_bp.route("/activate", methods=["POST"])
def activate():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("monetization.index"))

    token = (request.form.get("license_key") or "").strip()
    ok, payload = verify_key(token)

    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor()
        if not ok:
            cur.execute(
                "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
                (sid, "VERIFY_FAIL", str(payload)),
            )
            db.commit()
            flash("Invalid or corrupted license key.", "danger")
            return redirect(url_for("monetization.index"))

        # Compare school UID in key to the school's code (acts as UID)
        school_uid = payload.get("school_uid")
        code = _get_school_code(db, sid)
        if not code or str(code) != str(school_uid):
            cur.execute(
                "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
                (sid, "VERIFY_FAIL", "UID mismatch"),
            )
            db.commit()
            flash("License does not belong to this school.", "danger")
            return redirect(url_for("monetization.index"))

        # Deactivate previous active plans
        cur.execute("UPDATE school_plans SET is_active=0 WHERE school_id=%s AND is_active=1", (sid,))
        db.commit()

        # Create new plan
        exp_str = payload.get("expires_at") or None
        expires_at = None
        if exp_str:
            try:
                expires_at = datetime.fromisoformat(exp_str.replace("Z", ""))
            except Exception:
                expires_at = None
        plan_code = payload.get("plan_code", "PREMIUM")
        cur.execute(
            "INSERT INTO school_plans (school_id, plan_code, expires_at, is_active, activated_at) VALUES (%s, %s, %s, 1, NOW())",
            (sid, plan_code, expires_at),
        )
        db.commit()

        # Reset and insert features
        cur.execute("DELETE FROM school_features WHERE school_id=%s", (sid,))
        for feat in payload.get("features", []) or []:
            cur.execute(
                "INSERT INTO school_features (school_id, feature_code, is_enabled) VALUES (%s, %s, 1)",
                (sid, str(feat)),
            )
        db.commit()

        # Log event
        cur.execute(
            "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
            (sid, "ACTIVATE", str(payload)),
        )
        db.commit()
    finally:
        db.close()

    flash(f"{plan_code} plan activated.", "success")
    return redirect(url_for("monetization.index"))


@monetization_bp.route("/submit_payment", methods=["POST"])
def submit_payment():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("monetization.index"))

    plan_choice = (request.form.get("plan_choice") or "").upper()
    if plan_choice not in ("MONTHLY", "YEARLY", "LIFETIME"):
        flash("Choose a valid plan (Monthly, Yearly, Lifetime).", "warning")
        return redirect(url_for("monetization.index"))

    # Fixed pricing (ignore user-input amount)
    pricing = {"MONTHLY": 1500, "YEARLY": 15000, "LIFETIME": 50000}
    amount = pricing.get(plan_choice)
    payer_phone = request.form.get("payer_phone") or None
    payer_name = request.form.get("payer_name") or None
    school_email = (request.form.get("school_email") or get_setting("SCHOOL_EMAIL") or "").strip()
    mpesa_message = (request.form.get("mpesa_message") or "").strip()
    if not mpesa_message:
        flash("Paste the M-PESA message.", "warning")
        return redirect(url_for("monetization.index"))

    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor()

        # Analyze and de-duplicate message
        msg_norm = _normalize_mpesa_message(mpesa_message)
        msg_fp = _fingerprint_message(msg_norm)
        ref_code = _extract_ref_code(msg_norm)
        amt_guess = _extract_amount(msg_norm)

        # Duplicate check (tolerant of older schemas)
        cur_dupe = db.cursor()
        try:
            cur_dupe.execute("SHOW COLUMNS FROM manual_payments LIKE 'message_fingerprint'")
            has_fp_col = bool(cur_dupe.fetchone())
        except Exception:
            has_fp_col = False

        if has_fp_col:
            cur_dupe.execute(
                "SELECT id FROM manual_payments WHERE message_fingerprint=%s OR (ref_code IS NOT NULL AND ref_code=%s) LIMIT 1",
                (msg_fp, ref_code),
            )
        elif ref_code:
            cur_dupe.execute(
                "SELECT id FROM manual_payments WHERE ref_code=%s LIMIT 1",
                (ref_code,),
            )
        else:
            # No reliable duplicate predicate available; skip duplicate check
            cur_dupe = None

        if cur_dupe and cur_dupe.fetchone():
            flash("This M-PESA message or reference appears already submitted.", "warning")
            return redirect(url_for("monetization.index"))

        # Optional: warn if parsed amount does not match fixed price
        if amt_guess is not None and amount is not None and abs(float(amt_guess) - float(amount)) > 1.0:
            flash("Note: Parsed amount differs from selected plan price. Admin will verify.", "warning")

        cur.execute(
            """
            INSERT INTO manual_payments (school_id, plan_choice, amount, payer_phone, payer_name, school_email, mpesa_message, msg_normalized, message_fingerprint, ref_code, amount_extracted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (sid, plan_choice, amount, payer_phone, payer_name, school_email, mpesa_message, msg_norm, msg_fp, ref_code, amt_guess),
        )
        db.commit()
        payment_id = cur.lastrowid or 0

        # Email the submission to the owner/admin inbox for manual review
        admin_email = current_app.config.get("OWNER_EMAIL") or "sewynssewadda@gmail.com"
        school_code = _get_school_code(db, sid) or "?"
        subject = f"Manual M-PESA submission | {plan_choice} | School {school_code}"
        # Include secure one-click approval links instead of portal verification
        token = _make_action_token(payment_id, sid, plan_choice)
        approve_url = url_for("monetization.email_approve", payment_id=payment_id, token=token, _external=True)
        reject_url = url_for("monetization.email_reject", payment_id=payment_id, token=token, _external=True)
        body = (
            f"School: {school_code} (ID {sid})\n"
            f"Plan: {plan_choice}\nAmount: {amount or ''}\n"
            f"Payer Name: {payer_name or ''}\nPayer Phone: {payer_phone or ''}\n"
            f"School Email: {school_email or ''}\n\n"
            f"M-PESA Message:\n{mpesa_message}\n\n"
            f"Normalized: {msg_norm}\nRef: {ref_code or '-'} | MsgFP: {msg_fp[:10]}...\n"
            f"Amount (parsed): {amt_guess or '-'} | Plan amount: {amount}\n\n"
            "Approve or reject directly from this email:\n"
            f"Approve: {approve_url}\n"
            f"Reject:  {reject_url}\n\n"
            "These links expire in 7 days."
        )
        try:
            send_email(admin_email, subject, body)
        except Exception:
            pass

        # Log event
        cur.execute(
            "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
            (sid, "PAYMENT_SUBMITTED", f"{plan_choice}|{amount}|{payer_phone}"),
        )
        db.commit()
    finally:
        db.close()

    flash("Payment submitted. Awaiting verification.", "success")
    return redirect(url_for("monetization.index"))


@monetization_bp.route("/verifications")
def verifications():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("monetization.index"))
    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM manual_payments WHERE school_id=%s AND status='PENDING' ORDER BY created_at DESC",
            (sid,),
        )
        items = cur.fetchall() or []

        # Duplicate-suspect pass: compare pending normalized messages to recent history
        if items:
            # Fetch more history across ALL schools to improve duplicate detection
            cur2 = db.cursor(dictionary=True)
            cur2.execute(
                "SELECT message_fingerprint, msg_normalized FROM manual_payments WHERE msg_normalized IS NOT NULL ORDER BY created_at DESC LIMIT 5000"
            )
            recent = cur2.fetchall() or []
            recent_norms = [r.get("msg_normalized") or "" for r in recent]
            for it in items:
                a = (it.get("msg_normalized") or it.get("mpesa_message") or "").upper()
                best = 0.0
                for b in recent_norms:
                    if not b:
                        continue
                    try:
                        s = difflib.SequenceMatcher(None, a, b).ratio()
                        if s > best:
                            best = s
                    except Exception:
                        continue
                it["suspect"] = best >= 0.9
                it["suspect_score"] = round(best, 3)
    finally:
        db.close()
    return render_template("admin/monetization_verify.html", items=items)


@monetization_bp.route("/search")
def search_submissions():
    guard = _require_admin()
    if guard is not None:
        return guard
    ref = (request.args.get("ref") or "").strip().upper()
    db = _db()
    rows = []
    try:
        ensure_monetization_tables(db)
        if ref:
            cur = db.cursor(dictionary=True)
            cur.execute(
                """
                SELECT id, school_id, plan_choice, amount, school_email, ref_code, status, created_at
                FROM manual_payments
                WHERE ref_code LIKE %s
                ORDER BY created_at DESC
                LIMIT 100
                """,
                (f"%{ref}%",),
            )
            rows = cur.fetchall() or []
    finally:
        db.close()
    return render_template("admin/monetization_search.html", ref=ref, results=rows)


def _plan_features_for_choice(choice: str) -> tuple[str, list[str], str | None]:
    choice = (choice or "").upper()
    # returns (plan_code, features, expires_iso_or_none)
    features_basic = ["multi_term", "templates_custom"]
    features_pro = features_basic + ["reports_advanced", "bulk_messaging"]
    features_elite = features_pro + ["ai_assistant"]

    if choice == "MONTHLY":
        exp = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return "MONTHLY", features_basic, exp
    if choice == "YEARLY":
        exp = (datetime.utcnow() + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return "YEARLY", features_pro, exp
    if choice == "LIFETIME":
        return "LIFETIME", features_elite, None
    # default fallback
    exp = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return "MONTHLY", features_basic, exp


@monetization_bp.route("/verify/<int:payment_id>", methods=["POST"])
def verify_payment(payment_id: int):
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("monetization.index"))

    plan_choice = (request.form.get("plan_choice") or "").upper()
    if plan_choice not in ("MONTHLY", "YEARLY", "LIFETIME"):
        flash("Choose a valid plan to issue.", "warning")
        return redirect(url_for("monetization.verifications"))

    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT * FROM manual_payments WHERE id=%s AND school_id=%s", (payment_id, sid))
        row = cur.fetchone()
        if not row or row.get("status") != "PENDING":
            flash("Payment not found or already processed.", "warning")
            return redirect(url_for("monetization.verifications"))

        plan_code, features, expires_iso = _plan_features_for_choice(plan_choice)
        school_uid = _get_school_code(db, sid) or str(sid)
        token = generate_key(school_uid, plan_code=plan_code, features=features, expires_at=expires_iso)

        # auto-activate for this school
        cur2 = db.cursor()
        cur2.execute("UPDATE school_plans SET is_active=0 WHERE school_id=%s AND is_active=1", (sid,))
        exp_dt = None
        if expires_iso:
            try:
                exp_dt = datetime.fromisoformat(expires_iso.replace("Z", ""))
            except Exception:
                exp_dt = None
        cur2.execute(
            "INSERT INTO school_plans (school_id, plan_code, expires_at, is_active, activated_at) VALUES (%s, %s, %s, 1, NOW())",
            (sid, plan_code, exp_dt),
        )
        cur2.execute("DELETE FROM school_features WHERE school_id=%s", (sid,))
        for feat in features:
            cur2.execute(
                "INSERT INTO school_features (school_id, feature_code, is_enabled) VALUES (%s, %s, 1)",
                (sid, str(feat)),
            )
        db.commit()

        # mark manual payment verified
        cur2.execute(
            "UPDATE manual_payments SET status='VERIFIED', verified_at=NOW() WHERE id=%s",
            (payment_id,),
        )
        db.commit()

        # email license token to school email
        school_email = row.get("school_email") or get_setting("SCHOOL_EMAIL") or ""
        if school_email:
            email_body = (
                "Thank you for upgrading your plan.\n\n"
                f"Plan: {plan_code}\n"
                f"Expires: {expires_iso or 'Lifetime'}\n\n"
                "Your license key (keep it safe):\n"
                f"{token}\n\n"
                "It has already been activated for your school. You can also paste it in the Monetization page if needed."
            )
            try:
                send_email(school_email, "Your License Key", email_body)
            except Exception:
                pass

        # log event
        cur2.execute(
            "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
            (sid, "ISSUE_LICENSE", plan_code),
        )
        db.commit()
    finally:
        db.close()

    flash("Payment verified and license issued.", "success")
    return redirect(url_for("monetization.verifications"))


# Email-based approval endpoints (secure, signed links)
@monetization_bp.route("/approve/<int:payment_id>")
def email_approve(payment_id: int):
    token = request.args.get("token") or ""
    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT * FROM manual_payments WHERE id=%s", (payment_id,))
        row = cur.fetchone()
        if not row:
            flash("Payment not found.", "warning")
            return redirect(url_for("monetization.verifications"))
        real_sid = int(row.get("school_id") or 0)
        plan_choice = (row.get("plan_choice") or "").upper()
        if not _check_action_token(token, payment_id, real_sid, plan_choice):
            flash("Approval link is invalid or expired.", "danger")
            return redirect(url_for("monetization.verifications"))
        if (row.get("status") or "").upper() != "PENDING":
            flash("Payment already processed.", "info")
            return redirect(url_for("monetization.verifications"))

        plan_code, features, expires_iso = _plan_features_for_choice(plan_choice)
        school_uid = _get_school_code(db, real_sid) or str(real_sid)
        token_key = generate_key(school_uid, plan_code=plan_code, features=features, expires_at=expires_iso)

        cur2 = db.cursor()
        cur2.execute("UPDATE school_plans SET is_active=0 WHERE school_id=%s AND is_active=1", (real_sid,))
        exp_dt = None
        if expires_iso:
            try:
                exp_dt = datetime.fromisoformat(expires_iso.replace("Z", ""))
            except Exception:
                exp_dt = None
        cur2.execute(
            "INSERT INTO school_plans (school_id, plan_code, expires_at, is_active, activated_at) VALUES (%s, %s, %s, 1, NOW())",
            (real_sid, plan_code, exp_dt),
        )
        cur2.execute("DELETE FROM school_features WHERE school_id=%s", (real_sid,))
        for feat in features:
            cur2.execute(
                "INSERT INTO school_features (school_id, feature_code, is_enabled) VALUES (%s, %s, 1)",
                (real_sid, str(feat)),
            )
        cur2.execute(
            "UPDATE manual_payments SET status='VERIFIED', verified_at=NOW() WHERE id=%s",
            (payment_id,),
        )

        school_email = row.get("school_email") or get_setting("SCHOOL_EMAIL") or ""
        if school_email:
            email_body = (
                "Your upgrade has been approved.\n\n"
                f"Plan: {plan_code}\n"
                f"Expires: {expires_iso or 'Lifetime'}\n\n"
                "Your license key (keep it safe):\n"
                f"{token_key}\n\n"
                "It is already active for your school."
            )
            try:
                send_email(school_email, "Your License Key", email_body)
            except Exception:
                pass
        cur2.execute(
            "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
            (real_sid, "ISSUE_LICENSE", plan_code),
        )
        db.commit()
    finally:
        db.close()
    flash("Payment approved. License activated and emailed.", "success")
    return redirect(url_for("monetization.verifications"))


@monetization_bp.route("/reject/<int:payment_id>")
def email_reject(payment_id: int):
    token = request.args.get("token") or ""
    db = _db()
    try:
        ensure_monetization_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT * FROM manual_payments WHERE id=%s", (payment_id,))
        row = cur.fetchone()
        if not row:
            flash("Payment not found.", "warning")
            return redirect(url_for("monetization.verifications"))
        real_sid = int(row.get("school_id") or 0)
        plan_choice = (row.get("plan_choice") or "").upper()
        if not _check_action_token(token, payment_id, real_sid, plan_choice):
            flash("Rejection link is invalid or expired.", "danger")
            return redirect(url_for("monetization.verifications"))
        if (row.get("status") or "").upper() != "PENDING":
            flash("Payment already processed.", "info")
            return redirect(url_for("monetization.verifications"))

        cur2 = db.cursor()
        cur2.execute("UPDATE manual_payments SET status='REJECTED', verified_at=NOW() WHERE id=%s", (payment_id,))
        cur2.execute(
            "INSERT INTO license_events (school_id, event_type, details) VALUES (%s, %s, %s)",
            (real_sid, "PAYMENT_REJECTED", plan_choice),
        )
        db.commit()
    finally:
        db.close()
    flash("Payment rejected. School remains on basic plan.", "info")
    return redirect(url_for("monetization.verifications"))
