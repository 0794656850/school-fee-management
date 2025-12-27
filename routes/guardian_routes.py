from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, session, jsonify
from extensions import limiter
import os
import mysql.connector
import json
from urllib.parse import urlparse
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

from utils.tenant import slugify_code
from routes.student_portal import (
    _sign_token,
    ensure_mpesa_student_table,
    _verify_token,
    record_mpesa_payment_if_missing,
)  # reuse same token scheme
from routes.term_routes import get_or_seed_current_term, ensure_academic_terms_table
from utils.security import verify_password, hash_password
from utils.document_qr import build_document_qr
from utils.db_helpers import (
    ensure_guardian_receipts_table,
    ensure_profile_deletion_requests_table,
    ensure_parent_portal_columns,
)
from utils.payment_proofs import (
    allowed_proof_file,
    extract_proof_metadata,
    format_status_label,
    save_payment_proof_file,
)
from utils.payment_sources import (
    log_payment_status,
    record_payment_source,
)
from utils.settings import get_setting
from utils.rasa_bot import rasa_is_available, rasa_parse
from utils.auto_credit import auto_apply_credit_if_new_term
from utils.timezone_helpers import east_africa_now, format_east_africa
from werkzeug.utils import secure_filename
import base64
import requests
from utils.login_otp import generate_login_otp, mask_email, send_portal_login_otp
try:
    from utils.gmail_api import send_email as gmail_send_email, send_email_html as gmail_send_email_html
except Exception:
    def gmail_send_email(*args, **kwargs):  # type: ignore
        return False

    def gmail_send_email_html(*args, **kwargs):  # type: ignore
        return False


guardian_bp = Blueprint("guardian", __name__, url_prefix="/g")

STATUS_BADGES = {
    "pending": "bg-amber-50 text-amber-700",
    "in_review": "bg-sky-50 text-sky-700",
    "verified": "bg-emerald-50 text-emerald-700",
    "rejected": "bg-rose-50 text-rose-600",
    "accepted": "bg-emerald-50 text-emerald-700",
}

LOGIN_OTP_EXPIRES_MINUTES = 10


def _resolve_reminder_email_column(cursor) -> str | None:
    preferred = (get_setting("REMINDER_EMAIL_COLUMN") or "").strip()
    candidates = [preferred] if preferred else []
    candidates.extend(["email", "parent_email"])
    for cand in candidates:
        if not cand:
            continue
        try:
            cursor.execute("SHOW COLUMNS FROM students LIKE %s", (cand,))
            if cursor.fetchone():
                return cand
        except Exception:
            continue
    return None


def _guardian_email_for_otp(db, student_id: int, school_id: int | None) -> str:
    if not student_id:
        return ""
    cur = db.cursor(dictionary=True)
    email_col = _resolve_reminder_email_column(cur)
    if not email_col:
        return ""
    try:
        if school_id:
            cur.execute(f"SELECT {email_col} AS email FROM students WHERE id=%s AND school_id=%s", (student_id, school_id))
        else:
            cur.execute(f"SELECT {email_col} AS email FROM students WHERE id=%s", (student_id,))
        row = cur.fetchone() or {}
        return (row.get("email") or "").strip()
    except Exception:
        return ""


def _guardian_otp_context() -> dict:
    return session.get("guardian_otp_context", {})


def _clear_guardian_otp_context() -> None:
    session.pop("guardian_otp_context", None)


def _format_guardian_timestamp(value):
    if not value:
        return ""
    try:
        formatted = format_east_africa(value, "%d %b %Y %H:%M")
        if formatted:
            return formatted
        return str(value)
    except Exception:
        return str(value)


def _guardian_receipts_for_student(student_id: int, school_id: int, limit: int = 6):
    receipts = []
    if not student_id or not school_id:
        return receipts
    db = _db()
    try:
        ensure_guardian_receipts_table(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, status, amount, description, admin_note, rejection_reason,
                   file_path, created_at, updated_at
            FROM guardian_receipts
            WHERE student_id=%s AND school_id=%s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (student_id, school_id, limit),
        )
        rows = cur.fetchall() or []
        for row in rows:
            status = (row.get("status") or "pending").lower()
            status_label = format_status_label(status)
            file_path = (row.get("file_path") or "").strip()
            file_url = ""
            if file_path:
                try:
                    file_url = url_for("static", filename=file_path)
                except Exception:
                    file_url = file_path
            receipts.append(
                {
                    "id": row.get("id"),
                    "status": status,
                    "status_label": status_label,
                    "status_classes": STATUS_BADGES.get(status, STATUS_BADGES["pending"]),
                    "amount": float(row.get("amount") or 0),
                    "description": (row.get("description") or "").strip(),
                    "notes": (row.get("admin_note") or row.get("rejection_reason") or "").strip(),
                    "analysis": (row.get("analysis") or "").strip(),
                    "file_url": file_url,
                    "created_at": _format_guardian_timestamp(row.get("created_at") or row.get("updated_at")),
                }
            )
    finally:
        try:
            db.close()
        except Exception:
            pass
    return receipts


def _describe_proof_authenticity(metadata: dict[str, str | None]) -> str:
    hints: list[str] = []
    if not metadata:
        return "Unable to read the proof automatically; manual verification recommended."
    bank = metadata.get("bank")
    amount = metadata.get("amount")
    date = metadata.get("date")
    if bank:
        hints.append(f"Channel detected: {bank}")
    if amount:
        hints.append(f"Amount detected: {amount}")
    if date:
        hints.append(f"Date detected: {date}")
    raw_text = (metadata.get("raw") or "").strip()
    has_raw = bool(raw_text)
    if "fake" in raw_text.lower():
        hints.append("Text mentions fake, please review carefully")
    status = "Looks authentic" if len(hints) >= 2 else ("Likely authentic" if len(hints) == 1 or has_raw else "Pending manual verification")
    hint_text = ", ".join(hints)
    return f"{status}. {hint_text}" if hint_text else f"{status}."




def _alert_school_of_parent_deletion(student: dict[str, Any], caretaker_name: str, reason: str | None) -> None:
    school_email = (get_setting("SCHOOL_EMAIL") or get_setting("ACCOUNTS_EMAIL") or "").strip()
    if not school_email:
        return
    child_name = student.get("name") or f"Student #{student.get('id')}"
    subject = f"Parent portal deletion request for {child_name}"
    html_body = (
        f"<p>Hi finance team,</p>"
        f"<p>{caretaker_name or 'A guardian'} requested to delete their parent portal record for {child_name}.</p>"
        f"<p>Reason: {reason or 'Not provided'}.</p>"
        "<p>Review the guardian verification queue to honor this request.</p>"
    )
    plain_body = (
        f"Hi finance team,\n\n"
        f"{caretaker_name or 'A guardian'} requested their portal account removal for {child_name}.\n"
        f"Reason: {reason or 'Not provided'}.\n\n"
        "Review and confirm the action within the admin area."
    )
    try:
        if not gmail_send_email_html(school_email, subject, html_body):
            gmail_send_email(school_email, subject, plain_body)
    except Exception:
        pass


def _db():
    cfg = current_app.config
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")

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
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _guardian_upload_path(school_id: int) -> Path:
    relative = current_app.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR", "uploads/guardian_receipts")
    root = Path(current_app.root_path) / "static" / relative
    target = root / str(school_id)
    target.mkdir(parents=True, exist_ok=True)
    return target


def _allowed_receipt_file(filename: str) -> bool:
    allowed = {".png", ".jpg", ".jpeg", ".pdf"}
    ext = Path(filename).suffix.lower()
    return ext in allowed


@guardian_bp.route("", strict_slashes=False)
@guardian_bp.route("/", strict_slashes=False)
def guardian_index():
    """Guardian landing page -> render login directly."""
    return render_template("guardian_login.html")


@guardian_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("8 per minute", methods=["POST"])  # throttle brute-force
def guardian_login():
    """Secure guardian/parent login by school, last name and admission number."""
    if request.method == "POST":
        school_raw = (request.form.get("school") or "").strip()
        candidate_pw = (request.form.get("admission_no") or request.form.get("regNo") or "").strip()
        provided_name = (request.form.get("student_last_name") or request.form.get("last_name") or request.form.get("student_name") or request.form.get("student_full_name") or request.form.get("name") or "").strip()
        # Accept either last name or full name; derive last token for matching
        last_name = provided_name.split()[-1] if provided_name else ""

        if not school_raw or not candidate_pw or not last_name:
            flash("Enter school name/code, student's last name and admission number.", "warning")
            return redirect(url_for("guardian.guardian_login"))

        code = slugify_code(school_raw)
        db = _db(); cur = db.cursor(dictionary=True)
        try:
            # Resolve school by code OR exact name (case-insensitive)
            cur.execute(
                "SELECT id FROM schools WHERE code=%s OR LOWER(TRIM(name)) = LOWER(TRIM(%s)) LIMIT 1",
                (code, school_raw,)
            )
            row = cur.fetchone()
            if not row:
                flash("School not found. Please confirm the school name.", "error")
                return redirect(url_for("guardian.guardian_login"))
            school_id = int(row.get("id") if isinstance(row, dict) else row[0])

            # Ensure password column exists
            try:
                from routes.student_auth import ensure_student_portal_columns  # lazy import
                ensure_student_portal_columns(db)
            except Exception:
                pass
            try:
                ensure_parent_portal_columns(db)
            except Exception:
                pass
            cur.execute(
                """
                SELECT id, name, admission_no AS regNo, portal_password_hash, parent_portal_archived
                FROM students
                WHERE school_id=%s AND LOWER(TRIM(SUBSTRING_INDEX(name, ' ', -1))) = LOWER(TRIM(%s))
                ORDER BY id ASC
                """,
                (school_id, last_name,),
            )
            candidates = cur.fetchall() or []
            if not candidates:
                flash("Invalid details. Please confirm your school name and admission number.", "error")
                return redirect(url_for("guardian.guardian_login"))

            student_row = None
            archived_flag = False
            for s in candidates:
                stored = s.get("portal_password_hash")
                if s.get("parent_portal_archived"):
                    archived_flag = True
                    continue
                ok = False
                if stored:
                    ok = verify_password(stored, candidate_pw)
                else:
                    ok = (str(s.get("regNo") or "").strip() == candidate_pw)
                    if ok:
                        try:
                            cur2 = db.cursor()
                            cur2.execute(
                                "UPDATE students SET portal_password_hash=%s WHERE id=%s",
                                (hash_password(candidate_pw), int(s.get("id"))),
                            )
                            db.commit()
                        except Exception:
                            try: db.rollback()
                            except Exception: pass
                if ok:
                    student_row = s
                    break

            if not student_row:
                if archived_flag:
                    flash("This guardian account is archived. Contact the school to restore access or submit a new request.", "warning")
                else:
                    flash("Invalid details. Please confirm your school name and admission number.", "error")
                return redirect(url_for("guardian.guardian_login"))

            sid = int(student_row.get("id"))
            if current_app.config.get("PARENT_EMAIL_AUTH_ENABLED", False):
                cur.execute("SHOW COLUMNS FROM students LIKE 'parent_email_verified'")
                has_verified_col = bool(cur.fetchone())
                email_verified = False
                if has_verified_col:
                    try:
                        cur.execute(
                            "SELECT parent_email_verified FROM students WHERE id=%s AND school_id=%s",
                            (sid, school_id),
                        )
                        vrow = cur.fetchone()
                        if vrow is not None:
                            email_verified = bool(vrow[0] if not isinstance(vrow, dict) else vrow.get("parent_email_verified"))
                    except Exception:
                        email_verified = False
                if email_verified:
                    session["guardian_logged_in"] = True
                    session["guardian_student_id"] = sid
                    session["school_id"] = school_id
                    token = _sign_token(sid)
                    session["guardian_token"] = token
                    flash("Login successful.", "success")
                    return redirect(url_for("guardian.guardian_dashboard"))
                _clear_guardian_otp_context()
                target_email = _guardian_email_for_otp(db, sid, school_id)
                if not target_email:
                    flash("Add a guardian email on the student record to receive the verification code.", "warning")
                    return redirect(url_for("guardian.guardian_login"))
                otp_code = generate_login_otp()
                recipient_label = student_row.get("name") or provided_name or "Guardian"
                sent = send_portal_login_otp(
                    target_email,
                    recipient_label,
                    "Parent / Guardian Portal",
                    otp_code,
                    LOGIN_OTP_EXPIRES_MINUTES,
                )
                if not sent:
                    flash("Unable to send the verification code right now. Try again in a moment.", "error")
                    return redirect(url_for("guardian.guardian_login"))
                session["guardian_otp_context"] = {
                    "student_id": sid,
                    "school_id": school_id,
                    "name": recipient_label,
                    "email": target_email,
                    "code": otp_code,
                    "until": (datetime.now() + timedelta(minutes=LOGIN_OTP_EXPIRES_MINUTES)).timestamp(),
                    "sent_at": datetime.now().timestamp(),
                }
                flash("A verification code was sent to your email. Enter it below to continue.", "info")
                return redirect(url_for("guardian.guardian_login_otp"))
            session["guardian_logged_in"] = True
            session["guardian_student_id"] = sid
            session["school_id"] = school_id
            token = _sign_token(sid)
            session["guardian_token"] = token
            flash("Login successful.", "success")
            return redirect(url_for("guardian.guardian_dashboard"))
        finally:
            try:
                db.close()
            except Exception:
                pass

    return render_template("guardian_login.html")


@guardian_bp.route("/login/otp", methods=["GET", "POST"])
def guardian_login_otp():
    ctx = _guardian_otp_context()
    if not ctx:
        flash("Enter your login details first so we can deliver the verification code.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    if not current_app.config.get("PARENT_EMAIL_AUTH_ENABLED", False):
        flash("Parent email OTP login is temporarily disabled. Contact the school office to log in.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    now_ts = datetime.now().timestamp()
    remaining = max(0, int(ctx.get("until", 0) - now_ts))
    if remaining <= 0:
        _clear_guardian_otp_context()
        flash("The code expired after 10 minutes. Please login again.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        code = "".join(ch for ch in code if ch.isdigit())
        if not code:
            flash("Enter the six-digit code from your email.", "warning")
            return redirect(url_for("guardian.guardian_login_otp"))
        if code != ctx.get("code"):
            flash("Incorrect code. Check the email and try again.", "error")
            return redirect(url_for("guardian.guardian_login_otp"))

        sid = int(ctx.get("student_id") or 0)
        school_id = int(ctx.get("school_id") or 0)
        token = _sign_token(sid)
        session["guardian_logged_in"] = True
        session["guardian_student_id"] = sid
        session["guardian_token"] = token
        session["school_id"] = school_id
        db = None
        try:
            db = _db()
            cur = db.cursor()
            cur.execute("UPDATE students SET parent_email_verified=1, parent_email_verified_at=NOW() WHERE id=%s", (sid,))
            db.commit()
        except Exception:
            try:
                db and db.rollback()
            except Exception:
                pass
        finally:
            try:
                db and db.close()
            except Exception:
                pass
        _clear_guardian_otp_context()
        flash("Login successful.", "success")
        return redirect(url_for("guardian.guardian_dashboard"))

    return render_template(
        "login_otp.html",
        portal_title="Guardian portal verification",
        portal_label="Parent / Guardian OTP",
        heading="Verify your identity",
        summary="We sent a secure code to your registered email to make sure only you can view this student's fees.",
        email_display=mask_email(ctx.get("email")),
        countdown_seconds=remaining,
        countdown_label=f"{remaining // 60} min {remaining % 60} sec",
        form_action=url_for("guardian.guardian_login_otp"),
        resend_url=url_for("guardian.guardian_login_otp_resend"),
        back_url=url_for("guardian.guardian_login"),
        portal_note="Code expires after 10 minutes and is valid for a single login.",
    )


@guardian_bp.route("/login/otp/resend", methods=["POST"])
def guardian_login_otp_resend():
    ctx = _guardian_otp_context()
    if not ctx:
        flash("Start the login flow first so we know where to send the code.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    if not current_app.config.get("PARENT_EMAIL_AUTH_ENABLED", False):
        flash("Parent email OTP login is temporarily disabled. Contact the school office to log in.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    target_email = ctx.get("email")
    if not target_email:
        flash("No email is available for this account. Update the student record and try again.", "error")
        return redirect(url_for("guardian.guardian_login"))

    otp_code = generate_login_otp()
    ctx["code"] = otp_code
    ctx["until"] = (datetime.now() + timedelta(minutes=LOGIN_OTP_EXPIRES_MINUTES)).timestamp()
    ctx["sent_at"] = datetime.now().timestamp()
    session["guardian_otp_context"] = ctx
    sent = send_portal_login_otp(
        target_email,
        ctx.get("name") or "Guardian",
        "Parent / Guardian Portal",
        otp_code,
        LOGIN_OTP_EXPIRES_MINUTES,
    )
    if not sent:
        flash("Unable to resend the code. Try again shortly.", "error")
        return redirect(url_for("guardian.guardian_login_otp"))

    flash("Check your inbox; a fresh code has been sent.", "info")
    return redirect(url_for("guardian.guardian_login_otp"))


@guardian_bp.route("/dashboard", methods=["GET"])
def guardian_dashboard():
    """Guardian dashboard with student, fees and payments overview."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return redirect(url_for("guardian.guardian_login"))

    from routes.student_portal import _verify_token  # avoid cycle at import time
    student_id = _verify_token(token)
    if not student_id:
        flash("Session expired. Please login again.", "warning")
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT s.*, sc.name AS school_name, sc.code AS school_code
        FROM students s
        LEFT JOIN schools sc ON sc.id = s.school_id
        WHERE s.id=%s
        """,
        (student_id,)
    )
    student = cur.fetchone() or {}
    # Add admission number alias for templates expecting 'regNo'
    try:
        if student and ('regNo' not in student):
            _v = student.get('admission_no') or student.get('reg_no')
            if _v is not None:
                student['regNo'] = _v
    except Exception:
        pass

    ensure_academic_terms_table(db)
    year, term = get_or_seed_current_term(db)

    auto_credit_notice = None
    try:
        portal_url = url_for("guardian.guardian_dashboard", token=token, _external=True)
        auto_credit_notice = auto_apply_credit_if_new_term(
            db,
            student,
            int(session.get("school_id") or 0),
            year,
            term,
            portal_url,
        )
        if auto_credit_notice:
            student["credit"] = auto_credit_notice.get("new_credit", student.get("credit"))
            balance_col = "balance" if "balance" in student else ("fee_balance" if "fee_balance" in student else None)
            if balance_col:
                student[balance_col] = auto_credit_notice.get("new_balance", student.get(balance_col, 0))
            else:
                student["balance"] = auto_credit_notice.get("new_balance", student.get("balance", 0))
    except Exception:
        auto_credit_notice = None

    cur.execute(
        "SELECT id, amount, method, reference, date FROM payments WHERE student_id=%s ORDER BY date DESC, id DESC LIMIT 10",
        (student_id,)
    )
    payments = cur.fetchall() or []

    # Analytics data: monthly trend, method mix, averages
    analytics = {
        "monthly": {"labels": [], "values": [], "growth": 0},
        "avg_monthly": 0,
        "active_months": 0,
        "last_payment_date": None,
        "last_payment_amount": 0,
        "methods": [],
    }
    try:
        now = east_africa_now()
        months = []
        year_iter = now.year
        month_iter = now.month
        for _ in range(12):
            months.append((year_iter, month_iter))
            month_iter -= 1
            if month_iter == 0:
                month_iter = 12
                year_iter -= 1
        months.reverse()
        start_date = datetime(year=months[0][0], month=months[0][1], day=1)
        cur.execute(
            "SELECT DATE_FORMAT(date, '%%Y-%%m') AS ym, COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s AND date >= %s GROUP BY ym ORDER BY ym ASC",
            (student_id, start_date),
        )
        monthly_totals = {}
        for row in cur.fetchall() or []:
            if not row:
                continue
            ym = row.get("ym") if isinstance(row, dict) else (row[0] if row else None)
            total = row.get("total") if isinstance(row, dict) else (row[1] if row and len(row) > 1 else 0)
            if not ym:
                continue
            monthly_totals[str(ym)] = float(total or 0)
        labels = []
        values = []
        for yr, mo in months:
            key = f"{yr:04d}-{mo:02d}"
            labels.append(datetime(year=yr, month=mo, day=1).strftime("%b %y"))
            values.append(float(monthly_totals.get(key, 0)))
        total_paid = sum(values)
        avg_monthly = round(total_paid / len(values) if values else 0, 2)
        active_months = sum(1 for v in values if v > 0)
        growth = 0
        if len(values) >= 2:
            prev_val = values[-2]
            curr_val = values[-1]
            if prev_val:
                growth = round(((curr_val - prev_val) / prev_val) * 100, 1)
            elif curr_val:
                growth = round(curr_val * 100, 1)
        last_payment = payments[0] if payments else None
        last_payment_date = None
        last_payment_amount = 0
        if last_payment:
            raw_date = last_payment.get("date") if isinstance(last_payment, dict) else None
            formatted = format_east_africa(raw_date, "%b %d, %Y %H:%M")
            if formatted:
                last_payment_date = formatted
            elif raw_date is not None:
                last_payment_date = str(raw_date)
            last_payment_amount = float(last_payment.get("amount") or 0)

        cur.execute(
            "SELECT method, COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s GROUP BY method ORDER BY total DESC LIMIT 4",
            (student_id,),
        )
        methods = []
        for row in cur.fetchall() or []:
            if not row:
                continue
            source = row if isinstance(row, dict) else {}
            method_name = (source.get("method") if isinstance(row, dict) else row[0]) or "Other"
            total = source.get("total") if isinstance(row, dict) else (row[1] if len(row) > 1 else 0)
            methods.append({"method": method_name, "total": float(total or 0)})

        analytics = {
            "monthly": {"labels": labels, "values": values, "growth": growth},
            "avg_monthly": avg_monthly,
            "active_months": active_months,
            "last_payment_date": last_payment_date,
            "last_payment_amount": round(last_payment_amount, 2),
            "methods": methods,
        }
    except Exception:
        pass

    # Fee summary
    cur.execute("SELECT COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
    row = cur.fetchone(); paid_term = float((row.get("total") if isinstance(row, dict) else (row[0] if row else 0)) or 0)
    expected = 0.0
    try:
        cur.execute("SELECT COALESCE(SUM(amount),0) FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
        r1 = cur.fetchone(); expected = float((r1[0] if isinstance(r1, (list, tuple)) else (r1 or 0)) or 0)
        if expected <= 0:
            cur.execute("SELECT COALESCE(SUM(fee_amount),0) FROM term_fees WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
            r2 = cur.fetchone(); expected = float((r2[0] if isinstance(r2, (list, tuple)) else (r2 or 0)) or 0)
    except Exception:
        expected = 0.0
    try:
        bal = float(student.get("balance") or student.get("fee_balance") or 0)
    except Exception:
        bal = 0.0
    if expected <= 0 and (paid_term or bal):
        expected = paid_term + bal
    pct = int(round((paid_term / expected) * 100)) if expected > 0 else None

    try:
        from utils.settings import get_setting
        bursar_email = get_setting("SCHOOL_EMAIL") or get_setting("ACCOUNTS_EMAIL")
        bursar_phone = get_setting("SCHOOL_PHONE") or get_setting("ACCOUNTS_PHONE")
    except Exception:
        bursar_email = None; bursar_phone = None

    # Recent STK activity (pending/success/failed). Also map success rows to payment receipts.
    try:
        # Build a quick index by reference -> payment_id for linking receipts
        cur.execute(
            "SELECT id, reference FROM payments WHERE student_id=%s ORDER BY id DESC LIMIT 100",
            (student_id,),
        )
        _pindex = cur.fetchall() or []
        ref_to_pid = {}
        for r in _pindex:
            rid = r["id"] if isinstance(r, dict) else r[0]
            rref = r["reference"] if isinstance(r, dict) else r[1]
            if rref:
                ref_to_pid[str(rref)] = int(rid)

        cur.execute(
            """
            SELECT checkout_request_id, merchant_request_id, amount, phone, result_code, result_desc, mpesa_receipt, updated_at
            FROM mpesa_student_payments
            WHERE student_id=%s
            ORDER BY updated_at DESC, id DESC
            LIMIT 10
            """,
            (student_id,),
        )
        _stk_all = cur.fetchall() or []
        stk_activity = []
        for r in _stk_all:
            rc = r.get("result_code") if isinstance(r, dict) else None
            receipt = r.get("mpesa_receipt") if isinstance(r, dict) else None
            crid = r.get("checkout_request_id") if isinstance(r, dict) else None
            status = "pending" if (rc is None) else ("success" if str(rc) == "0" else "failed")
            # Derive reference for linking receipts
            ref = receipt or (f"MP_{crid}" if crid else None)
            pid = ref_to_pid.get(str(ref)) if ref else None
            stk_activity.append({
                "checkout_request_id": crid,
                "amount": r.get("amount") if isinstance(r, dict) else None,
                "phone": r.get("phone") if isinstance(r, dict) else None,
                "result_code": rc,
                "result_desc": r.get("result_desc") if isinstance(r, dict) else None,
                "updated_at": r.get("updated_at") if isinstance(r, dict) else None,
                "status": status,
                "payment_id": pid,
            })
    except Exception:
        stk_activity = []

    # Build multi-child list: match by parent email/phone when present; fallback to same last name
    siblings: list[dict] = []
    try:
        # Try parent email/phone columns
        key_email = None; key_phone = None
        try:
            cur2 = db.cursor()
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_email'");
            has_parent_email = bool(cur2.fetchone())
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_phone'");
            has_parent_phone = bool(cur2.fetchone())
        except Exception:
            has_parent_email = False; has_parent_phone = False
        if has_parent_email:
            key_email = (student.get('parent_email') or student.get('email') or '').strip()
        if has_parent_phone:
            key_phone = (student.get('parent_phone') or student.get('phone') or '').strip()
        if key_email or key_phone:
            q = ["SELECT id, name, admission_no AS regNo FROM students WHERE school_id=%s AND id<>%s"]
            ps = [int(student.get('school_id') or session.get('school_id') or 0), int(student_id)]
            if key_email:
                q.append("AND parent_email=%s")
                ps.append(key_email)
            if key_phone:
                q.append("AND parent_phone=%s")
                ps.append(key_phone)
            cur.execute(" ".join(q), tuple(ps))
            siblings = cur.fetchall() or []
        # Fallback to same last name within school
        if not siblings:
            last = str(student.get('name') or '').split()[-1] if (student and student.get('name')) else ''
            if last:
                cur.execute(
                    """
                    SELECT id, name, admission_no AS regNo FROM students
                    WHERE school_id=%s AND id<>%s AND LOWER(TRIM(SUBSTRING_INDEX(name,' ', -1))) = LOWER(TRIM(%s))
                    ORDER BY name
                    """,
                    (int(student.get('school_id') or session.get('school_id') or 0), int(student_id), last),
                )
                siblings = cur.fetchall() or []
    except Exception:
        siblings = []

    # Notices & announcements (premium): reuse newsletters as announcements
    announcements: list[dict] = []
    try:
        cur2 = db.cursor(dictionary=True)
        sid = int(student.get('school_id') or session.get('school_id') or 0)
        cur2.execute(
            """
            SELECT id, category, title, subject, html, created_at
            FROM newsletters
            WHERE (school_id=%s OR school_id IS NULL)
            ORDER BY id DESC
            LIMIT 8
            """,
            (sid,)
        )
        announcements = cur2.fetchall() or []
    except Exception:
        announcements = []

    db.close()
    proof_statuses = _guardian_receipts_for_student(student_id, session.get("school_id") or 0)
    return render_template(
        "guardian_dashboard.html",
        student=student,
        payments=payments,
        year=year,
        term=term,
        expected=expected,
        paid=paid_term,
        balance=bal if bal else max(expected - paid_term, 0.0),
        percent=pct,
        token=token,
        bursar_email=bursar_email,
        bursar_phone=bursar_phone,
        siblings=siblings,
        stk_activity=stk_activity,
        announcements=announcements,
        paypal_client_id=current_app.config.get("PAYPAL_CLIENT_ID") or "",
        paypal_currency=current_app.config.get("PAYPAL_CURRENCY") or "USD",
        analytics=analytics,
        proof_statuses=proof_statuses,
        auto_credit_notice=auto_credit_notice,
    )


@guardian_bp.route("/payment-proof/submit", methods=["POST"])
def guardian_payment_proof_submit():
    if not session.get("guardian_logged_in"):
        return jsonify({"ok": False, "error": "Authentication required"}), 403
    student_id = int(session.get("guardian_student_id") or 0)
    school_id = int(session.get("school_id") or 0)
    if not student_id or not school_id:
        return jsonify({"ok": False, "error": "Invalid session"}), 403
    proof_file = request.files.get("payment_proof")
    if not proof_file or not proof_file.filename:
        return jsonify({"ok": False, "error": "Attach a payment proof file"}), 400
    if not allowed_proof_file(proof_file.filename):
        return jsonify({"ok": False, "error": "Unsupported file format (PNG/JPG/PDF)"}), 400

    amount_raw = (request.form.get("amount") or "").strip()
    try:
        amount_val = float(amount_raw)
        if amount_val <= 0:
            raise ValueError
    except Exception:
        return jsonify({"ok": False, "error": "Enter a valid amount"}), 400

    payment_date_raw = (request.form.get("payment_date") or "").strip()
    payment_date = None
    if payment_date_raw:
        try:
            payment_date = datetime.strptime(payment_date_raw, "%Y-%m-%d").date()
        except Exception:
            payment_date = None

    guardian_name = (request.form.get("guardian_name") or "").strip()
    guardian_email = (request.form.get("guardian_email") or "").strip()
    guardian_phone = (request.form.get("guardian_phone") or "").strip()
    description = (request.form.get("description") or "").strip()
    notes = (request.form.get("notes") or "").strip()
    bank_name = (request.form.get("bank_name") or "").strip()

    db = _db()
    try:
        ensure_guardian_receipts_table(db)
        cur = db.cursor(dictionary=True)
        has_parent_name = False
        try:
            cur2 = db.cursor()
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_name'")
            has_parent_name = bool(cur2.fetchone())
        except Exception:
            pass
        select_cols = ["parent_email", "parent_phone", "phone", "name"]
        if has_parent_name:
            select_cols.insert(0, "parent_name")
        cur.execute(f"SELECT {', '.join(select_cols)} FROM students WHERE id=%s", (student_id,))
        student_row = cur.fetchone() or {}
        final_name = guardian_name or student_row.get("parent_name") or student_row.get("name") or "Parent"
        final_email = guardian_email or student_row.get("parent_email") or ""
        final_phone = guardian_phone or student_row.get("parent_phone") or student_row.get("phone") or ""
        path = save_payment_proof_file(proof_file, school_id)
        now = datetime.utcnow()
        metadata = extract_proof_metadata(path, description or None)
        analysis = _describe_proof_authenticity(metadata)
        cur2 = db.cursor()
        cur2.execute(
            """
            INSERT INTO guardian_receipts (
                school_id, student_id, guardian_name, guardian_email, guardian_phone,
                description, notes, file_path, status, payment_date, amount, bank_name,
                analysis, created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                school_id,
                student_id,
                final_name,
                final_email,
                final_phone,
                description,
                notes or description,
                path,
                "pending",
                payment_date,
                amount_val,
                bank_name,
                analysis,
                now,
                now,
            ),
        )
        receipt_id = cur2.lastrowid
        record_payment_source(
            db=db,
            school_id=school_id,
            student_id=student_id,
            source_type="proof_upload",
            source_ref=str(receipt_id),
            status="pending",
            amount=amount_val,
            raw_text=description,
        )
        log_payment_status(
            db=db,
            school_id=school_id,
            student_id=student_id,
            receipt_id=int(receipt_id or 0) or None,
            status="pending",
            actor="guardian",
            note="Proof uploaded",
        )
        db.commit()
        return jsonify({"ok": True, "message": "Payment proof submitted. We will review it shortly."})
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        try:
            db.close()
        except Exception:
            pass


@guardian_bp.route("/payment-proof/mpesa-text", methods=["POST"])
def guardian_payment_mpesa_text():
    if not session.get("guardian_logged_in"):
        return jsonify({"ok": False, "error": "Authentication required"}), 403
    student_id = int(session.get("guardian_student_id") or 0)
    school_id = int(session.get("school_id") or 0)
    if not student_id or not school_id:
        return jsonify({"ok": False, "error": "Invalid session"}), 403
    payload = request.get_json(silent=True) or request.form
    mpesa_text = (payload.get("mpesa_text") or "").strip()
    if not mpesa_text:
        return jsonify({"ok": False, "error": "Paste the M-Pesa message text."}), 400
    amount_raw = (payload.get("amount") or "").strip()
    try:
        amount_val = float(amount_raw)
        if amount_val <= 0:
            raise ValueError
    except Exception:
        return jsonify({"ok": False, "error": "Enter a valid amount."}), 400
    payment_date_raw = (payload.get("payment_date") or "").strip()
    payment_date = None
    if payment_date_raw:
        try:
            payment_date = datetime.strptime(payment_date_raw, "%Y-%m-%d").date()
        except Exception:
            payment_date = None
    bank_name = (payload.get("bank_name") or "M-Pesa").strip()
    account_ref = (payload.get("account_ref") or "").strip()

    db = _db()
    try:
        ensure_guardian_receipts_table(db)
        cur = db.cursor(dictionary=True)
        has_parent_name = False
        try:
            cur2 = db.cursor()
            cur2.execute("SHOW COLUMNS FROM students LIKE 'parent_name'")
            has_parent_name = bool(cur2.fetchone())
        except Exception:
            pass
        select_cols = ["parent_email", "parent_phone", "phone", "name"]
        if has_parent_name:
            select_cols.insert(0, "parent_name")
        cur.execute(f"SELECT {', '.join(select_cols)} FROM students WHERE id=%s", (student_id,))
        student_row = cur.fetchone() or {}
        final_name = student_row.get("parent_name") or student_row.get("name") or "Parent"
        final_email = student_row.get("parent_email") or ""
        final_phone = student_row.get("parent_phone") or student_row.get("phone") or ""
        snippet = (mpesa_text[:120] + "...") if len(mpesa_text) > 120 else mpesa_text
        description = "M-Pesa paybill confirmation"
        if account_ref:
            description += f" (Account: {account_ref})"
        if snippet:
            description += f": {snippet}"
        notes = f"MPesa message:\n{mpesa_text}"
        now = datetime.utcnow()
        cur2 = db.cursor()
        cur2.execute(
            """
            INSERT INTO guardian_receipts (
                school_id, student_id, guardian_name, guardian_email, guardian_phone,
                description, notes, file_path, status, payment_date, amount, bank_name,
                created_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                school_id,
                student_id,
                final_name,
                final_email,
                final_phone,
                description,
                notes,
                "",
                "pending",
                payment_date,
                amount_val,
                bank_name,
                now,
                now,
            ),
        )
        receipt_id = cur2.lastrowid
        record_payment_source(
            db=db,
            school_id=school_id,
            student_id=student_id,
            source_type="mpesa_text",
            source_ref=str(receipt_id),
            status="pending",
            amount=amount_val,
            raw_text=mpesa_text,
        )
        log_payment_status(
            db=db,
            school_id=school_id,
            student_id=student_id,
            receipt_id=int(receipt_id or 0) or None,
            status="pending",
            actor="guardian",
            note="M-Pesa message submitted",
        )
        db.commit()
        return jsonify({"ok": True, "message": "M-Pesa message sent to the school for verification."})
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        try:
            db.close()
        except Exception:
            pass


@guardian_bp.route("/payment-proof/statuses", methods=["GET"])
def guardian_payment_proof_statuses():
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid token"}), 403
    school_id = int(session.get("school_id") or 0)
    proofs = _guardian_receipts_for_student(student_id, school_id, limit=6)
    return jsonify({"ok": True, "proofs": proofs})


@guardian_bp.route("/switch")
def guardian_switch():
    """Switch current child to another allowed student id, then redirect to dashboard.

    Allows switching among siblings (same parent email/phone) or same last name within same school.
    """
    try:
        sid = int(request.args.get("sid", "0"))
    except Exception:
        sid = 0
    token = (session.get("guardian_token") or request.args.get("token") or "").strip()
    if not sid or not token:
        return redirect(url_for("guardian.guardian_login"))
    from routes.student_portal import _verify_token
    cur_sid = _verify_token(token)
    if not cur_sid:
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    # Fetch current + target
    cur.execute("SELECT id, name, school_id, parent_email, parent_phone, email, phone FROM students WHERE id=%s", (cur_sid,))
    a = cur.fetchone() or {}
    cur.execute("SELECT id, name, school_id, parent_email, parent_phone, email, phone FROM students WHERE id=%s", (sid,))
    b = cur.fetchone() or {}
    allowed = False
    try:
        if a and b and int(a.get('school_id') or 0) == int(b.get('school_id') or 0):
            last_a = str((a.get('name') or '').split()[-1]).strip().lower()
            last_b = str((b.get('name') or '').split()[-1]).strip().lower()
            if last_a and last_a == last_b:
                allowed = True
            ea = (a.get('parent_email') or a.get('email') or '').strip()
            eb = (b.get('parent_email') or b.get('email') or '').strip()
            pa = (a.get('parent_phone') or a.get('phone') or '').strip()
            pb = (b.get('parent_phone') or b.get('phone') or '').strip()
            if ea and eb and ea == eb:
                allowed = True
            if pa and pb and pa == pb:
                allowed = True
    except Exception:
        allowed = False
    if not allowed:
        db.close()
        flash("Not allowed to switch to this student.", "warning")
        return redirect(url_for("guardian.guardian_dashboard"))

    # Issue new token for the target student
    new_token = _sign_token(int(b.get('id')))
    session["guardian_token"] = new_token
    session["guardian_student_id"] = int(b.get('id'))
    session["school_id"] = int(b.get('school_id') or session.get('school_id') or 0)
    db.close()
    return redirect(url_for("guardian.guardian_dashboard", token=new_token))


@guardian_bp.route("/receipt/<int:payment_id>")
def guardian_receipt(payment_id: int):
    """Printable HTML receipt for a payment belonging to the logged-in student."""
    tok = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not tok:
        return redirect(url_for("guardian.guardian_login"))
    from routes.student_portal import _verify_token
    sid = _verify_token(tok)
    if not sid:
        return redirect(url_for("guardian.guardian_login"))

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT p.id, p.student_id AS sid, s.name AS student_name, s.class_name, p.amount, p.method,
               p.reference, p.date, p.term, p.year
        FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE p.id = %s AND p.student_id = %s
        """,
        (payment_id, sid),
    )
    payment = cur.fetchone()
    if not payment:
        db.close()
        flash("Payment not found for your account.", "error")
        return redirect(url_for("guardian.guardian_dashboard"))

    # Balance snapshot for display
    try:
        cur.execute("SELECT COALESCE(balance, fee_balance) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s", (sid,))
        srow = cur.fetchone() or {}
        bal = float(srow.get("balance") or 0)
        cred = float(srow.get("credit") or 0)
    except Exception:
        bal = None; cred = None
    db.close()

    brand = (current_app.config.get("APP_NAME") or f"{current_app.config.get('BRAND_NAME','Lovato_Tech')} {current_app.config.get('PORTAL_TITLE','Fee Management portal')}").strip()

    # Render a minimal, printable page (reusing receipt.html but without PDF link logic)
    try:
        verify_url = None
    except Exception:
        verify_url = None
    date_str = ""
    try:
        pdate = payment.get("date")
        if hasattr(pdate, "strftime"):
            date_str = pdate.strftime("%Y-%m-%d %H:%M")
        else:
            date_str = str(pdate or "")
    except Exception:
        date_str = ""
    auth_qr_data = build_document_qr(
        "guardian_receipt",
        {
            "rid": int(payment.get("id")),
            "sid": int(payment.get("sid")),
            "amt": round(float(payment.get("amount") or 0.0), 2),
            "cur": "KES",
            "name": payment.get("student_name") or "",
            "cls": payment.get("class_name") or "",
            "dt": date_str,
            "m": payment.get("method") or "",
            "ref": payment.get("reference") or "",
            "term": payment.get("term") or "",
            "year": payment.get("year") or "",
            "school_id": session.get("school_id"),
        },
    )
    return render_template(
        "guardian_receipt.html",
        brand=brand,
        payment=payment,
        current_balance=bal,
        current_credit=cred,
        payment_link=None,
        verify_url=verify_url,
        auth_qr_data=auth_qr_data,
    )


@guardian_bp.route("/upload-receipt", methods=["GET", "POST"])
def guardian_receipt_upload():
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not session.get("guardian_logged_in"):
        return redirect(url_for("guardian.guardian_login"))
    if not token:
        return redirect(url_for("guardian.guardian_login"))
    student_id = _verify_token(token)
    if not student_id:
        return redirect(url_for("guardian.guardian_login"))

    if request.method == "POST":
        file = request.files.get("receipt")
        if not file or not file.filename:
            flash("Select a file to upload.", "warning")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        if not _allowed_receipt_file(file.filename):
            flash("Unsupported file type. Use PNG/JPG/PDF.", "error")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        school_id = session.get("school_id") or 0
        dest_dir = _guardian_upload_path(int(school_id))
        filename = f"{uuid.uuid4().hex}_{secure_filename(file.filename)}"
        target_path = dest_dir / filename
        db_conn = None
        try:
            file.save(target_path)
            db_conn = _db()
            ensure_guardian_receipts_table(db_conn)
            now = datetime.utcnow()
            rel = os.path.join(str(current_app.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR", "uploads/guardian_receipts")), str(school_id), filename).replace("\\", "/")
            cur = db_conn.cursor()
            desc_text = (request.form.get("description") or "").strip()
            analysis_text = _describe_proof_authenticity(extract_proof_metadata(rel, desc_text or None))
            cur.execute(
                """
                INSERT INTO guardian_receipts
                    (school_id, student_id, guardian_name, guardian_email, guardian_phone, description, file_path, analysis, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    int(school_id),
                    student_id,
                    (request.form.get("guardian_name") or "").strip(),
                    (request.form.get("guardian_email") or "").strip(),
                    (request.form.get("guardian_phone") or "").strip(),
                    (request.form.get("description") or "").strip(),
                    rel,
                    analysis_text,
                    now,
                    now,
                ),
            )
            db_conn.commit()
            flash("Receipt uploaded and pending verification.", "success")
            return redirect(url_for("guardian.guardian_receipt_upload"))
        except Exception:
            flash("Upload failed; please try again.", "error")
        finally:
            if db_conn:
                try:
                    db_conn.close()
                except Exception:
                    pass
    return render_template("guardian_receipt_upload.html")


@guardian_bp.route("/make_payment", methods=["POST"])
@limiter.limit("3 per minute")
def guardian_make_payment():
    """Initiate M-Pesa STK push for the guardian context."""
    from utils.mpesa import stk_push, DarajaError
    token = (request.json.get("token") if request.is_json else (request.form.get("token") or None)) or session.get("guardian_token")
    if not token:
        return jsonify({"ok": False, "error": "Not logged in"}), 401

    from routes.student_portal import _verify_token
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired session"}), 403

    phone = (request.json.get("phone") if request.is_json else request.form.get("phone") or "").strip()
    try:
        amount = int((request.json.get("amount") if request.is_json else request.form.get("amount") or 0))
    except Exception:
        amount = 0
    if not phone or amount <= 0:
        return jsonify({"ok": False, "error": "Phone and amount are required"}), 400

    db = _db(); ensure_academic_terms_table(db); ensure_mpesa_student_table(db)
    cur = db.cursor(dictionary=True)
    cur.execute("SELECT school_id, name FROM students WHERE id=%s", (student_id,))
    srow = cur.fetchone() or {}
    school_id = srow.get("school_id")
    y, t = get_or_seed_current_term(db)

    account_ref = (srow.get("name") or f"STUDENT-{student_id}")[:20]
    trans_desc = f"Fees payment T{t}/{y}"
    try:
        res = stk_push(phone=phone, amount=amount, account_ref=account_ref, trans_desc=trans_desc)
    except DarajaError as e:
        db.close()
        return jsonify({"ok": False, "error": str(e)}), 400

    from datetime import datetime as _dt
    now = _dt.now()
    cur2 = db.cursor()
    cur2.execute(
        """
        INSERT INTO mpesa_student_payments
            (student_id, school_id, year, term, merchant_request_id, checkout_request_id, amount, phone, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            student_id,
            school_id,
            y,
            t,
            res.get("MerchantRequestID"),
            res.get("CheckoutRequestID"),
            amount,
            phone,
            now,
            now,
        ),
    )
    db.commit(); db.close()
    return jsonify({
        "ok": True,
        "message": "STK push sent. Check your phone to authorize.",
        "checkout_request_id": res.get("CheckoutRequestID"),
    })


@guardian_bp.route("/bank-connect", methods=["POST"])
@limiter.limit("6 per minute")
def guardian_bank_connect_payment():
    """Record a bank-connect payment for the guardian context (simulated approval)."""
    payload = request.get_json(silent=True) or request.form
    token = (payload.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired session"}), 403

    bank = (payload.get("bank") or payload.get("bank_name") or "").strip()
    account_last4 = (payload.get("account_last4") or "").strip()
    account_name = (payload.get("account_name") or "").strip()
    bank_phone = (payload.get("bank_phone") or "").strip()
    try:
        amount_val = float(payload.get("amount") or 0)
    except Exception:
        amount_val = 0.0
    if amount_val <= 0:
        return jsonify({"ok": False, "error": "Amount must be greater than zero."}), 400

    db = _db()
    try:
        ensure_academic_terms_table(db)
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT school_id, name FROM students WHERE id=%s", (student_id,))
        srow = cur.fetchone() or {}
        school_id = srow.get("school_id")
        if not school_id:
            return jsonify({"ok": False, "error": "Student not found."}), 404
        year, term = get_or_seed_current_term(db)
        ref = f"BNK-{uuid.uuid4().hex[:12].upper()}"
        if account_last4:
            ref = f"{ref}-{account_last4}"
        payment_id = record_mpesa_payment_if_missing(
            db=db,
            student_id=int(student_id),
            amount=amount_val,
            reference=ref,
            school_id=school_id,
            year=year,
            term=term,
            now=datetime.now(),
            method="Bank Connect",
        )
        if not payment_id:
            return jsonify({"ok": False, "error": "Payment already recorded or invalid."}), 400
    finally:
        try:
            db.close()
        except Exception:
            pass

    hint = f"{bank} • ****{account_last4}" if bank or account_last4 else "Bank Connect"
    msg = f"Bank payment of KES {amount_val:,.2f} recorded for {srow.get('name') or 'student'} ({hint})."
    return jsonify({"ok": True, "message": msg, "payment_id": payment_id})


@guardian_bp.route("/status", methods=["GET"])
def guardian_payment_status():
    """Check status of an M-Pesa checkout for the logged-in guardian.

    Query string: crid=<CheckoutRequestID>
    Returns {ok:bool, status:str, result_code:int|None, receipt:str|None}
    """
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    crid = (request.args.get("crid") or "").strip()
    if not token or not crid:
        return jsonify({"ok": False, "error": "Missing token or crid"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid session"}), 401

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT result_code, result_desc, mpesa_receipt, amount, phone, updated_at FROM mpesa_student_payments WHERE checkout_request_id=%s AND student_id=%s",
        (crid, sid),
    )
    row = cur.fetchone()
    db.close()
    if not row:
        return jsonify({"ok": True, "status": "pending", "result_code": None})
    rc = row.get("result_code")
    if rc is None:
        return jsonify({"ok": True, "status": "pending", "result_code": None})
    try:
        rc_int = int(rc)
    except (TypeError, ValueError):
        rc_int = 0
    if rc_int == 0:
        return jsonify({"ok": True, "status": "success", "result_code": 0, "receipt": row.get("mpesa_receipt")})
    if rc_int == -1:
        return jsonify({"ok": True, "status": "canceled", "result_code": -1, "message": row.get("result_desc")})
    return jsonify({"ok": True, "status": "failed", "result_code": rc_int, "message": row.get("result_desc")})


@guardian_bp.route("/cancel-stk", methods=["POST"])
@limiter.limit("6 per minute")
def guardian_cancel_stk_push():
    """Allow the guardian UI to mark a pending STK push as canceled."""
    payload = request.get_json(silent=True) or {}
    token = payload.get("token") or request.form.get("token") or session.get("guardian_token")
    crid = (payload.get("crid") or request.form.get("crid") or "").strip()
    if not token or not crid:
        return jsonify({"ok": False, "error": "Missing token or checkout_request_id"}), 400
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid session"}), 401

    db_conn = _db()
    try:
        ensure_mpesa_student_table(db_conn)
        cur = db_conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, result_code FROM mpesa_student_payments WHERE checkout_request_id=%s AND student_id=%s LIMIT 1",
            (crid, sid),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "error": "Payment not found"}), 404
        if row.get("result_code") is not None:
            return jsonify({"ok": False, "error": "Payment already processed"}), 400
        now = datetime.now()
        cur.execute(
            "UPDATE mpesa_student_payments SET result_code=%s, result_desc=%s, updated_at=%s WHERE id=%s",
            (-1, "Canceled by guardian", now, row.get("id")),
        )
        db_conn.commit()
        return jsonify({"ok": True, "message": "STK push canceled."})
    finally:
        try:
            db_conn.close()
        except Exception:
            pass


# --------- PayPal helper functions ---------
def _paypal_base_url() -> str:
    env = (current_app.config.get("PAYPAL_ENV") or "sandbox").strip().lower()
    return "https://api-m.paypal.com" if env == "live" else "https://api-m.sandbox.paypal.com"


def _paypal_access_token() -> str | None:
    cid = (current_app.config.get("PAYPAL_CLIENT_ID") or "").strip()
    sec = (current_app.config.get("PAYPAL_SECRET") or "").strip()
    if not cid or not sec:
        return None
    url = _paypal_base_url() + "/v1/oauth2/token"
    auth = base64.b64encode(f"{cid}:{sec}".encode("utf-8")).decode("ascii")
    try:
        res = requests.post(url, headers={"Authorization": f"Basic {auth}"}, data={"grant_type": "client_credentials"}, timeout=15)
        if res.ok:
            return res.json().get("access_token")
    except Exception:
        return None
    return None


@guardian_bp.route("/paypal/create-order", methods=["POST"])  # POST /g/paypal/create-order
@limiter.limit("6 per minute")
def guardian_paypal_create_order():
    token = (request.json.get("token") if request.is_json else request.form.get("token")) or session.get("guardian_token")
    if not token:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    try:
        amount = str(request.json.get("amount") if request.is_json else request.form.get("amount"))
        if not amount or float(amount) <= 0:
            return jsonify({"ok": False, "error": "Invalid amount"}), 400
    except Exception:
        return jsonify({"ok": False, "error": "Invalid amount"}), 400
    currency = (current_app.config.get("PAYPAL_CURRENCY") or "USD").strip().upper()
    at = _paypal_access_token()
    if not at:
        return jsonify({"ok": False, "error": "PayPal not configured"}), 400
    url = _paypal_base_url() + "/v2/checkout/orders"
    body = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "amount": {"currency_code": currency, "value": str(amount)},
                "description": "School fees payment",
            }
        ],
        "application_context": {"shipping_preference": "NO_SHIPPING"},
    }
    try:
        res = requests.post(url, json=body, headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json"}, timeout=20)
        if not res.ok:
            return jsonify({"ok": False, "error": res.text}), 400
        data = res.json()
        return jsonify({"ok": True, "id": data.get("id")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@guardian_bp.route("/paypal/capture", methods=["POST"])  # POST /g/paypal/capture
@limiter.limit("8 per minute")
def guardian_paypal_capture():
    token = (request.json.get("token") if request.is_json else request.form.get("token")) or session.get("guardian_token")
    order_id = (request.json.get("order_id") if request.is_json else request.form.get("order_id"))
    if not token or not order_id:
        return jsonify({"ok": False, "error": "Missing token or order id"}), 400
    from routes.student_portal import _verify_token
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid session"}), 403
    at = _paypal_access_token()
    if not at:
        return jsonify({"ok": False, "error": "PayPal not configured"}), 400
    # Capture
    url = _paypal_base_url() + f"/v2/checkout/orders/{order_id}/capture"
    try:
        res = requests.post(url, headers={"Authorization": f"Bearer {at}", "Content-Type": "application/json"}, timeout=20)
        if not res.ok:
            return jsonify({"ok": False, "error": res.text}), 400
        cap = res.json()
        status = cap.get("status")
        if status != "COMPLETED":
            return jsonify({"ok": False, "error": f"Status {status}"}), 400
        # Determine amount captured
        try:
            pu = (cap.get("purchase_units") or [{}])[0]
            capr = ((pu.get("payments") or {}).get("captures") or [{}])[0]
            amount_val = float(((capr.get("amount") or {}).get("value")) or 0)
            reference = capr.get("id") or order_id
        except Exception:
            amount_val = 0.0
            reference = order_id
        if amount_val <= 0:
            return jsonify({"ok": False, "error": "Zero amount"}), 400
        # Record payment and update student balance/credit
        db = _db(); cur = db.cursor(dictionary=True)
        cur.execute("SELECT school_id, COALESCE(balance, fee_balance) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s", (student_id,))
        srow = cur.fetchone() or {}
        school_id = int(srow.get("school_id") or 0)
        bal_before = float(srow.get("balance") or 0)
        cred_before = float(srow.get("credit") or 0)
        y, t = get_or_seed_current_term(db)
        # Insert payment
        cur2 = db.cursor()
        cur2.execute(
            "INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s,NOW(),%s)",
            (student_id, amount_val, "PayPal", t, y, reference, school_id),
        )
        # Update balance and credit (cap at expected per student context is not known here; use direct balance reduction)
        overpay = max(amount_val - bal_before, 0.0)
        new_bal = max(bal_before - amount_val, 0.0)
        new_credit = cred_before + overpay
        try:
            cur2.execute("UPDATE students SET balance=%s, credit=%s WHERE id=%s", (new_bal, new_credit, student_id))
        except Exception:
            # fallback legacy column name
            cur2.execute("UPDATE students SET fee_balance=%s, credit=%s WHERE id=%s", (new_bal, new_credit, student_id))
        db.commit(); db.close()
        return jsonify({"ok": True, "status": "COMPLETED", "payment_reference": reference})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500




@guardian_bp.route("/logout")
def guardian_logout():
    """Clear guardian session and return to login."""
    try:
        session.pop("guardian_logged_in", None)
        session.pop("guardian_student_id", None)
        session.pop("guardian_token", None)
    except Exception:
        pass
    return redirect(url_for("guardian.guardian_login"))


@guardian_bp.route("/analytics", methods=["GET"])
def guardian_analytics():
    """Return monthly payment analytics for a guardian-linked student."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403

    from datetime import datetime as _dt
    now = _dt.now(); year_now = now.year; year_prev = year_now - 1
    labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    cur_year = [0.0]*12; prev_year = [0.0]*12
    db = _db(); cur = db.cursor()
    try:
        # Current year
        cur.execute("SELECT MONTH(date) AS m, COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND YEAR(date)=%s GROUP BY MONTH(date)", (sid, year_now))
        for m, s in cur.fetchall() or []:
            if m and 1 <= int(m) <= 12:
                cur_year[int(m)-1] = float(s or 0)
        # Previous year
        cur.execute("SELECT MONTH(date) AS m, COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND YEAR(date)=%s GROUP BY MONTH(date)", (sid, year_prev))
        for m, s in cur.fetchall() or []:
            if m and 1 <= int(m) <= 12:
                prev_year[int(m)-1] = float(s or 0)
    except Exception:
        pass

    # Term summary
    y, t = get_or_seed_current_term(db)
    expected = paid = bal = pct = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(SUM(amount),0) FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        r = cur2.fetchone(); expected = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        expected = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(SUM(amount),0) FROM payments WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        r = cur2.fetchone(); paid = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        paid = 0
    try:
        cur2 = db.cursor()
        cur2.execute("SELECT COALESCE(balance, fee_balance) FROM students WHERE id=%s", (sid,))
        r = cur2.fetchone(); bal = float((r[0] if isinstance(r,(list,tuple)) else r) or 0)
    except Exception:
        bal = 0
    if expected <= 0:
        expected = paid + bal
    pct = int(round((paid/expected)*100)) if expected > 0 else 0
    db.close()
    return jsonify({
        "ok": True,
        "labels": labels,
        "current": cur_year,
        "previous": prev_year,
        "term": {"expected": expected, "paid": paid, "balance": bal, "percent": pct, "year": y, "t": t},
    })

def ensure_events_table(db) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NULL,
            title VARCHAR(200) NOT NULL,
            description TEXT NULL,
            category VARCHAR(40) NULL,
            start_date DATE NOT NULL,
            end_date DATE NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_events_school (school_id),
            INDEX idx_events_dates (start_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    db.commit()

@guardian_bp.route("/events", methods=["GET"])
def guardian_events():
    """List calendar events for the student's school for a specific month.

    Query: token, y, m
    """
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403
    try:
        y = int(request.args.get("y") or 0)
        m = int(request.args.get("m") or 0)
    except Exception:
        y = 0; m = 0
    from datetime import date
    today = date.today()
    if not y: y = today.year
    if not m or m < 1 or m > 12: m = today.month

    db = _db(); ensure_events_table(db)
    cur = db.cursor(dictionary=True)
    # Resolve school id
    cur.execute("SELECT school_id FROM students WHERE id=%s", (sid,))
    srow = cur.fetchone() or {}
    school_id = srow.get('school_id')
    # Build month range
    from calendar import monthrange
    last_day = monthrange(y, m)[1]
    start = f"{y:04d}-{m:02d}-01"; end = f"{y:04d}-{m:02d}-{last_day:02d}"
    cur.execute(
        """
        SELECT id, title, category, description, start_date, end_date
        FROM calendar_events
        WHERE (school_id=%s OR school_id IS NULL)
          AND start_date <= %s AND (end_date IS NULL OR end_date >= %s)
        ORDER BY start_date ASC, id ASC
        """,
        (school_id, end, start)
    )
    rows = cur.fetchall() or []
    db.close()
    return jsonify({"ok": True, "items": rows, "y": y, "m": m})


@guardian_bp.route("/ai_assistant", methods=["POST"])
def guardian_ai_assistant():
    """Guardian AI: if a provider is configured, route to it with context; otherwise fallback to built-ins."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or session.get("guardian_token") or "").strip()
    question = (data.get("question") or "").strip()
    if not token or not question:
        return jsonify({"ok": False, "error": "Missing token or question"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403

    # If AI provider is configured, use it first
    try:
        from utils.ai import ai_is_configured
        from ai_engine.query import handle_query as _handle_query
        if ai_is_configured():
            db = _db(); cur = db.cursor(dictionary=True)
            cur.execute("SELECT name, admission_no AS regNo FROM students WHERE id=%s", (sid,))
            s = cur.fetchone() or {}
            context = f"[Guardian Portal | Student: {s.get('name','')} ({s.get('regNo','')})] "
            payload = {"question": context + question, "school_id": session.get("school_id")}
            try:
                res = _handle_query(payload) or {}
            finally:
                db.close()
            ans = res.get("answer") or res.get("text") or res.get("response") or "I could not find an answer right now."
            return jsonify({"ok": True, "answer": ans, **{k:v for k,v in res.items() if k not in {"answer"}}})
    except Exception:
        pass

    db = _db(); cur = db.cursor(dictionary=True)
    try:
        q_lower = question.lower()

        if rasa_is_available():
            try:
                parsed = rasa_parse(question)
                intent_obj = parsed.get("intent") or {}
                intent = intent_obj.get("name")
                confidence = intent_obj.get("confidence")
                if intent:
                    answer = _handle_guardian_intent(intent, sid, db, cur)
                    if answer:
                        payload = {"ok": True, "answer": answer, "intent": intent}
                        if confidence is not None:
                            payload["intent_confidence"] = confidence
                        return jsonify(payload)
            except Exception:
                pass

        if _balance_trigger(q_lower):
            answer = _guardian_balance_response(sid, db, cur)
            if answer:
                return jsonify({"ok": True, "answer": answer, "intent": "balance_inquiry"})

        if _exam_trigger(q_lower):
            answer = _guardian_exam_response(sid, cur)
            if answer:
                return jsonify({"ok": True, "answer": answer, "intent": "exam_inquiry"})

        if _receipt_trigger(q_lower):
            return jsonify({"ok": True, "answer": _guardian_receipt_response(), "intent": "receipt_help"})

        return jsonify({
            "ok": True,
            "answer": "I can help with: fee balance, next exam date, and how to get receipts. Try asking: 'What is my child's fee balance?'.",
        })
    finally:
        try:
            cur.close()
        except Exception:
            pass
        db.close()


def _balance_trigger(q: str) -> bool:
    return any(word in q for word in ["balance", "fee balance", "how much do i owe", "outstanding"])


def _exam_trigger(q: str) -> bool:
    return any(word in q for word in ["exam", "test", "assessment"])


def _receipt_trigger(q: str) -> bool:
    return any(word in q for word in ["receipt", "proof", "download receipt"])


def _handle_guardian_intent(intent: str, sid: int, db, cur) -> str | None:
    if intent == "balance_inquiry":
        return _guardian_balance_response(sid, db, cur)
    if intent == "exam_inquiry":
        return _guardian_exam_response(sid, cur)
    if intent == "receipt_help":
        return _guardian_receipt_response()
    if intent == "greet":
        return "Hi! SmartEduPay is here to help you with balances, exam updates, and receipts."
    if intent == "goodbye":
        return "Goodbye! Reach back anytime if you need another update."
    if intent == "thank_you":
        return "You're very welcome. Happy to help!"
    return None


def _guardian_balance_response(sid: int, db, cur) -> str | None:
    bal = 0.0
    paid = 0.0
    expected = 0.0
    y, t = get_or_seed_current_term(db)
    try:
        cur.execute("SELECT COALESCE(balance, fee_balance) AS bal FROM students WHERE id=%s", (sid,))
        r = cur.fetchone() or {}
        bal = float(r.get('bal') or 0.0)
    except Exception:
        bal = 0.0
    try:
        cur.execute("SELECT COALESCE(SUM(amount),0) AS total FROM payments WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        p = cur.fetchone() or {}
        paid = float(p.get('total') or 0.0)
    except Exception:
        paid = 0.0
    try:
        cur.execute("SELECT COALESCE(SUM(amount),0) AS tot FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (sid, y, t))
        e = cur.fetchone() or {}
        expected = float(e.get('tot') or 0.0)
    except Exception:
        expected = 0.0
    if expected <= 0 and bal:
        expected = bal + paid
    pct = int(round((paid / expected) * 100)) if expected > 0 else 0
    return f"Your child's current balance is KES {bal:,.0f}. Paid this term: KES {paid:,.0f}. Completion: {pct}%."


def _guardian_exam_response(sid: int, cur) -> str | None:
    try:
        cur.execute("SELECT school_id FROM students WHERE id=%s", (sid,))
        school_row = cur.fetchone() or {}
        school_id = school_row.get("school_id")
        try:
            school_id = int(school_id or 0)
        except Exception:
            school_id = 0
        cur.execute(
            """
            SELECT title, subject
            FROM newsletters
            WHERE (school_id=%s OR school_id IS NULL)
              AND (LOWER(title) LIKE %s OR LOWER(subject) LIKE %s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (school_id, "%exam%", "%exam%"),
        )
        notice = cur.fetchone() or {}
        if notice:
            title = notice.get("title") or notice.get("subject") or "announcements"
            return f"Latest exam notice: {title}. Please check Notices for details."
    except Exception:
        return None
    return None


def _guardian_receipt_response() -> str:
    return "Open Recent Payments and click Print to get a downloadable receipt for any payment."


@guardian_bp.route("/notifications", methods=["GET"])
def guardian_notifications():
    """Return latest announcements/notices for the student's school."""
    token = (request.args.get("token") or session.get("guardian_token") or "").strip()
    if not token:
        return jsonify({"ok": False, "error": "Missing token"}), 400
    from routes.student_portal import _verify_token
    sid = _verify_token(token)
    if not sid:
        return jsonify({"ok": False, "error": "Invalid token"}), 403
    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute("SELECT school_id FROM students WHERE id=%s", (sid,))
    s = cur.fetchone() or {}
    sid_school = s.get('school_id')
    rows: list[dict] = []
    try:
        cur.execute(
            "SELECT id, category, title, subject, created_at FROM newsletters WHERE (school_id=%s OR school_id IS NULL) ORDER BY id DESC LIMIT 12",
            (sid_school,),
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []
    db.close()
    return jsonify({"ok": True, "items": rows})

