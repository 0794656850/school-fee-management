from flask import Blueprint, render_template, current_app, redirect, url_for, flash, request, session, jsonify
from decimal import Decimal
from datetime import datetime
import os
import random

import mysql.connector
from flask_mail import Message
from extensions import mail
from utils.notify import normalize_phone
from utils.gmail_api import send_email as gmail_send_email, has_valid_token
from utils.ujumbe_sms import send_ujumbe_sms, ujumbe_sms_configured
from utils.settings import get_setting
from utils.db_helpers import ensure_reminder_messages_table, ensure_parent_portal_columns
from routes.term_routes import (
    get_or_seed_current_term,
    ensure_academic_terms_table,
    ensure_invoices_tables,
)

DEFAULT_REMINDER_TEMPLATE = """📌 Payment Reminder (Gentle Reminder)

Subject: Friendly Fee Payment Reminder

Hello {name},

We hope you are well. Term {term_label} fees are expected at KES {expected_term_total}. {previous_term_note}
Current class: {class_label}. Kindly settle KES {balance} by {due_date} so the school can keep everything running smoothly.

If you have already settled this, please disregard this message.

"{quote}"

Thank you.
{institution}
{contact_details}
"""

REMINDER_QUOTES = [
    "Consistency in small payments keeps the classroom doors open wider.",
    "Together we keep the lights on and the lessons flowing.",
    "Timely fee contributions make every new term smoother for your child.",
    "Every cleared invoice is a promise fulfilled for the school community.",
    "Your attention to fees keeps the school ready for every learning adventure.",
]

reminder_bp = Blueprint('reminders', __name__, url_prefix='/reminders')


def _db_from_config():
    """Create a MySQL connection based on app config/env (mirrors app.py approach)."""
    # Prefer explicit env vars if present
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")

    uri = current_app.config.get("SQLALCHEMY_DATABASE_URI", "")
    if uri and uri.startswith("mysql"):
        try:
            from urllib.parse import urlparse
            parsed = urlparse(uri)
            if parsed.hostname:
                host = parsed.hostname
            if parsed.username:
                user = parsed.username
            if parsed.password:
                password = parsed.password
            if parsed.path and len(parsed.path) > 1:
                database = parsed.path.lstrip("/")
        except Exception:
            # Fall back to env/defaults if parsing fails
            pass

    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _smtp_configured() -> bool:
    """Return True if minimal SMTP settings are present for Flask-Mail.

    We consider SMTP available only when a `MAIL_SERVER` is set and there are
    credentials to authenticate (MAIL_USERNAME + MAIL_PASSWORD). This prevents
    attempts to send via an uninitialized smtplib connection which yields
    errors like: 'please run connect() first'.
    """
    try:
        cfg = current_app.config if current_app else {}
        server = (cfg.get('MAIL_SERVER') or '').strip()
        username = (cfg.get('MAIL_USERNAME') or '').strip()
        password = (cfg.get('MAIL_PASSWORD') or '').strip()
        return bool(server and username and password)
    except Exception:
        return False


def _detect_balance_column(cursor):
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    if has_balance:
        return "balance"

    cursor.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
    has_fee_balance = bool(cursor.fetchone())
    if has_fee_balance:
        return "fee_balance"

    return None



def _column_exists(cursor, name: str) -> bool:
    cursor.execute("SHOW COLUMNS FROM students LIKE %s", (name,))
    return bool(cursor.fetchone())


def _email_select_expr(cursor) -> str:
    """Return SQL expression that prioritizes parent-updated email for reminders."""
    has_parent = _column_exists(cursor, "parent_email")
    has_email = _column_exists(cursor, "email")
    if has_parent and has_email:
        return "COALESCE(NULLIF(parent_email,''), email) AS email"
    if has_parent:
        return "parent_email AS email"
    if has_email:
        return "email AS email"
    return "NULL AS email"


def _resolve_phone_column(cursor) -> str | None:
    """Pick a phone column for reminders: setting -> 'parent_phone' -> 'phone'."""
    from utils.settings import get_setting
    pref = (get_setting("REMINDER_PHONE_COLUMN") or "").strip()
    if pref and _column_exists(cursor, pref):
        return pref
    for cand in ("parent_phone", "phone"):
        if _column_exists(cursor, cand):
            return cand
    return None


def _phone_select_expr(cursor) -> str:
    """Return SQL expression that prefers parent_phone but falls back to phone."""
    has_parent = _column_exists(cursor, "parent_phone")
    has_phone = _column_exists(cursor, "phone")
    if has_parent and has_phone:
        return "COALESCE(parent_phone, phone) AS phone"
    if has_parent:
        return "parent_phone AS phone"
    if has_phone:
        return "phone AS phone"
    return "NULL AS phone"


def _comm_pref_select_expr(cursor) -> str:
    has_pref = _column_exists(cursor, "parent_preferred_channel")
    has_comm = _column_exists(cursor, "parent_comm_opt_in")
    has_email_opt = _column_exists(cursor, "parent_email_opt_in")
    has_sms_opt = _column_exists(cursor, "parent_sms_opt_in")
    pref_expr = "COALESCE(parent_preferred_channel,'auto') AS parent_preferred_channel" if has_pref else "'auto' AS parent_preferred_channel"
    comm_expr = "COALESCE(parent_comm_opt_in,1) AS parent_comm_opt_in" if has_comm else "1 AS parent_comm_opt_in"
    email_expr = "COALESCE(parent_email_opt_in,1) AS parent_email_opt_in" if has_email_opt else "1 AS parent_email_opt_in"
    sms_expr = "COALESCE(parent_sms_opt_in,1) AS parent_sms_opt_in" if has_sms_opt else "1 AS parent_sms_opt_in"
    return f"{pref_expr}, {comm_expr}, {email_expr}, {sms_expr}"


def _channel_allowed(student: dict, channel: str) -> bool:
    pref = (student.get("parent_preferred_channel") or "auto").strip().lower()
    comm_opt_in = bool(int(student.get("parent_comm_opt_in") if student.get("parent_comm_opt_in") is not None else 1))
    email_opt_in = bool(int(student.get("parent_email_opt_in") if student.get("parent_email_opt_in") is not None else 1))
    sms_opt_in = bool(int(student.get("parent_sms_opt_in") if student.get("parent_sms_opt_in") is not None else 1))
    if not comm_opt_in:
        return False
    if channel == "email" and not email_opt_in:
        return False
    if channel == "sms" and not sms_opt_in:
        return False
    if pref in {"email", "sms"} and pref != channel:
        return False
    return True

class _SafeDict(dict):
    def __missing__(self, key):  # graceful placeholder if unknown
        return '{' + key + '}'


def _contact_details() -> str:
    parts = []
    phone = (get_setting("SCHOOL_PHONE") or current_app.config.get("SUPPORT_PHONE") or "").strip()
    email = (get_setting("SCHOOL_EMAIL") or current_app.config.get("MAIL_USERNAME") or "").strip()
    if phone:
        parts.append(f"Phone: {phone}")
    if email:
        parts.append(f"Email: {email}")
    return " | ".join(parts) if parts else ""


def _term_reminder_context(student_id: int, school_id: int) -> dict[str, object]:
    context = {
        "term_label": "current term",
        "expected_term_total": "0.00",
        "previous_term_note": "Previous terms are fully settled.",
        "previous_outstanding": 0.0,
        "term_year": None,
        "term_term": None,
    }
    if not student_id or not school_id:
        return context
    db = _db_from_config()
    try:
        ensure_academic_terms_table(db)
        ensure_invoices_tables(db)
        cur = db.cursor(dictionary=True)
        year, term = get_or_seed_current_term(db)
        context["term_year"] = year
        context["term_term"] = term
        context["term_label"] = f"{year} Term {term}"
        cur.execute(
            "SELECT COALESCE(total,0) AS total FROM invoices WHERE student_id=%s AND school_id=%s AND year=%s AND term=%s",
            (student_id, school_id, year, term),
        )
        invoice = cur.fetchone() or {}
        expected_total = float(invoice.get("total") or 0)
        context["expected_term_total"] = f"{expected_total:,.2f}"
        prev_outstanding = 0.0
        cur.execute(
            """
            SELECT year, term, COALESCE(total,0) AS total
            FROM invoices
            WHERE student_id=%s AND school_id=%s AND (year<>%s OR term<>%s)
            """,
            (student_id, school_id, year, term),
        )
        for inv in cur.fetchall() or []:
            inv_year = inv.get("year")
            inv_term = inv.get("term")
            inv_total = float(inv.get("total") or 0)
            cur.execute(
                """
                SELECT COALESCE(SUM(amount),0) AS paid
                FROM payments
                WHERE student_id=%s AND school_id=%s AND year=%s AND term=%s
                """,
                (student_id, school_id, inv_year, inv_term),
            )
            paid_row = cur.fetchone() or {}
            paid_amount = float(paid_row.get("paid") or 0)
            prev_outstanding += max(inv_total - paid_amount, 0.0)
        context["previous_outstanding"] = prev_outstanding
        if prev_outstanding > 0:
            context["previous_term_note"] = (
                f"Previous term balance still due: KES {prev_outstanding:,.2f}."
            )
        else:
            context["previous_term_note"] = "Previous terms are fully settled."
    except Exception:
        pass
    finally:
        try:
            db.close()
        except Exception:
            pass
    return context


def _get_school_setting(db, school_id: int, key: str, default: str | None = None) -> str | None:
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT `value` FROM school_settings WHERE school_id=%s AND `key`=%s LIMIT 1",
            (school_id, key),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return str(row[0])
    except Exception:
        pass
    return default


def _contact_details_for_school(db, school_id: int) -> str:
    parts = []
    phone = _get_school_setting(db, school_id, "SCHOOL_PHONE") or ""
    email = _get_school_setting(db, school_id, "SCHOOL_EMAIL") or ""
    if phone:
        parts.append(f"Phone: {phone}")
    if email:
        parts.append(f"Email: {email}")
    return " | ".join(parts) if parts else ""


def _render_message(
    template: str,
    *,
    name: str,
    balance: Decimal,
    class_name: str | None,
    term_label: str,
    expected_term_total: str,
    previous_term_note: str,
    quote: str,
    school_name: str | None = None,
    contact_details: str | None = None,
    due_date: str | None = None,
) -> str:
    data = _SafeDict(
        name=name,
        balance=f"{balance:,.2f}",
        class_name=class_name or "",
        klass=class_name or "",
        cls=class_name or "",
        school=school_name or (get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school")),
        school_name=school_name or (get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school")),
        institution=school_name or (get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school")),
        purpose=f"{class_name + ' fees' if class_name else 'school fees'}",
        due_date=(due_date or get_setting("REMINDER_DUE_DATE") or "the upcoming due date"),
        contact_details=(contact_details or _contact_details()),
        class_label=f"{class_name or 'your class'}",
        term_label=term_label,
        expected_term_total=expected_term_total,
        previous_term_note=previous_term_note,
        quote=quote,
    )
    return (template or "").format_map(data)


def _sms_max_len() -> int:
    try:
        raw = (
            current_app.config.get("UJUMBE_SMS_MAX_LEN")
            or current_app.config.get("SMS_MAX_LEN")
            or os.environ.get("UJUMBE_SMS_MAX_LEN")
            or os.environ.get("SMS_MAX_LEN")
            or "320"
        )
        return max(int(str(raw).strip()), 0)
    except Exception:
        return 320


def _normalize_sms_body(message: str) -> tuple[str, bool]:
    lines = [line.strip() for line in (message or "").splitlines() if line.strip()]
    filtered = [line for line in lines if not line.lower().startswith("subject:")]
    body = " ".join(filtered)
    # Optional unicode stripping for SMS gateways that expect GSM/ASCII.
    try:
        strip_unicode = str(
            current_app.config.get("SMS_STRIP_UNICODE")
            or os.environ.get("SMS_STRIP_UNICODE")
            or "0"
        ).strip().lower() in ("1", "true", "yes")
        if strip_unicode:
            body = body.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    max_len = _sms_max_len()
    truncated = False
    if max_len and len(body) > max_len:
        truncated = True
        if max_len > 3:
            body = body[: max_len - 3].rstrip() + "..."
        else:
            body = body[:max_len]
    return body, truncated


def _log_reminder(
    *,
    student_id: int,
    school_id: int,
    guardian_email: str | None,
    guardian_phone: str | None,
    subject: str | None,
    body: str | None,
    status: str,
    channel: str = "email",
    sent_by: str | None = None,
) -> None:
    try:
        db = _db_from_config()
    except Exception:
        return
    try:
        ensure_reminder_messages_table(db)
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO reminder_messages
            (school_id, student_id, guardian_email, guardian_phone, channel, subject, body, status, sent_by, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                school_id,
                student_id,
                guardian_email,
                guardian_phone,
                channel,
                subject,
                body,
                status,
                sent_by or session.get("username") or "system",
                datetime.utcnow(),
            ),
        )
        db.commit()
    except Exception:
        pass
    finally:
        try:
            db.close()
        except Exception:
            pass


@reminder_bp.route('/')
def reminders_home():
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        ensure_parent_portal_columns(db)
    except Exception:
        pass

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return render_template(
            'reminders.html',
            default_message_template=(get_setting("REMINDER_DEFAULT_MESSAGE") or DEFAULT_REMINDER_TEMPLATE),
            students=[],
            classes=[],
            whatsapp_enabled=False,
            sms_enabled=ujumbe_sms_configured(),
            gmail_connected=False,
        )

    # Determine email/phone columns to use for reminders
    email_select = _email_select_expr(cursor)
    phone_select = _phone_select_expr(cursor)
    has_parent_email = _column_exists(cursor, "parent_email")
    has_email = _column_exists(cursor, "email")
    has_parent_phone = _column_exists(cursor, "parent_phone")
    has_phone = _column_exists(cursor, "phone")

    # Filters for Kâ€“12 operations
    selected_class = (request.args.get('class') or '').strip()
    q = (request.args.get('q') or '').strip()
    try:
        min_balance = float(request.args.get('min_balance') or 0)
    except Exception:
        min_balance = 0.0

    # Build query with optional filters
    base_sql = [
        f"SELECT id, name, class_name, {email_select}, {phone_select}, COALESCE({col}, 0) AS balance",
        "FROM students",
        "WHERE school_id = %s AND COALESCE(" + col + ", 0) > 0",
    ]
    params: list[object] = [session.get("school_id")]
    if selected_class:
        base_sql.append("AND class_name = %s")
        params.append(selected_class)
    # Optional search filter across name, ID, admission no (if present), and email
    if q:
        try:
            adm_has = _column_exists(cursor, 'admission_no')
        except Exception:
            adm_has = False
        like = f"%{q}%"
        clauses = ["name LIKE %s"]
        params.append(like)
        if adm_has:
            clauses.append("admission_no LIKE %s")
            params.append(like)
        try:
            from types import SimpleNamespace
            if has_parent_email:
                clauses.append("parent_email LIKE %s")
                params.append(like)
            if has_email:
                clauses.append("email LIKE %s")
                params.append(like)
            if has_parent_phone:
                clauses.append("parent_phone LIKE %s")
                params.append(like)
            if has_phone:
                clauses.append("phone LIKE %s")
                params.append(like)
        except Exception:
            pass
        try:
            qid = int(q)
            clauses.append("id = %s")
            params.append(qid)
        except Exception:
            pass
        base_sql.append("AND (" + " OR ".join(clauses) + ")")
    if min_balance and min_balance > 0:
        base_sql.append("AND COALESCE(" + col + ", 0) >= %s")
        params.append(min_balance)
    base_sql.append("ORDER BY COALESCE(" + col + ", 0) DESC, name ASC")

    cursor.execute("\n".join(base_sql), tuple(params))
    students = cursor.fetchall()

    # Distinct classes for filter dropdown
    cursor.execute("SELECT DISTINCT class_name FROM students WHERE school_id=%s AND class_name IS NOT NULL AND class_name <> '' ORDER BY class_name", (session.get("school_id"),))
    classes = [row[0] if not isinstance(row, dict) else row.get('class_name') for row in cursor.fetchall()]

    db.close()

    # Gmail connection status (token present)
    try:
        gmail_connected = has_valid_token()
    except Exception:
        gmail_connected = False

    return render_template(
        'reminders.html',
        students=students,
        classes=classes,
        selected_class=selected_class,
        q=q,
        min_balance=min_balance,
        default_message_template=(get_setting("REMINDER_DEFAULT_MESSAGE") or DEFAULT_REMINDER_TEMPLATE),
        email_enabled=True,
        sms_enabled=ujumbe_sms_configured(),
        gmail_connected=gmail_connected,
    )


@reminder_bp.route('/send/<int:student_id>', methods=['GET', 'POST'])
def send_email_reminder(student_id: int):
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        ensure_parent_portal_columns(db)
    except Exception:
        pass

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    # Determine email/phone columns to use for reminders
    email_select = _email_select_expr(cursor)
    phone_select = _phone_select_expr(cursor)

    comm_pref_select = _comm_pref_select_expr(cursor)
    cursor.execute(
        f"""
        SELECT id, school_id, name, class_name, {email_select}, {phone_select}, COALESCE({col}, 0) AS balance, {comm_pref_select}
        FROM students WHERE id = %s AND school_id = %s
        """,
        (student_id, session.get("school_id"))
    )
    student = cursor.fetchone()
    db.close()

    if not student:
        flash("Student not found.", "error")
        return redirect(url_for('reminders.reminders_home'))

    email_address = (student.get("email") or "").strip() or None
    phone_raw = (student.get("phone") or "").strip() or None
    balance = Decimal(str(student.get('balance') or 0))

    # Optional custom message from form/query with placeholders
    message_template = request.form.get('message') or request.args.get('message')
    template = message_template or DEFAULT_REMINDER_TEMPLATE
    term_context = _term_reminder_context(student_id, session.get("school_id"))
    quote = random.choice(REMINDER_QUOTES)
    message_body = _render_message(
        template,
        name=student['name'],
        balance=balance,
        class_name=student.get('class_name'),
        term_label=term_context.get("term_label") or "current term",
        expected_term_total=term_context.get("expected_term_total") or "0.00",
        previous_term_note=term_context.get("previous_term_note") or "",
        quote=quote,
    )

    # Prefer Gmail API OAuth2 sender if available; fallback to Flask-Mail
    subject = f"Fee reminder for {student['name']}"
    sent = False
    if not _channel_allowed(student, "email"):
        flash(f"Email reminder skipped for {student['name']} due to parent communication preferences.", "info")
    elif email_address:
        try:
            sent = gmail_send_email(email_address, subject, message_body)
        except Exception:
            sent = False
    if email_address and not sent:
        # Fallback to SMTP only if configured; otherwise show a helpful hint
        if _smtp_configured():
            try:
                school_sender = (
                    current_app.config.get('MAIL_SENDER')
                    or current_app.config.get('MAIL_DEFAULT_SENDER')
                    or get_setting('SCHOOL_EMAIL')
                    or current_app.config.get('MAIL_USERNAME')
                    or None
                )
                msg = Message(
                    subject=subject,
                    sender=school_sender,
                    recipients=[email_address],
                    body=message_body,
                )
                mail.send(msg)
                sent = True
            except Exception as e:
                flash(f"Failed to send email: {e}", "error")
        else:
            flash("Email sending is not configured. Connect Gmail (Reminders > Connect Gmail) or set MAIL_* SMTP settings.", "error")
    if sent and email_address:
        flash(f"Email reminder sent to {student['name']} ({email_address}).", "success")
    elif not email_address:
        flash(f"No email on record for {student['name']}. Reminder was not sent.", "warning")
    else:
        flash(f"Email reminder failed for {student['name']}.", "error")
    try:
        status = "sent" if sent else ("skipped" if not email_address else "failed")
        _log_reminder(
            student_id=student_id,
            school_id=int(student.get("school_id") or session.get("school_id") or 0),
            guardian_email=email_address,
            guardian_phone=phone_raw,
            subject=subject,
            body=message_body,
            status=status,
            channel="email",
        )
    except Exception:
        pass

    return redirect(url_for('reminders.reminders_home'))


@reminder_bp.route('/send_sms/<int:student_id>', methods=['POST'])
def send_sms_reminder(student_id: int):
    if not ujumbe_sms_configured():
        flash("SMS is not configured. Set UJUMBE_SMS_API_KEY and UJUMBE_EMAIL.", "error")
        return redirect(url_for('reminders.reminders_home'))

    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        ensure_parent_portal_columns(db)
    except Exception:
        pass

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    phone_select = _phone_select_expr(cursor)
    has_parent_phone = _column_exists(cursor, "parent_phone")
    has_phone = _column_exists(cursor, "phone")

    comm_pref_select = _comm_pref_select_expr(cursor)
    cursor.execute(
        f"""
        SELECT id, school_id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance, {comm_pref_select}
        FROM students WHERE id = %s AND school_id = %s
        """,
        (student_id, session.get("school_id")),
    )
    student = cursor.fetchone()
    db.close()

    if not student:
        flash("Student not found.", "error")
        return redirect(url_for('reminders.reminders_home'))

    phone_raw = (student.get("phone") or "").strip() or None
    phone_norm = normalize_phone(phone_raw) if phone_raw else None
    balance = Decimal(str(student.get('balance') or 0))

    message_template = request.form.get('message') or request.args.get('message')
    template = message_template or DEFAULT_REMINDER_TEMPLATE
    term_context = _term_reminder_context(student_id, session.get("school_id"))
    quote = random.choice(REMINDER_QUOTES)
    message_body = _render_message(
        template,
        name=student['name'],
        balance=balance,
        class_name=student.get('class_name'),
        term_label=term_context.get("term_label") or "current term",
        expected_term_total=term_context.get("expected_term_total") or "0.00",
        previous_term_note=term_context.get("previous_term_note") or "",
        quote=quote,
    )
    sms_body, truncated = _normalize_sms_body(message_body)
    if truncated:
        flash("SMS template was long and has been shortened to fit the SMS limit.", "info")
    if not sms_body.strip():
        flash("SMS message is empty after formatting. Please shorten the template.", "error")
        return redirect(url_for('reminders.reminders_home'))

    ok = False
    info = {}
    if not _channel_allowed(student, "sms"):
        flash(f"SMS reminder skipped for {student['name']} due to parent communication preferences.", "info")
    elif phone_norm:
        ok, info = send_ujumbe_sms(phone_norm, sms_body)

    if ok and phone_norm:
        detail = ""
        if info.get("description"):
            detail = f" {info.get('description')}"
        if info.get("available_credits"):
            detail += f" Credits left: {info.get('available_credits')}."
        flash(f"SMS reminder sent to {student['name']} ({phone_norm}).{detail}", "success")
    elif not phone_norm:
        flash(f"No phone on record for {student['name']}. SMS was not sent.", "warning")
    else:
        err = info.get("error") or info.get("description") or "Unknown error"
        flash(f"SMS reminder failed for {student['name']}. {err}", "error")

    try:
        status = "sent" if ok else ("skipped" if not phone_norm else "failed")
        _log_reminder(
            student_id=student_id,
            school_id=int(student.get("school_id") or session.get("school_id") or 0),
            guardian_email=None,
            guardian_phone=phone_norm or phone_raw,
            subject=None,
            body=sms_body,
            status=status,
            channel="sms",
        )
    except Exception:
        pass

    return redirect(url_for('reminders.reminders_home'))


@reminder_bp.route('/send_all', methods=['POST'])
def send_all_reminders():
    """Send reminders to all students with positive balances. Simple best-effort loop."""
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        ensure_parent_portal_columns(db)
    except Exception:
        pass

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    # Determine email/phone columns to use for reminders
    email_select = _email_select_expr(cursor)
    phone_select = _phone_select_expr(cursor)
    has_parent_email = _column_exists(cursor, "parent_email")
    has_email = _column_exists(cursor, "email")
    has_parent_phone = _column_exists(cursor, "parent_phone")
    has_phone = _column_exists(cursor, "phone")

    selected_class = (request.form.get("class") or "").strip()
    q = (request.form.get("q") or "").strip()
    try:
        min_balance = float(request.form.get("min_balance") or 0)
    except Exception:
        min_balance = 0.0

    base_sql = [
        f"SELECT id, school_id, name, class_name, {email_select}, {phone_select}, COALESCE({col}, 0) AS balance",
        "FROM students",
        "WHERE school_id=%s AND COALESCE(" + col + ", 0) > 0",
    ]
    params: list[object] = [session.get("school_id")]
    if selected_class:
        base_sql.append("AND class_name = %s")
        params.append(selected_class)
    if q:
        try:
            adm_has = _column_exists(cursor, "admission_no")
        except Exception:
            adm_has = False
        like = f"%{q}%"
        clauses = ["name LIKE %s"]
        params.append(like)
        if adm_has:
            clauses.append("admission_no LIKE %s")
            params.append(like)
        try:
            if has_parent_email:
                clauses.append("parent_email LIKE %s")
                params.append(like)
            if has_email:
                clauses.append("email LIKE %s")
                params.append(like)
            if has_parent_phone:
                clauses.append("parent_phone LIKE %s")
                params.append(like)
            if has_phone:
                clauses.append("phone LIKE %s")
                params.append(like)
        except Exception:
            pass
        try:
            qid = int(q)
            clauses.append("id = %s")
            params.append(qid)
        except Exception:
            pass
        base_sql.append("AND (" + " OR ".join(clauses) + ")")
    if min_balance and min_balance > 0:
        base_sql.append("AND COALESCE(" + col + ", 0) >= %s")
        params.append(min_balance)
    base_sql.append("ORDER BY id ASC")
    comm_pref_select = _comm_pref_select_expr(cursor)
    base_sql[0] = f"SELECT id, school_id, name, class_name, {email_select}, {phone_select}, COALESCE({col}, 0) AS balance, {comm_pref_select}"
    cursor.execute("\n".join(base_sql), tuple(params))
    students = cursor.fetchall()
    db.close()

    sent = 0
    failed = 0
    skipped = 0
    message_template = request.form.get('message', '')
    for s in students:
        email_address = (s.get("email") or "").strip() or None
        balance = Decimal(str(s.get('balance') or 0))
        template = message_template or DEFAULT_REMINDER_TEMPLATE
        term_context = _term_reminder_context(s['id'], session.get("school_id"))
        quote = random.choice(REMINDER_QUOTES)
        msg = _render_message(
            template,
            name=s['name'],
            balance=balance,
            class_name=s.get('class_name'),
            term_label=term_context.get("term_label") or "current term",
            expected_term_total=term_context.get("expected_term_total") or "0.00",
            previous_term_note=term_context.get("previous_term_note") or "",
            quote=quote,
        )
        # Try Gmail API first
        subject = f"Fee reminder for {s['name']}"
        ok = False
        if not _channel_allowed(s, "email"):
            skipped += 1
        elif not email_address:
            skipped += 1
        else:
            try:
                ok = gmail_send_email(email_address, subject, msg)
            except Exception:
                ok = False
            if not ok:
                if _smtp_configured():
                    try:
                        school_sender = (
                            current_app.config.get('MAIL_SENDER')
                            or current_app.config.get('MAIL_DEFAULT_SENDER')
                            or get_setting('SCHOOL_EMAIL')
                            or current_app.config.get('MAIL_USERNAME')
                            or None
                        )
                        m = Message(
                            subject=subject,
                            sender=school_sender,
                            recipients=[email_address],
                            body=msg,
                        )
                        mail.send(m)
                        ok = True
                    except Exception:
                        ok = False
            if ok:
                sent += 1
            else:
                failed += 1
        try:
            _log_reminder(
                student_id=int(s["id"]),
                school_id=int(s.get("school_id") or session.get("school_id") or 0),
                guardian_email=email_address,
                guardian_phone=(s.get("phone") or "").strip() or None,
                subject=subject,
                body=msg,
                status="sent" if ok else ("skipped" if not email_address else "failed"),
                channel="email",
            )
        except Exception:
            pass

    flash(f"Bulk reminders completed. Email sent: {sent}, Failed: {failed}, Skipped (no email): {skipped}.", "info")
    return redirect(url_for('reminders.reminders_home'))


@reminder_bp.route('/send_all_sms', methods=['POST'])
def send_all_sms_reminders():
    # Send SMS reminders to all students with positive balances (best-effort).
    if not ujumbe_sms_configured():
        flash("SMS is not configured. Set UJUMBE_SMS_API_KEY and UJUMBE_EMAIL.", "error")
        return redirect(url_for('reminders.reminders_home'))

    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        ensure_parent_portal_columns(db)
    except Exception:
        pass

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    phone_select = _phone_select_expr(cursor)
    has_parent_phone = _column_exists(cursor, "parent_phone")
    has_phone = _column_exists(cursor, "phone")

    selected_class = (request.form.get("class") or "").strip()
    q = (request.form.get("q") or "").strip()
    try:
        min_balance = float(request.form.get("min_balance") or 0)
    except Exception:
        min_balance = 0.0

    base_sql = [
        f"SELECT id, school_id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance",
        "FROM students",
        "WHERE school_id=%s AND COALESCE(" + col + ", 0) > 0",
    ]
    params: list[object] = [session.get("school_id")]
    if selected_class:
        base_sql.append("AND class_name = %s")
        params.append(selected_class)
    if q:
        try:
            adm_has = _column_exists(cursor, "admission_no")
        except Exception:
            adm_has = False
        like = f"%{q}%"
        clauses = ["name LIKE %s"]
        params.append(like)
        if adm_has:
            clauses.append("admission_no LIKE %s")
            params.append(like)
        if has_parent_phone:
            clauses.append("parent_phone LIKE %s")
            params.append(like)
        if has_phone:
            clauses.append("phone LIKE %s")
            params.append(like)
        try:
            qid = int(q)
            clauses.append("id = %s")
            params.append(qid)
        except Exception:
            pass
        base_sql.append("AND (" + " OR ".join(clauses) + ")")
    if min_balance and min_balance > 0:
        base_sql.append("AND COALESCE(" + col + ", 0) >= %s")
        params.append(min_balance)
    base_sql.append("ORDER BY id ASC")
    comm_pref_select = _comm_pref_select_expr(cursor)
    base_sql[0] = f"SELECT id, school_id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance, {comm_pref_select}"
    cursor.execute("\n".join(base_sql), tuple(params))
    students = cursor.fetchall()
    db.close()

    sent = 0
    failed = 0
    skipped = 0
    truncated_any = False
    empty_any = False
    message_template = request.form.get('message', '')
    for s in students:
        phone_raw = (s.get("phone") or "").strip() or None
        phone_norm = normalize_phone(phone_raw) if phone_raw else None
        balance = Decimal(str(s.get('balance') or 0))
        template = message_template or DEFAULT_REMINDER_TEMPLATE
        term_context = _term_reminder_context(s['id'], session.get("school_id"))
        quote = random.choice(REMINDER_QUOTES)
        msg = _render_message(
            template,
            name=s['name'],
            balance=balance,
            class_name=s.get('class_name'),
            term_label=term_context.get("term_label") or "current term",
            expected_term_total=term_context.get("expected_term_total") or "0.00",
            previous_term_note=term_context.get("previous_term_note") or "",
            quote=quote,
        )
        sms_body, truncated = _normalize_sms_body(msg)
        if truncated:
            truncated_any = True
        if not sms_body.strip():
            skipped += 1
            empty_any = True
            continue
        ok = False
        info = {}
        if not _channel_allowed(s, "sms"):
            skipped += 1
        elif not phone_norm:
            skipped += 1
        else:
            ok, info = send_ujumbe_sms(phone_norm, sms_body)
            if ok:
                sent += 1
            else:
                failed += 1
        try:
            _log_reminder(
                student_id=int(s["id"]),
                school_id=int(s.get("school_id") or session.get("school_id") or 0),
                guardian_email=None,
                guardian_phone=phone_norm or phone_raw,
                subject=None,
                body=sms_body,
                status="sent" if ok else ("skipped" if not phone_norm else "failed"),
                channel="sms",
            )
        except Exception:
            pass

    summary = f"Bulk SMS completed. Sent: {sent}, Failed: {failed}, Skipped (no phone): {skipped}."
    if info.get("available_credits"):
        summary += f" Credits left: {info.get('available_credits')}."
    if failed and info.get("error"):
        summary += f" Last error: {info.get('error')}."
    if truncated_any:
        summary += " Template was shortened to fit SMS limits."
    if empty_any:
        summary += " Some messages were empty after formatting and were skipped."
    flash(summary, "info")
    return redirect(url_for('reminders.reminders_home'))


@reminder_bp.route('/test_email', methods=['POST'])
def test_email_endpoint():
    """Send a single test email using the same pipeline (Gmail API -> Flask-Mail).

    Request JSON: {"to": "address@example.com", "message": "optional body"}
    Falls back to SCHOOL_EMAIL or MAIL_SENDER when 'to' is not provided.
    """
    # Allow dry-run for connectivity checks without sending
    try:
        if (request.args.get('dry') or "").lower() in ('1','true','yes'):
            return jsonify({"ok": True, "via": "dry-run"})
    except Exception:
        pass

    to = None
    try:
        data = request.get_json(silent=True) or {}
        to = (data.get('to') or '').strip()
        message = (data.get('message') or '').strip()
    except Exception:
        message = ''

    if not to:
        to = (get_setting('SCHOOL_EMAIL') or current_app.config.get('MAIL_USERNAME') or current_app.config.get('MAIL_SENDER') or '').strip()
    if not to:
        return jsonify({"ok": False, "error": "No recipient available. Provide 'to' or set SCHOOL_EMAIL/MAIL_SENDER."}), 400

    subject = "Fee Reminder Test"
    body = message or "This is a test email from the Fee Reminder Center. If you received this, email sending is working."

    # Try Gmail API first
    try:
        if gmail_send_email(to, subject, body):
            return jsonify({"ok": True, "via": "gmail_api"})
    except Exception:
        pass

    # Fallback to Flask-Mail/SMTP
    if not _smtp_configured():
        return jsonify({"ok": False, "error": "SMTP not configured. Set MAIL_SERVER/MAIL_USERNAME/MAIL_PASSWORD or use Gmail OAuth."}), 400
    try:
        sender = (
            current_app.config.get('MAIL_SENDER')
            or current_app.config.get('MAIL_DEFAULT_SENDER')
            or get_setting('SCHOOL_EMAIL')
            or current_app.config.get('MAIL_USERNAME')
            or None
        )
        msg = Message(subject=subject, sender=sender, recipients=[to], body=body)
        mail.send(msg)
        return jsonify({"ok": True, "via": "smtp"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def auto_send_reminders_for_school(school_id: int, min_balance: float = 0.01) -> tuple[int, int]:
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)
    try:
        try:
            ensure_parent_portal_columns(db)
        except Exception:
            pass
        col = _detect_balance_column(cursor)
        if not col:
            return 0, 0
        email_select = _email_select_expr(cursor)
        phone_select = _phone_select_expr(cursor)
        comm_pref_select = _comm_pref_select_expr(cursor)
        cursor.execute(
            f"""
            SELECT id, name, class_name, {email_select}, {phone_select}, COALESCE({col}, 0) AS balance, {comm_pref_select}
            FROM students
            WHERE school_id=%s AND COALESCE({col}, 0) > %s
            ORDER BY id ASC
            """,
            (school_id, min_balance),
        )
        students = cursor.fetchall() or []
        template = _get_school_setting(db, school_id, "REMINDER_DEFAULT_MESSAGE") or DEFAULT_REMINDER_TEMPLATE
        school_name = _get_school_setting(db, school_id, "SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school")
        due_date = _get_school_setting(db, school_id, "REMINDER_DUE_DATE") or "the upcoming due date"
        contact_details = _contact_details_for_school(db, school_id)
        school_email = _get_school_setting(db, school_id, "SCHOOL_EMAIL") or None
    finally:
        try:
            db.close()
        except Exception:
            pass

    sent = 0
    skipped = 0
    for s in students:
        balance = Decimal(str(s.get('balance') or 0))
        term_context = _term_reminder_context(s['id'], school_id)
        quote = random.choice(REMINDER_QUOTES)
        msg = _render_message(
            template,
            name=s['name'],
            balance=balance,
            class_name=s.get('class_name'),
            term_label=term_context.get("term_label") or "current term",
            expected_term_total=term_context.get("expected_term_total") or "0.00",
            previous_term_note=term_context.get("previous_term_note") or "",
            quote=quote,
            school_name=school_name,
            contact_details=contact_details,
            due_date=due_date,
        )
        subject = f"Fee reminder for {s['name']}"
        ok = False
        if _channel_allowed(s, "email") and s.get('email'):
            try:
                ok = gmail_send_email(s['email'], subject, msg)
            except Exception:
                ok = False
            if not ok and _smtp_configured():
                try:
                    school_sender = (
                        current_app.config.get('MAIL_SENDER')
                        or current_app.config.get('MAIL_DEFAULT_SENDER')
                        or school_email
                        or current_app.config.get('MAIL_USERNAME')
                        or None
                    )
                    m = Message(
                        subject=subject,
                        sender=school_sender,
                        recipients=[s['email']],
                        body=msg,
                    )
                    mail.send(m)
                    ok = True
                except Exception:
                    ok = False
        status = "sent" if ok else ("skipped" if (not s.get("email") or not _channel_allowed(s, "email")) else "failed")
        if ok:
            sent += 1
        else:
            skipped += 1
        try:
            _log_reminder(
                student_id=int(s["id"]),
                school_id=school_id,
                guardian_email=s.get("email"),
                guardian_phone=(s.get("phone") or "").strip() or None,
                subject=subject,
                body=msg,
                status=status,
                channel="email",
                sent_by="system",
            )
        except Exception:
            pass

    return sent, skipped


def run_auto_reminders(min_balance: float = 0.01) -> None:
    try:
        db = _db_from_config()
    except Exception:
        return
    try:
        cur = db.cursor()
        cur.execute("SELECT id FROM schools")
        school_ids = [int(r[0]) for r in (cur.fetchall() or []) if r]
    except Exception:
        school_ids = []
    finally:
        try:
            db.close()
        except Exception:
            pass
    for sid in school_ids:
        try:
            auto_send_reminders_for_school(sid, min_balance=min_balance)
        except Exception:
            continue












