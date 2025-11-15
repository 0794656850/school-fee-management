from flask import Blueprint, render_template, current_app, redirect, url_for, flash, request, session, jsonify
from decimal import Decimal
import os

import mysql.connector
from flask_mail import Message
from extensions import mail
from utils.notify import normalize_phone  # legacy import; unused after email switch
from utils.gmail_api import send_email as gmail_send_email, has_valid_token
from utils.settings import get_setting

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


def _resolve_email_column(cursor) -> str | None:
    """Pick an email column for reminders: setting -> 'email' -> 'parent_email'."""
    from utils.settings import get_setting
    pref = (get_setting("REMINDER_EMAIL_COLUMN") or "").strip()
    if pref and _column_exists(cursor, pref):
        return pref
    for cand in ("email", "parent_email"):
        if _column_exists(cursor, cand):
            return cand
    return None

class _SafeDict(dict):
    def __missing__(self, key):  # graceful placeholder if unknown
        return '{' + key + '}'


def _render_message(template: str, *, name: str, balance: Decimal, class_name: str | None) -> str:
    data = _SafeDict(
        name=name,
        balance=f"{balance:,.2f}",
        class_name=class_name or "",
        klass=class_name or "",
        cls=class_name or "",
        school=get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school"),
        school_name=get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME", "the school"),
    )
    return (template or "").format_map(data)


@reminder_bp.route('/')
def reminders_home():
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return render_template('reminders.html', default_message_template=(get_setting("REMINDER_DEFAULT_MESSAGE") or ''), students=[], classes=[], whatsapp_enabled=False)

    # Determine email column to use for reminders
    email_col = _resolve_email_column(cursor)
    email_select = f"{email_col} AS email" if email_col else "NULL AS email"

    # Filters for Kâ€“12 operations
    selected_class = (request.args.get('class') or '').strip()
    q = (request.args.get('q') or '').strip()
    try:
        min_balance = float(request.args.get('min_balance') or 0)
    except Exception:
        min_balance = 0.0

    # Build query with optional filters
    base_sql = [
        f"SELECT id, name, class_name, {email_select}, COALESCE({col}, 0) AS balance",
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
            # email_col defined above; use when available
            if email_col:
                clauses.append(f"{email_col} LIKE %s")
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
        default_message_template=(get_setting("REMINDER_DEFAULT_MESSAGE") or ''),
        email_enabled=True,
        gmail_connected=gmail_connected,
    )


@reminder_bp.route('/send/<int:student_id>', methods=['GET', 'POST'])
def send_email_reminder(student_id: int):
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    # Determine email column to use for reminders
    email_col = _resolve_email_column(cursor)
    email_select = f"{email_col} AS email" if email_col else "NULL AS email"

    cursor.execute(
        f"""
        SELECT id, name, class_name, {email_select}, COALESCE({col}, 0) AS balance
        FROM students WHERE id = %s AND school_id = %s
        """,
        (student_id, session.get("school_id"))
    )
    student = cursor.fetchone()
    db.close()

    if not student:
        flash("Student not found.", "error")
        return redirect(url_for('reminders.reminders_home'))

    if not student.get('email'):
        flash("Student has no email on record.", "warning")
        return redirect(url_for('reminders.reminders_home'))
    balance = Decimal(str(student.get('balance') or 0))

    # Optional custom message from form/query with placeholders
    message_template = request.form.get('message') or request.args.get('message')
    if message_template:
        message_body = _render_message(message_template, name=student['name'], balance=balance, class_name=student.get('class_name'))
    else:
        message_body = (
            f"Hello {student['name']}, this is a fee reminder from the school. "
            f"Your outstanding balance is KES {balance:,.2f}. "
            f"Kindly clear at your earliest convenience."
        )

    # Prefer Gmail API OAuth2 sender if available; fallback to Flask-Mail
    subject = f"Fee reminder for {student['name']}"
    sent = False
    try:
        sent = gmail_send_email(student['email'], subject, message_body)
    except Exception:
        sent = False
    if not sent:
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
                    recipients=[student['email']],
                    body=message_body,
                )
                mail.send(msg)
                sent = True
            except Exception as e:
                flash(f"Failed to send email: {e}", "error")
        else:
            flash("Email sending is not configured. Connect Gmail (Reminders > Connect Gmail) or set MAIL_* SMTP settings.", "error")
    if sent:
        flash(f"Email reminder sent to {student['name']} ({student['email']}).", "success")

    return redirect(url_for('reminders.reminders_home'))


@reminder_bp.route('/send_all', methods=['POST'])
def send_all_reminders():
    """Send reminders to all students with positive balances. Simple best-effort loop."""
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    # Determine email column to use for reminders
    email_col = _resolve_email_column(cursor)
    email_select = f"{email_col} AS email" if email_col else "NULL AS email"

    cursor.execute(
        f"""
        SELECT id, name, class_name, {email_select}, COALESCE({col}, 0) AS balance
        FROM students
        WHERE school_id=%s AND COALESCE({col}, 0) > 0
        ORDER BY id ASC
        """
    , (session.get("school_id"),))
    students = cursor.fetchall()
    db.close()

    sent = 0
    skipped = 0
    message_template = request.form.get('message', '')
    for s in students:
        if not s.get('email'):
            skipped += 1
            continue
        balance = Decimal(str(s.get('balance') or 0))
        if message_template:
            msg = _render_message(message_template, name=s['name'], balance=balance, class_name=s.get('class_name'))
        else:
            msg = (
                f"Hello {s['name']}, this is a fee reminder from the school. "
                f"Your outstanding balance is KES {balance:,.2f}. "
                f"Kindly clear at your earliest convenience."
            )
        # Try Gmail API first
        subject = f"Fee reminder for {s['name']}"
        ok = False
        try:
            ok = gmail_send_email(s['email'], subject, msg)
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
                        recipients=[s['email']],
                        body=msg,
                    )
                    mail.send(m)
                    ok = True
                except Exception:
                    ok = False
            else:
                ok = False
        if ok:
            sent += 1
        else:
            skipped += 1

    flash(f"Bulk reminders completed. Sent: {sent}, Skipped/Failed: {skipped}.", "info")
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












