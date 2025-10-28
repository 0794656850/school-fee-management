from flask import Blueprint, render_template, current_app, redirect, url_for, flash, request, session
from decimal import Decimal
import os

import mysql.connector
from utils.notify import normalize_phone
from utils.whatsapp import (
    whatsapp_is_configured,
    send_whatsapp_text,
    send_whatsapp_template,
)
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



def _phone_column_exists(cursor, col_name: str | None = None):
    from utils.settings import get_setting
    name = (col_name or get_setting("REMINDER_PHONE_COLUMN") or "phone").strip() or "phone"
    cursor.execute("SHOW COLUMNS FROM students LIKE %s", (name,))
    return bool(cursor.fetchone())

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

    # Determine phone column to use for reminders
    from utils.settings import get_setting as _gs
    conf_col = (_gs("REMINDER_PHONE_COLUMN") or "phone").strip() or "phone"
    has_phone = _phone_column_exists(cursor, conf_col)
    phone_select = f"{conf_col} AS phone" if has_phone else "NULL AS phone"

    # Filters for K–12 operations
    selected_class = (request.args.get('class') or '').strip()
    try:
        min_balance = float(request.args.get('min_balance') or 0)
    except Exception:
        min_balance = 0.0

    # Build query with optional filters
    base_sql = [
        f"SELECT id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance",
        "FROM students",
        "WHERE school_id = %s AND COALESCE(" + col + ", 0) > 0",
    ]
    params: list[object] = [session.get("school_id")]
    if selected_class:
        base_sql.append("AND class_name = %s")
        params.append(selected_class)
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

    wa_ok, _ = whatsapp_is_configured()
    return render_template(
        'reminders.html',
        students=students,
        classes=classes,
        selected_class=selected_class,
        min_balance=min_balance,
        default_message_template=(get_setting("REMINDER_DEFAULT_MESSAGE") or ''),
        whatsapp_enabled=wa_ok,
    )


@reminder_bp.route('/send/<int:student_id>', methods=['GET', 'POST'])
def send_sms_reminder(student_id: int):
    db = _db_from_config()
    cursor = db.cursor(dictionary=True)

    col = _detect_balance_column(cursor)
    if not col:
        db.close()
        flash("No valid balance column found in 'students' table.", "error")
        return redirect(url_for('reminders.reminders_home'))

    # Determine phone column to use for reminders
    from utils.settings import get_setting as _gs
    conf_col = (_gs("REMINDER_PHONE_COLUMN") or "phone").strip() or "phone"
    has_phone = _phone_column_exists(cursor, conf_col)
    phone_select = f"{conf_col} AS phone" if has_phone else "NULL AS phone"

    cursor.execute(
        f"""
        SELECT id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance
        FROM students WHERE id = %s AND school_id = %s
        """,
        (student_id, session.get("school_id"))
    )
    student = cursor.fetchone()
    db.close()

    if not student:
        flash("Student not found.", "error")
        return redirect(url_for('reminders.reminders_home'))

    if not student.get('phone'):
        flash("Student has no phone number on record.", "warning")
        return redirect(url_for('reminders.reminders_home'))

    to_number = normalize_phone(student['phone'])
    balance = Decimal(str(student.get('balance') or 0))

    # Channel: WhatsApp only (Twilio SMS removed)
    wa_ok, _ = whatsapp_is_configured()

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

    # Prefer template if configured, else plain text (requires open 24h session)
    if not wa_ok:
        flash("WhatsApp is not configured. Set WHATSAPP_* in environment.", "error")
        return redirect(url_for('reminders.reminders_home'))
    template = (get_setting('WHATSAPP_TEMPLATE_NAME') or current_app.config.get('WHATSAPP_TEMPLATE_NAME') or '')
    lang = (get_setting('WHATSAPP_TEMPLATE_LANG') or current_app.config.get('WHATSAPP_TEMPLATE_LANG', 'en_US'))
    if template:
        ok, err = send_whatsapp_template(
            to_number,
            template_name=template,
            language=lang,
            body_parameters=[student['name'], f"{balance:,.2f}"]
        )
    else:
        ok, err = send_whatsapp_text(to_number, message_body)
    if ok:
        flash(f"WhatsApp reminder sent to {student['name']} ({to_number}).", "success")
    else:
        flash(f"Failed to send WhatsApp message: {err}", "error")

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

    # Determine phone column to use for reminders
    from utils.settings import get_setting as _gs
    conf_col = (_gs("REMINDER_PHONE_COLUMN") or "phone").strip() or "phone"
    has_phone = _phone_column_exists(cursor, conf_col)
    phone_select = f"{conf_col} AS phone" if has_phone else "NULL AS phone"

    cursor.execute(
        f"""
        SELECT id, name, class_name, {phone_select}, COALESCE({col}, 0) AS balance
        FROM students
        WHERE school_id=%s AND COALESCE({col}, 0) > 0
        ORDER BY id ASC
        """
    , (session.get("school_id"),))
    students = cursor.fetchall()
    db.close()

    # WhatsApp only
    wa_ok, _ = whatsapp_is_configured()

    sent = 0
    skipped = 0
    message_template = request.form.get('message', '')
    for s in students:
        if not s.get('phone'):
            skipped += 1
            continue
        to_number = normalize_phone(s['phone'])
        balance = Decimal(str(s.get('balance') or 0))
        if message_template:
            msg = _render_message(message_template, name=s['name'], balance=balance, class_name=s.get('class_name'))
        else:
            msg = (
                f"Hello {s['name']}, this is a fee reminder from the school. "
                f"Your outstanding balance is KES {balance:,.2f}. "
                f"Kindly clear at your earliest convenience."
            )
        if not wa_ok:
            ok = False
        else:
            template = (get_setting('WHATSAPP_TEMPLATE_NAME') or current_app.config.get('WHATSAPP_TEMPLATE_NAME') or '')
            lang = (get_setting('WHATSAPP_TEMPLATE_LANG') or current_app.config.get('WHATSAPP_TEMPLATE_LANG', 'en_US'))
            if template:
                ok, _err = send_whatsapp_template(
                    to_number,
                    template_name=template,
                    language=lang,
                    body_parameters=[s['name'], f"{balance:,.2f}"]
                )
            else:
                ok, _err = send_whatsapp_text(to_number, msg)
        if ok:
            sent += 1
        else:
            skipped += 1

    flash(f"Bulk reminders completed. Sent: {sent}, Skipped/Failed: {skipped}.", "info")
    return redirect(url_for('reminders.reminders_home'))







