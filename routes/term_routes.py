from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session
import csv
import json
import hmac
import hashlib
import mysql.connector
from io import StringIO
from urllib.parse import urlparse
from datetime import date, datetime
import os
from utils.pro import is_pro_enabled, upgrade_url
from utils.audit import log_event
from utils.gmail_api import send_email_html as gmail_send_email_html
from utils.classes import promote_class_name
from utils.settings import get_setting, set_school_setting
from routes.newsletter_routes import ensure_newsletters_table
from utils.ai import ai_is_configured, chat_anything
from markupsafe import escape

try:
    from flask_mail import Message
    from extensions import mail
except Exception:
    Message = None  # type: ignore
    mail = None  # type: ignore

term_bp = Blueprint("terms", __name__, url_prefix="/terms")


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


def ensure_academic_terms_table(conn) -> None:
    cur = conn.cursor()
    # Create base table if missing
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS academic_terms (
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            label VARCHAR(64),
            start_date DATE,
            end_date DATE,
            is_current TINYINT(1) DEFAULT 0,
            school_id INT NULL
        )
        """
    )
    conn.commit()
    # Ensure school_id column exists and is indexed
    try:
        cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'school_id'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE academic_terms ADD COLUMN school_id INT NULL")
            conn.commit()
        try:
            cur.execute("CREATE INDEX idx_academic_terms_school ON academic_terms(school_id)")
            conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    # Ensure term lifecycle columns exist
    try:
        cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'status'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE academic_terms ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'DRAFT' AFTER end_date")
        cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'opens_at'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE academic_terms ADD COLUMN opens_at DATETIME NULL AFTER status")
        cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'closes_at'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE academic_terms ADD COLUMN closes_at DATETIME NULL AFTER opens_at")
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    # Ensure composite unique (school_id, year, term); drop legacy unique if present
    try:
        try:
            cur.execute("SHOW INDEX FROM academic_terms WHERE Key_name='uq_year_term'")
            if cur.fetchone():
                try:
                    cur.execute("ALTER TABLE academic_terms DROP INDEX uq_year_term")
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass
        cur.execute("SHOW INDEX FROM academic_terms WHERE Key_name='uq_school_year_term'")
        if not cur.fetchone():
            cur.execute("CREATE UNIQUE INDEX uq_school_year_term ON academic_terms(school_id, year, term)")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_payments_term_columns(conn) -> None:
    cur = conn.cursor()
    # term column
    cur.execute("SHOW COLUMNS FROM payments LIKE 'term'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE payments ADD COLUMN term TINYINT NULL AFTER method")
        conn.commit()
    # year column
    cur.execute("SHOW COLUMNS FROM payments LIKE 'year'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE payments ADD COLUMN year INT NULL AFTER term")
        conn.commit()


def ensure_student_enrollments_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS student_enrollments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            class_name VARCHAR(50),
            opening_balance DECIMAL(12,2) DEFAULT 0,
            closing_balance DECIMAL(12,2) DEFAULT 0,
            status VARCHAR(20) DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_student_year (student_id, year)
        )
        """
    )
    conn.commit()

    # Add a one-time retention flag on students to support real-world cases
    # where a learner repeats the current class for the upcoming year.
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'retain_next_year'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE students ADD COLUMN retain_next_year TINYINT(1) NOT NULL DEFAULT 0 AFTER class_name")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _resolve_email_column(cursor) -> str | None:
    try:
        cursor.execute("SHOW COLUMNS FROM students LIKE 'email'")
        if cursor.fetchone():
            return "email"
        cursor.execute("SHOW COLUMNS FROM students LIKE 'parent_email'")
        if cursor.fetchone():
            return "parent_email"
    except Exception:
        pass
    return None


def _detect_balance_column(conn) -> str:
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return "balance" if has_balance else "fee_balance"


def _send_term_memos(db, year: int, term: int, due_date=None) -> tuple[int, int]:
    """Send premium term memo emails to all students for the specified year/term.

    Returns (sent_count, skipped_count).
    """
    try:
        cur = db.cursor(dictionary=True)
        email_col = _resolve_email_column(cur)
        if not email_col:
            return (0, 0)
        sid = session.get("school_id") if session else None
        # Join invoices for total due per student
        if sid:
            cur.execute(
                f"""
                SELECT s.id, s.name, s.class_name, s.{email_col} AS email, i.total
                FROM students s
                LEFT JOIN invoices i ON i.student_id = s.id AND i.year=%s AND i.term=%s
                WHERE s.school_id=%s
                ORDER BY s.name ASC
                """,
                (year, term, sid),
            )
        else:
            cur.execute(
                f"""
                SELECT s.id, s.name, s.class_name, s.{email_col} AS email, i.total
                FROM students s
                LEFT JOIN invoices i ON i.student_id = s.id AND i.year=%s AND i.term=%s
                ORDER BY s.name ASC
                """,
                (year, term),
            )
        students = cur.fetchall() or []

        school_name = (get_setting("SCHOOL_NAME") or "School")
        subject = f"{school_name} Term {term} Memo - {year}"
        sent = 0
        skipped = 0
        # Render per-student HTML and send
        for s in students:
            to_addr = (s.get("email") or "").strip() if s else ""
            if not to_addr:
                skipped += 1
                continue
            try:
                # Try include fee structure from invoice items if available
                items = []
                try:
                    cur_i = db.cursor(dictionary=True)
                    cur_i.execute(
                        """
                        SELECT it.description, it.amount
                        FROM invoices inv
                        JOIN invoice_items it ON it.invoice_id = inv.id
                        WHERE inv.student_id=%s AND inv.year=%s AND inv.term=%s
                        ORDER BY it.id ASC
                        """,
                        (s.get("id"), year, term),
                    )
                    items = cur_i.fetchall() or []
                except Exception:
                    items = []
                html_body = render_template(
                    "email_term_memo.html",
                    brand=school_name,
                    student_name=s.get("name"),
                    class_name=s.get("class_name"),
                    year=year,
                    term=term,
                    due_date=str(due_date) if due_date else None,
                    amount_due=float(s.get("total") or 0.0),
                    fee_items=items,
                )
                ok = gmail_send_email_html(to_addr, subject, html_body)
            except Exception:
                ok = False
            if ok:
                sent += 1
            else:
                skipped += 1
        return (sent, skipped)
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _apply_term_fee_amount(db, student_id, year, term, fee_amount, bal_col, school_id):
    cur = db.cursor(dictionary=True)
    pcur = db.cursor()
    try:
        cur.execute(
            "SELECT fee_amount, initial_fee, adjusted_fee, discount, final_fee FROM term_fees WHERE student_id=%s AND year=%s AND term=%s",
            (student_id, year, term),
        )
        prev = cur.fetchone() or {}
        prev_final = float(prev.get("final_fee") if prev.get("final_fee") is not None else prev.get("fee_amount") or 0.0)

        new_initial = None
        new_adjusted = None
        if prev:
            new_adjusted = float(fee_amount)
        else:
            new_initial = float(fee_amount)

        row_discount = float(prev.get("discount") or 0.0)
        effective_fee = (
            new_adjusted
            if new_adjusted is not None
            else (prev.get("adjusted_fee") if prev.get("adjusted_fee") is not None else None)
        )
        if effective_fee is None:
            effective_fee = new_initial if new_initial is not None else (prev.get("initial_fee") or 0.0)
        effective_fee = float(effective_fee or 0.0)

        discount_exceeded = False
        if row_discount and row_discount > effective_fee:
            discount_exceeded = True
            row_discount = effective_fee

        new_final = max(float(effective_fee) - float(row_discount or 0.0), 0.0)

        if prev:
            pcur.execute(
                "UPDATE term_fees SET fee_amount=%s, final_fee=%s, adjusted_fee=%s WHERE student_id=%s AND year=%s AND term=%s",
                (new_final, new_final, new_adjusted, student_id, year, term),
            )
        else:
            pcur.execute(
                "INSERT INTO term_fees (student_id, year, term, fee_amount, initial_fee, final_fee) VALUES (%s,%s,%s,%s,%s,%s)",
                (student_id, year, term, new_final, new_initial, new_final),
            )

        delta = float(new_final) - float(prev_final)
        if abs(delta) > 0:
            pcur.execute(
                f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                (delta, student_id, school_id),
            )

        try:
            cur.execute(
                "SELECT COALESCE(credit,0) AS credit, COALESCE(" + bal_col + ",0) AS bal FROM students WHERE id=%s AND school_id=%s",
                (student_id, school_id),
            )
            row = cur.fetchone() or {"credit": 0, "bal": 0}
            avail = float(row.get("credit") or 0)
            bal_now = float(row.get("bal") or 0)
            apply = min(avail, max(bal_now, 0))
            if apply > 0:
                pcur.execute(
                    f"UPDATE students SET {bal_col} = {bal_col} - %s, credit = credit - %s WHERE id=%s AND school_id=%s",
                    (apply, apply, student_id, school_id),
                )
                try:
                    payment_cur = db.cursor()
                    payment_cur.execute(
                        "INSERT INTO payments (student_id, amount, method, reference, date, year, term, school_id) VALUES (%s,%s,%s,%s,NOW(),%s,%s,%s)",
                        (student_id, apply, 'Credit Transfer', 'Auto-apply starting term credit', year, term, school_id),
                    )
                    payment_cur.close()
                except Exception:
                    pass
        except Exception:
            pass

        return {
            "delta": delta,
            "new_final": new_final,
            "prev_final": prev_final,
            "is_adjustment": bool(prev),
            "discount_exceeded": discount_exceeded,
        }
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            pcur.close()
        except Exception:
            pass

def _auto_compose_term_comms(db, year: int, term: int) -> None:
    """Create premium-ready draft newsletter and memo when a term is set/opened.

    - Writes two rows into `newsletters` (category 'newsletter' and 'memo').
    - Uses AI when configured to generate modern, parent-friendly HTML blocks.
    - Idempotent per school/year/term by checking for existing titles.
    """
    try:
        ensure_newsletters_table(db)
    except Exception:
        # If table missing and cannot be created, skip silently
        return


TERM_EVENT_CONFIG = {
    "flat_fee": {
        "headline": "Fees published",
        "subject": "{school} • Term {term} {year} flat fees are live",
        "summary": "The flat fee schedule for Term {term} {year} has been published and invoices were generated.",
        "category": "term_event",
    },
    "open": {
        "headline": "Term opened",
        "subject": "{school} • Term {term} {year} is now open",
        "summary": "Term {term} {year} is officially open for learning, and we look forward to a successful term.",
        "category": "term_event",
    },
    "close": {
        "headline": "Term closed",
        "subject": "{school} • Term {term} {year} has closed",
        "summary": "Term {term} {year} has been closed. Thank you for the support and the timely payments.",
        "category": "term_event",
    },
}


def _smtp_ready() -> bool:
    try:
        cfg = current_app.config
        server = (cfg.get("MAIL_SERVER") or "").strip()
        username = (cfg.get("MAIL_USERNAME") or "").strip()
        password = (cfg.get("MAIL_PASSWORD") or "").strip()
        return bool(server and username and password)
    except Exception:
        return False


def _term_event_email_sender() -> str | None:
    sender = (
        get_setting("SCHOOL_EMAIL")
        or current_app.config.get("MAIL_SENDER")
        or current_app.config.get("MAIL_DEFAULT_SENDER")
        or current_app.config.get("MAIL_USERNAME")
    )
    return sender.strip() if sender else None


def _collect_guardian_emails(db, school_id: int | None = None) -> list[str]:
    emails: set[str] = set()
    cur = None
    try:
        cur = db.cursor(dictionary=True)
        email_col = _resolve_email_column(cur)
        if not email_col:
            return []
        where = f"{email_col} IS NOT NULL AND {email_col} <> ''"
        params: list = []
        if school_id is not None:
            where += " AND school_id=%s"
            params.append(school_id)
        cur.execute(f"SELECT DISTINCT {email_col} AS email FROM students WHERE {where}", tuple(params))
        for row in cur.fetchall() or []:
            value = (row.get("email") or "").strip()
            if value:
                emails.add(value)
    except Exception:
        pass
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
    return sorted(emails)


def _send_term_event_emails(emails: list[str], subject: str, html: str) -> int:
    if not emails:
        return 0
    sent = 0
    smtp_ok = _smtp_ready() and Message and mail
    sender = _term_event_email_sender()
    if smtp_ok and not sender:
        smtp_ok = False
    for to_addr in emails:
        address = to_addr.strip()
        if not address:
            continue
        ok = False
        try:
            ok = gmail_send_email_html(address, subject, html)
        except Exception:
            ok = False
        if not ok and smtp_ok and Message and mail and sender:
            try:
                msg = Message(subject=subject, sender=sender, recipients=[address], html=html)
                mail.send(msg)
                ok = True
            except Exception:
                ok = False
        if ok:
            sent += 1
    return sent


def _term_event_html(full_title: str, summary: str, details: dict[str, str | int | None], portal_url: str) -> str:
    parts = [
        "<div style=\"font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Arial;color:#0f172a;\">",
        f"<h2 style=\"margin-bottom:8px;font-size:1.25rem;\">{escape(full_title)}</h2>",
        f"<p style=\"margin-bottom:6px;font-size:1rem;\">{escape(summary)}</p>",
    ]
    due = details.get("due_date")
    if due:
        parts.append(f"<p style=\"margin-bottom:4px;\"><strong>Due date:</strong> {escape(str(due))}</p>")
    invoices = details.get("invoice_count")
    if invoices:
        parts.append(f"<p style=\"margin-bottom:4px;\"><strong>Invoices:</strong> {escape(str(invoices))} generated</p>")
    event_time = details.get("timestamp")
    if event_time:
        if hasattr(event_time, "strftime"):
            timestamp = event_time.strftime("%Y-%m-%d %H:%M")
        else:
            timestamp = str(event_time)
        parts.append(f"<p style=\"margin-bottom:4px;\"><strong>Updated:</strong> {escape(timestamp)}</p>")
    parts.append(
        f"<p style=\"margin-top:12px;font-size:0.95rem;\">"
        f"Visit the <a href=\"{escape(portal_url)}\" style=\"color:#4f46e5;\">SmartEduPay portal</a> to view invoices, receipts, and term communication.</p>"
    )
    parts.append("</div>")
    return "".join(parts)


def _publish_term_event(
    db,
    year: int,
    term: int,
    event_kind: str,
    details: dict[str, str | int | datetime | None] | None = None,
) -> None:
    cfg = TERM_EVENT_CONFIG.get(event_kind)
    if not cfg:
        return
    details = details or {}
    school = (get_setting("SCHOOL_NAME") or "School").strip()
    subject = cfg["subject"].format(school=school, year=year, term=term)
    title = f"{school}: Term {term} {year} • {cfg['headline']}"
    portal_url = url_for("dashboard", _external=True)
    html = _term_event_html(title, cfg["summary"].format(year=year, term=term), details, portal_url)

    try:
        ensure_newsletters_table(db)
    except Exception:
        return
    cur = None
    try:
        cur = db.cursor()
        sid = session.get("school_id") if session else None
        params = [title, sid, sid]
        cur.execute(
            "SELECT COUNT(*) AS c FROM newsletters WHERE title=%s AND (school_id=%s OR (school_id IS NULL AND %s IS NULL))",
            tuple(params),
        )
        if (cur.fetchone() or {}).get("c", 0):
            return
        cat = cfg.get("category") or "term_event"
        cur.execute(
            "INSERT INTO newsletters (school_id, category, title, subject, html, audience_type, audience_value, created_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (sid, cat, title, subject, html, "all", None, None),
        )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
    emails = _collect_guardian_emails(db, session.get("school_id") if session else None)
    if emails:
        _send_term_event_emails(emails, subject, html)

    try:
        cur = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None

        # Gather context for copy generation
        school = (get_setting("SCHOOL_NAME") or "School").strip()
        # Try to read configured dates for the term
        start_date, end_date = None, None
        try:
            if sid:
                cur.execute(
                    "SELECT start_date, end_date FROM academic_terms WHERE year=%s AND term=%s AND (school_id=%s OR school_id IS NULL) ORDER BY school_id DESC LIMIT 1",
                    (year, term, sid),
                )
            else:
                cur.execute(
                    "SELECT start_date, end_date FROM academic_terms WHERE year=%s AND term=%s ORDER BY id DESC LIMIT 1",
                    (year, term),
                )
            r = cur.fetchone() or {}
            start_date = r.get("start_date")
            end_date = r.get("end_date")
        except Exception:
            start_date, end_date = None, None

        # Unique-ish titles per term
        n_title = f"{school}: Term {term} {year} — Welcome & Key Updates"
        m_title = f"{school}: Term {term} {year} — Fees Memo"

        # Skip if they already exist for this school/term
        try:
            if sid:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM newsletters WHERE school_id=%s AND title IN (%s,%s)",
                    (sid, n_title, m_title),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM newsletters WHERE title IN (%s,%s)",
                    (n_title, m_title),
                )
            c = (cur.fetchone() or {}).get("c", 0)
            if int(c or 0) >= 2:
                return
        except Exception:
            # If COUNT fails (older MySQL), fall back to opportunistic inserts
            pass

        # Prepare modern HTML blocks (AI if available, else templated)
        dates_line = None
        if start_date or end_date:
            try:
                s = str(start_date) if start_date else "TBA"
                e = str(end_date) if end_date else "TBA"
                dates_line = f"<p class=\"text-gray-700\"><strong>Term Window:</strong> {s} — {e}</p>"
            except Exception:
                dates_line = None

        base_style = (
            "<style>body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Arial}" 
            ".card{border:1px solid #e5e7eb;border-radius:12px;padding:16px;margin:12px 0}" 
            ".cta{display:inline-block;background:#4f46e5;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none}</style>"
        )

        if ai_is_configured():
            # Compose with AI for premium tone
            prompt_ctx = (
                f"You are an assistant for a school fee portal. School: {school}. "
                f"Create HTML sections with short headings and friendly copy. "
                f"Term: {term} {year}. Start date: {start_date or 'TBA'}. End date: {end_date or 'TBA'}. "
                "Include: welcome note, important dates, fees reminder, payment options, contact & support. "
                "Keep paragraphs short. Avoid external images; use basic HTML only."
            )
            nl_html = chat_anything([
                {"role": "system", "content": "Write clean, semantic HTML only. No <html> or <body> tags."},
                {"role": "user", "content": prompt_ctx},
            ])
            memo_ctx = (
                f"Draft a concise HTML memo for parents about Term {term} {year} at {school}. "
                "Sections: Total fees due (leave as a friendly reminder, no amounts), how to pay, key dates, office hours. "
                "Tone: warm, clear, professional."
            )
            memo_html = chat_anything([
                {"role": "system", "content": "Return only HTML fragments; no scripts, no external images."},
                {"role": "user", "content": memo_ctx},
            ])
        else:
            # Fallback handcrafted blocks
            nl_html = (
                f"{base_style}<div class='card'><h2>Welcome to Term {term} • {year}</h2>"
                f"<p>Dear Parents/Guardians, welcome back to {school}. We’re excited to begin a new term together.</p>"
                f"{dates_line or ''}</div>"
                "<div class='card'><h3>What’s Inside This Term</h3><ul>"
                "<li>Academic focus and classroom routines</li>"
                "<li>Co-curricular highlights and events</li>"
                "<li>Student wellbeing and support</li>"
                "</ul></div>"
                "<div class='card'><h3>Fees & Payments</h3><p>Please clear outstanding balances promptly to support operations."
                " You can pay via the usual channels (cash office, bank, or mobile money) and keep your receipt for records.</p>"
                "<a class='cta' href='#'>View Payment Options</a></div>"
                "<div class='card'><h3>We’re Here to Help</h3><p>For any assistance, contact the office. Thank you for your continued support.</p></div>"
            )
            memo_html = (
                f"{base_style}<div class='card'><h2>Term {term} {year} — Parent Memo</h2>"
                f"{dates_line or ''}"
                "<p>Please ensure fees are settled promptly. Payment options remain unchanged.</p>"
                "<ul><li>Office hours: Mon–Fri, 8:00am–5:00pm</li>"
                "<li>Keep payment references for reconciliation</li>"
                "<li>Reach out if you need clarifications</li></ul>"
                "</div>"
            )

        # Insert drafts
        cur_i = db.cursor()
        sid_val = session.get("school_id") if session else None
        try:
            if sid_val:
                cur_i.execute(
                    "INSERT IGNORE INTO newsletters (school_id, category, title, subject, html, audience_type, audience_value, created_by)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (sid_val, "newsletter", n_title, f"{school} • Term {term} {year} Updates", nl_html, "all", None, None),
                )
                cur_i.execute(
                    "INSERT IGNORE INTO newsletters (school_id, category, title, subject, html, audience_type, audience_value, created_by)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (sid_val, "memo", m_title, f"{school} • Term {term} {year} Memo", memo_html, "all", None, None),
                )
            else:
                cur_i.execute(
                    "INSERT IGNORE INTO newsletters (category, title, subject, html, audience_type, audience_value, created_by)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    ("newsletter", n_title, f"{school} • Term {term} {year} Updates", nl_html, "all", None, None),
                )
                cur_i.execute(
                    "INSERT IGNORE INTO newsletters (category, title, subject, html, audience_type, audience_value, created_by)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    ("memo", m_title, f"{school} • Term {term} {year} Memo", memo_html, "all", None, None),
                )
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
    except Exception:
        # Never break term flows due to auto-compose errors
        return


def infer_kenya_term_for_date(d: date) -> int:
    # Typical Kenyan school terms (approx ranges):
    # Term 1: Janâ€“Apr, Term 2: Mayâ€“Aug, Term 3: Sepâ€“Nov/Dec
    m = d.month
    if 1 <= m <= 4:
        return 1
    if 5 <= m <= 8:
        return 2
    return 3


def get_or_seed_current_term(conn) -> tuple[int, int]:
    """Return (year, term). Seed current year/terms if table is empty.

    Also auto-set is_current based on today falling within a configured range; otherwise
    infer by month.
    """
    ensure_academic_terms_table(conn)
    cur = conn.cursor(dictionary=True)
    sid = session.get("school_id") if session else None

    # 1) Respect per-school override if explicitly set by user in settings
    try:
        if sid:
            y_cfg = get_setting("CURRENT_YEAR")
            t_cfg = get_setting("CURRENT_TERM")
            if y_cfg and t_cfg:
                y_val = int(y_cfg)
                t_val = int(t_cfg)
                if t_val in (1, 2, 3):
                    # Best-effort: mark matching academic_terms row as current for consistency
                    try:
                        cur2 = conn.cursor()
                        try:
                            cur2.execute("UPDATE academic_terms SET is_current=0 WHERE school_id=%s", (sid,))
                        except Exception:
                            cur2.execute("UPDATE academic_terms SET is_current=0")
                        try:
                            cur2.execute(
                                "UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s AND school_id=%s",
                                (y_val, t_val, sid),
                            )
                        except Exception:
                            cur2.execute(
                                "UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s",
                                (y_val, t_val),
                            )
                        # If row doesn't exist, upsert a minimal placeholder
                        if cur2.rowcount == 0:
                            try:
                                cur2.execute(
                                    "INSERT INTO academic_terms (year, term, label, is_current, school_id) VALUES (%s,%s,%s,1,%s)",
                                    (y_val, t_val, f"Term {t_val}", sid),
                                )
                            except Exception:
                                cur2.execute(
                                    "INSERT IGNORE INTO academic_terms (year, term, is_current) VALUES (%s,%s,1)",
                                    (y_val, t_val),
                                )
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    return y_val, t_val
    except Exception:
        # Ignore settings parsing issues and continue with DB-based resolution
        pass
    if sid:
        # Count terms for this school; seed if none
        try:
            cur.execute("SELECT COUNT(*) AS c FROM academic_terms WHERE school_id=%s", (sid,))
            c = (cur.fetchone() or {}).get("c", 0)
        except Exception:
            # Fallback to global count if column missing
            cur.execute("SELECT COUNT(*) AS c FROM academic_terms")
            c = (cur.fetchone() or {}).get("c", 0)
    else:
        cur.execute("SELECT COUNT(*) AS c FROM academic_terms")
        c = (cur.fetchone() or {}).get("c", 0)
    today = date.today()
    if not c:
        y = today.year
        # Seed three standard terms with approx dates
        seed = [
            (y, 1, "Term 1", date(y, 1, 3), date(y, 4, 15)),
            (y, 2, "Term 2", date(y, 5, 5), date(y, 8, 15)),
            (y, 3, "Term 3", date(y, 9, 1), date(y, 11, 30)),
        ]
        cur2 = conn.cursor()
        for yy, t, lbl, s, e in seed:
            if sid:
                try:
                    cur2.execute(
                        "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (yy, t, lbl, s, e, 0, sid),
                    )
                except Exception:
                    # Fallback for legacy schema without school_id
                    cur2.execute(
                        "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current) VALUES (%s,%s,%s,%s,%s,%s)",
                        (yy, t, lbl, s, e, 0),
                    )
            else:
                cur2.execute(
                    "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current) VALUES (%s,%s,%s,%s,%s,%s)",
                    (yy, t, lbl, s, e, 0),
                )
        conn.commit()

    # Try to find current by date range
    if sid:
        try:
            cur.execute(
                "SELECT year, term FROM academic_terms WHERE is_current=1 AND school_id=%s ORDER BY year DESC, term DESC LIMIT 1",
                (sid,),
            )
        except Exception:
            cur.execute(
                "SELECT year, term FROM academic_terms WHERE is_current=1 ORDER BY year DESC, term DESC LIMIT 1"
            )
    else:
        cur.execute(
            "SELECT year, term FROM academic_terms WHERE is_current=1 ORDER BY year DESC, term DESC LIMIT 1"
        )
    row = cur.fetchone()
    if row:
        return int(row["year"]), int(row["term"])

    if sid:
        try:
            cur.execute(
                "SELECT year, term, start_date, end_date FROM academic_terms WHERE school_id=%s ORDER BY year DESC, term ASC",
                (sid,),
            )
        except Exception:
            cur.execute(
                "SELECT year, term, start_date, end_date FROM academic_terms ORDER BY year DESC, term ASC"
            )
    else:
        cur.execute(
            "SELECT year, term, start_date, end_date FROM academic_terms ORDER BY year DESC, term ASC"
        )
    rows = cur.fetchall() or []
    for r in rows:
        s = r.get("start_date")
        e = r.get("end_date")
        if s and e and s <= today <= e:
            # set as current once
            cur2 = conn.cursor()
            if sid:
                try:
                    cur2.execute("UPDATE academic_terms SET is_current=0 WHERE school_id=%s", (sid,))
                    cur2.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s AND school_id=%s", (r["year"], r["term"], sid))
                except Exception:
                    cur2.execute("UPDATE academic_terms SET is_current=0")
                    cur2.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s", (r["year"], r["term"]))
            else:
                cur2.execute("UPDATE academic_terms SET is_current=0")
                cur2.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s", (r["year"], r["term"]))
            conn.commit()
            return int(r["year"]), int(r["term"])

    # Fallback: infer by month
    return today.year, infer_kenya_term_for_date(today)


@term_bp.route("/current")
def current_term_api():
    """Return the current academic year/term as JSON for UI badges."""
    try:
        db = _db()
        try:
            y, t = get_or_seed_current_term(db)
            return jsonify({"year": int(y), "term": int(t), "ok": True})
        finally:
            db.close()
    except Exception:
        # Fallback to date-based inference to avoid breaking the UI
        today = date.today()
        return jsonify({"year": today.year, "term": infer_kenya_term_for_date(today), "ok": False})


@term_bp.route("/")
def manage_terms():
    db = _db()
    try:
        ensure_academic_terms_table(db)
        cur = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None
        # Fetch terms list
        if sid:
            try:
                cur.execute("SELECT * FROM academic_terms WHERE school_id=%s ORDER BY year DESC, term ASC", (sid,))
            except Exception:
                cur.execute("SELECT * FROM academic_terms ORDER BY year DESC, term ASC")
        else:
            cur.execute("SELECT * FROM academic_terms ORDER BY year DESC, term ASC")
        terms = cur.fetchall()
        # Determine current (year, term)
        y, t = get_or_seed_current_term(db)

        # Dashboard summary cards to satisfy admin.html context
        # Total students
        if sid:
            cur.execute("SELECT COUNT(*) AS c FROM students WHERE school_id=%s", (sid,))
        else:
            cur.execute("SELECT COUNT(*) AS c FROM students")
        total_students = (cur.fetchone() or {}).get("c", 0)

        # Total collected (current term only)
        if sid:
            try:
                cur.execute(
                    "SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE school_id=%s AND year=%s AND term=%s",
                    (sid, y, t),
                )
            except Exception:
                cur.execute("SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE year=%s AND term=%s", (y, t))
        else:
            cur.execute("SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE year=%s AND term=%s", (y, t))
        total_collected = float((cur.fetchone() or {}).get("t", 0) or 0)

        # Outstanding balance (handle either balance or fee_balance column)
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        col = "balance" if has_balance else "fee_balance"
        if sid:
            cur.execute(f"SELECT COALESCE(SUM({col}),0) AS t FROM students WHERE school_id=%s", (sid,))
        else:
            cur.execute(f"SELECT COALESCE(SUM({col}),0) AS t FROM students")
        total_balance = float((cur.fetchone() or {}).get("t", 0) or 0)
    finally:
        db.close()

    # WhatsApp integration status
    try:
        from utils.whatsapp import whatsapp_is_configured
        wa_ok, wa_reason = whatsapp_is_configured()
    except Exception:
        wa_ok, wa_reason = False, None

    return render_template(
        "admin.html",
        now=datetime.now(),
        total_students=total_students,
        total_collected=total_collected,
        total_balance=total_balance,
        whatsapp_ok=wa_ok,
        whatsapp_reason=wa_reason,
        terms=terms,
        current_term=t,
        current_year=y,
    )


@term_bp.route("/start_new_year", methods=["POST"])
def start_new_year():
    """Seed the next academic year and create enrollment rows for all students.

    - Seeds 3 standard terms for the next year if missing.
    - Carries forward each student's current balance as opening_balance.
    - Records class_name snapshot for the new year.
    """
    db = _db()
    try:
        ensure_academic_terms_table(db)
        ensure_student_enrollments_table(db)
        cur = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None

        # Determine current and next year
        current_year, _ = get_or_seed_current_term(db)
        next_year = current_year + 1

        # Seed next year's terms if not present for this school
        if sid:
            try:
                cur.execute("SELECT COUNT(*) AS c FROM academic_terms WHERE year=%s AND school_id=%s", (next_year, sid))
            except Exception:
                cur.execute("SELECT COUNT(*) AS c FROM academic_terms WHERE year=%s", (next_year,))
        else:
            cur.execute("SELECT COUNT(*) AS c FROM academic_terms WHERE year=%s", (next_year,))
        has_terms = (cur.fetchone() or {}).get("c", 0) > 0
        if not has_terms:
            seed = [
                (next_year, 1, "Term 1", date(next_year, 1, 3), date(next_year, 4, 15)),
                (next_year, 2, "Term 2", date(next_year, 5, 5), date(next_year, 8, 15)),
                (next_year, 3, "Term 3", date(next_year, 9, 1), date(next_year, 11, 30)),
            ]
            cur2 = db.cursor()
            for yy, t, lbl, s, e in seed:
                if sid:
                    try:
                        cur2.execute(
                            "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current, school_id) VALUES (%s,%s,%s,%s,%s,0,%s)",
                            (yy, t, lbl, s, e, sid),
                        )
                    except Exception:
                        cur2.execute(
                            "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current) VALUES (%s,%s,%s,%s,%s,0)",
                            (yy, t, lbl, s, e),
                        )
                else:
                    cur2.execute(
                        "INSERT IGNORE INTO academic_terms (year, term, label, start_date, end_date, is_current) VALUES (%s,%s,%s,%s,%s,0)",
                        (yy, t, lbl, s, e),
                    )
            db.commit()

        # Detect balance column
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        # Create enrollment for all existing students if missing (scoped to school)
        # Fetch retention flags to decide promotion per student
        try:
            cur.execute("SHOW COLUMNS FROM students LIKE 'retain_next_year'")
            has_retain = bool(cur.fetchone())
        except Exception:
            has_retain = False

        sel_cols = "id, class_name, COALESCE(" + bal_col + ",0) AS bal"
        if has_retain:
            sel_cols += ", retain_next_year"
        if sid:
            cur.execute(f"SELECT {sel_cols} FROM students WHERE school_id=%s", (sid,))
        else:
            cur.execute(f"SELECT {sel_cols} FROM students")
        students = cur.fetchall() or []
        created = 0
        cur2 = db.cursor()
        for s in students:
            current_class = (s.get("class_name") or "").strip()
            retained = bool(s.get("retain_next_year")) if isinstance(s, dict) else False
            next_class = current_class if retained else promote_class_name(current_class)
            if sid:
                try:
                    cur2.execute(
                        "SELECT id FROM student_enrollments WHERE student_id=%s AND year=%s AND school_id=%s",
                        (s["id"], next_year, sid),
                    )
                except Exception:
                    cur2.execute(
                        "SELECT id FROM student_enrollments WHERE student_id=%s AND year=%s",
                        (s["id"], next_year),
                    )
            else:
                cur2.execute(
                    "SELECT id FROM student_enrollments WHERE student_id=%s AND year=%s",
                    (s["id"], next_year),
                )
            if cur2.fetchone():
                continue
            status_val = "retained" if retained else "active"
            if sid:
                try:
                    cur2.execute(
                        "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status, school_id) VALUES (%s,%s,%s,%s,%s,%s)",
                        (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), status_val, sid),
                    )
                except Exception:
                    cur2.execute(
                        "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status) VALUES (%s,%s,%s,%s,%s)",
                        (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), status_val),
                    )
            else:
                cur2.execute(
                    "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status) VALUES (%s,%s,%s,%s,%s)",
                    (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), status_val),
                )
            created += 1

            # Update the student's current class to the promoted one when applicable
            if (not retained) and next_class and next_class != current_class:
                try:
                    if sid:
                        cur2.execute("UPDATE students SET class_name=%s WHERE id=%s AND school_id=%s", (next_class, s["id"], sid))
                    else:
                        cur2.execute("UPDATE students SET class_name=%s WHERE id=%s", (next_class, s["id"]))
                except Exception:
                    pass
            # Reset one-time retention flag after rollover
            if retained:
                try:
                    if sid:
                        cur2.execute("UPDATE students SET retain_next_year=0 WHERE id=%s AND school_id=%s", (s["id"], sid))
                    else:
                        cur2.execute("UPDATE students SET retain_next_year=0 WHERE id=%s", (s["id"]))
                except Exception:
                    pass
        db.commit()
        flash(f"Prepared {next_year}: seeded terms, promoted classes, and created {created} enrollments.", "success")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error starting new year: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_terms"))


@term_bp.route("/set_current", methods=["POST"])
def set_current():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    if not year or term not in (1, 2, 3):
        flash("Provide a valid year and term (1-3).", "warning")
        return redirect(url_for("terms.manage_terms"))

    db = _db()
    try:
        ensure_academic_terms_table(db)
        cur = db.cursor()
        sid = session.get("school_id") if session else None
        # Enforce term state machine: only one OPEN; moving sets status
        if sid:
            try:
                cur.execute("UPDATE academic_terms SET is_current=0 WHERE school_id=%s", (sid,))
                cur.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s AND school_id=%s", (year, term, sid))
                # Upsert if the row does not exist for this school
                if cur.rowcount == 0:
                    try:
                        cur.execute(
                            "INSERT INTO academic_terms (year, term, label, is_current, school_id) VALUES (%s,%s,%s,1,%s)",
                            (year, term, f"Term {term}", sid),
                        )
                    except Exception:
                        cur.execute(
                            "INSERT IGNORE INTO academic_terms (year, term, is_current) VALUES (%s,%s,1)",
                            (year, term),
                        )
            except Exception:
                cur.execute("UPDATE academic_terms SET is_current=0")
                cur.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s", (year, term))
        else:
            cur.execute("UPDATE academic_terms SET is_current=0")
            cur.execute("UPDATE academic_terms SET is_current=1 WHERE year=%s AND term=%s", (year, term))
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT IGNORE INTO academic_terms (year, term, is_current) VALUES (%s,%s,1)",
                    (year, term),
                )
        db.commit()
        # Persist explicit user selection to per-school settings (used as authoritative source)
        try:
            if sid:
                set_school_setting("CURRENT_YEAR", str(year), school_id=sid)
                set_school_setting("CURRENT_TERM", str(term), school_id=sid)
        except Exception:
            # Non-fatal; DB marks still apply
            pass
        # Compose premium drafts for newsletter + memo
        try:
            _auto_compose_term_comms(db, year, term)
        except Exception:
            pass
        flash(f"Set current term to {year} - Term {term}.", "success")
    except Exception as e:
        db.rollback()
        flash(f"Error setting current term: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_terms"))


    # Note: Duplicate '/current' route definition removed below to avoid
    # endpoint collision (terms.current_term_api).


@term_bp.route("/open", methods=["POST"])
def open_term_route():
    db = _db()
    try:
        ensure_academic_terms_table(db)
        cur = db.cursor()
        sid = session.get("school_id") if session else None
        year = request.form.get("year", type=int)
        term = request.form.get("term", type=int)
        if not (year and term in (1, 2, 3)):
            flash("Provide a valid year and term.", "warning")
            return redirect(url_for("terms.manage_terms"))
        # ensure only one open
        if sid:
            cur.execute("SELECT COUNT(*) FROM academic_terms WHERE school_id=%s AND status='OPEN'", (sid,))
        else:
            cur.execute("SELECT COUNT(*) FROM academic_terms WHERE status='OPEN'")
        open_cnt = int((cur.fetchone() or [0])[0])
        if open_cnt > 0:
            flash("Another term is already OPEN. Close it first.", "warning")
            return redirect(url_for("terms.manage_terms"))
        # Flip status and timestamp and mark as current. Allow transition from any status.
        if sid:
            # Clear current for this school and set target as OPEN + current
            try:
                cur.execute("UPDATE academic_terms SET is_current=0 WHERE school_id=%s", (sid,))
            except Exception:
                cur.execute("UPDATE academic_terms SET is_current=0")
            cur.execute(
                "UPDATE academic_terms SET status='OPEN', opens_at=NOW(), is_current=1 WHERE year=%s AND term=%s AND school_id=%s",
                (year, term, sid),
            )
            if cur.rowcount == 0:
                # Upsert if missing
                try:
                    cur.execute(
                        "INSERT INTO academic_terms (year, term, label, start_date, end_date, status, opens_at, is_current, school_id) VALUES (%s,%s,%s,NULL,NULL,'OPEN',NOW(),1,%s)",
                        (year, term, f"Term {term}", sid),
                    )
                except Exception:
                    cur.execute(
                        "INSERT IGNORE INTO academic_terms (year, term, status, opens_at, is_current) VALUES (%s,%s,'OPEN',NOW(),1)",
                        (year, term),
                    )
        else:
            cur.execute("UPDATE academic_terms SET is_current=0")
            cur.execute(
                "UPDATE academic_terms SET status='OPEN', opens_at=NOW(), is_current=1 WHERE year=%s AND term=%s",
                (year, term),
            )
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT IGNORE INTO academic_terms (year, term, status, opens_at, is_current) VALUES (%s,%s,'OPEN',NOW(),1)",
                    (year, term),
                )
        db.commit()
        # Audit removed
        # Auto-compose communications drafts for the opened term
        try:
            _auto_compose_term_comms(db, year, term)
        except Exception:
            pass
        try:
            _publish_term_event(db, year, term, "open", {"timestamp": datetime.utcnow()})
        except Exception:
            pass
        flash("Term opened.", "success")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error opening term: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_terms"))


@term_bp.route("/close", methods=["POST"])
def close_term_route():
    db = _db()
    try:
        ensure_academic_terms_table(db)
        cur = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None
        year = request.form.get("year", type=int)
        term = request.form.get("term", type=int)
        if not (year and term in (1, 2, 3)):
            flash("Provide a valid year and term.", "warning")
            return redirect(url_for("terms.manage_terms"))
        # Ensure current status is OPEN
        if sid:
            cur.execute("SELECT id FROM academic_terms WHERE year=%s AND term=%s AND school_id=%s AND status='OPEN'", (year, term, sid))
        else:
            cur.execute("SELECT id FROM academic_terms WHERE year=%s AND term=%s AND status='OPEN'", (year, term))
        row = cur.fetchone()
        if not row:
            flash("Only an OPEN term can be closed.", "warning")
            return redirect(url_for("terms.manage_terms"))
        # Close term and clear is_current if it was current
        if sid:
            cur.execute("UPDATE academic_terms SET status='CLOSED', closes_at=NOW(), is_current=IF(is_current=1,0,is_current) WHERE id=%s AND school_id=%s", (row.get('id'), sid))
        else:
            cur.execute("UPDATE academic_terms SET status='CLOSED', closes_at=NOW(), is_current=IF(is_current=1,0,is_current) WHERE id=%s", (row.get('id'),))
        db.commit()
        # Audit removed
        flash("Term closed.", "success")
        try:
            _publish_term_event(db, year, term, "close", {"timestamp": datetime.utcnow()})
        except Exception:
            pass
        # TODO: apply rollover credits into next term (future enhancement)
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error closing term: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_terms"))


def ensure_term_fees_table(conn) -> None:
    cur = conn.cursor()
    # Base table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS term_fees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            fee_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
            -- Extended fields to support real-world adjustments/discounts
            initial_fee DECIMAL(12,2) NULL,
            adjusted_fee DECIMAL(12,2) NULL,
            discount DECIMAL(12,2) NULL,
            final_fee DECIMAL(12,2) NULL,
            school_id INT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_student_year_term (student_id, year, term),
            KEY idx_year_term (year, term)
        )
        """
    )
    conn.commit()

    # Make sure new columns exist on legacy databases
    def _ensure_col(name: str, ddl: str):
        try:
            cur.execute(f"SHOW COLUMNS FROM term_fees LIKE '{name}'")
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE term_fees ADD COLUMN {ddl}")
                conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    _ensure_col("initial_fee", "initial_fee DECIMAL(12,2) NULL AFTER fee_amount")
    _ensure_col("adjusted_fee", "adjusted_fee DECIMAL(12,2) NULL AFTER initial_fee")
    _ensure_col("discount", "discount DECIMAL(12,2) NULL AFTER adjusted_fee")
    _ensure_col("final_fee", "final_fee DECIMAL(12,2) NULL AFTER discount")
    _ensure_col("school_id", "school_id INT NULL AFTER final_fee")


@term_bp.route("/fees")
def manage_term_fees():
    """Admin page to set per-student fee for a specific term.

    - Defaults to the current academic year/term.
    - Shows students with their current balance/credit and any existing term fee.
    """
    db = _db()
    try:
        ensure_academic_terms_table(db)
        ensure_term_fees_table(db)
        pro = is_pro_enabled()
        if pro:
            ensure_fee_components_table(db)
            ensure_class_fee_defaults_table(db)
            ensure_student_fee_items_table(db)
            ensure_discounts_table(db)
        y, t = get_or_seed_current_term(db)
        qy = (int((request.args.get("year") or y)))
        qt = (int((request.args.get("term") or t or 0)) or None)

        cur = db.cursor(dictionary=True)
        # Balance column detection
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        # Students (scoped to school)
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute(
                f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE school_id=%s ORDER BY name ASC",
                (sid,),
            )
        else:
            cur.execute(
                f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students ORDER BY name ASC"
            )
        students = cur.fetchall() or []
        student_ids = [s["id"] for s in students]

        components = []
        comp_name_map = {}
        if pro:
            # Components (catalog)
            cur.execute("SELECT id, name, code, default_amount, is_optional FROM fee_components ORDER BY name ASC")
            components = cur.fetchall() or []
            comp_name_map = {c["id"]: c.get("name") for c in components}

        # Distinct classes (scoped to school)
        if sid:
            cur.execute("SELECT DISTINCT class_name FROM students WHERE school_id=%s AND class_name IS NOT NULL AND class_name <> '' ORDER BY class_name", (sid,))
        else:
            cur.execute("SELECT DISTINCT class_name FROM students WHERE class_name IS NOT NULL AND class_name <> '' ORDER BY class_name")
        classes = [r["class_name"] for r in (cur.fetchall() or [])]

        class_defaults = {}
        if pro:
            # Class defaults for selected term
            cur.execute(
                "SELECT class_name, component_id, amount FROM class_fee_defaults WHERE year=%s AND term=%s",
                (qy, qt),
            )
            class_defaults_rows = cur.fetchall() or []
            for r in class_defaults_rows:
                class_defaults.setdefault(r["class_name"], {})[r["component_id"]] = float(r.get("amount") or 0)

        # Student overrides/items for selected term
        items_map = {}
        if is_pro_enabled() and student_ids:
            ph = ",".join(["%s"] * len(student_ids))
            cur.execute(
                f"SELECT student_id, component_id, amount FROM student_term_fee_items WHERE year=%s AND term=%s AND student_id IN ({ph})",
                (qy, qt, *student_ids),
            )
            for r in (cur.fetchall() or []):
                items_map.setdefault(r["student_id"], {})[r["component_id"]] = float(r.get("amount") or 0)

        # Student discounts for selected term
        discount_map = {}
        if is_pro_enabled() and student_ids:
            ph = ",".join(["%s"] * len(student_ids))
            cur.execute(
                f"SELECT student_id, kind, value FROM discounts WHERE year=%s AND term=%s AND student_id IN ({ph})",
                (qy, qt, *student_ids),
            )
            for r in (cur.fetchall() or []):
                discount_map[r["student_id"]] = {"kind": r.get("kind"), "value": float(r.get("value") or 0)}

        # Legacy flat term fees (fallback). Prefer stored final_fee when present
        if student_ids:
            ph = ",".join(["%s"] * len(student_ids))
            cur.execute(
                f"SELECT student_id, COALESCE(final_fee, fee_amount) AS fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
                (qy, qt, *student_ids),
            )
            legacy_map = {r["student_id"]: float(r.get("fee_amount") or 0) for r in (cur.fetchall() or [])}
        else:
            legacy_map = {}

        # Compute totals for each student
        comp_ids = [c["id"] for c in components]
        comp_defaults_global = {c["id"]: float(c.get("default_amount") or 0) for c in components} if components else {}
        for s in students:
            sid = s["id"]
            klass = s.get("class_name")
            per_comp = {}
            computed_total = 0.0
            disc = None
            if is_pro_enabled() and components:
                # Aggregate per component: class default -> override -> fallback to global default
                total = 0.0
                for cid in comp_ids:
                    amount = comp_defaults_global.get(cid, 0.0)
                    if klass and klass in class_defaults and cid in class_defaults[klass]:
                        amount = class_defaults[klass][cid]
                    if sid in items_map and cid in items_map[sid]:
                        amount = items_map[sid][cid]
                    per_comp[cid] = amount
                    total += amount
                disc = discount_map.get(sid)
                discount_val = 0.0
                if disc:
                    if disc.get("kind") == "percent":
                        discount_val = round(total * (disc.get("value", 0.0) / 100.0), 2)
                    else:
                        discount_val = float(disc.get("value") or 0.0)
                computed_total = max(total - discount_val, 0.0)
            s["computed_components"] = per_comp
            s["computed_total"] = computed_total
            s["discount"] = disc
            s["legacy_fee"] = legacy_map.get(sid)

    finally:
        db.close()

    return render_template(
        "term_fees.html",
        year=qy,
        term=qt,
        students=students,
        components=components,
        classes=classes,
        class_defaults=class_defaults,
        comp_name_map=comp_name_map,
        is_pro=is_pro_enabled(),
        upgrade_link=upgrade_url(),
    )


@term_bp.route("/fees/set", methods=["POST"])
def set_term_fee():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    student_id = request.form.get("student_id", type=int)
    fee_amount = request.form.get("fee_amount", type=float)

    if not (year and term in (1,2,3) and student_id and fee_amount is not None and fee_amount >= 0):
        flash("Provide valid year, term (1-3), student and non-negative fee amount.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year or "", term=term or ""))

    db = _db()
    try:
        ensure_term_fees_table(db)
        bal_col = _detect_balance_column(db)
        result = _apply_term_fee_amount(
            db,
            student_id,
            year,
            term,
            fee_amount,
            bal_col,
            session.get("school_id") if session else None,
        )
        db.commit()
        if result.get("discount_exceeded"):
            flash("Discount exceeds adjusted/initial fee; final set to 0.", "warning")
        delta = result.get("delta", 0.0)
        if result.get("is_adjustment"):
            flash(f"Adjusted fee saved. Final: KES {result.get('new_final', 0):,.2f}. Balance delta: KES {delta:,.2f}", "success")
        else:
            flash(f"Initial fee saved. Final: KES {result.get('new_final', 0):,.2f}. Applied to balance.", "success")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error saving term fee: {e}", "error")
        # After saving, attempt to email term fee structure automatically
        try:
            _conn_email = _db()
            cur = _conn_email.cursor(dictionary=True)
            # Resolve recipient email column
            email_col = _resolve_email_column(cur)
            if email_col:
                # Fetch student profile including class and email
                sid = session.get("school_id") if session else None
                if sid:
                    cur.execute(
                        f"SELECT id, name, class_name, {email_col} AS email FROM students WHERE id=%s AND school_id=%s",
                        (student_id, sid),
                    )
                else:
                    cur.execute(
                        f"SELECT id, name, class_name, {email_col} AS email FROM students WHERE id=%s",
                        (student_id,),
                    )
                srow = cur.fetchone() or None
                to_addr = (srow.get("email") or "").strip() if srow else ""
                if to_addr:
                    # Build fee structure: prefer itemized components; fallback to final flat fee
                    items = []
                    total_due = 0.0
                    try:
                        cur.execute(
                            """
                            SELECT fc.name AS description, si.amount
                            FROM student_term_fee_items si
                            JOIN fee_components fc ON fc.id = si.component_id
                            WHERE si.student_id=%s AND si.year=%s AND si.term=%s
                            ORDER BY fc.name ASC
                            """,
                            (student_id, year, term),
                        )
                        items = cur.fetchall() or []
                        total_due = float(sum([float(i.get("amount") or 0) for i in items]))
                    except Exception:
                        items = []
                    if not items:
                        # Fallback to legacy flat fee
                        total_due = float(fee_amount or 0.0)
                        items = [{"description": "Term Fee", "amount": total_due}]
                    # Try to fetch due date from invoice if present
                    due_str = None
                    try:
                        cur.execute(
                            "SELECT due_date FROM invoices WHERE student_id=%s AND year=%s AND term=%s",
                            (student_id, year, term),
                        )
                        inv = cur.fetchone()
                        if inv and inv.get("due_date"):
                            due_str = str(inv.get("due_date"))
                    except Exception:
                        pass
                    subject = f"{(get_setting('SCHOOL_NAME') or 'School')} Term {term} Memo - {year}"
                    html_body = render_template(
                        "email_term_memo.html",
                        brand=(get_setting("SCHOOL_NAME") or "School"),
                        student_name=srow.get("name") if srow else "Student",
                        class_name=srow.get("class_name") if srow else None,
                        year=year,
                        term=term,
                        due_date=due_str,
                        amount_due=total_due,
                        fee_items=items,
                    )
                    try:
                        gmail_send_email_html(to_addr, subject, html_body)
                    except Exception:
                        pass
            try:
                _conn_email.close()
            except Exception:
                pass
        except Exception:
            # Ignore email errors to keep UX smooth
            pass
    finally:
        db.close()

    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/import", methods=["POST"])
def import_term_fees():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    csv_file = request.files.get("tuition_csv")
    if not (year and term in (1, 2, 3)):
        flash("Provide a valid year and term before importing.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year or "", term=term or ""))
    if not csv_file or not csv_file.filename:
        flash("Please upload a CSV file with tuition rows.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))

    try:
        decoded = csv_file.read().decode("utf-8-sig")
    except Exception:
        flash("Unable to read the uploaded file. Ensure it is a UTF-8 encoded CSV.", "error")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))

    try:
        reader = csv.DictReader(StringIO(decoded))
    except csv.Error as exc:
        flash(f"Invalid CSV format: {exc}", "error")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))

    if not reader.fieldnames:
        flash("The CSV file must include a header row.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))

    school_id = session.get("school_id") if session else None
    db = _db()
    imported = 0
    errors: list[str] = []
    try:
        ensure_term_fees_table(db)
        bal_col = _detect_balance_column(db)
        lookup_cur = db.cursor(dictionary=True)
        try:
            def _extract_amount(row):
                for key in ("amount", "fee", "tuition", "tuition_fee", "term_fee"):
                    raw = (row.get(key) or "").strip()
                    if not raw:
                        continue
                    try:
                        return float(raw)
                    except ValueError:
                        continue
                return None

            def _find_student_id(row):
                for key in ("student_id", "id"):
                    raw = (row.get(key) or "").strip()
                    if not raw:
                        continue
                    try:
                        candidate = int(raw)
                    except ValueError:
                        continue
                    query = "SELECT id FROM students WHERE id=%s"
                    params = [candidate]
                    if school_id is not None:
                        query += " AND school_id=%s"
                        params.append(school_id)
                    lookup_cur.execute(query, tuple(params))
                    found = lookup_cur.fetchone()
                    if found:
                        return found["id"]
                for key in ("reg_no", "regNo", "admission_no"):
                    raw = (row.get(key) or "").strip()
                    if not raw:
                        continue
                    query = "SELECT id FROM students WHERE admission_no=%s"
                    params = [raw]
                    if school_id is not None:
                        query += " AND school_id=%s"
                        params.append(school_id)
                    lookup_cur.execute(query, tuple(params))
                    found = lookup_cur.fetchone()
                    if found:
                        return found["id"]
                return None

            has_data = any((col.strip() for col in reader.fieldnames if col))
            if not has_data:
                flash("CSV header row appears empty.", "warning")
                return redirect(url_for("terms.manage_term_fees", year=year, term=term))

            for idx, row in enumerate(reader, start=1):
                if not any(str(v or "").strip() for v in row.values()):
                    continue
                amount = _extract_amount(row)
                if amount is None or amount < 0:
                    errors.append(f"Row {idx}: invalid fee amount.")
                    continue
                student_id = _find_student_id(row)
                if not student_id:
                    errors.append(f"Row {idx}: student not found.")
                    continue
                try:
                    _apply_term_fee_amount(db, student_id, year, term, amount, bal_col, school_id)
                    imported += 1
                except Exception as exc:
                    errors.append(f"Row {idx}: {exc}")
        finally:
            try:
                lookup_cur.close()
            except Exception:
                pass

        db.commit()
        summary = f"Imported tuition fees for {imported} student(s)."
        level = "success"
        if errors:
            note = "; ".join(errors[:3])
            if len(errors) > 3:
                note += " …"
            summary += f" Skipped {len(errors)} row(s): {note}"
            level = "warning" if imported else "error"
        flash(summary, level)
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Failed to import tuition fees: {exc}", "error")
    finally:
        try:
            db.close()
        except Exception:
            pass
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/memo/<int:student_id>/<int:year>/<int:term>.pdf")
def term_memo_pdf(student_id: int, year: int, term: int):
    """Generate a modern PDF memo for a student's term fee, including structure.

    Produces a branded PDF similar to the HTML email, with an itemized
    fee structure when available, and a total due card.
    """
    db = _db()
    try:
        cur = db.cursor(dictionary=True)
        sid = session.get("school_id") if session else None
        # Student profile
        if sid:
            cur.execute(
                "SELECT id, name, class_name FROM students WHERE id=%s AND school_id=%s",
                (student_id, sid),
            )
        else:
            cur.execute("SELECT id, name, class_name FROM students WHERE id=%s", (student_id,))
        srow = cur.fetchone()
        if not srow:
            flash("Student not found.", "error")
            return redirect(url_for("terms.manage_term_fees", year=year, term=term))

        # Try to fetch itemized components
        items = []
        try:
            cur.execute(
                """
                SELECT fc.name AS description, si.amount
                FROM student_term_fee_items si
                JOIN fee_components fc ON fc.id = si.component_id
                WHERE si.student_id=%s AND si.year=%s AND si.term=%s
                ORDER BY fc.name ASC
                """,
                (student_id, year, term),
            )
            items = cur.fetchall() or []
        except Exception:
            items = []
        # Total due and fallback
        total_due = 0.0
        if items:
            total_due = float(sum([float(i.get("amount") or 0) for i in items]))
        else:
            try:
                cur.execute(
                    "SELECT COALESCE(final_fee, fee_amount) AS fee FROM term_fees WHERE student_id=%s AND year=%s AND term=%s",
                    (student_id, year, term),
                )
                row = cur.fetchone() or {"fee": 0}
                total_due = float(row.get("fee") or 0)
                items = [{"description": "Term Fee", "amount": total_due}]
            except Exception:
                total_due = 0.0
                items = []

        # Due date from invoice if available
        due_str = None
        try:
            cur.execute(
                "SELECT due_date FROM invoices WHERE student_id=%s AND year=%s AND term=%s",
                (student_id, year, term),
            )
            inv = cur.fetchone()
            if inv and inv.get("due_date"):
                due_str = str(inv.get("due_date"))
        except Exception:
            pass

        # School branding
        school_name = (
            get_setting("SCHOOL_NAME")
            or get_setting("APP_NAME")
            or current_app.config.get("APP_NAME")
            or current_app.config.get("BRAND_NAME")
            or get_setting("BRAND_NAME")
            or "School"
        )
        school_address = get_setting("SCHOOL_ADDRESS") or ""
        school_phone = get_setting("SCHOOL_PHONE") or ""
        school_email = get_setting("SCHOOL_EMAIL") or ""

        # Build PDF
        from io import BytesIO
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.pdfgen import canvas

        buf = BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        width, height = A4
        x_margin = 18 * mm
        content_gap = 7 * mm

        brand_indigo = colors.HexColor("#4338ca")
        brand_cyan = colors.HexColor("#06b6d4")
        light_bg = colors.HexColor("#eef2ff")
        soft_border = colors.HexColor("#e2e8f0")

        # Header bar
        header_h = 32 * mm
        c.setFillColor(brand_indigo)
        c.rect(0, height - header_h, width, header_h, fill=1, stroke=0)

        # Header text and badge
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(x_margin, height - 12 * mm, str(school_name))
        c.setFont("Helvetica", 10)
        sub = " ".join(filter(None, [school_address, school_phone, school_email]))
        c.setFillColor(colors.whitesmoke)
        c.drawString(x_margin, height - 17 * mm, sub)
        # Badge
        badge_w, badge_h = 30 * mm, 10 * mm
        badge_x = width - x_margin - badge_w
        badge_y = height - 15 * mm - (badge_h / 2)
        c.setFillColor(colors.white)
        c.roundRect(badge_x, badge_y, badge_w, badge_h, 3 * mm, fill=1, stroke=0)
        c.setFillColor(brand_indigo)
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(badge_x + badge_w / 2, badge_y + 3.5 * mm, "TERM MEMO")

        # Content origin
        y = height - header_h - 14 * mm

        # Amount card
        card_h = 18 * mm
        card_y = y - card_h
        c.setFillColor(light_bg)
        c.setStrokeColor(soft_border)
        c.roundRect(x_margin, card_y, width - 2 * x_margin, card_h, 4 * mm, fill=1, stroke=0)
        c.setFont("Helvetica", 10)
        c.setFillColor(colors.HexColor("#64748b"))
        c.drawCentredString(width / 2, card_y + card_h - 6.5 * mm, "Total Due This Term")
        c.setFillColor(colors.HexColor("#0f172a"))
        c.setFont("Helvetica-Bold", 20)
        c.drawCentredString(width / 2, card_y + 6 * mm, f"KES {float(total_due):,.2f}")
        y = card_y - content_gap

        # Key-value rows function
        def draw_kv(label: str, value: str):
            nonlocal y
            c.setFont("Helvetica", 10)
            c.setFillColor(colors.HexColor("#64748b"))
            c.drawString(x_margin, y, label)
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 11)
            c.drawRightString(width - x_margin, y, value)
            y -= 7 * mm

        draw_kv("Student", str(srow.get("name") or ""))
        draw_kv("Class", str(srow.get("class_name") or "N/A"))
        draw_kv("Year / Term", f"{year} / {term}")
        if due_str:
            draw_kv("Due Date", str(due_str))

        # Separator
        c.setStrokeColor(colors.lightgrey)
        c.setDash(1, 2)
        c.line(x_margin, y, width - x_margin, y)
        c.setDash()
        y -= 6 * mm

        # Fee structure table
        c.setFont("Helvetica-Bold", 12)
        c.setFillColor(colors.HexColor("#0f172a"))
        c.drawString(x_margin, y, "Fee Structure")
        y -= 7 * mm
        c.setFont("Helvetica", 10)
        for it in items:
            if y < 30 * mm:
                c.showPage()
                y = height - 20 * mm
                c.setFont("Helvetica", 10)
            desc = str(it.get("description") or "Item")
            amt = float(it.get("amount") or 0)
            c.setFillColor(colors.HexColor("#0f172a"))
            c.drawString(x_margin, y, desc)
            c.drawRightString(width - x_margin, y, f"KES {amt:,.2f}")
            y -= 6 * mm

        # Footer note
        y = max(y, 24 * mm)
        c.setFillColor(colors.HexColor("#64748b"))
        c.setFont("Helvetica", 10)
        c.drawCentredString(width / 2, y, "Kindly review and arrange payment by the due date.")

        c.showPage()
        c.save()
        pdf_bytes = buf.getvalue()
        buf.close()
        from flask import Response
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=term_memo_{student_id}_{year}_T{term}.pdf",
            },
        )
    finally:
        try:
            db.close()
        except Exception:
            pass


# ------------------- Premium: Components, Defaults, Discounts, Invoices -------------------

def ensure_fee_components_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fee_components (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            code VARCHAR(50) UNIQUE,
            default_amount DECIMAL(12,2) DEFAULT 0,
            is_optional TINYINT(1) DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def ensure_class_fee_defaults_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS class_fee_defaults (
            id INT AUTO_INCREMENT PRIMARY KEY,
            class_name VARCHAR(50) NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            component_id INT NOT NULL,
            amount DECIMAL(12,2) NOT NULL DEFAULT 0,
            UNIQUE KEY uq_class_term_component (class_name, year, term, component_id)
        )
        """
    )
    conn.commit()


def ensure_student_fee_items_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS student_term_fee_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            component_id INT NOT NULL,
            amount DECIMAL(12,2) NOT NULL DEFAULT 0,
            UNIQUE KEY uq_student_term_component (student_id, year, term, component_id)
        )
        """
    )
    conn.commit()


def ensure_discounts_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS discounts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            kind ENUM('amount','percent') NOT NULL,
            value DECIMAL(12,2) NOT NULL DEFAULT 0,
            reason VARCHAR(255),
            UNIQUE KEY uq_student_term (student_id, year, term)
        )
        """
    )
    conn.commit()


def ensure_invoices_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            due_date DATE,
            status ENUM('draft','sent','partial','paid','void') DEFAULT 'draft',
            total DECIMAL(12,2) DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_student_term (student_id, year, term)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            invoice_id INT NOT NULL,
            description VARCHAR(200) NOT NULL,
            component_id INT NULL,
            amount DECIMAL(12,2) NOT NULL,
            INDEX idx_invoice (invoice_id)
        )
        """
    )
    conn.commit()


@term_bp.route("/invoices")
def invoices_list():
    db = _db()
    try:
        ensure_invoices_tables(db)
        # Auto-refresh: keep invoices in sync on page load.
        try:
            y_auto, t_auto = get_or_seed_current_term(db)
            year_auto = request.args.get("year", type=int) or y_auto
            term_auto = request.args.get("term", type=int) or t_auto
            _generate_or_update_invoices(db, year_auto, term_auto)
        except Exception as _e:
            try:
                db.rollback()
            except Exception:
                pass
            flash(f"Invoice auto-refresh skipped: {_e}", "warning")
        # Resolve year/term defaults
        y, t = get_or_seed_current_term(db)
        year = request.args.get("year", type=int) or y
        term = request.args.get("term", type=int) or t

        # Optional search across invoice id, student name, class
        q = (request.args.get("q") or "").strip()

        cur = db.cursor(dictionary=True)
        base_sql = (
            """
            SELECT i.id, i.student_id, i.year, i.term, i.due_date, i.status, i.total,
                   s.name AS student_name, s.class_name
            FROM invoices i
            JOIN students s ON s.id = i.student_id
            WHERE i.year = %s AND i.term = %s
            """
        )
        params = [year, term]
        # Scope to current school when available
        sid = session.get("school_id") if session else None
        if sid:
            base_sql += " AND s.school_id = %s"
            params.append(sid)
        if q:
            like = f"%{q}%"
            base_sql += " AND (s.name LIKE %s OR s.class_name LIKE %s OR CAST(i.id AS CHAR) LIKE %s)"
            params.extend([like, like, like])
        base_sql += " ORDER BY s.name ASC"

        cur.execute(base_sql, params)
        rows = cur.fetchall() or []
        return render_template("invoices.html", year=year, term=term, q=q, invoices=rows)
    finally:
        db.close()


@term_bp.route("/invoices/<int:invoice_id>")
def invoice_view(invoice_id: int):
    db = _db()
    try:
        ensure_invoices_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            """
            SELECT i.*, s.name AS student_name, s.class_name
            FROM invoices i
            JOIN students s ON s.id = i.student_id
            WHERE i.id = %s
            """,
            (invoice_id,),
        )
        invoice = cur.fetchone()
        # Enforce school scoping if applicable
        sid = session.get("school_id") if session else None
        if invoice and sid:
            try:
                cur.execute(
                    "SELECT 1 FROM students WHERE id=%s AND school_id=%s",
                    (invoice.get("student_id"), sid),
                )
                if not cur.fetchone():
                    invoice = None
            except Exception:
                pass
        if not invoice:
            flash("Invoice not found.", "error")
            return redirect(url_for("terms.invoices_list"))

        cur.execute(
            "SELECT description, component_id, amount FROM invoice_items WHERE invoice_id=%s ORDER BY id ASC",
            (invoice_id,),
        )
        items = cur.fetchall() or []

        # Compute late payment penalty if applicable (per-school settings)
        penalty_amount = 0.0
        penalty_label = None
        try:
            from utils.settings import get_setting
            from datetime import date as _date
            kind = (get_setting("LATE_PENALTY_KIND") or "").strip()  # 'percent' or 'flat'
            val_raw = get_setting("LATE_PENALTY_VALUE") or "0"
            grace_raw = get_setting("LATE_PENALTY_GRACE_DAYS") or "0"
            try:
                val = float(val_raw)
            except Exception:
                val = 0.0
            try:
                grace = int(float(grace_raw))
            except Exception:
                grace = 0
            due = invoice.get("due_date")
            if due:
                # if today past (due + grace)
                today = _date.today()
                try:
                    is_overdue = today > (due)
                except Exception:
                    is_overdue = today > due
                if grace and is_overdue:
                    # apply grace by shifting due
                    try:
                        from datetime import timedelta as _td
                        is_overdue = today > (due + _td(days=grace))
                    except Exception:
                        pass
                if is_overdue and val > 0:
                    base = float(invoice.get("total") or 0)
                    if kind == "percent":
                        penalty_amount = round(base * (val / 100.0), 2)
                        penalty_label = f"Late Penalty ({val:.0f}%)"
                    elif kind == "flat":
                        penalty_amount = round(val, 2)
                        penalty_label = "Late Penalty"
        except Exception:
            penalty_amount = 0.0
            penalty_label = None

        # Build signed QR payload for authenticity
        try:
            sid = session.get("school_id") if session else None
            qr_payload = {
                "t": "invoice",
                "iid": int(invoice.get("id")),
                "sid": int(invoice.get("student_id")),
                "name": invoice.get("student_name") or "",
                "cls": invoice.get("class_name") or "",
                "year": invoice.get("year") or "",
                "term": invoice.get("term") or "",
                "due": str(invoice.get("due_date") or ""),
                "status": invoice.get("status") or "",
                "total": round(float(invoice.get("total") or 0.0), 2),
                "school_id": sid,
            }
            canon = json.dumps(qr_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            from flask import current_app as _ca
            sig = hmac.new((_ca.secret_key or "secret").encode("utf-8"), canon, hashlib.sha256).hexdigest()[:20]
            qr_payload["sig"] = sig
            auth_qr_data = json.dumps(qr_payload, separators=(",", ":"))
        except Exception:
            auth_qr_data = ""

        return render_template("invoice.html", invoice=invoice, items=items, penalty_amount=penalty_amount, penalty_label=penalty_label, auth_qr_data=auth_qr_data)
    finally:
        db.close()


@term_bp.route("/fees/components", methods=["POST"])
def add_component():
    name = (request.form.get("name") or "").strip()
    code = (request.form.get("code") or "").strip() or None
    default_amount = request.form.get("default_amount", type=float) or 0.0
    is_optional = 1 if request.form.get("is_optional") else 0
    if not name:
        flash("Component name is required.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
    db = _db()
    try:
        ensure_fee_components_table(db)
        cur = db.cursor()
        cur.execute(
            "INSERT INTO fee_components (name, code, default_amount, is_optional) VALUES (%s,%s,%s,%s)",
            (name, code, default_amount, is_optional),
        )
        db.commit()
        flash("Component added.", "success")
        try:
            log_event("edit_fee_component", target=f"component:{code or name}", detail=f"Default {default_amount:.2f} ({'optional' if is_optional else 'required'})")
        except Exception:
            pass
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error adding component: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees"))


@term_bp.route("/fees/class_defaults", methods=["POST"])
def set_class_defaults():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    class_name = (request.form.get("class_name") or "").strip()
    if not (year and term in (1,2,3) and class_name):
        flash("Provide year, term and class.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))
    db = _db()
    try:
        ensure_class_fee_defaults_table(db)
        # Expect fields like comp_<id> = amount
        cur = db.cursor()
        for k, v in request.form.items():
            if not k.startswith("comp_"):
                continue
            try:
                cid = int(k.split("_", 1)[1])
            except Exception:
                continue
            amt = 0.0
            try:
                amt = float(v)
            except Exception:
                amt = 0.0
            cur.execute(
                "INSERT INTO class_fee_defaults (class_name, year, term, component_id, amount) VALUES (%s,%s,%s,%s,%s)"
                " ON DUPLICATE KEY UPDATE amount=VALUES(amount)",
                (class_name, year, term, cid, amt),
            )
        db.commit()
        flash("Class defaults saved.", "success")
        try:
            log_event("update_class_defaults", target=f"class:{class_name}", detail=f"{year} T{term} defaults applied")
        except Exception:
            pass
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving class defaults: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/student_items", methods=["POST"])
def set_student_items():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    student_id = request.form.get("student_id", type=int)
    if not (year and term in (1,2,3) and student_id):
        flash("Provide year, term and student.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))
    db = _db()
    try:
        ensure_student_fee_items_table(db)
        cur = db.cursor()
        # Remove previous items then insert new set
        cur.execute("DELETE FROM student_term_fee_items WHERE student_id=%s AND year=%s AND term=%s", (student_id, year, term))
        for k, v in request.form.items():
            if not k.startswith("comp_"):
                continue
            try:
                cid = int(k.split("_", 1)[1])
            except Exception:
                continue
            amt = 0.0
            try:
                amt = float(v)
            except Exception:
                amt = 0.0
            cur.execute(
                "INSERT INTO student_term_fee_items (student_id, year, term, component_id, amount) VALUES (%s,%s,%s,%s,%s)",
                (student_id, year, term, cid, amt),
            )
        db.commit()
        flash("Student items saved.", "success")
        try:
            log_event("update_student_items", target=f"student:{student_id}", detail=f"{year} T{term} items set")
        except Exception:
            pass
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving student items: {e}", "error")
        # After saving, attempt to email term fee structure automatically
        try:
            _conn_email = _db()
            c = _conn_email.cursor(dictionary=True)
            email_col = _resolve_email_column(c)
            if email_col:
                sid = session.get("school_id") if session else None
                if sid:
                    c.execute(
                        f"SELECT id, name, class_name, {email_col} AS email FROM students WHERE id=%s AND school_id=%s",
                        (student_id, sid),
                    )
                else:
                    c.execute(
                        f"SELECT id, name, class_name, {email_col} AS email FROM students WHERE id=%s",
                        (student_id,),
                    )
                srow = c.fetchone() or None
                to_addr = (srow.get("email") or "").strip() if srow else ""
                if to_addr:
                    # Fetch the just-saved items
                    c.execute(
                        """
                        SELECT fc.name AS description, si.amount
                        FROM student_term_fee_items si
                        JOIN fee_components fc ON fc.id = si.component_id
                        WHERE si.student_id=%s AND si.year=%s AND si.term=%s
                        ORDER BY fc.name ASC
                        """,
                        (student_id, year, term),
                    )
                    items = c.fetchall() or []
                    total_due = float(sum([float(i.get("amount") or 0) for i in items]))
                    subject = f"{(get_setting('SCHOOL_NAME') or 'School')} Term {term} Memo - {year}"
                    html_body = render_template(
                        "email_term_memo.html",
                        brand=(get_setting("SCHOOL_NAME") or "School"),
                        student_name=srow.get("name") if srow else "Student",
                        class_name=srow.get("class_name") if srow else None,
                        year=year,
                        term=term,
                        due_date=None,
                        amount_due=total_due,
                        fee_items=items,
                    )
                    try:
                        gmail_send_email_html(to_addr, subject, html_body)
                    except Exception:
                        pass
            try:
                _conn_email.close()
            except Exception:
                pass
        except Exception:
            pass
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/discount", methods=["POST"])
def set_discount():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    student_id = request.form.get("student_id", type=int)
    kind = request.form.get("kind")
    value = request.form.get("value", type=float)
    reason = (request.form.get("reason") or "").strip() or None
    if not (year and term in (1,2,3) and student_id and kind in ("amount","percent") and value is not None):
        flash("Provide year, term, student and valid discount.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))
    db = _db()
    try:
        ensure_discounts_table(db)
        cur = db.cursor()
        cur.execute(
            "INSERT INTO discounts (student_id, year, term, kind, value, reason) VALUES (%s,%s,%s,%s,%s,%s)"
            " ON DUPLICATE KEY UPDATE kind=VALUES(kind), value=VALUES(value), reason=VALUES(reason)",
            (student_id, year, term, kind, value, reason),
        )
        db.commit()
        flash("Discount saved.", "success")
        try:
            log_event("edit_fee_discount", target=f"student:{student_id}", detail=f"{kind} {value} applied")
        except Exception:
            pass
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving discount: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/generate_invoices", methods=["POST"])
def generate_invoices():
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    due_date_raw = request.form.get("due_date")
    send_memo = (request.form.get("send_memo") in ("1", "true", "on"))
    # Normalize due_date: allow empty -> NULL, or ensure proper date object
    due_date = None
    if due_date_raw:
        try:
            # Expecting YYYY-MM-DD from the input[type=date]
            due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
        except Exception:
            # If parsing fails, keep it NULL to avoid MySQL 1292 errors
            due_date = None
    if not (year and term in (1,2,3)):
        flash("Provide a valid year and term.", "warning")
        return redirect(url_for("terms.manage_term_fees", year=year, term=term))
    db = _db()
    try:
        created = _generate_or_update_invoices(db, year, term, due_date)
        db.commit()
        flash(f"Generated/updated {created} invoices for {year} T{term}.", "success")
        try:
            log_event("generate_invoices", target=f"term:{year}T{term}", detail=f"{created} invoices")
        except Exception:
            pass
        # Premium-only: optionally send memo emails
        if send_memo:
            sent, skipped = _send_term_memos(db, year, term, due_date)
            if sent:
                flash(f"Term memos emailed to {sent} students (skipped {skipped}).", "success")
            else:
                flash("No term memos sent (no emails found or email not configured).", "warning")
        try:
            _publish_term_event(
                db,
                year,
                term,
                "flat_fee",
                {"invoice_count": created, "due_date": due_date},
            )
        except Exception:
            pass
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error generating invoices: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))

def _generate_or_update_invoices(db, year:int, term:int, due_date=None) -> int:
    """Core invoice generation used by both the POST route and the list auto-refresh.

    Falls back to legacy flat fees (table `term_fees`) when no itemized components
    are defined for a student so invoices always have a total.
    """
    ensure_invoices_tables(db)
    ensure_fee_components_table(db)
    ensure_class_fee_defaults_table(db)
    ensure_student_fee_items_table(db)
    ensure_discounts_table(db)
    ensure_term_fees_table(db)

    # If due_date not provided, try academic term end date, else +14 days
    if not due_date:
        try:
            curd = db.cursor(dictionary=True)
            yx, tx = year, term
            sid = session.get("school_id") if session else None
            if sid:
                curd.execute(
                    "SELECT end_date FROM academic_terms WHERE year=%s AND term=%s AND (school_id=%s OR school_id IS NULL) ORDER BY school_id DESC LIMIT 1",
                    (yx, tx, sid),
                )
            else:
                curd.execute(
                    "SELECT end_date FROM academic_terms WHERE year=%s AND term=%s ORDER BY id DESC LIMIT 1",
                    (yx, tx),
                )
            r = curd.fetchone()
            if r and r.get("end_date"):
                due_date = r.get("end_date")
            else:
                from datetime import date as _d, timedelta as _td
                due_date = _d.today() + _td(days=14)
        except Exception:
            pass

    cur = db.cursor(dictionary=True)
    sid = session.get("school_id") if session else None
    if sid:
        cur.execute("SELECT id, name, class_name FROM students WHERE school_id=%s ORDER BY name ASC", (sid,))
    else:
        cur.execute("SELECT id, name, class_name FROM students ORDER BY name ASC")
    students = cur.fetchall() or []
    if not students:
        return 0

    # Component catalog
    cur.execute("SELECT id, name, default_amount FROM fee_components ORDER BY name ASC")
    comps = cur.fetchall() or []
    comp_defaults = {c["id"]: float(c.get("default_amount") or 0) for c in comps}

    # Class defaults
    cur.execute("SELECT class_name, component_id, amount FROM class_fee_defaults WHERE year=%s AND term=%s", (year, term))
    class_rows = cur.fetchall() or []
    class_defaults = {}
    for r in class_rows:
        class_defaults.setdefault(r["class_name"], {})[r["component_id"]] = float(r.get("amount") or 0)

    # Student overrides
    ids = [s["id"] for s in students]
    items_map = {}
    if ids:
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT student_id, component_id, amount FROM student_term_fee_items WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        for r in (cur.fetchall() or []):
            items_map.setdefault(r["student_id"], {})[r["component_id"]] = float(r.get("amount") or 0)

    # Discounts
    discount_map = {}
    if ids:
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT student_id, kind, value FROM discounts WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        for r in (cur.fetchall() or []):
            discount_map[r["student_id"]] = {"kind": r.get("kind"), "value": float(r.get("value") or 0)}

    # Legacy flat fees map for fallback
    fee_flat = {}
    if ids:
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT student_id, COALESCE(final_fee, fee_amount) AS fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
            (year, term, *ids),
        )
        fee_flat = {r["student_id"]: float(r.get("fee_amount") or 0) for r in (cur.fetchall() or [])}

    cur_i = db.cursor()
    created = 0
    for s in students:
        sid_s = s["id"]
        klass = s.get("class_name")

        # Compute per-component charge
        total = 0.0
        per_comp = []
        for c in comps:
            cid = c["id"]
            amt = comp_defaults.get(cid, 0.0)
            if klass and klass in class_defaults and cid in class_defaults[klass]:
                amt = class_defaults[klass][cid]
            if sid_s in items_map and cid in items_map[sid_s]:
                amt = items_map[sid_s][cid]
            if amt and amt > 0:
                per_comp.append((cid, c.get("name"), amt))
                total += amt

        # If nothing itemized, fall back to legacy flat fee for this student
        if total == 0 and fee_flat.get(sid_s, 0) > 0:
            total = fee_flat[sid_s]
            per_comp = [(None, "Term Fee", total)]

        # Apply discount if any
        disc = discount_map.get(sid_s)
        discount_val = 0.0
        if disc:
            if disc.get("kind") == "percent":
                discount_val = round(total * (disc.get("value", 0.0) / 100.0), 2)
            else:
                discount_val = float(disc.get("value") or 0.0)
        grand = max(total - discount_val, 0.0)

        # Upsert invoice
        cur_i.execute(
            "INSERT INTO invoices (student_id, year, term, due_date, status, total) VALUES (%s,%s,%s,%s,%s,%s)"
            " ON DUPLICATE KEY UPDATE due_date=VALUES(due_date), total=VALUES(total)",
            (sid_s, year, term, due_date, 'draft', grand),
        )
        inv_id = cur_i.lastrowid
        if not inv_id:
            cur.execute("SELECT id FROM invoices WHERE student_id=%s AND year=%s AND term=%s", (sid_s, year, term))
            rr = cur.fetchone()
            inv_id = rr["id"] if rr else None
        if not inv_id:
            continue

        # Reset items and insert
        cur_i.execute("DELETE FROM invoice_items WHERE invoice_id=%s", (inv_id,))
        for cid, cname, amt in per_comp:
            cur_i.execute(
                "INSERT INTO invoice_items (invoice_id, description, component_id, amount) VALUES (%s,%s,%s,%s)",
                (inv_id, cname, cid, amt),
            )
        if discount_val > 0:
            cur_i.execute(
                "INSERT INTO invoice_items (invoice_id, description, component_id, amount) VALUES (%s,%s,%s,%s)",
                (inv_id, 'Discount', None, -discount_val),
            )
        created += 1

    return created

@term_bp.route("/summary")
def term_summary():
    """Summary per student for a selected year and term.

    Carry-in = opening_balance (year) + fees of prior terms - payments of prior terms.
    Closing (to date for this term) = carry-in + this term's fee - this term's payments.
    """
    db = _db()
    try:
        ensure_academic_terms_table(db)
        ensure_term_fees_table(db)
        ensure_student_enrollments_table(db)
        y, t = get_or_seed_current_term(db)
        year = request.args.get("year", type=int) or y
        term = request.args.get("term", type=int) or t
        class_filter = (request.args.get("class") or "").strip()

        cur = db.cursor(dictionary=True)
        # Detect balance column for display consistency
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        # Base students list (optionally by class)
        sid = session.get("school_id") if session else None
        if class_filter:
            if sid:
                cur.execute(
                    f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE class_name = %s AND school_id=%s ORDER BY name ASC",
                    (class_filter, sid),
                )
            else:
                cur.execute(
                    f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE class_name = %s ORDER BY name ASC",
                    (class_filter,),
                )
        else:
            if sid:
                cur.execute(
                    f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE school_id=%s ORDER BY name ASC",
                    (sid,),
                )
            else:
                cur.execute(
                    f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit FROM students ORDER BY name ASC"
                )
        students = cur.fetchall() or []
        ids = [s["id"] for s in students]
        if not ids:
            # Also provide class list for filter dropdown
            if sid:
                cur.execute("SELECT DISTINCT class_name FROM students WHERE school_id=%s AND class_name IS NOT NULL AND class_name <> '' ORDER BY class_name", (sid,))
            else:
                cur.execute("SELECT DISTINCT class_name FROM students WHERE class_name IS NOT NULL AND class_name <> '' ORDER BY class_name")
            classes = [r["class_name"] for r in (cur.fetchall() or [])]
            return render_template("term_summary.html", year=year, term=term, rows=[], totals={}, classes=classes, class_filter=class_filter)

        # Helper to produce IN clause
        def id_in_clause(seq):
            return ",".join(["%s"] * len(seq))

        # Term fees (this term): prefer itemized if present, otherwise legacy flat
        # Itemized
        cur.execute(
            f"SELECT student_id, COALESCE(SUM(amount),0) AS tsum FROM student_term_fee_items WHERE year=%s AND term=%s AND student_id IN ({id_in_clause(ids)}) GROUP BY student_id",
            (year, term, *ids),
        )
        items_sum_map = {r["student_id"]: float(r.get("tsum") or 0) for r in (cur.fetchall() or [])}
        # Discounts
        cur.execute(
            f"SELECT student_id, kind, value FROM discounts WHERE year=%s AND term=%s AND student_id IN ({id_in_clause(ids)})",
            (year, term, *ids),
        )
        disc_map = {r["student_id"]: {"kind": r.get("kind"), "value": float(r.get("value") or 0)} for r in (cur.fetchall() or [])}
        # Legacy flat
        cur.execute(
            f"SELECT student_id, COALESCE(final_fee, fee_amount) AS fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({id_in_clause(ids)})",
            (year, term, *ids),
        )
        legacy_map = {r["student_id"]: float(r.get("fee_amount") or 0) for r in (cur.fetchall() or [])}
        # Choose effective fee per student
        fee_map = {}
        for sid in ids:
            base = items_sum_map.get(sid)
            if base is not None and base > 0:
                d = disc_map.get(sid)
                if d:
                    if d.get("kind") == "percent":
                        base = max(base - round(base * (d.get("value", 0.0) / 100.0), 2), 0.0)
                    else:
                        base = max(base - float(d.get("value") or 0), 0.0)
                fee_map[sid] = base
            else:
                fee_map[sid] = legacy_map.get(sid, 0.0)

        # Payments in this term (exclude Credit Transfer)
        cur.execute(
            f"SELECT student_id, COALESCE(SUM(amount),0) AS amt FROM payments WHERE year=%s AND term=%s AND (method IS NULL OR method <> 'Credit Transfer') AND student_id IN ({id_in_clause(ids)}) GROUP BY student_id",
            (year, term, *ids),
        )
        pay_term_map = {r["student_id"]: float(r.get("amt") or 0) for r in (cur.fetchall() or [])}

        # Opening balance for the year
        cur.execute(
            f"SELECT student_id, COALESCE(opening_balance,0) AS ob FROM student_enrollments WHERE year=%s AND student_id IN ({id_in_clause(ids)})",
            (year, *ids),
        )
        opening_map = {r["student_id"]: float(r.get("ob") or 0) for r in (cur.fetchall() or [])}

        # Fees of prior terms this year
        cur.execute(
            f"SELECT student_id, COALESCE(SUM(COALESCE(final_fee, fee_amount)),0) AS fsum FROM term_fees WHERE year=%s AND term < %s AND student_id IN ({id_in_clause(ids)}) GROUP BY student_id",
            (year, term, *ids),
        )
        fees_prior_map = {r["student_id"]: float(r.get("fsum") or 0) for r in (cur.fetchall() or [])}

        # Payments of prior terms this year (exclude Credit Transfer)
        cur.execute(
            f"SELECT student_id, COALESCE(SUM(amount),0) AS psum FROM payments WHERE year=%s AND term < %s AND (method IS NULL OR method <> 'Credit Transfer') AND student_id IN ({id_in_clause(ids)}) GROUP BY student_id",
            (year, term, *ids),
        )
        pays_prior_map = {r["student_id"]: float(r.get("psum") or 0) for r in (cur.fetchall() or [])}

        rows = []
        totals = {"carry_in": 0.0, "term_fee": 0.0, "payments": 0.0, "closing": 0.0}
        for s in students:
            sid = s["id"]
            opening = opening_map.get(sid, 0.0)
            fees_prior = fees_prior_map.get(sid, 0.0)
            pays_prior = pays_prior_map.get(sid, 0.0)
            carry_in = opening + fees_prior - pays_prior
            term_fee = fee_map.get(sid, 0.0)
            paid_term = pay_term_map.get(sid, 0.0)
            closing = carry_in + term_fee - paid_term
            rows.append({
                "id": sid,
                "name": s.get("name"),
                "class_name": s.get("class_name"),
                "carry_in": carry_in,
                "term_fee": term_fee,
                "payments": paid_term,
                "closing": closing,
                "credit": float(s.get("credit") or 0),
            })
            totals["carry_in"] += carry_in
            totals["term_fee"] += term_fee
            totals["payments"] += paid_term
            totals["closing"] += closing

        # classes for filter dropdown
        if sid:
            cur.execute("SELECT DISTINCT class_name FROM students WHERE school_id=%s AND class_name IS NOT NULL AND class_name <> '' ORDER BY class_name", (sid,))
        else:
            cur.execute("SELECT DISTINCT class_name FROM students WHERE class_name IS NOT NULL AND class_name <> '' ORDER BY class_name")
        classes = [r["class_name"] for r in (cur.fetchall() or [])]

    finally:
        db.close()

    return render_template("term_summary.html", year=year, term=term, rows=rows, totals=totals, classes=classes, class_filter=class_filter)
