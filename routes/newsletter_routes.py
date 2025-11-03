from __future__ import annotations

from flask import (
    Blueprint,
    current_app,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)
import os
import mysql.connector
from urllib.parse import urlparse

from utils.gmail_api import send_email_html as gmail_send_email_html
from utils.settings import get_setting
from utils.pro import is_pro_enabled


newsletter_bp = Blueprint("newsletters", __name__, url_prefix="/newsletters")


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


def ensure_newsletters_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS newsletters (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NULL,
            category VARCHAR(20) NOT NULL DEFAULT 'newsletter',
            title VARCHAR(200) NOT NULL,
            subject VARCHAR(200) NOT NULL,
            html LONGTEXT NOT NULL,
            audience_type VARCHAR(20) NOT NULL DEFAULT 'all', -- all | class | emails
            audience_value VARCHAR(255) NULL,
            created_by VARCHAR(100) NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("CREATE INDEX idx_newsletters_school ON newsletters(school_id)")
    except Exception:
        pass
    conn.commit()


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


@newsletter_bp.route("/")
def index():
    try:
        if not is_pro_enabled(current_app):
            flash("Newsletters are available in Pro. Please upgrade.", "warning")
            return redirect(url_for('monetization.index'))
    except Exception:
        pass
    db = _db()
    try:
        ensure_newsletters_table(db)
    except Exception:
        pass
    cur = db.cursor(dictionary=True)
    sid = session.get("school_id") if session else None
    if sid:
        cur.execute("SELECT * FROM newsletters WHERE school_id=%s ORDER BY id DESC", (sid,))
    else:
        cur.execute("SELECT * FROM newsletters ORDER BY id DESC")
    rows = cur.fetchall() or []
    db.close()
    return render_template("newsletters.html", newsletters=rows)


@newsletter_bp.route("/compose", methods=["GET", "POST"])
def compose():
    try:
        if not is_pro_enabled(current_app):
            flash("Newsletters are available in Pro. Please upgrade.", "warning")
            return redirect(url_for('monetization.index'))
    except Exception:
        pass
    if request.method == "GET":
        return render_template("newsletters_compose.html")

    category = (request.form.get("category") or "newsletter").strip()
    title = (request.form.get("title") or "").strip()
    subject = (request.form.get("subject") or title).strip()
    html = (request.form.get("html") or "").strip()
    audience_type = (request.form.get("audience_type") or "all").strip()
    audience_value = (request.form.get("audience_value") or "").strip()
    send_now = (request.form.get("send_now") in ("1", "true", "on"))

    if not title or not subject or not html:
        flash("Title, subject and content are required.", "warning")
        return render_template("newsletters_compose.html",
                               category=category,
                               title=title,
                               subject=subject,
                               html=html,
                               audience_type=audience_type,
                               audience_value=audience_value)

    db = _db()
    try:
        ensure_newsletters_table(db)
    except Exception:
        pass
    cur = db.cursor()
    sid = session.get("school_id") if session else None
    created_by = None
    try:
        created_by = (session.get("username") or session.get("user_email") or None)
    except Exception:
        created_by = None
    cur.execute(
        """
        INSERT INTO newsletters (school_id, category, title, subject, html, audience_type, audience_value, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (sid, category, title, subject, html, audience_type, audience_value, created_by),
    )
    db.commit()
    new_id = cur.lastrowid
    db.close()

    if send_now:
        return redirect(url_for("newsletters.send_now", newsletter_id=new_id))
    flash("Newsletter saved.", "success")
    return redirect(url_for("newsletters.index"))


@newsletter_bp.route("/send/<int:newsletter_id>")
def send_now(newsletter_id: int):
    try:
        if not is_pro_enabled(current_app):
            flash("Newsletters are available in Pro. Please upgrade.", "warning")
            return redirect(url_for('monetization.index'))
    except Exception:
        pass
    db = _db()
    try:
        ensure_newsletters_table(db)
    except Exception:
        pass
    cur = db.cursor(dictionary=True)
    sid = session.get("school_id") if session else None
    if sid:
        cur.execute("SELECT * FROM newsletters WHERE id=%s AND (school_id=%s OR school_id IS NULL)", (newsletter_id, sid))
    else:
        cur.execute("SELECT * FROM newsletters WHERE id=%s", (newsletter_id,))
    row = cur.fetchone()
    if not row:
        db.close()
        flash("Newsletter not found.", "warning")
        return redirect(url_for("newsletters.index"))

    # Determine recipients
    recipients: list[str] = []
    aud = (row.get("audience_type") or "all").lower()
    aud_val = (row.get("audience_value") or "").strip()
    email_col = None
    try:
        email_col = _resolve_email_column(cur)
    except Exception:
        email_col = None

    if aud == "emails" and aud_val:
        # Split by comma/newline/semicolons
        import re
        for part in re.split(r"[\s,;]+", aud_val):
            part = part.strip()
            if part:
                recipients.append(part)
    else:
        if not email_col:
            db.close()
            flash("No email column found on students table.", "warning")
            return redirect(url_for("newsletters.index"))
        if aud == "class" and aud_val:
            if sid:
                cur.execute(
                    f"SELECT {email_col} AS email FROM students WHERE school_id=%s AND class_name=%s AND {email_col} IS NOT NULL AND {email_col} <> ''",
                    (sid, aud_val),
                )
            else:
                cur.execute(
                    f"SELECT {email_col} AS email FROM students WHERE class_name=%s AND {email_col} IS NOT NULL AND {email_col} <> ''",
                    (aud_val,),
                )
        else:
            if sid:
                cur.execute(
                    f"SELECT {email_col} AS email FROM students WHERE school_id=%s AND {email_col} IS NOT NULL AND {email_col} <> ''",
                    (sid,),
                )
            else:
                cur.execute(
                    f"SELECT {email_col} AS email FROM students WHERE {email_col} IS NOT NULL AND {email_col} <> ''",
                )
        rows = cur.fetchall() or []
        for r in rows:
            try:
                e = (r.get("email") or "").strip()
            except Exception:
                e = (r[0] if r else "")
                e = (e or "").strip()
            if e:
                recipients.append(e)

    # Deduplicate recipients
    recipients = list(dict.fromkeys(recipients))

    # Build HTML body envelope
    school = (get_setting("SCHOOL_NAME") or current_app.config.get("APP_NAME") or "School")
    html_body = render_template(
        "email_newsletter.html",
        brand=school,
        title=row.get("title"),
        content_html=row.get("html") or "",
    )

    subject = row.get("subject") or row.get("title") or f"{school} Newsletter"

    sent = 0
    failed = 0
    for to in recipients:
        ok = False
        try:
            ok = gmail_send_email_html(to, subject, html_body)
        except Exception:
            ok = False
        if ok:
            sent += 1
        else:
            failed += 1

    db.close()
    flash(f"Newsletter sent. Recipients: {len(recipients)}. Sent: {sent}, Failed: {failed}.", "info")
    return redirect(url_for("newsletters.index"))

