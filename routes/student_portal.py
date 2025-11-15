from __future__ import annotations

from flask import Blueprint, current_app, render_template, request, jsonify, abort, url_for, Response, flash, redirect
import hmac
import hashlib
import os
from datetime import datetime
from urllib.parse import urlparse
import mysql.connector

from utils.mpesa import stk_push, DarajaError
from utils.schema import get_admission_select_and_column
from routes.term_routes import get_or_seed_current_term, ensure_academic_terms_table
from utils.settings import get_setting


student_portal_bp = Blueprint("student_portal", __name__, url_prefix="/portal")


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


def ensure_mpesa_student_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mpesa_student_payments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            school_id INT NULL,
            year INT NULL,
            term TINYINT NULL,
            merchant_request_id VARCHAR(64),
            checkout_request_id VARCHAR(64),
            result_code INT DEFAULT NULL,
            result_desc VARCHAR(255) DEFAULT NULL,
            mpesa_receipt VARCHAR(32) DEFAULT NULL,
            phone VARCHAR(32) DEFAULT NULL,
            amount DECIMAL(10,2) DEFAULT NULL,
            raw_callback TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            INDEX idx_checkout (checkout_request_id),
            INDEX idx_student (student_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()


def ensure_student_portal_salt_column(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'portal_salt'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE students ADD COLUMN portal_salt VARCHAR(32) NULL AFTER phone")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_guardian_messages_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guardian_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            school_id INT NULL,
            name VARCHAR(128) NULL,
            email VARCHAR(190) NULL,
            phone VARCHAR(40) NULL,
            category VARCHAR(32) NULL,
            subject VARCHAR(190) NULL,
            message TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_student (student_id),
            INDEX idx_school (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()


def _sign_token(student_id: int, issued_at: int | None = None) -> str:
    sid = str(int(student_id))
    ts = str(int(issued_at or int(datetime.now().timestamp())))
    secret = (current_app.config.get("SECRET_KEY") or os.environ.get("SECRET_KEY") or "secret123").encode("utf-8")
    # Include per-student salt if present
    salt = ""
    try:
        db = _db(); ensure_student_portal_salt_column(db)
        cur = db.cursor()
        cur.execute("SELECT portal_salt FROM students WHERE id=%s", (int(student_id),))
        row = cur.fetchone()
        if row:
            salt = (row[0] or "")
        db.close()
    except Exception:
        pass
    mac = hmac.new(secret, f"portal:{sid}:{ts}:{salt}".encode("utf-8"), hashlib.sha256).hexdigest()[:24]
    return f"{sid}.{ts}.{mac}"


def _verify_token(token: str) -> int | None:
    try:
        sid, ts, mac = token.split(".")
        secret = (current_app.config.get("SECRET_KEY") or os.environ.get("SECRET_KEY") or "secret123").encode("utf-8")
        # Include per-student salt if present
        salt = ""
        try:
            db = _db(); ensure_student_portal_salt_column(db)
            cur = db.cursor()
            cur.execute("SELECT portal_salt FROM students WHERE id=%s", (int(sid),))
            row = cur.fetchone()
            if row:
                salt = (row[0] or "")
            db.close()
        except Exception:
            pass
        check = hmac.new(secret, f"portal:{sid}:{ts}:{salt}".encode("utf-8"), hashlib.sha256).hexdigest()[:24]
        if not hmac.compare_digest(mac, check):
            return None
        # Optional expiration
        try:
            max_days = int(get_setting("PORTAL_TOKEN_MAX_AGE_DAYS", current_app.config.get("PORTAL_TOKEN_MAX_AGE_DAYS", 180)) or 180)
        except Exception:
            max_days = 180
        try:
            ts_i = int(ts)
        except Exception:
            return None
        if max_days > 0:
            from datetime import timedelta
            if datetime.fromtimestamp(ts_i) < (datetime.now() - timedelta(days=max_days)):
                return None
        # Optional rollover cutoff to revoke older links
        try:
            rollover = get_setting("PORTAL_TOKEN_ROLLOVER_AT", None)
            if rollover:
                roll_i = int(rollover)
                if ts_i < roll_i:
                    return None
        except Exception:
            pass
        return int(sid)
    except Exception:
        return None


@student_portal_bp.app_template_global()
def student_portal_link(student_id: int) -> str:
    """Helper to generate a signed student portal link inside templates."""
    token = _sign_token(student_id)
    try:
        return url_for("student_portal.view", token=token)
    except Exception:
        return f"/portal/{token}"


@student_portal_bp.route("/<token>")
def view(token: str):
    if token == "me":
        # Allow signed-in student to use their portal without a token
        from flask import session as _session
        sid = int(_session.get("student_id") or 0)
        if not _session.get("student_logged_in") or not sid:
            return redirect(url_for("student_auth.student_login"))
        # Generate a token on the fly and redirect, so page URLs remain canonical
        return redirect(url_for("student_portal.view", token=_sign_token(sid)))

    student_id = _verify_token(token)
    if not student_id:
        abort(403)

    db = _db()
    cur = db.cursor(dictionary=True)
    # Fetch student
    cur.execute("SELECT * FROM students WHERE id=%s", (student_id,))
    student = cur.fetchone()
    if not student:
        db.close()
        abort(404)

    # Current term/year for this school
    ensure_academic_terms_table(db)
    year, term = get_or_seed_current_term(db)

    # Payments history for quick receipts
    cur.execute(
        """
        SELECT id, amount, method, reference, date
        FROM payments
        WHERE student_id=%s
        ORDER BY date DESC, id DESC
        """,
        (student_id,),
    )
    payments = cur.fetchall() or []

    # Try fetch current invoice id (if invoices feature used)
    invoice_id = None
    try:
        cur.execute(
            "SELECT id FROM invoices WHERE student_id=%s AND year=%s AND term=%s",
            (student_id, year, term),
        )
        row = cur.fetchone()
        if row:
            invoice_id = row.get("id")
    except Exception:
        invoice_id = None

    db.close()
    return render_template(
        "student_portal.html",
        student=student,
        payments=payments,
        invoice_id=invoice_id,
        token=token,
        year=year,
        term=term,
    )


@student_portal_bp.route("/initiate", methods=["POST"], endpoint="initiate")
def initiate_payment():
    """Initiate an M-Pesa STK push for a specific student via signed token."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    phone = (data.get("phone") or "").strip()
    try:
        amount = int(float(data.get("amount") or 0))
    except Exception:
        amount = 0
    if amount <= 0:
        return jsonify({"ok": False, "error": "Invalid amount"}), 400

    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired token"}), 403

    db = _db()
    ensure_mpesa_student_table(db)
    ensure_academic_terms_table(db)
    year, term = get_or_seed_current_term(db)
    cur = db.cursor(dictionary=True)

    # Determine school_id and student details
    adm_sel, _ = get_admission_select_and_column(cur)
    cur.execute(f"SELECT id, name, {adm_sel}, school_id FROM students WHERE id=%s", (student_id,))
    student = cur.fetchone()
    if not student:
        db.close()
        return jsonify({"ok": False, "error": "Student not found"}), 404
    school_id = student.get("school_id")

    account_ref = (student.get("regNo") or f"STD-{student_id}")[:20]
    trans_desc = f"School fees for {student.get('name', 'Student')}"

    try:
        res = stk_push(phone=phone, amount=amount, account_ref=account_ref, trans_desc=trans_desc)
    except DarajaError as e:
        db.close()
        return jsonify({"ok": False, "error": str(e)}), 400

    now = datetime.now()
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
            year,
            term,
            res.get("MerchantRequestID"),
            res.get("CheckoutRequestID"),
            amount,
            phone,
            now,
            now,
        ),
    )
    db.commit()
    db.close()

    return jsonify({
        "ok": True,
        "message": "STK push sent. Check your phone to authorize.",
        "checkout_request_id": res.get("CheckoutRequestID"),
    })


@student_portal_bp.route("/status", methods=["GET"])
def status():
    """Poll status for a given checkout_request_id or by latest student STK."""
    token = (request.args.get("token") or "").strip()
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired token"}), 403

    checkout_id = (request.args.get("checkout_request_id") or "").strip()
    db = _db()
    cur = db.cursor(dictionary=True)
    if checkout_id:
        cur.execute("SELECT * FROM mpesa_student_payments WHERE checkout_request_id=%s", (checkout_id,))
    else:
        cur.execute(
            "SELECT * FROM mpesa_student_payments WHERE student_id=%s ORDER BY updated_at DESC, id DESC LIMIT 1",
            (student_id,),
        )
    row = cur.fetchone()

    result = {"ok": True, "found": bool(row)}
    recorded = False
    receipt = None
    if row:
        receipt = row.get("mpesa_receipt")
        result.update({
            "result_code": row.get("result_code"),
            "result_desc": row.get("result_desc"),
            "amount": float(row.get("amount") or 0),
            "receipt": receipt,
            "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        })
        if receipt:
            cur2 = db.cursor()
            cur2.execute("SELECT COUNT(*) FROM payments WHERE reference=%s AND student_id=%s", (receipt, student_id))
            r = cur2.fetchone()
            if r and (isinstance(r, (list, tuple)) and r[0] or (r or 0)):
                recorded = True
    db.close()
    result["recorded"] = recorded
    return jsonify(result)


@student_portal_bp.route("/contact", methods=["POST"])
def contact_school():
    """Allow a parent/guardian to send an enquiry to the school email.

    Expects JSON or form body with: token, name, email, phone, category, subject, message.
    """
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or request.form.get("token") or "").strip()
    student_id = _verify_token(token)
    if not student_id:
        return jsonify({"ok": False, "error": "Invalid or expired token"}), 403

    name = (data.get("name") or request.form.get("name") or "").strip()
    email = (data.get("email") or request.form.get("email") or "").strip()
    phone = (data.get("phone") or request.form.get("phone") or "").strip()
    category = (data.get("category") or request.form.get("category") or "General").strip()
    subject = (data.get("subject") or request.form.get("subject") or f"Guardian enquiry ({category})").strip()
    message = (data.get("message") or request.form.get("message") or "").strip()
    if not message:
        return jsonify({"ok": False, "error": "Message is required"}), 400

    db = _db()
    cur = db.cursor(dictionary=True)
    adm_sel, _ = get_admission_select_and_column(cur)
    cur.execute(f"SELECT school_id, name, {adm_sel} FROM students WHERE id=%s", (student_id,))
    srow = cur.fetchone() or {}
    school_id = srow.get("school_id")
    ensure_guardian_messages_table(db)
    from datetime import datetime as _dt
    now = _dt.now()
    cur2 = db.cursor()
    cur2.execute(
        "INSERT INTO guardian_messages (student_id, school_id, name, email, phone, category, subject, message, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (student_id, school_id, name or None, email or None, phone or None, category or None, subject or None, message, now),
    )
    db.commit()

    # Determine destination email and send
    to_email = get_setting("SCHOOL_EMAIL") or (current_app.config.get("MAIL_DEFAULT_SENDER") or current_app.config.get("MAIL_USERNAME") or "")
    if to_email:
        try:
            html = f"""
            <p><strong>Guardian Enquiry</strong></p>
            <p><strong>Student:</strong> {srow.get('name','')} ({srow.get('regNo','')})</p>
            <p><strong>From:</strong> {name or 'Unknown'} | {email or ''} | {phone or ''}</p>
            <p><strong>Category:</strong> {category}</p>
            <p><strong>Message:</strong><br/>{message.replace('\n','<br/>')}</p>
            <p style=\"color:#64748b\">This message was sent from the Parent/Guardian portal.</p>
            """
            try:
                from utils.gmail_api import send_email_html as gmail_send_email_html
                gmail_send_email_html(to_email, subject, html)
            except Exception:
                from utils.gmail_api import send_email as gmail_send_email
                gmail_send_email(to_email, subject, f"Guardian message (student {srow.get('name','')}). {message}")
        except Exception:
            pass
    db.close()
    return jsonify({"ok": True, "message": "Your message has been sent to the school."})


@student_portal_bp.route("/statement/<token>.pdf")
def statement_pdf(token: str):
    """Generate a simple PDF statement for the student with balance and recent payments."""
    student_id = _verify_token(token)
    if not student_id:
        abort(403)

    db = _db()
    cur = db.cursor(dictionary=True)
    adm_sel, _ = get_admission_select_and_column(cur)
    cur.execute(f"SELECT id, name, {adm_sel}, class_name, balance FROM students WHERE id=%s", (student_id,))
    student = cur.fetchone()
    if not student:
        db.close()
        abort(404)
    cur.execute(
        "SELECT date, amount, method, reference FROM payments WHERE student_id=%s ORDER BY date DESC, id DESC LIMIT 50",
        (student_id,),
    )
    payments = cur.fetchall() or []
    db.close()

    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    from io import BytesIO

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    x, y = 20*mm, height - 20*mm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, "Student Statement")
    y -= 8*mm
    c.setFont("Helvetica", 11)
    c.drawString(x, y, f"Name: {student.get('name', '')}")
    y -= 6*mm
    c.drawString(x, y, f"Adm No: {student.get('regNo', '')}    Class: {student.get('class_name', '')}")
    y -= 6*mm
    c.drawString(x, y, f"Current Balance: KES {float(student.get('balance') or 0):,.2f}")
    y -= 10*mm

    c.setFont("Helvetica-Bold", 12)
    c.drawString(x, y, "Recent Payments")
    y -= 7*mm
    c.setFont("Helvetica", 10)
    c.drawString(x, y, "Date")
    c.drawString(x + 40*mm, y, "Amount")
    c.drawString(x + 80*mm, y, "Method")
    c.drawString(x + 120*mm, y, "Reference")
    y -= 5*mm
    c.line(x, y, width - 20*mm, y)
    y -= 3*mm

    for p in payments:
        if y < 25*mm:
            c.showPage()
            y = height - 20*mm
        date_str = p.get("date")
        if hasattr(date_str, "strftime"):
            date_str = date_str.strftime("%Y-%m-%d")
        c.drawString(x, y, str(date_str or ""))
        c.drawRightString(x + 70*mm, y, f"KES {float(p.get('amount') or 0):,.2f}")
        c.drawString(x + 80*mm, y, str(p.get("method") or ""))
        c.drawString(x + 120*mm, y, str(p.get("reference") or ""))
        y -= 6*mm

    c.showPage()
    c.save()
    pdf = buffer.getvalue()
    buffer.close()
    return Response(pdf, mimetype="application/pdf", headers={
        "Content-Disposition": f"attachment; filename=statement_{student_id}.pdf"
    })


def _require_admin():
    from flask import session
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return redirect(url_for("admin.login"))


@student_portal_bp.route("/rotate/<int:student_id>", methods=["POST"]) 
def rotate_student(student_id: int):
    guard = _require_admin()
    if guard is not None:
        return guard
    db = _db(); ensure_student_portal_salt_column(db)
    cur = db.cursor()
    new_salt = os.urandom(6).hex()
    try:
        cur.execute("UPDATE students SET portal_salt=%s WHERE id=%s", (new_salt, student_id))
        db.commit()
        flash("Student portal link invalidated.", "success")
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        flash("Failed to invalidate portal link.", "error")
    finally:
        db.close()
    return redirect(url_for("student_detail", student_id=student_id))


@student_portal_bp.route("/latest", methods=["GET"])
def latest_portal_payment():
    from flask import session
    school_id = session.get("school_id")
    db = _db(); cur = db.cursor(dictionary=True)
    try:
        if school_id:
            cur.execute(
                """
                SELECT updated_at, amount, student_id FROM mpesa_student_payments
                WHERE result_code=0 AND school_id=%s
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (school_id,),
            )
        else:
            cur.execute(
                "SELECT updated_at, amount, student_id FROM mpesa_student_payments WHERE result_code=0 ORDER BY updated_at DESC, id DESC LIMIT 1"
            )
        row = cur.fetchone()
    except Exception:
        row = None
    finally:
        db.close()
    if not row:
        return jsonify({"ok": True, "found": False})
    return jsonify({
        "ok": True,
        "found": True,
        "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        "amount": float(row.get("amount") or 0),
        "student_id": row.get("student_id"),
    })
