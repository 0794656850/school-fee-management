from __future__ import annotations

import csv
from io import StringIO
from datetime import datetime, timedelta

from apscheduler.triggers.cron import CronTrigger
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app, jsonify, Response
from typing import Any
import mysql.connector

from extensions import limiter

from utils.whatsapp import whatsapp_is_configured, send_whatsapp_text, send_whatsapp_template
from utils.settings import get_setting, set_setting, set_school_setting
from utils.security import verify_password, hash_password, is_hashed
from utils.pro import is_pro_enabled, set_license_key, get_license_key, upgrade_url
from utils.audit import fetch_audit_logs, log_event
from utils.db_helpers import ensure_guardian_receipts_table
from utils.tenant import get_or_create_school, bootstrap_new_school, ensure_schools_table, slugify_code
from utils.users import (
    ensure_user_tables,
    list_school_users,
    get_user_by_username,
    create_user,
    ensure_school_user,
    count_school_users,
    set_user_password,
    set_user_active,
)
from utils.backup import (
    BackupException,
    backup_root_for_school,
    create_backup,
    get_backup_history,
    restore_backup_snapshot,
)
from utils.timezone_helpers import EAST_AFRICA_TZ, east_africa_now, format_east_africa
from routes.reminder_routes import DEFAULT_REMINDER_TEMPLATE


admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


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


def ensure_pro_activations_table(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pro_activations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            mpesa_ref VARCHAR(32) NOT NULL UNIQUE,
            amount DECIMAL(10,2) NULL,
            activated_at DATETIME NOT NULL,
            notes VARCHAR(255) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def _require_admin():
    # Allow global admin OR a logged-in school user with admin/owner role
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return redirect(url_for("admin.login"))


def _get_global_admin_password() -> str:
    config_value = (current_app.config.get("ADMIN_PASSWORD") or "").strip()
    if config_value:
        return config_value
    saved_school = session.pop("school_id", None)
    try:
        return (get_setting("ADMIN_PASSWORD") or "").strip()
    finally:
        if saved_school is not None:
            session["school_id"] = saved_school


def _verify_admin_current_password(candidate: str) -> bool:
    stored_admin = _get_global_admin_password() or "9133"
    if verify_password(stored_admin, candidate):
        return True

    portal_password = (get_setting("APP_LOGIN_PASSWORD") or "").strip()
    if portal_password and verify_password(portal_password, candidate):
        return True

    return False


def _build_payment_filter_state(sid: int):
    args = request.args
    student = (args.get("student") or "").strip()
    method = (args.get("method") or "").strip()
    year = (args.get("year") or "").strip()
    term = (args.get("term") or "").strip()
    start_date = (args.get("start_date") or "").strip()
    end_date = (args.get("end_date") or "").strip()
    min_amount = (args.get("min_amount") or "").strip()
    max_amount = (args.get("max_amount") or "").strip()
    filters = {
        "student": student,
        "method": method,
        "year": year,
        "term": term,
        "start_date": start_date,
        "end_date": end_date,
        "min_amount": min_amount,
        "max_amount": max_amount,
    }

    conds = ["p.school_id=%s"]
    params = [sid]

    if student:
        term_like = f"%{student.lower()}%"
        conds.append("(LOWER(s.name) LIKE %s OR LOWER(s.admission_no) LIKE %s OR LOWER(COALESCE(p.reference,'')) LIKE %s)")
        params.extend([term_like, term_like, term_like])
    if method:
        conds.append("p.method = %s")
        params.append(method)
    if year:
        try:
            conds.append("p.year = %s")
            params.append(int(year))
        except ValueError:
            pass
    if term:
        try:
            conds.append("p.term = %s")
            params.append(int(term))
        except ValueError:
            pass
    if start_date:
        conds.append("p.date >= %s")
        params.append(start_date)
    if end_date:
        conds.append("p.date <= %s")
        params.append(end_date)
    if min_amount:
        try:
            conds.append("p.amount >= %s")
            params.append(float(min_amount))
        except ValueError:
            pass
    if max_amount:
        try:
            conds.append("p.amount <= %s")
            params.append(float(max_amount))
        except ValueError:
            pass

    where = " AND ".join(conds)
    return filters, where, params


def _serialize_audit_log(log: dict[str, Any]) -> dict[str, Any]:
    ts = log.get("created_at")
    return {
        "id": log.get("id"),
        "school_id": log.get("school_id"),
        "user_id": log.get("user_id"),
        "username": log.get("username") or log.get("user") or "System",
        "user_role": log.get("user_role") or log.get("role") or "System",
        "action": log.get("action") or "System change",
        "target": log.get("target"),
        "detail": log.get("detail"),
        "status": log.get("status") or "Success",
        "module": log.get("module"),
        "action_type": log.get("action_type"),
        "ip_address": log.get("ip_address"),
        "device_info": log.get("device_info"),
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else None,
    }


@admin_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("6 per minute", methods=["POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "").strip()
        if _verify_admin_current_password(password):
            session["is_admin"] = True
            flash("Welcome, admin!", "success")
            return redirect(url_for("admin.dashboard"))
        log_event(
            "security",
            "admin_login_failed",
            detail=f"Failed admin login from {request.remote_addr or 'unknown IP'}",
        )
        flash("Invalid password.", "error")
        return redirect(url_for("admin.login"))
    return render_template("admin_login.html")


@admin_bp.route("/logout")
def logout():
    # No audit logout logging
    session.pop("is_admin", None)
    flash("Logged out.", "info")
    return redirect(url_for("admin.login"))


@admin_bp.route("")
@admin_bp.route("/")
def dashboard():
    guard = _require_admin()
    if guard is not None:
        return guard

    db = _db(); cur = db.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) AS c FROM students WHERE school_id=%s", (session.get("school_id"),)); total_students = cur.fetchone()["c"]
    cur.execute("SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE school_id=%s", (session.get("school_id"),)); total_collected = float(cur.fetchone()["t"] or 0)
    cur.execute("SHOW COLUMNS FROM students LIKE 'balance'"); col = 'balance' if cur.fetchone() else 'fee_balance'
    cur.execute(f"SELECT COALESCE(SUM({col}),0) AS t FROM students WHERE school_id=%s", (session.get("school_id"),)); total_balance = float(cur.fetchone()["t"] or 0)
    db.close()

    wa_ok, wa_reason = whatsapp_is_configured()
    # M-Pesa configuration status
    def _cfg_or(key: str) -> str:
        v = current_app.config.get(key) or get_setting(key)
        return (v or "").strip()
    mpesa_ok = all([
        _cfg_or("DARAJA_CONSUMER_KEY"),
        _cfg_or("DARAJA_CONSUMER_SECRET"),
        _cfg_or("DARAJA_SHORT_CODE"),
        _cfg_or("DARAJA_PASSKEY"),
    ])
    return render_template(
        "admin.html",
        now=datetime.now(),
        total_students=total_students,
        total_collected=total_collected,
        total_balance=total_balance,
        whatsapp_ok=wa_ok,
        whatsapp_reason=wa_reason,
        pro_enabled=is_pro_enabled(),
        upgrade_link=upgrade_url(),
        mpesa_ok=mpesa_ok,
    )


@admin_bp.route("/payment-records")
def payment_records():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before reviewing payments.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.payment_records")))

    filters, where, params = _build_payment_filter_state(sid)
    limit_raw = request.args.get("limit")
    try:
        limit = int(limit_raw)
    except (TypeError, ValueError):
        limit = 200
    limit = max(50, min(limit, 2000))

    db = _db()
    try:
        cur = db.cursor(dictionary=True)
        cur.execute("SELECT DISTINCT COALESCE(method,'Other') AS method FROM payments WHERE school_id=%s ORDER BY method ASC", (sid,))
        method_choices = [row["method"] for row in cur.fetchall() if row and row.get("method")]

        payment_sql = f"""
            SELECT
                p.id,
                s.name AS student_name,
                s.class_name,
                p.year,
                p.term,
                p.amount,
                p.method,
                p.reference,
                DATE_FORMAT(p.date, '%Y-%m-%d') AS date
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE {where}
            ORDER BY p.date DESC, p.id DESC
            LIMIT %s
        """
        cur.execute(payment_sql, params + [limit])
        payments = cur.fetchall() or []

        summary_sql = f"""
            SELECT
                COUNT(*) AS total_count,
                COALESCE(SUM(amount), 0) AS total_amount,
                COALESCE(AVG(amount), 0) AS avg_amount,
                COALESCE(MAX(amount), 0) AS max_amount
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE {where}
        """
        cur.execute(summary_sql, params)
        row = cur.fetchone() or {}
        summary = {
            "total_count": int(row.get("total_count") or 0),
            "total_amount": float(row.get("total_amount") or 0),
            "avg_amount": float(row.get("avg_amount") or 0),
            "max_amount": float(row.get("max_amount") or 0),
        }

        trend_sql = f"""
            SELECT DATE_FORMAT(p.date, '%%Y-%%m') AS ym, COALESCE(SUM(p.amount), 0) AS total
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE {where} AND p.date >= %s
            GROUP BY ym
            ORDER BY ym ASC
        """
        trend_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        cur.execute(trend_sql, params + [trend_start])
        trend_rows = cur.fetchall() or []

        method_sql = f"""
            SELECT COALESCE(p.method,'Other') AS method, COALESCE(SUM(p.amount), 0) AS total
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE {where}
            GROUP BY method
            ORDER BY total DESC
            LIMIT 8
        """
        cur.execute(method_sql, params)
        method_rows = cur.fetchall() or []
    finally:
        try:
            db.close()
        except Exception:
            pass

    export_url = url_for("admin.payment_records_export", **request.args.to_dict(flat=True))

    return render_template(
        "admin/payment_records.html",
        payments=payments,
        filters=filters,
        summary=summary,
        trend_rows=trend_rows,
        method_rows=method_rows,
        method_choices=method_choices,
        export_url=export_url,
        limit=limit,
    )


@admin_bp.route("/payment-records/export")
def payment_records_export():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before reviewing payments.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.payment_records_export")))

    filters, where, params = _build_payment_filter_state(sid)
    db = _db()
    try:
        cur = db.cursor(dictionary=True)
        export_sql = f"""
            SELECT
                DATE_FORMAT(p.date, '%%Y-%%m-%%d') AS date,
                s.name AS student_name,
                s.class_name,
                p.year,
                p.term,
                p.amount,
                p.method,
                p.reference
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE {where}
            ORDER BY p.date DESC, p.id DESC
        """
        cur.execute(export_sql, params)
        rows = cur.fetchall() or []
    finally:
        try:
            db.close()
        except Exception:
            pass

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["Date", "Student", "Class", "Year", "Term", "Amount (KES)", "Method", "Reference"])
    for row in rows:
        writer.writerow([
            row.get("date") or "",
            row.get("student_name") or "",
            row.get("class_name") or "",
            row.get("year") or "",
            row.get("term") or "",
            row.get("amount") or "",
            row.get("method") or "",
            row.get("reference") or "",
        ])

    resp = Response(csv_buffer.getvalue(), mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = "attachment; filename=fee_payment_records.csv"
    return resp


@admin_bp.route("/users", methods=["GET", "POST"])
def manage_users():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school first.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.manage_users")))

    db = _db(); ensure_user_tables(db)

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        if action == "add":
            username = (request.form.get("username") or "").strip()
            email = (request.form.get("email") or "").strip() or None
            role = (request.form.get("role") or "staff").strip()
            password = (request.form.get("password") or "").strip()
            if not username or not password:
                flash("Username and password are required.", "warning")
                db.close(); return redirect(url_for("admin.manage_users"))
            # Pro gate: allow multiple users only for Pro; otherwise restrict to 1
            try:
                current = count_school_users(db, int(sid))
            except Exception:
                current = 0
            if current >= 1 and not is_pro_enabled(current_app):
                db.close()
                flash("Multi-user is a Pro feature. Upgrade to add more users.", "info")
                return redirect(url_for("admin.billing"))
            # Create or reuse existing user
            existing = get_user_by_username(db, username)
            uid = None
            if existing:
                uid = int(existing["id"]) if isinstance(existing, dict) else int(existing[0])
                set_user_password(db, uid, hash_password(password))
            else:
                uid = create_user(db, username, email, hash_password(password))
            ensure_school_user(db, uid, int(sid), role or "staff")
            flash("User saved.", "success")
            db.close(); return redirect(url_for("admin.manage_users"))
        elif action in ("deactivate","activate"):
            uid = int(request.form.get("user_id") or 0)
            set_user_active(db, uid, action == "activate")
            db.close(); flash("User updated.", "success")
            return redirect(url_for("admin.manage_users"))

    items = list_school_users(db, int(sid))
    db.close()
    return render_template("admin_users.html", users=items, is_pro=is_pro_enabled(current_app))


@admin_bp.route("/schools", methods=["GET", "POST"])
def manage_schools():
    guard = _require_admin()
    if guard is not None:
        return guard
    # Schools management UI is disabled. Redirect to dashboard.
    return redirect(url_for("admin.dashboard"))


@admin_bp.route("/guardian-receipts", methods=["GET", "POST"])
def guardian_receipts():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before reviewing uploads.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.guardian_receipts")))

    db = _db()
    ensure_guardian_receipts_table(db)
    cur = db.cursor(dictionary=True)
    try:
        if request.method == "POST":
            rid = int(request.form.get("receipt_id") or 0)
            action = (request.form.get("action") or "").strip().lower()
            if rid and action in ("verify", "reject"):
                now = datetime.utcnow()
                status = "verified" if action == "verify" else "rejected"
                cur.execute(
                    """
                    UPDATE guardian_receipts
                    SET status=%s, verified_by=%s, verified_at=%s, updated_at=%s
                    WHERE id=%s AND school_id=%s
                    """,
                    (status, session.get("username") or "Admin", now, now, rid, sid),
                )
                db.commit()
                flash(f"Receipt {status}.", "success")
        cur.execute(
            """
            SELECT * FROM guardian_receipts
            WHERE school_id=%s
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (sid,),
        )
        receipts = cur.fetchall() or []
    finally:
        try:
            db.close()
        except Exception:
            pass

    return render_template("admin/guardian_receipts.html", receipts=receipts)

    # The logic below is intentionally bypassed to prevent
    # any Schools management UI or text from rendering.


@admin_bp.route("/whatsapp/test", methods=["POST"])
def whatsapp_test():
    guard = _require_admin()
    if guard is not None:
        return guard
    to = (request.form.get("to") or "").strip()
    msg = (request.form.get("message") or "").strip() or "Hello from Fee Management (test)."
    if not to:
        flash("Enter a destination number.", "warning")
        return redirect(url_for("admin.dashboard"))
    template = current_app.config.get("WHATSAPP_TEMPLATE_NAME") or ""
    if template:
        ok, err = send_whatsapp_template(to, template_name=template, language=current_app.config.get("WHATSAPP_TEMPLATE_LANG", "en_US"), body_parameters=[msg])
    else:
        ok, err = send_whatsapp_text(to, msg)
    if ok:
        flash("WhatsApp test message sent.", "success")
    else:
        flash(f"WhatsApp send failed: {err}", "error")
    return redirect(url_for("admin.dashboard"))


@admin_bp.route("/billing", methods=["GET", "POST"])
def billing():
    guard = _require_admin()
    if guard is not None:
        return guard
    if request.method == "POST":
        # Treat input as M-Pesa reference (unique receipt/transaction code)
        raw_ref = (request.form.get("license_key") or "").strip()
        ref = "".join(ch for ch in raw_ref.upper() if ch.isalnum())
        if not ref:
            flash("Enter an M-Pesa reference.", "error")
            return redirect(url_for("admin.billing"))

        # Basic format guard (M-Pesa codes are typically 10–12 alphanumerics)
        if len(ref) < 8 or len(ref) > 20:
            flash("That doesn’t look like a valid M-Pesa reference.", "error")
            return redirect(url_for("admin.billing"))

        # Enforce one-time use via DB table
        db = _db(); ensure_pro_activations_table(db); cur = db.cursor(dictionary=True)
        cur.execute("SELECT id FROM pro_activations WHERE mpesa_ref=%s LIMIT 1", (ref,))
        row = cur.fetchone()
        if row:
            db.close()
            flash("This M-Pesa reference was already used.", "error")
            return redirect(url_for("admin.billing"))

        # Optionally, you can verify the reference via Daraja API here.
        # For now we trust the reference and activate Pro.
        from datetime import datetime as _dt
        cur = db.cursor()
        cur.execute(
            "INSERT INTO pro_activations (mpesa_ref, activated_at) VALUES (%s, %s)",
            (ref, _dt.now()),
        )
        db.commit(); db.close()

        # Generate a license-like key from the reference so existing checks pass
        import hashlib as _hashlib
        h6 = _hashlib.sha1(ref.encode("utf-8")).hexdigest()[:6].upper()
        license_key = f"CS-PRO-{ref}-{h6}"
        set_license_key(license_key)
        flash("M-Pesa reference accepted. Pro features unlocked!", "success")
        return redirect(url_for("admin.billing"))
    # Compute mpesa config status for template hints
    def _cfg_or(key: str) -> str:
        v = current_app.config.get(key) or get_setting(key)
        return (v or "").strip()
    mpesa_ok = all([
        _cfg_or("DARAJA_CONSUMER_KEY"),
        _cfg_or("DARAJA_CONSUMER_SECRET"),
        _cfg_or("DARAJA_SHORT_CODE"),
        _cfg_or("DARAJA_PASSKEY"),
    ])
    return render_template(
        "billing.html",
        is_pro=is_pro_enabled(),
        license_key=get_license_key(),
        upgrade_link=upgrade_url(),
        mpesa_ok=mpesa_ok,
    )


@admin_bp.route("/audit")
def audit_logs():
    guard = _require_admin()
    if guard is not None:
        return guard
    logs = fetch_audit_logs(session.get("school_id"))
    sanitized_logs = [_serialize_audit_log(log) for log in logs]
    return render_template("admin/audit_logs.html", logs=sanitized_logs)


@admin_bp.route("/audit/logs")
def audit_logs_stream():
    guard = _require_admin()
    if guard is not None:
        return guard
    logs = fetch_audit_logs(session.get("school_id"), limit=200)
    payload = [_serialize_audit_log(log) for log in logs]
    return jsonify({"logs": payload})


@admin_bp.route("/mpesa", methods=["GET", "POST"])
def mpesa_config():
    guard = _require_admin()
    if guard is not None:
        return guard

    keys = [
        "DARAJA_ENV",
        "DARAJA_CONSUMER_KEY",
        "DARAJA_CONSUMER_SECRET",
        "DARAJA_SHORT_CODE",
        "DARAJA_PASSKEY",
        "DARAJA_CALLBACK_URL",
        "DARAJA_ACCOUNT_REF",
        "DARAJA_TRANSACTION_DESC",
        "DARAJA_B2C_SHORT_CODE",
        "DARAJA_B2C_INITIATOR_NAME",
        "DARAJA_B2C_SECURITY_CREDENTIAL",
        "DARAJA_B2C_RESULT_URL",
        "DARAJA_B2C_TIMEOUT_URL",
        "DARAJA_B2C_COMMAND",
        "DARAJA_B2C_OCCASION",
    ]

    if request.method == "POST":
        for k in keys:
            val = (request.form.get(k) or "").strip()
            set_setting(k, val)
            # Also reflect immediately in process config
            current_app.config[k] = val
        flash("M-Pesa configuration saved.", "success")
        return redirect(url_for("admin.mpesa_config"))

    # Read current values (env/config override DB if present)
    values = {}
    for k in keys:
        v = (current_app.config.get(k) or get_setting(k) or "").strip()
        values[k] = v
    return render_template("mpesa_config.html", values=values)


@admin_bp.route("/school", methods=["GET", "POST"])
def school_profile():
    guard = _require_admin()
    if guard is not None:
        return guard

    keys = [
        "APP_NAME",
        "BRAND_NAME",
        "PORTAL_TITLE",
        "SCHOOL_ADDRESS",
        "SCHOOL_PHONE",
        "SCHOOL_EMAIL",
        "SCHOOL_WEBSITE",
        "SCHOOL_PAYBILL",
    ]
    penalty_keys = [
        "LATE_PENALTY_KIND",
        "LATE_PENALTY_VALUE",
        "LATE_PENALTY_GRACE_DAYS",
    ]

    if request.method == "POST":
        sid = session.get("school_id")
        if not sid:
            flash("Select a school before saving its profile.", "warning")
            return redirect(url_for("choose_school", next=url_for("admin.school_profile")))
        for k in keys:
            val = (request.form.get(k) or "").strip()
            # Save per-school to avoid cross-tenant conflicts
            set_school_setting(k, val, school_id=sid)

        # Handle logo upload (optional)
        try:
            from werkzeug.utils import secure_filename
            file = request.files.get("SCHOOL_LOGO")
            if file and file.filename:
                fname = secure_filename(file.filename)
                # Only allow basic image extensions
                allowed = {"png", "jpg", "jpeg", "gif", "webp"}
                ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
                if ext in allowed:
                    # Save under static/uploads/schools/<sid>/logo.<ext>
                    import os
                    sid = int(session.get("school_id") or 0)
                    subdir = os.path.join("static", "uploads", "schools", str(sid))
                    os.makedirs(subdir, exist_ok=True)
                    target_name = f"logo.{ext}"
                    path_fs = os.path.join(subdir, target_name)
                    file.save(path_fs)
                    # Store relative static path for template usage via url_for('static', filename=...)
                    rel = os.path.join("uploads", "schools", str(sid), target_name).replace("\\", "/")
                    set_school_setting("SCHOOL_LOGO_URL", rel, school_id=sid)
                    flash("Logo uploaded.", "success")
                else:
                    flash("Unsupported logo format. Use PNG/JPG/GIF/WEBP.", "warning")
        except Exception as e:
            flash(f"Logo upload failed: {e}", "error")

        # Persist penalty settings
        try:
            for k in penalty_keys:
                val = (request.form.get(k) or "").strip()
                set_school_setting(k, val, school_id=sid)
        except Exception:
            pass

        flash("School profile saved.", "success")
        return redirect(url_for("admin.school_profile"))

    # Load current values with config/env override
    values = {}
    for k in keys:
        v = (get_setting(k) or current_app.config.get(k) or "").strip()
        values[k] = v
    # Include existing logo path if any
    values["SCHOOL_LOGO_URL"] = (get_setting("SCHOOL_LOGO_URL") or current_app.config.get("SCHOOL_LOGO_URL") or "").strip()
    # Penalty settings
    values["LATE_PENALTY_KIND"] = (get_setting("LATE_PENALTY_KIND") or current_app.config.get("LATE_PENALTY_KIND") or "").strip()
    values["LATE_PENALTY_VALUE"] = (get_setting("LATE_PENALTY_VALUE") or current_app.config.get("LATE_PENALTY_VALUE") or "").strip()
    values["LATE_PENALTY_GRACE_DAYS"] = (get_setting("LATE_PENALTY_GRACE_DAYS") or current_app.config.get("LATE_PENALTY_GRACE_DAYS") or "").strip()
    return render_template("school_profile.html", values=values)


@admin_bp.route("/school/delete", methods=["POST"])
def school_profile_delete():
    guard = _require_admin()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before deleting its profile.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.school_profile")))
    confirmation = (request.form.get("delete_confirmation") or "").strip().lower()
    reason = (request.form.get("delete_reason") or "").strip()
    if confirmation not in ("delete", "delete school", "archive"):
        flash("Type DELETE or DELETE SCHOOL to confirm profile removal.", "warning")
        return redirect(url_for("admin.school_profile"))
    now = datetime.utcnow()
    set_school_setting("SCHOOL_PROFILE_DELETED_AT", now.isoformat(), school_id=sid)
    set_school_setting("SCHOOL_STATUS", "archived", school_id=sid)
    set_school_setting("SCHOOL_PROFILE_DELETE_REASON", reason, school_id=sid)
    session.pop("school_id", None)
    session.pop("school_code", None)
    flash("School profile archived. Please contact support if you need to restore it.", "success")
    return redirect(url_for("auth.entry"))


@admin_bp.route("/settings", methods=["GET", "POST"])
def access_settings():
    guard = _require_admin()
    if guard is not None:
        return guard

    sid = session.get("school_id")

    form_kind = (request.form.get("form") or "").strip().lower() if request.method == "POST" else ""

    if request.method == "POST":
        if form_kind == "access":
            username = (request.form.get("APP_LOGIN_USERNAME") or "").strip()
            password = (request.form.get("APP_LOGIN_PASSWORD") or "").strip()
            if not sid:
                flash("Select a school before editing access settings.", "warning")
                return redirect(url_for("choose_school", next=url_for("admin.access_settings")))
            if username:
                set_school_setting("APP_LOGIN_USERNAME", username, school_id=sid)
            if password:
                try:
                    set_school_setting("APP_LOGIN_PASSWORD", hash_password(password), school_id=sid)
                except Exception:
                    set_school_setting("APP_LOGIN_PASSWORD", password, school_id=sid)
            flash("Access settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "admin_pwd":
            current = (request.form.get("current_password") or "").strip()
            new = (request.form.get("new_password") or "").strip()
            confirm = (request.form.get("confirm_password") or "").strip()
            stored = _get_global_admin_password() or "9133"
            if not _verify_admin_current_password(current):
                flash("Current password is incorrect.", "error")
                return redirect(url_for("admin.access_settings"))
            if not new or len(new) < 6:
                flash("New password must be at least 6 characters.", "warning")
                return redirect(url_for("admin.access_settings"))
            if new != confirm:
                flash("New password and confirmation do not match.", "warning")
                return redirect(url_for("admin.access_settings"))
            if verify_password(stored, new):
                flash("New password must be different from the current one.", "warning")
                return redirect(url_for("admin.access_settings"))
            new_hash = hash_password(new)
            try:
                set_setting("ADMIN_PASSWORD", new_hash)
                current_app.config["ADMIN_PASSWORD"] = new_hash
                try:
                    if sid:
                        set_school_setting("APP_LOGIN_PASSWORD", new_hash, school_id=sid)
                except Exception:
                    pass
                flash("Admin password updated successfully.", "success")
            except Exception as e:
                flash(f"Failed to update password: {e}", "error")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "whatsapp":
            token = (request.form.get("WHATSAPP_ACCESS_TOKEN") or "").strip()
            phone_id = (request.form.get("WHATSAPP_PHONE_NUMBER_ID") or "").strip()
            template_name = (request.form.get("WHATSAPP_TEMPLATE_NAME") or "").strip()
            template_lang = (request.form.get("WHATSAPP_TEMPLATE_LANG") or "en_US").strip() or "en_US"
            set_setting("WHATSAPP_ACCESS_TOKEN", token)
            set_setting("WHATSAPP_PHONE_NUMBER_ID", phone_id)
            set_setting("WHATSAPP_TEMPLATE_NAME", template_name)
            set_setting("WHATSAPP_TEMPLATE_LANG", template_lang)
            current_app.config["WHATSAPP_ACCESS_TOKEN"] = token
            current_app.config["WHATSAPP_PHONE_NUMBER_ID"] = phone_id
            current_app.config["WHATSAPP_TEMPLATE_NAME"] = template_name
            current_app.config["WHATSAPP_TEMPLATE_LANG"] = template_lang
            flash("WhatsApp settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "reminders":
            email_col = (request.form.get("REMINDER_EMAIL_COLUMN") or "email").strip() or "email"
            default_msg = (request.form.get("REMINDER_DEFAULT_MESSAGE") or "").strip()
            if not sid:
                flash("Select a school before editing reminder settings.", "warning")
                return redirect(url_for("choose_school", next=url_for("admin.access_settings")))
            set_school_setting("REMINDER_EMAIL_COLUMN", email_col, school_id=sid)
            set_school_setting("REMINDER_DEFAULT_MESSAGE", default_msg, school_id=sid)
            flash("Reminder settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "ai":
            # Store Vertex AI configuration (env still supported)
            ai_keys = [
                "VERTEX_PROJECT_ID",
                "VERTEX_LOCATION",
                "GOOGLE_APPLICATION_CREDENTIALS",
                "VERTEX_GEMINI_MODEL",
            ]
            for k in ai_keys:
                val = (request.form.get(k) or "").strip()
                set_setting(k, val)
                try:
                    current_app.config[k] = val
                except Exception:
                    pass
            flash("Vertex AI settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "portal":
            # Portal token config and global rotation
            max_age = (request.form.get("PORTAL_TOKEN_MAX_AGE_DAYS") or "").strip()
            rotate = (request.form.get("ROTATE_ALL_NOW") or "").strip()
            if not sid:
                flash("Select a school before editing portal tokens.", "warning")
                return redirect(url_for("choose_school", next=url_for("admin.access_settings")))
            if max_age:
                try:
                    set_school_setting("PORTAL_TOKEN_MAX_AGE_DAYS", int(max_age), school_id=sid)
                except Exception:
                    set_school_setting("PORTAL_TOKEN_MAX_AGE_DAYS", max_age, school_id=sid)
            if rotate == "1":
                try:
                    now_ts = int(datetime.now().timestamp())
                    set_school_setting("PORTAL_TOKEN_ROLLOVER_AT", now_ts, school_id=sid)
                except Exception:
                    pass
            flash("Portal settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

    values = {}
    values["APP_LOGIN_USERNAME"] = (get_setting("APP_LOGIN_USERNAME") or current_app.config.get("LOGIN_USERNAME", "user")).strip()
    values["APP_LOGIN_PASSWORD"] = ""
    values["WHATSAPP_ACCESS_TOKEN"] = (get_setting("WHATSAPP_ACCESS_TOKEN") or current_app.config.get("WHATSAPP_ACCESS_TOKEN", ""))
    values["WHATSAPP_PHONE_NUMBER_ID"] = (get_setting("WHATSAPP_PHONE_NUMBER_ID") or current_app.config.get("WHATSAPP_PHONE_NUMBER_ID", ""))
    values["WHATSAPP_TEMPLATE_NAME"] = (get_setting("WHATSAPP_TEMPLATE_NAME") or current_app.config.get("WHATSAPP_TEMPLATE_NAME", ""))
    values["WHATSAPP_TEMPLATE_LANG"] = (get_setting("WHATSAPP_TEMPLATE_LANG") or current_app.config.get("WHATSAPP_TEMPLATE_LANG", "en_US") or "en_US")
    values["REMINDER_EMAIL_COLUMN"] = (get_setting("REMINDER_EMAIL_COLUMN") or "email")
    values["REMINDER_DEFAULT_MESSAGE"] = (get_setting("REMINDER_DEFAULT_MESSAGE") or DEFAULT_REMINDER_TEMPLATE)

    # Portal token settings
    values["PORTAL_TOKEN_MAX_AGE_DAYS"] = (get_setting("PORTAL_TOKEN_MAX_AGE_DAYS") or current_app.config.get("PORTAL_TOKEN_MAX_AGE_DAYS", 180))
    values["PORTAL_TOKEN_ROLLOVER_AT"] = (get_setting("PORTAL_TOKEN_ROLLOVER_AT") or "")

    # Vertex AI settings (pre-fill from DB or current config)
    values["VERTEX_PROJECT_ID"] = (get_setting("VERTEX_PROJECT_ID") or current_app.config.get("VERTEX_PROJECT_ID", ""))
    values["VERTEX_LOCATION"] = (get_setting("VERTEX_LOCATION") or current_app.config.get("VERTEX_LOCATION", "us-central1"))
    values["GOOGLE_APPLICATION_CREDENTIALS"] = (get_setting("GOOGLE_APPLICATION_CREDENTIALS") or current_app.config.get("GOOGLE_APPLICATION_CREDENTIALS", ""))
    values["VERTEX_GEMINI_MODEL"] = (get_setting("VERTEX_GEMINI_MODEL") or current_app.config.get("VERTEX_GEMINI_MODEL", "gemini-1.5-flash"))

    wa_ok, wa_reason = whatsapp_is_configured()
    pro = is_pro_enabled(current_app)
    return render_template("admin_settings.html", values=values, whatsapp_enabled=wa_ok, whatsapp_reason=wa_reason, pro_enabled=pro)


@admin_bp.route("/security", methods=["GET", "POST"])
def admin_security():
    guard = _require_admin()
    if guard is not None:
        return guard

    if request.method == "POST":
        current = (request.form.get("current_password") or "").strip()
        new = (request.form.get("new_password") or "").strip()
        confirm = (request.form.get("confirm_password") or "").strip()
        stored = _get_global_admin_password() or "9133"

        if not _verify_admin_current_password(current):
            flash("Current password is incorrect.", "error")
            return redirect(url_for("admin.admin_security"))
        if not new or len(new) < 6:
            flash("New password must be at least 6 characters.", "warning")
            return redirect(url_for("admin.admin_security"))
        if new != confirm:
            flash("New password and confirmation do not match.", "warning")
            return redirect(url_for("admin.admin_security"))
        if verify_password(stored, new):
            flash("New password must be different from the current one.", "warning")
            return redirect(url_for("admin.admin_security"))
        try:
            set_setting("ADMIN_PASSWORD", hash_password(new))
            flash("Admin password updated successfully.", "success")
        except Exception as e:
            flash(f"Failed to update password: {e}", "error")
        return redirect(url_for("admin.admin_security"))

    return redirect(url_for("admin.access_settings"))


@admin_bp.route("/backups", methods=["GET", "POST"])
def admin_backups():
    guard = _require_admin()
    if guard is not None:
        return guard

    sid = session.get("school_id")
    if not sid:
        flash("Select a school before managing backups.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.admin_backups")))

    history = []
    if request.method == "POST":
        try:
            result = create_backup(current_app, reason="manual admin trigger", school_id=sid)
            flash(
                f"Backup created ({format_east_africa(result['timestamp'], '%b %d, %Y %H:%M')}) and stored in history.",
                "success",
            )
        except BackupException as exc:
            flash(f"Backup failed: {exc}", "error")

    history = get_backup_history(current_app, limit=8, school_id=sid)

    schedule_str = current_app.config.get("BACKUP_SCHEDULE", "0 3 * * *")
    try:
        trigger = CronTrigger.from_crontab(schedule_str, timezone=EAST_AFRICA_TZ)
        next_run = trigger.get_next_fire_time(None, east_africa_now())
        next_run_readable = format_east_africa(next_run, "%b %d, %Y %H:%M") if next_run else None
    except Exception:
        next_run_readable = None

    backup_root = backup_root_for_school(current_app, sid)
    backup_directory = str(backup_root)
    backup_keep_days = current_app.config.get("BACKUP_KEEP_DAYS", 60)
    return render_template(
        "admin_backups.html",
        history=history,
        schedule=schedule_str,
        next_run=next_run_readable,
        backup_directory=backup_directory,
        backup_keep_days=backup_keep_days,
    )


@admin_bp.route("/backups/restore", methods=["POST"])
def admin_backups_restore():
    guard = _require_admin()
    if guard is not None:
        return guard

    sid = session.get("school_id")
    if not sid:
        flash("Select a school before restoring backups.", "warning")
        return redirect(url_for("choose_school", next=url_for("admin.admin_backups")))

    history = get_backup_history(current_app, limit=1, school_id=sid)
    if not history:
        flash("No backup history available to restore.", "warning")
        return redirect(url_for("admin.admin_backups"))

    latest = history[0]
    snapshot_path = latest.get("snapshot")
    if not snapshot_path:
        flash("Latest backup has no snapshot file to restore.", "error")
        return redirect(url_for("admin.admin_backups"))

    from pathlib import Path
    snapshot = Path(snapshot_path)
    if not snapshot.exists():
        flash("Snapshot file is missing. Cannot restore.", "error")
        return redirect(url_for("admin.admin_backups"))

    try:
        restore_result = restore_backup_snapshot(latest, current_app)
    except BackupException as exc:
        flash(f"Restore failed: {exc}", "error")
        return redirect(url_for("admin.admin_backups"))

    db_info = restore_result.get("database") or {}
    db_status = db_info.get("status")
    if db_status != "ok":
        reason = db_info.get("reason") or "unknown error"
        flash(f"Database restore failed: {reason}", "error")
        return redirect(url_for("admin.admin_backups"))

    assets_result = restore_result.get("assets") or []
    asset_issues = [a for a in assets_result if a.get("status") != "ok"]

    flash(
        f"Restored data from {format_east_africa(latest.get('timestamp'), '%b %d, %Y %H:%M')} (snapshot: {snapshot.name}).",
        "success",
    )
    if asset_issues:
        flash(
            f"{len(asset_issues)} asset archive(s) failed to restore; check logs for details.",
            "warning",
        )
    log_event(
        "backup_restore",
        detail=(
            f"Restore triggered for snapshot {snapshot.name} (db import ok, "
            f"{len(assets_result) - len(asset_issues)} assets refreshed, "
            f"{len(asset_issues)} issues)"
        ),
        target="backup:latest",
    )
    return redirect(url_for("admin.admin_backups"))


