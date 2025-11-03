from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
from datetime import datetime
import mysql.connector

from utils.whatsapp import whatsapp_is_configured, send_whatsapp_text, send_whatsapp_template
from utils.settings import get_setting, set_setting, set_school_setting
from utils.security import verify_password, hash_password, is_hashed
from utils.pro import is_pro_enabled, set_license_key, get_license_key, upgrade_url
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
from utils.security import hash_password


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


@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "").strip()
        stored = (get_setting("ADMIN_PASSWORD") or "").strip() or "9133"
        if verify_password(stored, password):
            session["is_admin"] = True
            flash("Welcome, admin!", "success")
            return redirect(url_for("admin.dashboard"))
        flash("Invalid password.", "error")
        return redirect(url_for("admin.login"))
    return render_template("admin_login.html")


@admin_bp.route("/logout")
def logout():
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
    ]
    penalty_keys = [
        "LATE_PENALTY_KIND",
        "LATE_PENALTY_VALUE",
        "LATE_PENALTY_GRACE_DAYS",
    ]

    if request.method == "POST":
        for k in keys:
            val = (request.form.get(k) or "").strip()
            # Save per-school to avoid cross-tenant conflicts
            set_school_setting(k, val)
            # Reflect immediately in running app
            current_app.config[k] = val

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
                    set_school_setting("SCHOOL_LOGO_URL", rel)
                    # Reflect immediately
                    current_app.config["SCHOOL_LOGO_URL"] = rel
                    flash("Logo uploaded.", "success")
                else:
                    flash("Unsupported logo format. Use PNG/JPG/GIF/WEBP.", "warning")
        except Exception as e:
            flash(f"Logo upload failed: {e}", "error")

        # Persist penalty settings
        try:
            for k in penalty_keys:
                val = (request.form.get(k) or "").strip()
                set_school_setting(k, val)
                current_app.config[k] = val
        except Exception:
            pass

        flash("School profile saved.", "success")
        return redirect(url_for("admin.school_profile"))

    # Load current values with config/env override
    values = {}
    for k in keys:
        v = (current_app.config.get(k) or get_setting(k) or "").strip()
        values[k] = v
    # Include existing logo path if any
    values["SCHOOL_LOGO_URL"] = (current_app.config.get("SCHOOL_LOGO_URL") or get_setting("SCHOOL_LOGO_URL") or "").strip()
    # Penalty settings
    values["LATE_PENALTY_KIND"] = (current_app.config.get("LATE_PENALTY_KIND") or get_setting("LATE_PENALTY_KIND") or "").strip()
    values["LATE_PENALTY_VALUE"] = (current_app.config.get("LATE_PENALTY_VALUE") or get_setting("LATE_PENALTY_VALUE") or "").strip()
    values["LATE_PENALTY_GRACE_DAYS"] = (current_app.config.get("LATE_PENALTY_GRACE_DAYS") or get_setting("LATE_PENALTY_GRACE_DAYS") or "").strip()
    return render_template("school_profile.html", values=values)


@admin_bp.route("/settings", methods=["GET", "POST"])
def access_settings():
    guard = _require_admin()
    if guard is not None:
        return guard

    form_kind = (request.form.get("form") or "").strip().lower() if request.method == "POST" else ""

    if request.method == "POST":
        if form_kind == "access":
            username = (request.form.get("APP_LOGIN_USERNAME") or "").strip()
            password = (request.form.get("APP_LOGIN_PASSWORD") or "").strip()
            if username:
                set_school_setting("APP_LOGIN_USERNAME", username)
                current_app.config["LOGIN_USERNAME"] = username or current_app.config.get("LOGIN_USERNAME", "user")
            if password:
                try:
                    set_school_setting("APP_LOGIN_PASSWORD", hash_password(password))
                except Exception:
                    set_school_setting("APP_LOGIN_PASSWORD", password)
            flash("Access settings saved.", "success")
            return redirect(url_for("admin.access_settings"))

        elif form_kind == "admin_pwd":
            current = (request.form.get("current_password") or "").strip()
            new = (request.form.get("new_password") or "").strip()
            confirm = (request.form.get("confirm_password") or "").strip()
            stored = (get_setting("ADMIN_PASSWORD") or "").strip() or "9133"
            if not verify_password(stored, current):
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
            try:
                set_setting("ADMIN_PASSWORD", hash_password(new))
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
            set_school_setting("REMINDER_EMAIL_COLUMN", email_col)
            set_school_setting("REMINDER_DEFAULT_MESSAGE", default_msg)
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

    values = {}
    values["APP_LOGIN_USERNAME"] = (get_setting("APP_LOGIN_USERNAME") or current_app.config.get("LOGIN_USERNAME", "user")).strip()
    values["APP_LOGIN_PASSWORD"] = ""
    values["WHATSAPP_ACCESS_TOKEN"] = (get_setting("WHATSAPP_ACCESS_TOKEN") or current_app.config.get("WHATSAPP_ACCESS_TOKEN", ""))
    values["WHATSAPP_PHONE_NUMBER_ID"] = (get_setting("WHATSAPP_PHONE_NUMBER_ID") or current_app.config.get("WHATSAPP_PHONE_NUMBER_ID", ""))
    values["WHATSAPP_TEMPLATE_NAME"] = (get_setting("WHATSAPP_TEMPLATE_NAME") or current_app.config.get("WHATSAPP_TEMPLATE_NAME", ""))
    values["WHATSAPP_TEMPLATE_LANG"] = (get_setting("WHATSAPP_TEMPLATE_LANG") or current_app.config.get("WHATSAPP_TEMPLATE_LANG", "en_US") or "en_US")
    values["REMINDER_EMAIL_COLUMN"] = (get_setting("REMINDER_EMAIL_COLUMN") or "email")
    values["REMINDER_DEFAULT_MESSAGE"] = (get_setting("REMINDER_DEFAULT_MESSAGE") or "Hello {name}, this is a fee reminder from {school_name}. Your outstanding balance is KES {balance}. Kindly clear at your earliest convenience.")

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

        stored = (get_setting("ADMIN_PASSWORD") or "").strip() or "9133"
        if not verify_password(stored, current):
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


