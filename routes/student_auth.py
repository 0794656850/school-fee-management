from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, session, flash, current_app
import mysql.connector
from urllib.parse import urlparse
import os
from datetime import datetime, timedelta
from utils.security import verify_password, hash_password
from utils.tenant import slugify_code, get_or_create_school
from utils.login_otp import generate_login_otp, mask_email, send_portal_login_otp

try:
    from utils.gmail_api import (
        send_email as gmail_send_email,
        send_email_html as gmail_send_email_html,
    )
except Exception:
    def gmail_send_email(*args, **kwargs):  # type: ignore
        return False
    def gmail_send_email_html(*args, **kwargs):  # type: ignore
        return False


student_auth_bp = Blueprint("student_auth", __name__, url_prefix="/s")

LOGIN_OTP_EXPIRES_MINUTES = 20


def _student_otp_context() -> dict:
    return session.get("student_otp_context", {})


def _clear_student_otp_context() -> None:
    session.pop("student_otp_context", None)


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


def ensure_student_portal_columns(conn) -> None:
    cur = conn.cursor()
    # password hash column
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'portal_password_hash'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE students ADD COLUMN portal_password_hash VARCHAR(256) NULL AFTER phone")
    except Exception:
        try: conn.rollback()
        except Exception: pass
    # account email (optional per-student email to login)
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'account_email'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE students ADD COLUMN account_email VARCHAR(190) NULL AFTER portal_password_hash")
    except Exception:
        try: conn.rollback()
        except Exception: pass
    try:
        conn.commit()
    except Exception:
        pass


@student_auth_bp.route("/login", methods=["GET", "POST"])
def student_login():
    if request.method == "POST":
        school_raw = (request.form.get("school") or "").strip()
        reg_no = (request.form.get("regNo") or request.form.get("admission_no") or "").strip()
        password = (request.form.get("password") or "").strip()
        if not school_raw or not reg_no or not password:
            flash("Enter school code/name, admission number and password.", "warning")
            return redirect(url_for("student_auth.student_login"))

        code = slugify_code(school_raw)
        db = _db(); cur = db.cursor(dictionary=True)
        try:
            # Resolve school id
            cur.execute("SELECT id FROM schools WHERE code=%s", (code,))
            row = cur.fetchone()
            if not row:
                flash("School not found. Check the code.", "error")
                return redirect(url_for("student_auth.student_login"))
            school_id = int(row["id"]) if isinstance(row, dict) else int(row[0])
            # Ensure columns
            ensure_student_portal_columns(db)
            # Find student
            cur.execute("SELECT id, name, admission_no AS regNo, portal_password_hash FROM students WHERE school_id=%s AND admission_no=%s", (school_id, reg_no))
            s = cur.fetchone()
            if not s or not s.get("portal_password_hash"):
                flash("Account not found. Please sign up first.", "warning")
                return redirect(url_for("student_auth.student_signup"))
            if not verify_password(s.get("portal_password_hash"), password):
                flash("Incorrect password.", "error")
                return redirect(url_for("student_auth.student_login"))
            target_email = (s.get("email") or s.get("parent_email") or "").strip()
            if not target_email:
                flash("No email on record. Ask the school to add one to proceed with OTP login.", "warning")
                return redirect(url_for("student_auth.student_login"))
            _clear_student_otp_context()
            otp_code = generate_login_otp()
            sent = send_portal_login_otp(
                target_email,
                s.get("name") or str(s.get("admission_no")) or "Student",
                "Student Portal",
                otp_code,
                LOGIN_OTP_EXPIRES_MINUTES,
            )
            if not sent:
                flash("Failed to send the verification code. Try again shortly.", "error")
                return redirect(url_for("student_auth.student_login"))
            session["student_otp_context"] = {
                "student_id": int(s["id"]),
                "school_id": school_id,
                "name": s.get("name") or "",
                "email": target_email,
                "code": otp_code,
                "until": (datetime.now() + timedelta(minutes=LOGIN_OTP_EXPIRES_MINUTES)).timestamp(),
                "sent_at": datetime.now().timestamp(),
            }
            flash("We sent a one-time code to your email. Enter it to continue.", "info")
            return redirect(url_for("student_auth.student_login_verify"))
        finally:
            try: db.close()
            except Exception: pass
    return render_template("student_login.html")


@student_auth_bp.route("/login/verify", methods=["GET", "POST"])
def student_login_verify():
    ctx = _student_otp_context()
    if not ctx:
        flash("Enter your credentials first so we can send your login code.", "warning")
        return redirect(url_for("student_auth.student_login"))

    now_ts = datetime.now().timestamp()
    remaining = max(0, int(ctx.get("until", 0) - now_ts))
    if remaining <= 0:
        _clear_student_otp_context()
        flash("The code expired after 20 minutes. Log in again to receive a fresh one.", "warning")
        return redirect(url_for("student_auth.student_login"))

    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        if not code:
            flash("Enter the six-digit code sent to your email.", "warning")
            return redirect(url_for("student_auth.student_login_verify"))
        if code != ctx.get("code"):
            flash("Wrong code. Check your email and try again.", "error")
            return redirect(url_for("student_auth.student_login_verify"))

        sid = int(ctx.get("student_id") or 0)
        school_id = int(ctx.get("school_id") or 0)
        session["student_logged_in"] = True
        session["student_id"] = sid
        session["student_name"] = ctx.get("name") or ""
        session["school_id"] = school_id
        _clear_student_otp_context()
        flash("Welcome back! You're logged in.", "success")
        return redirect(url_for("student_portal.view", token="me"))

    return render_template(
        "login_otp.html",
        portal_title="Student portal verification",
        portal_label="Student OTP",
        heading="Check your inbox",
        summary="Enter the code we emailed to confirm it is really you accessing the student portal.",
        email_display=mask_email(ctx.get("email")),
        countdown_seconds=remaining,
        countdown_label=f"{remaining // 60} min {remaining % 60} sec",
        form_action=url_for("student_auth.student_login_verify"),
        resend_url=url_for("student_auth.student_login_verify_resend"),
        back_url=url_for("student_auth.student_login"),
        portal_note="Codes refresh every 20 minutes for extra safety.",
    )


@student_auth_bp.route("/login/verify/resend", methods=["POST"])
def student_login_verify_resend():
    ctx = _student_otp_context()
    if not ctx:
        flash("Log in first so we know where to send the new code.", "warning")
        return redirect(url_for("student_auth.student_login"))

    target_email = ctx.get("email")
    if not target_email:
        flash("No email is associated with this account. Ask the school to add one.", "error")
        return redirect(url_for("student_auth.student_login"))

    otp_code = generate_login_otp()
    ctx["code"] = otp_code
    ctx["until"] = (datetime.now() + timedelta(minutes=LOGIN_OTP_EXPIRES_MINUTES)).timestamp()
    ctx["sent_at"] = datetime.now().timestamp()
    session["student_otp_context"] = ctx
    sent = send_portal_login_otp(
        target_email,
        ctx.get("name") or "Student",
        "Student Portal",
        otp_code,
        LOGIN_OTP_EXPIRES_MINUTES,
    )
    if not sent:
        flash("Unable to resend the code right now. Try again shortly.", "error")
        return redirect(url_for("student_auth.student_login_verify"))

    flash("We resent a new code to your email. It will arrive in seconds.", "info")
    return redirect(url_for("student_auth.student_login_verify"))


@student_auth_bp.route("/signup", methods=["GET", "POST"])
def student_signup():
    # Two-stage signup: verify identity (DOB if present, else email OTP), then set password
    stage = request.args.get("stage", "start")
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        db = _db(); cur = db.cursor(dictionary=True)
        ensure_student_portal_columns(db)
        try:
            if action == "start":
                school_raw = (request.form.get("school") or "").strip()
                reg_no = (request.form.get("regNo") or request.form.get("admission_no") or "").strip()
                if not school_raw or not reg_no:
                    flash("Enter school and admission number.", "warning")
                    return redirect(url_for("student_auth.student_signup"))
                code = slugify_code(school_raw)
                cur.execute("SELECT id, name FROM schools WHERE code=%s", (code,))
                sc = cur.fetchone()
                if not sc:
                    flash("School not found.", "error")
                    return redirect(url_for("student_auth.student_signup"))
                school_id = int(sc["id"]) if isinstance(sc, dict) else int(sc[0])
                cur.execute("SELECT id, name, admission_no AS regNo, email, parent_email FROM students WHERE school_id=%s AND admission_no=%s", (school_id, reg_no))
                s = cur.fetchone()
                if not s:
                    flash("We couldn't find your record. Contact the school office.", "error")
                    return redirect(url_for("student_auth.student_signup"))
                # If DOB column exists, require it to verify
                dob_ok = False
                try:
                    cur2 = db.cursor()
                    cur2.execute("SHOW COLUMNS FROM students LIKE 'dob'")
                    if cur2.fetchone():
                        dob_str = (request.form.get("dob") or "").strip()
                        if not dob_str:
                            flash("Enter Date of Birth to verify.", "warning")
                            return redirect(url_for("student_auth.student_signup"))
                        try:
                            cur2 = db.cursor()
                            cur2.execute("SELECT dob FROM students WHERE id=%s", (int(s["id"]),))
                            row = cur2.fetchone()
                            if row and str(row[0]) == dob_str:
                                dob_ok = True
                        except Exception:
                            dob_ok = False
                except Exception:
                    pass
                if not dob_ok:
                    # Fall back to email OTP
                    target = (s.get("email") or s.get("parent_email") or "").strip()
                    if not target:
                        flash("No email on record. Ask the school to add your email or DOB.", "warning")
                        return redirect(url_for("student_auth.student_signup"))
                    import random
                    otp = str(random.randint(100000, 999999))
                    session["signup_otp"] = otp
                    session["signup_student_id"] = int(s["id"])
                    session["signup_school_id"] = school_id
                    session["signup_until"] = (datetime.now() + timedelta(minutes=10)).timestamp()
                    subject = "Verify your student account"
                    html = f"<p>Your verification code is <strong>{otp}</strong>. It expires in 10 minutes.</p>"
                    sent = gmail_send_email_html(target, subject, html) or gmail_send_email(target, subject, f"Code: {otp}")
                    if not sent:
                        flash("Failed to send verification email. Try again later.", "error")
                        return redirect(url_for("student_auth.student_signup"))
                    flash("We sent a verification code to your email.", "info")
                    return redirect(url_for("student_auth.student_signup", stage="verify"))
                # If DOB matched, proceed to set password
                session["signup_student_id"] = int(s["id"])
                session["signup_school_id"] = school_id
                session["signup_until"] = (datetime.now() + timedelta(minutes=10)).timestamp()
                return redirect(url_for("student_auth.student_signup", stage="setpwd"))

            elif action == "verify":
                code = (request.form.get("code") or "").strip()
                if not code or code != session.get("signup_otp"):
                    flash("Invalid verification code.", "error")
                    return redirect(url_for("student_auth.student_signup", stage="verify"))
                if float(session.get("signup_until", 0)) < datetime.now().timestamp():
                    flash("Verification expired. Start again.", "warning")
                    return redirect(url_for("student_auth.student_signup"))
                return redirect(url_for("student_auth.student_signup", stage="setpwd"))

            elif action == "setpwd":
                if float(session.get("signup_until", 0)) < datetime.now().timestamp():
                    flash("Session expired. Start again.", "warning")
                    return redirect(url_for("student_auth.student_signup"))
                sid = int(session.get("signup_student_id", 0) or 0)
                school_id = int(session.get("signup_school_id", 0) or 0)
                if not sid or not school_id:
                    flash("Start signup again.", "warning")
                    return redirect(url_for("student_auth.student_signup"))
                pwd = (request.form.get("password") or "").strip()
                cpwd = (request.form.get("confirm_password") or "").strip()
                if not pwd or len(pwd) < 6 or pwd != cpwd:
                    flash("Password must be 6+ chars and match.", "warning")
                    return redirect(url_for("student_auth.student_signup", stage="setpwd"))
                hp = hash_password(pwd)
                cur = db.cursor()
                cur.execute("UPDATE students SET portal_password_hash=%s WHERE id=%s", (hp, sid))
                db.commit()
                # Login and go to portal
                session["student_logged_in"] = True
                session["student_id"] = sid
                session["school_id"] = school_id
                flash("Account created.", "success")
                return redirect(url_for("student_portal.view", token="me"))
        finally:
            try: db.close()
            except Exception: pass

    # Render step templates
    return render_template("student_signup.html", stage=stage)


@student_auth_bp.route("/logout")
def student_logout():
    session.pop("student_logged_in", None)
    session.pop("student_id", None)
    session.pop("student_name", None)
    flash("Logged out.", "info")
    return redirect(url_for("student_auth.student_login"))


@student_auth_bp.route("/password", methods=["GET", "POST"])
def change_password():
    """Allow a signed-in student/guardian to change their portal password."""
    from flask import session as _session
    if not _session.get("student_logged_in") or not int(_session.get("student_id") or 0):
        flash("Please log in first.", "warning")
        return redirect(url_for("student_auth.student_login"))
    sid = int(_session.get("student_id"))

    if request.method == "POST":
        current = (request.form.get("current_password") or "").strip()
        newpwd = (request.form.get("new_password") or "").strip()
        confirm = (request.form.get("confirm_password") or "").strip()
        if not newpwd or len(newpwd) < 6 or newpwd != confirm:
            flash("Password must be at least 6 characters and match.", "warning")
            return redirect(url_for("student_auth.change_password"))
        db = _db(); cur = db.cursor(dictionary=True)
        try:
            ensure_student_portal_columns(db)
            cur.execute("SELECT portal_password_hash FROM students WHERE id=%s", (sid,))
            row = cur.fetchone() or {}
            if row.get("portal_password_hash") and not verify_password(row.get("portal_password_hash"), current):
                flash("Current password is incorrect.", "error")
                return redirect(url_for("student_auth.change_password"))
            hp = hash_password(newpwd)
            cur2 = db.cursor()
            cur2.execute("UPDATE students SET portal_password_hash=%s WHERE id=%s", (hp, sid))
            db.commit()
            flash("Password updated.", "success")
            return redirect(url_for("student_portal.view", token="me"))
        finally:
            try: db.close()
            except Exception: pass

    return render_template("student_change_password.html")

