from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, Response, session
import json
import mysql.connector
from datetime import datetime
import csv
from io import StringIO
from urllib.parse import urlparse
import os
from config import Config
from routes.reminder_routes import reminder_bp
from routes.admin_routes import admin_bp
from routes.auth_routes import auth_bp
from routes.credit_routes import credit_bp
from routes.credit_routes import ensure_credit_ops_table, ensure_students_credit_column, ensure_credit_transfers_table
from routes.term_routes import (
    term_bp,
    ensure_academic_terms_table,
    ensure_payments_term_columns,
    get_or_seed_current_term,
    ensure_student_enrollments_table,
    ensure_term_fees_table,
)
# Extend term routes (bulk flat fees)
import routes.term_flat_routes  # noqa: F401 - registers extra routes on term_bp
from utils.notify import normalize_phone
from utils.gmail_api import send_email as gmail_send_email
from routes.mpesa_routes import mpesa_bp
from utils.settings import get_setting, set_school_setting
from utils.users import ensure_user_tables
# Audit trail removed
from routes.ai_routes import ai_bp
from utils.audit import ensure_audit_table, log_event
from utils.ledger import ensure_ledger_table, add_entry
from utils.tenant import (
    ensure_school_id_columns,
    ensure_schools_table,
    get_or_create_school,
    slugify_code,
    bootstrap_new_school,
    ensure_unique_indices_per_school,
)

app = Flask(__name__)

# Load configuration from Config (falls back to sensible defaults inside Config)
app.config.from_object(Config)

# Ensure secret key is set (Config provides default). Keeping compatibility if env overrides.
app.secret_key = app.config.get("SECRET_KEY", os.environ.get("SECRET_KEY", "secret123"))

# Register blueprints
app.register_blueprint(reminder_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(credit_bp)
app.register_blueprint(term_bp)
app.register_blueprint(mpesa_bp)
app.register_blueprint(ai_bp)


# Inject branding/config variables into all templates for universal theming
@app.context_processor
def inject_branding():
    # Resolve brand/app names with DB settings taking precedence over config defaults
    # Prefer per-school settings, gracefully falling back to app/global config
    school_name = (
        get_setting("SCHOOL_NAME")
        or get_setting("APP_NAME")
        or app.config.get("APP_NAME")
    )
    brand = (
        get_setting("BRAND_NAME")
        or (school_name or "Fee Management System")
    )
    portal_title = (
        get_setting("PORTAL_TITLE")
        or app.config.get("PORTAL_TITLE")
        or "Fee Management portal"
    )
    app_title = ((school_name or brand) + f" {portal_title}").strip()
    # Resolve current academic term/year (best effort)
    cy, ct = None, None
    try:
        _conn = get_db_connection()
        try:
            cy, ct = get_or_seed_current_term(_conn)
        finally:
            _conn.close()
    except Exception:
        pass
    # Per-school first login marker for welcome banner
    first_login_at = None
    try:
        sid = session.get("school_id") if session else None
        if sid:
            _conn = get_db_connection()
            try:
                cur = _conn.cursor()
                cur.execute("SELECT first_login_at FROM schools WHERE id=%s", (sid,))
                row = cur.fetchone()
                if row is not None:
                    try:
                        first_login_at = row[0] if not isinstance(row, dict) else row.get("first_login_at")
                    except Exception:
                        first_login_at = None
            finally:
                _conn.close()
    except Exception:
        pass
    # Resolve per-school logo if uploaded
    school_logo_url = get_setting("SCHOOL_LOGO_URL")
    logo_primary = school_logo_url or app.config.get("LOGO_PRIMARY", "css/lovato_logo.jpg")
    logo_secondary = school_logo_url or app.config.get("LOGO_SECONDARY", "css/lovato_logo1.jpg")

    # Late payment penalty settings (per-school)
    late_kind = (get_setting("LATE_PENALTY_KIND") or "").strip()  # 'percent' or 'flat'
    try:
        late_value = float(get_setting("LATE_PENALTY_VALUE") or 0)
    except Exception:
        late_value = 0.0
    try:
        late_grace = int(float(get_setting("LATE_PENALTY_GRACE_DAYS") or 0))
    except Exception:
        late_grace = 0

    return {
        "BRAND_NAME": brand,
        "PORTAL_TITLE": portal_title,
        "APP_TITLE": app_title,
        "LOGO_PRIMARY": logo_primary,
        "LOGO_SECONDARY": logo_secondary,
        "FAVICON": app.config.get("FAVICON", logo_primary),
        "CURRENT_YEAR": cy,
        "CURRENT_TERM": ct,
        "PRO_PRICE_KES": app.config.get("PRO_PRICE_KES", 1500),
        # School profile (for printables/docs)
        "SCHOOL_NAME": school_name or brand,
        "SCHOOL_ADDRESS": get_setting("SCHOOL_ADDRESS") or "",
        "SCHOOL_PHONE": get_setting("SCHOOL_PHONE") or "",
        "SCHOOL_EMAIL": get_setting("SCHOOL_EMAIL") or "",
        "SCHOOL_WEBSITE": get_setting("SCHOOL_WEBSITE") or "",
        "SUPPORT_PHONE": app.config.get("SUPPORT_PHONE", "+254794656850"),
        "SCHOOL_FIRST_LOGIN_AT": first_login_at,
        # Penalties (for invoice rendering)
        "LATE_PENALTY_KIND": late_kind,
        "LATE_PENALTY_VALUE": late_value,
        "LATE_PENALTY_GRACE_DAYS": late_grace,
        # Raw uploaded logo path if present
        "SCHOOL_LOGO_URL": school_logo_url or "",
    }

# ---------- AUTH GUARD ----------
@app.before_request
def require_login_for_app():
    # Allow static files and login routes
    path = request.path or "/"
    allowed_prefixes = (
        "/static/",
        "/mpesa/callback",
    )
    allowed_exact = {
        "/auth/login",
        "/auth/register",
        "/auth/register_school",
        "/admin/login",
        "/choose_school",
    }
    # Allow admin blueprint to self-guard; we just don't block its login
    if any(path.startswith(p) for p in allowed_prefixes) or path in allowed_exact:
        return None

    # If not logged in, redirect to login (except for admin routes which have own guard)
    if not session.get("user_logged_in"):
        # Let admin area be reachable separately (e.g., /admin, /admin/..)
        if path.startswith("/admin"):
            return None
        # API endpoints should also be protected
        if path != "/auth/login":
            return redirect(url_for("auth.login", next=path))
    # After login, require a selected school for app routes
    if not (path.startswith("/admin") or path.startswith("/auth")):
        if not session.get("school_id") and path != "/choose_school":
            return redirect(url_for("choose_school", next=path))
    return None


# Convenience: /login -> /auth/login
@app.route("/login")
def login_redirect():
    return redirect(url_for("auth.login"))


# ---------- DATABASE CONNECTION ----------
def get_db_connection():
    """Establish a connection to the MySQL database."""
    # Prefer credentials from SQLALCHEMY_DATABASE_URI to avoid hardcoding
    uri = app.config.get("SQLALCHEMY_DATABASE_URI", "")
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")

    if uri:
        try:
            parsed = urlparse(uri)
            # Only attempt to parse for MySQL-style URIs
            if parsed.scheme.startswith("mysql"):
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


# Ensure compatibility columns/tables exist on boot (without audit)
_c = None
try:
    _c = get_db_connection()
    try:
        # Ensure multi-tenant scaffolding exists
        ensure_schools_table(_c)
        ensure_school_id_columns(
            _c,
            (
                "students",
                "payments",
                "credit_operations",
                "credit_transfers",
                "academic_terms",
                "student_enrollments",
                "term_fees",
            ),
        )
        ensure_students_credit_column(_c)
        ensure_credit_ops_table(_c)
        ensure_credit_transfers_table(_c)
        # Academic term scaffolding
        ensure_academic_terms_table(_c)
        ensure_payments_term_columns(_c)
        # User tables for multi-user (premium-ready)
        try:
            ensure_user_tables(_c)
        except Exception:
            pass
        ensure_student_enrollments_table(_c)
        ensure_term_fees_table(_c)
        # Strengthen per-school uniqueness where safe
        try:
            ensure_unique_indices_per_school(_c)
        except Exception:
            pass
    except Exception:
        pass
finally:
    try:
        _c and _c.close()
    except Exception:
        pass

# ---------- SCHOOL SELECTION ----------
@app.route("/choose_school", methods=["GET", "POST"])
def choose_school():
    if request.method == "POST":
        raw_code = (request.form.get("school_code") or "").strip()
        name = (request.form.get("school_name") or "").strip()
        code = slugify_code(raw_code or name)
        if not code:
            flash("Enter a school name or code.", "warning")
            return redirect(url_for("choose_school"))
        db = get_db_connection()
        created = False
        try:
            # Check if exists first to decide on bootstrapping
            cur = db.cursor()
            cur.execute("SELECT id FROM schools WHERE code=%s", (code,))
            existing = cur.fetchone()
            sid = None
            if existing:
                sid = int(existing[0]) if not isinstance(existing, dict) else int(existing.get("id"))
            if not sid:
                # Restrict creating new schools to admin area
                if not session.get("is_admin"):
                    db.close()
                    flash("Only an administrator can create a new school. Please sign in to Admin and create it there.", "warning")
                    return redirect(url_for("admin.login", next=url_for("choose_school")))
                sid = get_or_create_school(db, code=code, name=name or code)
                created = True
            if created:
                try:
                    bootstrap_new_school(db, sid, name or code, code)
                except Exception:
                    pass
        finally:
            db.close()
        session["school_id"] = sid
        session["school_code"] = code
        if created:
            flash("School created. Sign in with default credentials: user / 9133.", "info")
        next_url = request.args.get("next") or request.form.get("next") or url_for("dashboard")
        return redirect(next_url)
    return render_template("choose_school.html", next_url=request.args.get("next", ""))
# ---------- DASHBOARD ----------
@app.route("/")
def dashboard():
    """Main dashboard with summary cards and recent payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Resolve current academic context
    try:
        current_year, current_term = get_or_seed_current_term(db)
    except Exception:
        current_year, current_term = None, None

    # Totals
    cursor.execute("SELECT COUNT(*) AS total FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_students = cursor.fetchone()["total"]

    # Total collected for current term (fallback to lifetime if context missing)
    if current_year and current_term in (1, 2, 3):
        cursor.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s",
            (session.get("school_id"), current_year, current_term),
        )
    else:
        cursor.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s",
            (session.get("school_id"),),
        )
    total_collected = cursor.fetchone()["total_collected"]

    # Detect correct balance column
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_balance = cursor.fetchone()["total_balance"]

    # Total credit
    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_credit = cursor.fetchone()["total_credit"]

    # Recent payments
    recent_sql_base = (
        """
        SELECT p.id, s.name, s.class_name, p.amount, p.method, p.date
        FROM payments p
        JOIN students s ON p.student_id = s.id
        WHERE p.school_id=%s {extra}
        ORDER BY p.date DESC
        LIMIT 5
        """
    )
    if current_year and current_term in (1, 2, 3):
        cursor.execute(
            recent_sql_base.format(extra="AND p.year=%s AND p.term=%s"),
            (session.get("school_id"), current_year, current_term),
        )
    else:
        cursor.execute(
            recent_sql_base.format(extra=""),
            (session.get("school_id"),),
        )
    recent_payments = cursor.fetchall()

    # Compute this term outstanding (fees for current term minus payments in current term)
    term_outstanding = 0.0
    try:
        if current_year and current_term in (1, 2, 3):
            school_id = session.get("school_id")
            cursor.execute("SELECT id FROM students WHERE school_id=%s", (school_id,))
            _rows = cursor.fetchall() or []
            ids = [r.get("id") for r in _rows]
            if ids:
                def _in_clause(seq):
                    return ",".join(["%s"] * len(seq))
                # Itemized per student
                cursor.execute(
                    f"SELECT student_id, COALESCE(SUM(amount),0) AS tsum FROM student_term_fee_items WHERE year=%s AND term=%s AND student_id IN ({_in_clause(ids)}) GROUP BY student_id",
                    (current_year, current_term, *ids),
                )
                items_map = {r["student_id"]: float(r.get("tsum") or 0) for r in (cursor.fetchall() or [])}
                # Legacy flat per student
                cursor.execute(
                    f"SELECT student_id, COALESCE(SUM(fee_amount),0) AS tsum FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({_in_clause(ids)}) GROUP BY student_id",
                    (current_year, current_term, *ids),
                )
                legacy_map = {r["student_id"]: float(r.get("tsum") or 0) for r in (cursor.fetchall() or [])}
                total_fee = sum(items_map.values())
                for sid in ids:
                    if sid not in items_map:
                        total_fee += float(legacy_map.get(sid) or 0)
                # Payments this term
                cursor.execute(
                    "SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s",
                    (school_id, current_year, current_term),
                )
                total_pay = float((cursor.fetchone() or {}).get("t", 0) or 0)
                term_outstanding = max(total_fee - total_pay, 0.0)
    except Exception:
        term_outstanding = 0.0

    db.close()
    return render_template(
        "dashboard.html",
        total_students=total_students,
        total_fees_collected=total_collected,
        pending_balance=total_balance,
        total_credit=total_credit,
        recent_payments=recent_payments,
        term_outstanding=term_outstanding
    )


# ---------- REAL-TIME DASHBOARD API ----------
@app.route("/api/dashboard_data")
def dashboard_data():
    """Return real-time dashboard totals."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute("SELECT COUNT(*) AS total_students FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_students = cursor.fetchone()["total_students"]

    # Use current term for totals where applicable
    try:
        cy, ct = get_or_seed_current_term(db)
    except Exception:
        cy, ct = None, None
    if cy and ct in (1, 2, 3):
        cursor.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s",
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            "SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s",
            (session.get("school_id"),),
        )
    total_collected = cursor.fetchone()["total_collected"]

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_balance = cursor.fetchone()["total_balance"]

    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_credit = cursor.fetchone()["total_credit"]

    # Compute term outstanding if we have a current year/term
    term_outstanding = 0.0
    try:
        if cy and ct in (1, 2, 3):
            school_id = session.get("school_id")
            cursor.execute("SELECT id FROM students WHERE school_id=%s", (school_id,))
            _rows = cursor.fetchall() or []
            ids = [r.get("id") for r in _rows]
            if ids:
                def _in_clause(seq):
                    return ",".join(["%s"] * len(seq))
                cursor.execute(
                    f"SELECT student_id, COALESCE(SUM(amount),0) AS tsum FROM student_term_fee_items WHERE year=%s AND term=%s AND student_id IN ({_in_clause(ids)}) GROUP BY student_id",
                    (cy, ct, *ids),
                )
                items_map = {r["student_id"]: float(r.get("tsum") or 0) for r in (cursor.fetchall() or [])}
                cursor.execute(
                    f"SELECT student_id, COALESCE(SUM(fee_amount),0) AS tsum FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({_in_clause(ids)}) GROUP BY student_id",
                    (cy, ct, *ids),
                )
                legacy_map = {r["student_id"]: float(r.get("tsum") or 0) for r in (cursor.fetchall() or [])}
                total_fee = sum(items_map.values())
                for sid in ids:
                    if sid not in items_map:
                        total_fee += float(legacy_map.get(sid) or 0)
                cursor.execute(
                    "SELECT COALESCE(SUM(amount),0) AS t FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s",
                    (school_id, cy, ct),
                )
                total_pay = float((cursor.fetchone() or {}).get("t", 0) or 0)
                term_outstanding = max(total_fee - total_pay, 0.0)
    except Exception:
        term_outstanding = 0.0

    db.close()
    return jsonify({
        "total_students": total_students,
        "total_collected": float(total_collected or 0),
        "total_balance": float(total_balance or 0),
        "total_credit": float(total_credit or 0),
        "term_outstanding": float(term_outstanding or 0)
    })


# ---------- LEDGER VIEW ----------
@app.route("/students/<int:student_id>/ledger")
def student_ledger(student_id: int):
    db = get_db_connection()
    try:
        ensure_ledger_table(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM ledger_entries WHERE school_id=%s AND student_id=%s ORDER BY ts DESC, id DESC",
            (session.get("school_id"), student_id),
        )
        entries = cur.fetchall() or []
    finally:
        db.close()
    return render_template("student_ledger.html", entries=entries, student_id=student_id)


# ---------- FORECAST API ----------
@app.route("/api/forecast_collections")
def forecast_collections():
    db = get_db_connection()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT DATE_FORMAT(date, '%Y-%m') AS ym, SUM(amount) AS total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s
            GROUP BY DATE_FORMAT(date, '%Y-%m')
            ORDER BY ym DESC
            LIMIT 6
            """,
            (session.get("school_id"),),
        )
        rows = cur.fetchall() or []
        hist = [float(r.get('total') or 0) for r in rows][::-1]
        avg = sum(hist) / len(hist) if hist else 0.0
        forecast = [round(avg, 2)] * 3
        return jsonify({"ok": True, "method": "moving_average", "horizon": 3, "forecast": forecast})
    finally:
        db.close()


# ---------- STUDENTS ----------
@app.route("/students")
def students():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM students WHERE school_id=%s ORDER BY id DESC", (session.get("school_id"),))
    students = cursor.fetchall()
    db.close()
    return render_template("students.html", students=students)


# ---------- GLOBAL SEARCH API ----------
@app.route("/api/search")
def global_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"students": [], "payments": []})
    db = get_db_connection()
    cur = db.cursor(dictionary=True)
    try:
        # Determine balance column
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        like = f"%{q}%"
        # Students by name/admission/class
        cur.execute(
            f"""
            SELECT id, name, class_name, admission_no, COALESCE({bal_col},0) AS balance, COALESCE(credit,0) AS credit
            FROM students
            WHERE school_id=%s AND (name LIKE %s OR admission_no LIKE %s OR class_name LIKE %s)
            ORDER BY name ASC
            LIMIT 15
            """,
            (session.get("school_id"), like, like, like),
        )
        students = cur.fetchall() or []

        # Payments by reference
        cur.execute(
            """
            SELECT p.id, p.reference, p.amount, DATE_FORMAT(p.date, '%Y-%m-%d') AS date, s.name AS student_name
            FROM payments p
            LEFT JOIN students s ON s.id = p.student_id
            WHERE p.school_id=%s AND (p.reference LIKE %s)
            ORDER BY p.date DESC
            LIMIT 10
            """,
            (session.get("school_id"), like),
        )
        payments = cur.fetchall() or []
    finally:
        db.close()
    return jsonify({"students": students, "payments": payments})


@app.route("/add_student", methods=["GET", "POST"])
def add_student():
    """Add a new student (reject duplicates by admission number only)."""
    if request.method == "POST":
        name = request.form["name"].strip()
        admission_no = request.form.get("admission_no", "").strip()
        class_name = request.form["class_name"].strip()
        phone = request.form.get("phone", "").strip()
        total_fees = float(request.form.get("total_fees", 0))

        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        # Detect correct column
        cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cursor.fetchone())
        cursor.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
        has_fee_balance = bool(cursor.fetchone())
        # Detect optional phone column
        cursor.execute("SHOW COLUMNS FROM students LIKE 'phone'")
        has_phone_col = bool(cursor.fetchone())

        # --- DUPLICATE CHECK (Admission No only) ---
        if admission_no:
            cursor.execute(
                """
                SELECT id FROM students
                WHERE LOWER(admission_no) = LOWER(%s) AND school_id=%s
                """,
                (admission_no, session.get("school_id")),
            )
            existing = cursor.fetchone()
            if existing:
                db.close()
                flash(f"Admission Number '{admission_no}' already exists in the system.", "warning")
                return redirect(url_for("students"))

        # Insert
        params = None
        if has_balance:
            if has_phone_col:
                sql = "INSERT INTO students (name, admission_no, class_name, phone, balance, credit, school_id) VALUES (%s, %s, %s, %s, %s, 0, %s)"
                params = (name, admission_no, class_name, phone, total_fees, session.get("school_id"))
            else:
                sql = "INSERT INTO students (name, admission_no, class_name, balance, credit, school_id) VALUES (%s, %s, %s, %s, 0, %s)"
                params = (name, admission_no, class_name, total_fees, session.get("school_id"))
        elif has_fee_balance:
            if has_phone_col:
                sql = "INSERT INTO students (name, admission_no, class_name, phone, fee_balance, credit, school_id) VALUES (%s, %s, %s, %s, %s, 0, %s)"
                params = (name, admission_no, class_name, phone, total_fees, session.get("school_id"))
            else:
                sql = "INSERT INTO students (name, admission_no, class_name, fee_balance, credit, school_id) VALUES (%s, %s, %s, %s, 0, %s)"
                params = (name, admission_no, class_name, total_fees, session.get("school_id"))
        else:
            db.close()
            flash("No valid balance column found in 'students' table!", "error")
            return redirect(url_for("students"))

        try:
            cursor.execute(sql, params)
            student_id = cursor.lastrowid
            try:
                ensure_student_enrollments_table(db)
                cy, _ct = get_or_seed_current_term(db)
                cur2 = db.cursor()
                cur2.execute(
                    "INSERT IGNORE INTO student_enrollments (student_id, year, class_name, opening_balance, status, school_id) VALUES (%s,%s,%s,%s,%s,%s)",
                    (student_id, cy, class_name, total_fees, "active", session.get("school_id")),
                )
            except Exception:
                pass
            db.commit()
            # audit removed
            flash(f"✅ Student '{name}' added successfully!", "success")
        except Exception as e:
            db.rollback()
            flash(f"Error adding student: {e}", "error")
        finally:
            db.close()

        return redirect(url_for("students"))

    return render_template("add_student.html")


# ---------- EDIT STUDENT ----------
@app.route("/student/<int:student_id>/edit", methods=["GET", "POST"])
def edit_student(student_id):
    """Edit existing student details and update balance/fee_balance accordingly."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Detect schema
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    cursor.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
    has_fee_balance = bool(cursor.fetchone())
    cursor.execute("SHOW COLUMNS FROM students LIKE 'phone'")
    has_phone_col = bool(cursor.fetchone())

    cursor.execute("SELECT * FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
    student = cursor.fetchone()

    if not student:
        db.close()
        flash("Student not found.", "error")
        return redirect(url_for("students"))

    if request.method == "POST":
        name = request.form.get("name", student.get("name", "")).strip()
        admission_no = request.form.get("admission_no", student.get("admission_no", "")).strip()
        class_name = request.form.get("class_name", student.get("class_name", "")).strip()
        balance_val = float(request.form.get("balance", student.get("balance") or student.get("fee_balance") or 0))
        phone_val = (request.form.get("phone") or student.get("phone") or "").strip()

        # Enforce unique admission_no if changed and provided
        if admission_no and admission_no.lower() != (student.get("admission_no") or "").lower():
            cursor.execute(
                "SELECT id FROM students WHERE LOWER(admission_no) = LOWER(%s) AND school_id=%s",
                (admission_no, session.get("school_id"))
            )
            exists = cursor.fetchone()
            if exists:
                db.close()
                flash("Admission Number already exists.", "warning")
                return redirect(url_for("edit_student", student_id=student_id))

        # Build dynamic update
        sets = ["name = %s", "admission_no = %s", "class_name = %s"]
        params = [name, admission_no or None, class_name]
        if has_balance:
            sets.append("balance = %s")
            params.append(balance_val)
        elif has_fee_balance:
            sets.append("fee_balance = %s")
            params.append(balance_val)
        if has_phone_col:
            sets.append("phone = %s")
            params.append(phone_val or None)
        params.append(student_id)
        params.append(session.get("school_id"))

        try:
            cursor.execute(f"UPDATE students SET {', '.join(sets)} WHERE id = %s AND school_id=%s", tuple(params))
            db.commit()
            # audit removed
            flash("Student updated successfully!", "success")
        except Exception as e:
            db.rollback()
            flash(f"Error updating student: {e}", "error")
        finally:
            db.close()
        return redirect(url_for("student_detail", student_id=student_id))

    # Ensure template has a 'balance' key for display regardless of schema
    if "balance" not in student:
        student["balance"] = student.get("fee_balance")

    db.close()
    return render_template("edit_student.html", student=student)


@app.route("/delete_student/<int:student_id>", methods=["POST"])
def delete_student(student_id):
    """Delete student and related payments."""
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("DELETE FROM payments WHERE student_id = %s AND school_id=%s", (student_id, session.get("school_id")))
    cursor.execute("DELETE FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
    db.commit()
    # audit removed
    db.close()
    flash("Student deleted successfully!", "success")
    return redirect(url_for("students"))


# ---------- SEARCH ----------
@app.route("/search_student")
def search_student():
    """Search by name, class, or admission number."""
    query = request.args.get("query", "").strip()
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    sid = session.get("school_id")
    if query:
        like = f"%{query}%"
        cursor.execute(
            """
            SELECT * FROM students
            WHERE school_id=%s AND (name LIKE %s OR class_name LIKE %s OR admission_no LIKE %s)
            ORDER BY id DESC
            """,
            (sid, like, like, like),
        )
    else:
        cursor.execute("SELECT * FROM students WHERE school_id=%s ORDER BY id DESC", (sid,))

    students = cursor.fetchall()
    db.close()
    return jsonify(students)


# ---------- DUPLICATE CHECK API ----------
@app.route("/check_student_exists")
def check_student_exists():
    """AJAX check if Admission Number already exists."""
    admission_no = request.args.get("admission_no", "").strip().lower()
    if not admission_no:
        return jsonify({"exists": False})

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id FROM students
        WHERE LOWER(admission_no) = %s AND school_id=%s
        """,
        (admission_no, session.get("school_id")),
    )
    exists = bool(cursor.fetchone())
    db.close()

    return jsonify({"exists": exists})


# ---------- EXPORT REPORTS ----------
@app.route("/export_students")
def export_students():
    """Export all student records as CSV."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT 
            name AS 'Name', 
            admission_no AS 'Admission No', 
            class_name AS 'Class', 
            COALESCE(balance, fee_balance) AS 'Balance (KES)', 
            COALESCE(credit, 0) AS 'Credit (KES)'
        FROM students
        WHERE school_id=%s
        ORDER BY class_name, name
        """,
        (session.get("school_id"),),
    )
    students = cursor.fetchall()
    db.close()

    if not students:
        return Response("No students found.", mimetype="text/plain")

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=students[0].keys())
    writer.writeheader()
    writer.writerows(students)
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=students_report.csv"}
    )


@app.route("/export_payments")
def export_payments():
    """Export all payment records as CSV."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT 
            s.name AS 'Student Name',
            s.admission_no AS 'Admission No',
            s.class_name AS 'Class',
            p.year AS 'Year',
            p.term AS 'Term',
            p.amount AS 'Amount (KES)',
            p.method AS 'Method',
            p.reference AS 'Reference',
            p.date AS 'Date'
        FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE p.school_id=%s
        ORDER BY p.date DESC
        """,
        (session.get("school_id"),),
    )
    payments = cursor.fetchall()
    db.close()

    if not payments:
        return Response("No payments found.", mimetype="text/plain")

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=payments[0].keys())
    writer.writeheader()
    writer.writerows(payments)
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=payments_report.csv"}
    )


# ---------- STUDENT DETAIL ----------
@app.route("/student/<int:student_id>")
def student_detail(student_id):
    """View student profile and payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SELECT * FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
    student = cursor.fetchone()

    cursor.execute(
        """
        SELECT * FROM payments
        WHERE student_id = %s AND school_id=%s
        ORDER BY date DESC
        """,
        (student_id, session.get("school_id"))
    )
    payments = cursor.fetchall()
    # Overpay records from credit operations (if table exists)
    try:
        ensure_credit_ops_table(db)
        cursor.execute(
            """
            SELECT ts, amount, reference, method
            FROM credit_operations
            WHERE student_id = %s AND op_type = 'overpay' AND school_id=%s
            ORDER BY ts DESC
            """,
            (student_id, session.get("school_id"))
        )
        overpays = cursor.fetchall()
        # Credit transfer records (both directions) if table exists
        ensure_credit_transfers_table(db)
        # Outgoing transfers with destination name
        cursor.execute(
            """
            SELECT ct.*, s.name AS to_name
            FROM credit_transfers ct
            JOIN students s ON s.id = ct.to_student_id
            WHERE ct.from_student_id = %s AND ct.school_id=%s
            ORDER BY ct.ts DESC
            """,
            (student_id, session.get("school_id"))
        )
        transfers_out = cursor.fetchall()
        # Incoming transfers with source name
        cursor.execute(
            """
            SELECT ct.*, s.name AS from_name
            FROM credit_transfers ct
            JOIN students s ON s.id = ct.from_student_id
            WHERE ct.to_student_id = %s AND ct.school_id=%s
            ORDER BY ct.ts DESC
            """,
            (student_id, session.get("school_id"))
        )
        transfers_in = cursor.fetchall()
    except Exception:
        overpays = []
        transfers_out = []
        transfers_in = []
    db.close()

    if not student:
        flash("Student not found.", "error")
        return redirect(url_for("students"))

    # ✅ Pass datetime to template
    return render_template(
        "view_student.html",
        student=student,
        payments=payments,
        overpays=overpays,
        transfers_out=transfers_out,
        transfers_in=transfers_in,
        datetime=datetime,
    )


# ---------- PAYMENTS ----------
@app.route("/payments", methods=["GET", "POST"])
def payments():
    """Add or list payments."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    if request.method == "POST":
        student_id = request.form["student_id"]
        amount = float(request.form["amount"])
        method = request.form["method"]
        reference = (request.form.get("reference") or "").strip()
        payment_date = datetime.now()
        carry_overpay = (request.form.get("carry_overpay") in ("1", "true", "on", "yes"))
        # Academic context (Term/Year)
        try:
            form_year = request.form.get("year", type=int)
            form_term = request.form.get("term", type=int)
        except Exception:
            form_year, form_term = None, None

        # Enforce term state: only allow payments in OPEN term
        try:
            cy_check, ct_check = get_or_seed_current_term(db)
            cur_gate = db.cursor()
            sid_gate = session.get("school_id")
            cur_gate.execute(
                "SELECT status FROM academic_terms WHERE year=%s AND term=%s AND school_id=%s",
                (cy_check, ct_check, sid_gate),
            )
            trow = cur_gate.fetchone()
            if trow is not None:
                status_val = trow[0] if not isinstance(trow, dict) else (trow.get("status") or "DRAFT")
                if status_val != "OPEN":
                    db.close()
                    flash("Payments are locked until the current term is OPEN.", "warning")
                    return redirect(url_for("payments"))
        except Exception:
            pass

        # Detect correct balance column
        cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cursor.fetchone())
        column = "balance" if has_balance else "fee_balance"

        cursor.execute(f"SELECT {column}, credit FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
        student = cursor.fetchone()

        # Fetch student's contact info for notifications
        cursor.execute("SHOW COLUMNS FROM students LIKE 'phone'")
        _has_phone_col = bool(cursor.fetchone())
        student_name = None
        student_phone = None
        if _has_phone_col:
            cursor.execute("SELECT name, phone FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
            row = cursor.fetchone() or {}
            student_name = row.get("name")
            student_phone = row.get("phone")
        else:
            cursor.execute("SELECT name FROM students WHERE id = %s AND school_id=%s", (student_id, session.get("school_id")))
            row = cursor.fetchone() or {}
            student_name = row.get("name")

        if not student:
            db.close()
            flash("Student not found!", "error")
            return redirect(url_for("payments"))

        current_balance = float(student[column] or 0)
        current_credit = float(student["credit"] or 0)

        if amount > current_balance:
            overpaid = amount - current_balance
            new_balance = 0
            # if admin chooses to carry to next term, do NOT add to credit now
            new_credit = current_credit if carry_overpay else (current_credit + overpaid)
        else:
            new_balance = current_balance - amount
            new_credit = current_credit
        # Track total overpay for logging (guaranteed non-negative)
        overpaid_total = max(amount - current_balance, 0.0)

        # Enforce unique reference if provided (case-insensitive)
        if reference:
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM payments WHERE LOWER(reference) = LOWER(%s) AND school_id=%s",
                (reference, session.get("school_id")),
            )
            ref_exists = (cursor.fetchone() or {}).get("cnt", 0)
            if ref_exists:
                db.close()
                flash("Reference already exists. Use a unique REF.", "warning")
                return redirect(url_for("payments", student_id=student_id))

        # Ensure term/year columns exist and compute defaults
        try:
            ensure_payments_term_columns(db)
        except Exception:
            pass
        if not (form_year and form_term in (1, 2, 3)):
            try:
                cy, ct = get_or_seed_current_term(db)
            except Exception:
                cy, ct = payment_date.year, None
        else:
            cy, ct = form_year, form_term

        cursor.execute(
            """
            INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (student_id, amount, method, ct, cy, reference or None, payment_date, session.get("school_id")),
        )
        payment_id = cursor.lastrowid

        cursor.execute(
            f"UPDATE students SET {column} = %s, credit = %s WHERE id = %s AND school_id=%s",
            (new_balance, new_credit, student_id, session.get("school_id")),
        )
        db.commit()
        # Ledger + Audit
        try:
            ensure_ledger_table(db)
            add_entry(
                db,
                school_id=int(session.get("school_id")),
                student_id=int(student_id),
                entry_type='credit',
                amount=float(amount),
                ref=reference or None,
                description=f"Payment via {method}",
                link_type='payment',
                link_id=int(payment_id),
            )
        except Exception:
            pass
        try:
            ensure_audit_table(db)
            log_event(db, int(session.get("school_id")), session.get("username"), 'add_payment', 'payment', int(payment_id), {
                'student_id': int(student_id), 'amount': float(amount), 'method': method, 'reference': reference or None
            })
        except Exception:
            pass

        # Optionally carry any overpay to next term by creating a forward payment entry
        carried_payment_id = None
        if carry_overpay and overpaid_total > 0 and ct in (1, 2, 3):
            try:
                # work out next (year, term)
                next_term = 1 if ct == 3 else (ct + 1)
                next_year = (cy + 1) if ct == 3 else cy
                carry_ref = (reference or f"PMT-{payment_id}") + "-CF"
                cursor.execute(
                    """
                    INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (student_id, overpaid_total, "Carry Forward", next_term, next_year, carry_ref, payment_date, session.get("school_id")),
                )
                carried_payment_id = cursor.lastrowid
                db.commit()
                # Ledger entry for carry-forward credit
                try:
                    ensure_ledger_table(db)
                    add_entry(
                        db,
                        school_id=int(session.get("school_id")),
                        student_id=int(student_id),
                        entry_type='credit',
                        amount=float(overpaid_total),
                        ref=carry_ref,
                        description=f"Carry forward from {cy} T{ct}",
                        link_type='payment',
                        link_id=int(carried_payment_id),
                    )
                except Exception:
                    pass
            except Exception:
                # If carry write fails, fall back to adding to credit to avoid loss
                try:
                    cursor.execute(
                        "UPDATE students SET credit = credit + %s WHERE id = %s AND school_id=%s",
                        (overpaid_total, student_id, session.get("school_id")),
                    )
                    db.commit()
                except Exception:
                    pass

        # If there was an overpay, record it in credit operations for student detail visibility
        try:
            if overpaid_total > 0:
                ensure_credit_ops_table(db)
                cur2 = db.cursor()
                cur2.execute(
                    "INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        datetime.utcnow(),
                        session.get("username"),
                        int(student_id),
                        "overpay",
                        overpaid_total,
                        reference or None,
                        method,
                        json.dumps({
                            "payment_id": int(payment_id),
                            "carried": bool(carry_overpay),
                            "carried_payment_id": int(carried_payment_id) if carried_payment_id else None
                        }),
                        session.get("school_id"),
                    ),
                )
                db.commit()
        except Exception:
            # Do not block normal flow if logging fails
            pass
        # audit removed

        # Send receipt via Email (legacy head copy)
        brand = app.config.get("APP_NAME", "Your School")
        pretty_ref = reference or "N/A"
        email_body = (
            f"{brand}: Payment received.\n"
            f"Hi {student_name}, thank you for your payment of KES {amount:,.2f} via {method} (Ref: {pretty_ref}).\n"
            f"Your new balance is KES {new_balance:,.2f}. Credit on account: KES {new_credit:,.2f}."
        )
        email_subject = f"Payment receipt for {student_name} — KES {amount:,.2f}"
        # Attempt send only if an email was captured earlier in this context (not shown in head copy)
        try:
            # This head copy may not have email; safe no-op if undefined
            if 'student_email' in locals() and student_email:
                gmail_send_email(student_email, email_subject, email_body)
        except Exception:
            pass
        db.close()

        flash(f"Payment of KES {amount:,.2f} recorded! Remaining balance: KES {new_balance:,.2f}, Credit: KES {new_credit:,.2f}", "success")
        return redirect(url_for("payments"))

    # ✅ NEW: read preselected student from query param
    selected_student_id = request.args.get("student_id", type=int)

    cursor.execute(
        """
        SELECT p.*, s.name AS student_name, s.class_name
        FROM payments p
        JOIN students s ON p.student_id = s.id
        WHERE p.school_id=%s
        ORDER BY p.date DESC
        """,
        (session.get("school_id"),),
    )
    payments = cursor.fetchall()
    
    cursor.execute("SELECT id, name FROM students WHERE school_id=%s ORDER BY name ASC", (session.get("school_id"),))
    students = cursor.fetchall()
    db.close()

    return render_template("payments.html", payments=payments, students=students, selected_student_id=selected_student_id)


# ---------- PAYMENT RECEIPT (Printable) ----------
@app.route("/payments/<int:payment_id>/receipt")
def payment_receipt(payment_id: int):
    """Render a compact, one-page printable receipt for a payment."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Fetch payment with student info
    cursor.execute(
        """
        SELECT p.*, s.name AS student_name, s.class_name, s.id AS sid
        FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE p.id = %s AND p.school_id=%s
        """,
        (payment_id, session.get("school_id")),
    )
    payment = cursor.fetchone()
    if not payment:
        db.close()
        flash("Payment not found.", "error")
        return redirect(url_for("payments"))

    # Determine current balance/credit (for display only)
    try:
        cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cursor.fetchone())
        column = "balance" if has_balance else "fee_balance"

        cursor.execute(
            f"SELECT {column} AS balance, credit FROM students WHERE id = %s AND school_id=%s",
            (payment.get("sid"), session.get("school_id")),
        )
        sc = cursor.fetchone() or {}
        current_balance = float((sc.get("balance") or 0))
        current_credit = float((sc.get("credit") or 0))
    except Exception:
        current_balance = None
        current_credit = None

    db.close()

    brand = (
        app.config.get("APP_NAME")
        or f"{app.config.get('BRAND_NAME', 'Lovato_Tech')} {app.config.get('PORTAL_TITLE', 'Fee Management portal')}"
    ).strip()
    # Build a verification URL for authenticity (scannable)
    try:
        verify_url = url_for('payment_receipt', payment_id=payment_id, _external=True)
    except Exception:
        verify_url = ""

    return render_template(
        "receipt.html",
        brand=brand,
        payment=payment,
        current_balance=current_balance,
        current_credit=current_credit,
        payment_link=app.config.get("PAYMENT_LINK", ""),
        verify_url=verify_url,
    )


# ---------- PAYMENT RECEIPT (PDF Download) ----------
@app.route("/payments/<int:payment_id>/receipt.pdf")
def payment_receipt_pdf(payment_id: int):
    """Generate a PDF receipt for a payment with school details."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT p.id, p.student_id AS sid, s.name AS student_name, s.class_name, p.amount, p.method,
               p.reference, p.date, p.term, p.year
        FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE p.id = %s AND p.school_id=%s
        """,
        (payment_id, session.get("school_id")),
    )
    payment = cursor.fetchone()
    if not payment:
        db.close()
        flash("Payment not found.", "error")
        return redirect(url_for("payments"))

    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    bal_col = "balance" if has_balance else "fee_balance"
    cursor.execute(
        f"SELECT COALESCE({bal_col}, 0) AS bal, COALESCE(credit, 0) AS credit FROM students WHERE id=%s AND school_id=%s",
        (payment.get("sid"), session.get("school_id")),
    )
    srow = cursor.fetchone() or {"bal": 0.0, "credit": 0.0}
    db.close()

    school_name = (
        get_setting("SCHOOL_NAME")
        or get_setting("APP_NAME")
        or app.config.get("APP_NAME")
        or app.config.get("BRAND_NAME")
        or get_setting("BRAND_NAME")
        or "School"
    )
    school_address = get_setting("SCHOOL_ADDRESS") or ""
    school_phone = get_setting("SCHOOL_PHONE") or ""
    school_email = get_setting("SCHOOL_EMAIL") or ""

    from io import BytesIO
    from reportlab.lib.pagesizes import A5
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.pdfgen import canvas

    # Styled, modern PDF layout to mirror HTML receipt
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A5)
    width, height = A5
    x_margin = 14 * mm
    content_gap = 6 * mm

    # Brand colors (match HTML theme)
    brand_indigo = colors.HexColor("#4338ca")
    brand_cyan = colors.HexColor("#06b6d4")
    light_bg = colors.HexColor("#eef2ff")
    soft_border = colors.HexColor("#e2e8f0")

    # Header bar
    header_h = 28 * mm
    c.setFillColor(brand_indigo)
    c.setStrokeColor(brand_indigo)
    c.rect(0, height - header_h, width, header_h, fill=1, stroke=0)

    # Optional logo on header
    logo_w = 16 * mm
    logo_h = 16 * mm
    logo_x = x_margin
    logo_y = height - header_h + (header_h - logo_h) / 2
    try:
        static_folder = app.static_folder or os.path.join(app.root_path, "static")
        logo_rel = app.config.get("LOGO_PRIMARY", "css/lovato_logo.jpg")
        logo_path = os.path.join(static_folder, logo_rel.replace("\\", "/"))
        if os.path.exists(logo_path):
            c.drawImage(logo_path, logo_x, logo_y, width=logo_w, height=logo_h, preserveAspectRatio=True, mask='auto')
            text_x = logo_x + logo_w + 6
        else:
            text_x = x_margin
    except Exception:
        text_x = x_margin

    # Header text
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(text_x, height - 11 * mm, str(school_name))
    sub = " ".join(filter(None, [school_address, school_phone, school_email])) or "Official Payment Receipt"
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.whitesmoke)
    c.drawString(text_x, height - 15.5 * mm, sub)

    # PAID badge on the right
    badge_w, badge_h = 22 * mm, 8 * mm
    badge_x = width - x_margin - badge_w
    badge_y = height - 12 * mm - (badge_h / 2)
    c.setFillColor(colors.white)
    c.setStrokeColor(colors.white)
    c.roundRect(badge_x, badge_y, badge_w, badge_h, 2 * mm, fill=1, stroke=0)
    c.setFillColor(brand_indigo)
    c.setFont("Helvetica-Bold", 8.5)
    c.drawCentredString(badge_x + badge_w / 2, badge_y + 2.6 * mm, "PAID")

    # Content origin (below header)
    y = height - header_h - 10 * mm

    # Amount card
    card_h = 14 * mm
    card_y = y - card_h
    c.setFillColor(light_bg)
    c.setStrokeColor(soft_border)
    c.roundRect(x_margin, card_y, width - 2 * x_margin, card_h, 3 * mm, fill=1, stroke=0)
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor("#64748b"))
    c.drawCentredString(width / 2, card_y + card_h - 5 * mm, "Amount Paid")
    c.setFillColor(colors.HexColor("#0f172a"))
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(width / 2, card_y + 4.8 * mm, f"KES {float(payment.get('amount') or 0):,.2f}")
    y = card_y - content_gap

    # Separator (dashed)
    c.setStrokeColor(colors.lightgrey)
    c.setDash(1, 2)
    c.line(x_margin, y, width - x_margin, y)
    c.setDash()
    y -= content_gap

    # Key-value rows
    def draw_kv(label: str, value: str):
        nonlocal y
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor("#64748b"))
        c.drawString(x_margin, y, label)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(width - x_margin, y, value)
        y -= 6.5 * mm

    pdate = payment.get("date")
    if hasattr(pdate, "strftime"):
        date_str = pdate.strftime("%Y-%m-%d %H:%M")
    else:
        date_str = str(pdate or "N/A")

    draw_kv("Receipt No.", f"#{payment['id']}")
    draw_kv("Date", date_str)
    draw_kv("Student", str(payment.get("student_name") or ""))
    draw_kv("Class", str(payment.get("class_name") or "N/A"))
    draw_kv("Method", str(payment.get("method") or "N/A"))
    draw_kv("Reference", str(payment.get("reference") or "N/A"))
    draw_kv("Year / Term", f"{payment.get('year') or 'N/A'} / {payment.get('term') or 'N/A'}")

    # Secondary separator
    y += 1.5 * mm
    c.setStrokeColor(colors.lightgrey)
    c.setDash(1, 2)
    c.line(x_margin, y, width - x_margin, y)
    c.setDash()
    y -= 7.5 * mm

    draw_kv("Current Balance", f"KES {float(srow['bal'] or 0):,.2f}")
    draw_kv("Credit on Account", f"KES {float(srow['credit'] or 0):,.2f}")

    # Footer
    c.setFillColor(colors.HexColor("#64748b"))
    c.setFont("Helvetica", 9)
    c.drawCentredString(width / 2, max(y, 22 * mm), "Thank you for your payment.")

    # Add QR code for authenticity (link back to this receipt)
    try:
        from reportlab.graphics.barcode import qr as rl_qr
        from reportlab.graphics.shapes import Drawing
        from reportlab.graphics import renderPDF
        verify_url = url_for("payment_receipt", payment_id=payment_id, _external=True)
        qr_widget = rl_qr.QrCodeWidget(verify_url)
        b = qr_widget.getBounds()
        size = 26 * mm
        w = b[2] - b[0]
        h = b[3] - b[1]
        d = Drawing(size, size)
        qr_widget.transform = [size / w, 0, 0, size / h, 0, 0]
        d.add(qr_widget)
        renderPDF.draw(d, c, width - x_margin - size, 10 * mm)
        c.setFont("Helvetica", 7.5)
        c.setFillColor(colors.HexColor("#64748b"))
        c.drawRightString(width - x_margin, 9 * mm, "Scan to verify receipt")
    except Exception:
        pass

    c.showPage()
    c.save()
    pdf_bytes = buf.getvalue()
    buf.close()

    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=receipt_{payment_id}.pdf",
        },
    )


# ---------- ANALYTICS ----------
@app.route("/analytics")
def analytics():
    """Render analytics dashboard."""
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    column = "balance" if has_balance else "fee_balance"

    cursor.execute("SELECT COUNT(*) AS total FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_students = cursor.fetchone()["total"]

    cursor.execute(
        "SELECT COALESCE(SUM(amount), 0) AS total_collected FROM payments WHERE method <> 'Credit Transfer' AND school_id=%s",
        (session.get("school_id"),),
    )
    total_collected = cursor.fetchone()["total_collected"]

    cursor.execute(f"SELECT COALESCE(SUM({column}), 0) AS total_balance FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_balance = cursor.fetchone()["total_balance"]

    cursor.execute("SELECT COALESCE(SUM(credit), 0) AS total_credit FROM students WHERE school_id=%s", (session.get("school_id"),))
    total_credit = cursor.fetchone()["total_credit"]

    db.close()
    return render_template(
        "analytics.html",
        total_students=total_students,
        total_collected=total_collected,
        total_balance=total_balance,
        total_credit=total_credit
    )

# Alias route for Collections (same view as analytics)
@app.route("/collections")
def collections_overview():
    return analytics()


# ---------- ANALYTICS DATA (LIVE) ----------
@app.route("/api/analytics_data")
def analytics_data():
    """Provide live analytics for charts and class summary.

    Returns keys:
      - monthly_data: [{month, total}]
      - daily_trend: [{day, total}] last 30 days
      - class_summary: [{class_name, total_students, total_paid, total_pending, total_credit, percent_paid}]
      - method_breakdown: [{method, count, total}]
      - top_debtors: [{name, class_name, balance}]
      - mom: {current_month_total, prev_month_total, percent_change}
      - meta: {active_classes}
    """
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Resolve current academic context
    try:
        cy, ct = get_or_seed_current_term(db)
    except Exception:
        cy, ct = None, None

    # Monthly totals (by first day label for readability)
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT DATE_FORMAT(MIN(date), '%b %Y') AS month, SUM(amount) AS total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            GROUP BY YEAR(date), MONTH(date)
            ORDER BY YEAR(date), MONTH(date)
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT DATE_FORMAT(MIN(date), '%b %Y') AS month, SUM(amount) AS total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s
            GROUP BY YEAR(date), MONTH(date)
            ORDER BY YEAR(date), MONTH(date)
            """,
            (session.get("school_id"),),
        )
    monthly_data = cursor.fetchall()

    # Daily trend - last 30 days
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT DATE(date) AS day, SUM(amount) AS total
            FROM payments
            WHERE date >= (CURRENT_DATE - INTERVAL 29 DAY)
              AND method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            GROUP BY DATE(date)
            ORDER BY DATE(date)
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT DATE(date) AS day, SUM(amount) AS total
            FROM payments
            WHERE date >= (CURRENT_DATE - INTERVAL 29 DAY)
              AND method <> 'Credit Transfer' AND school_id=%s
            GROUP BY DATE(date)
            ORDER BY DATE(date)
            """,
            (session.get("school_id"),),
        )
    daily_trend = cursor.fetchall()

    # Class summary
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT 
                s.class_name,
                COUNT(s.id) AS total_students,
                COALESCE(SUM(p.amount), 0) AS total_paid,
                COALESCE(SUM(COALESCE(s.balance, s.fee_balance)), 0) AS total_pending,
                COALESCE(SUM(s.credit), 0) AS total_credit
            FROM students s
            LEFT JOIN payments p ON s.id = p.student_id AND p.method <> 'Credit Transfer' AND p.school_id=%s AND p.year=%s AND p.term=%s
            WHERE s.school_id=%s
            GROUP BY s.class_name
            ORDER BY s.class_name
            """,
            (session.get("school_id"), cy, ct, session.get("school_id")),
        )
    else:
        cursor.execute(
            """
            SELECT 
                s.class_name,
                COUNT(s.id) AS total_students,
                COALESCE(SUM(p.amount), 0) AS total_paid,
                COALESCE(SUM(COALESCE(s.balance, s.fee_balance)), 0) AS total_pending,
                COALESCE(SUM(s.credit), 0) AS total_credit
            FROM students s
            LEFT JOIN payments p ON s.id = p.student_id AND p.method <> 'Credit Transfer' AND p.school_id=%s
            WHERE s.school_id=%s
            GROUP BY s.class_name
            ORDER BY s.class_name
            """,
            (session.get("school_id"), session.get("school_id")),
        )
    class_summary = cursor.fetchall()

    # Payment method breakdown
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT method, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE school_id=%s AND year=%s AND term=%s
            GROUP BY method
            ORDER BY total DESC
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT method, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS total
            FROM payments
            WHERE school_id=%s
            GROUP BY method
            ORDER BY total DESC
            """,
            (session.get("school_id"),),
        )
    method_breakdown = cursor.fetchall()

    # Top debtors (highest balances)
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cursor.fetchone())
    balance_col = "balance" if has_balance else "fee_balance"

    cursor.execute(
        f"""
        SELECT name, class_name, COALESCE({balance_col}, 0) AS balance
        FROM students
        WHERE school_id=%s
        ORDER BY COALESCE({balance_col}, 0) DESC
        LIMIT 5
        """,
        (session.get("school_id"),),
    )
    top_debtors = cursor.fetchall()

    # Month-over-month change
    if cy and ct in (1, 2, 3):
        cursor.execute(
            """
            SELECT 
                SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN amount ELSE 0 END) AS current_month_total,
                SUM(CASE WHEN DATE_FORMAT(date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m') THEN amount ELSE 0 END) AS prev_month_total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s AND year=%s AND term=%s
            """,
            (session.get("school_id"), cy, ct),
        )
    else:
        cursor.execute(
            """
            SELECT 
                SUM(CASE WHEN YEAR(date) = YEAR(CURRENT_DATE) AND MONTH(date) = MONTH(CURRENT_DATE) THEN amount ELSE 0 END) AS current_month_total,
                SUM(CASE WHEN DATE_FORMAT(date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m') THEN amount ELSE 0 END) AS prev_month_total
            FROM payments
            WHERE method <> 'Credit Transfer' AND school_id=%s
            """,
            (session.get("school_id"),),
        )
    mom_row = cursor.fetchone() or {"current_month_total": 0, "prev_month_total": 0}
    current_month_total = float(mom_row.get("current_month_total") or 0)
    prev_month_total = float(mom_row.get("prev_month_total") or 0)
    percent_change = (
        round(((current_month_total - prev_month_total) / prev_month_total) * 100, 1)
        if prev_month_total > 0
        else (100.0 if current_month_total > 0 else 0.0)
    )

    # Meta: active classes
    cursor.execute("SELECT COUNT(DISTINCT class_name) AS active_classes FROM students WHERE school_id=%s", (session.get("school_id"),))
    active_classes = (cursor.fetchone() or {}).get("active_classes", 0)

    db.close()

    # Enrich class summary with percent_paid
    for row in class_summary:
        paid = float(row["total_paid"] or 0)
        pending = float(row["total_pending"] or 0)
        total = paid + pending
        row["percent_paid"] = round((paid / total * 100), 1) if total > 0 else 0

    return jsonify(
        {
            "monthly_data": monthly_data,
            "daily_trend": daily_trend,
            "class_summary": class_summary,
            "method_breakdown": method_breakdown,
            "top_debtors": top_debtors,
            "mom": {
                "current_month_total": current_month_total,
                "prev_month_total": prev_month_total,
                "percent_change": percent_change,
            },
            "meta": {"active_classes": int(active_classes or 0)},
        }
    )


# ---------- DOCUMENTATION ----------
@app.route("/docs")
def docs():
    """In-app documentation hub with optional promo/how-to video.

    Looks for a local video file under static/media with the title provided by the user.
    Supported extensions: .mp4, .webm, .mov. If present, renders a <video> player.
    """
    base_name = "Stop Guessing Fees_ Lovato_Tech Made Easy"
    exts = [".mp4", ".webm", ".mov"]
    video_rel = None
    try:
        static_root = os.path.join(app.root_path, "static", "media")
        if not os.path.isdir(static_root):
            try:
                os.makedirs(static_root, exist_ok=True)
            except Exception:
                pass
        for ext in exts:
            candidate = os.path.join(static_root, base_name + ext)
            if os.path.exists(candidate):
                video_rel = f"media/{base_name}{ext}"
                break
    except Exception:
        video_rel = None

    return render_template(
        "docs.html",
        video_path=video_rel,
        video_title=base_name,
    )


# ---------- RUN ----------
if __name__ == "__main__":
    app.run(debug=True)
