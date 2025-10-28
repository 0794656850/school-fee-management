from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session
import mysql.connector
from urllib.parse import urlparse
from datetime import date, datetime
import os
from utils.pro import is_pro_enabled, upgrade_url
from utils.classes import promote_class_name

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
        if sid:
            cur.execute("SELECT id, class_name, COALESCE(" + bal_col + ",0) AS bal FROM students WHERE school_id=%s", (sid,))
        else:
            cur.execute("SELECT id, class_name, COALESCE(" + bal_col + ",0) AS bal FROM students")
        students = cur.fetchall() or []
        created = 0
        cur2 = db.cursor()
        for s in students:
            current_class = (s.get("class_name") or "").strip()
            next_class = promote_class_name(current_class)
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
            if sid:
                try:
                    cur2.execute(
                        "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status, school_id) VALUES (%s,%s,%s,%s,%s,%s)",
                        (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), "active", sid),
                    )
                except Exception:
                    cur2.execute(
                        "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status) VALUES (%s,%s,%s,%s,%s)",
                        (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), "active"),
                    )
            else:
                cur2.execute(
                    "INSERT INTO student_enrollments (student_id, year, class_name, opening_balance, status) VALUES (%s,%s,%s,%s,%s)",
                    (s["id"], next_year, (next_class or current_class), float(s.get("bal") or 0), "active"),
                )
            created += 1

            # Update the student's current class to the promoted one when applicable
            if next_class and next_class != current_class:
                try:
                    if sid:
                        cur2.execute("UPDATE students SET class_name=%s WHERE id=%s AND school_id=%s", (next_class, s["id"], sid))
                    else:
                        cur2.execute("UPDATE students SET class_name=%s WHERE id=%s", (next_class, s["id"]))
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
        flash(f"Set current term to {year} - Term {term}.", "success")
    except Exception as e:
        db.rollback()
        flash(f"Error setting current term: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_terms"))


@term_bp.route("/current")
def current_term_api():
    db = _db()
    try:
        y, t = get_or_seed_current_term(db)
    finally:
        db.close()
    return jsonify({"year": y, "term": t})


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
        # Flip status and timestamp
        if sid:
            cur.execute("UPDATE academic_terms SET status='OPEN', opens_at=NOW() WHERE year=%s AND term=%s AND school_id=%s AND status='DRAFT'", (year, term, sid))
        else:
            cur.execute("UPDATE academic_terms SET status='OPEN', opens_at=NOW() WHERE year=%s AND term=%s AND status='DRAFT'", (year, term))
        db.commit()
        # Audit
        try:
            from utils.audit import ensure_audit_table, log_event
            ensure_audit_table(db)
            log_event(db, sid, None, 'open_term', 'term', None, {'year': year, 'term': term})
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
        # Close term
        if sid:
            cur.execute("UPDATE academic_terms SET status='CLOSED', closes_at=NOW() WHERE id=%s AND school_id=%s", (row.get('id'), sid))
        else:
            cur.execute("UPDATE academic_terms SET status='CLOSED', closes_at=NOW() WHERE id=%s", (row.get('id'),))
        db.commit()
        # Audit
        try:
            from utils.audit import ensure_audit_table, log_event
            ensure_audit_table(db)
            log_event(db, sid, None, 'close_term', 'term', int(row.get('id')), {'year': year, 'term': term})
        except Exception:
            pass
        flash("Term closed.", "success")
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS term_fees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            year INT NOT NULL,
            term TINYINT NOT NULL,
            fee_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_student_year_term (student_id, year, term),
            KEY idx_year_term (year, term)
        )
        """
    )
    conn.commit()


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

        # Legacy flat term fees (fallback)
        if student_ids:
            ph = ",".join(["%s"] * len(student_ids))
            cur.execute(
                f"SELECT student_id, fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
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
        cur = db.cursor(dictionary=True)
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"

        cur.execute(
            "SELECT fee_amount FROM term_fees WHERE student_id=%s AND year=%s AND term=%s",
            (student_id, year, term),
        )
        prev = cur.fetchone()
        prev_amt = float(prev["fee_amount"]) if prev and prev.get("fee_amount") is not None else 0.0
        delta = float(fee_amount) - prev_amt

        if prev:
            cur.execute(
                "UPDATE term_fees SET fee_amount=%s WHERE student_id=%s AND year=%s AND term=%s",
                (fee_amount, student_id, year, term),
            )
        else:
            cur.execute(
                "INSERT INTO term_fees (student_id, year, term, fee_amount) VALUES (%s,%s,%s,%s)",
                (student_id, year, term, fee_amount),
            )

        if abs(delta) > 0:
            cur.execute(
                f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id = %s AND school_id=%s",
                (delta, student_id, session.get("school_id") if session else None),
            )

        db.commit()
        if prev:
            flash(f"Updated term fee. Delta applied to balance: KES {delta:,.2f}", "success")
        else:
            flash(f"Set term fee and applied to balance: KES {fee_amount:,.2f}", "success")
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        flash(f"Error saving term fee: {e}", "error")
    finally:
        db.close()

    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


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
    if not is_pro_enabled():
        flash("Invoices are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
    db = _db()
    try:
        ensure_invoices_tables(db)
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
    if not is_pro_enabled():
        flash("Invoices are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
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

        return render_template("invoice.html", invoice=invoice, items=items, penalty_amount=penalty_amount, penalty_label=penalty_label)
    finally:
        db.close()


@term_bp.route("/fees/components", methods=["POST"])
def add_component():
    if not is_pro_enabled():
        flash("Fee components are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
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
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error adding component: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees"))


@term_bp.route("/fees/class_defaults", methods=["POST"])
def set_class_defaults():
    if not is_pro_enabled():
        flash("Class defaults are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
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
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving class defaults: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/student_items", methods=["POST"])
def set_student_items():
    if not is_pro_enabled():
        flash("Student fee item overrides are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
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
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving student items: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/discount", methods=["POST"])
def set_discount():
    if not is_pro_enabled():
        flash("Discounts are a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
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
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error saving discount: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))


@term_bp.route("/fees/generate_invoices", methods=["POST"])
def generate_invoices():
    if not is_pro_enabled():
        flash("Invoice generation is a Pro feature.", "warning")
        return redirect(url_for("terms.manage_term_fees"))
    year = request.form.get("year", type=int)
    term = request.form.get("term", type=int)
    due_date_raw = request.form.get("due_date")
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
        ensure_invoices_tables(db)
        ensure_fee_components_table(db)
        ensure_class_fee_defaults_table(db)
        ensure_student_fee_items_table(db)
        ensure_discounts_table(db)

        cur = db.cursor(dictionary=True)
        # Students
        sid = session.get("school_id") if session else None
        if sid:
            cur.execute("SELECT id, name, class_name FROM students WHERE school_id=%s ORDER BY name ASC", (sid,))
        else:
            cur.execute("SELECT id, name, class_name FROM students ORDER BY name ASC")
        students = cur.fetchall() or []
        if not students:
            flash("No students to invoice.", "warning")
            return redirect(url_for("terms.manage_term_fees", year=year, term=term))

        # Components
        cur.execute("SELECT id, name, default_amount FROM fee_components ORDER BY name ASC")
        comps = cur.fetchall() or []
        comp_defaults = {c["id"]: float(c.get("default_amount") or 0) for c in comps}

        # Class defaults
        cur.execute("SELECT class_name, component_id, amount FROM class_fee_defaults WHERE year=%s AND term=%s", (year, term))
        class_rows = cur.fetchall() or []
        class_defaults = {}
        for r in class_rows:
            class_defaults.setdefault(r["class_name"], {})[r["component_id"]] = float(r.get("amount") or 0)

        # Student item overrides
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

        cur_i = db.cursor()
        created = 0
        for s in students:
            sid = s["id"]
            klass = s.get("class_name")

            # Compute per-component charge
            total = 0.0
            per_comp = []
            for c in comps:
                cid = c["id"]
                amt = comp_defaults.get(cid, 0.0)
                if klass and klass in class_defaults and cid in class_defaults[klass]:
                    amt = class_defaults[klass][cid]
                if sid in items_map and cid in items_map[sid]:
                    amt = items_map[sid][cid]
                if amt and amt > 0:
                    per_comp.append((cid, c.get("name"), amt))
                    total += amt

            disc = discount_map.get(sid)
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
                (sid, year, term, due_date, 'draft', grand),
            )
            # Get invoice id (lastrowid works for insert; fetch id otherwise)
            inv_id = cur_i.lastrowid
            if not inv_id:
                cur.execute("SELECT id FROM invoices WHERE student_id=%s AND year=%s AND term=%s", (sid, year, term))
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
                desc = "Discount"
                cur_i.execute(
                    "INSERT INTO invoice_items (invoice_id, description, component_id, amount) VALUES (%s,%s,%s,%s)",
                    (inv_id, desc, None, -discount_val),
                )
            created += 1

        db.commit()
        flash(f"Generated/updated {created} invoices for {year} T{term}.", "success")
    except Exception as e:
        try: db.rollback()
        except Exception: pass
        flash(f"Error generating invoices: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("terms.manage_term_fees", year=year, term=term))

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
            f"SELECT student_id, fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({id_in_clause(ids)})",
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
            f"SELECT student_id, COALESCE(SUM(fee_amount),0) AS fsum FROM term_fees WHERE year=%s AND term < %s AND student_id IN ({id_in_clause(ids)}) GROUP BY student_id",
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
