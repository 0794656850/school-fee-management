from __future__ import annotations

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    current_app,
    session,
    Response,
)
import csv
from io import StringIO
from decimal import Decimal
import os
import mysql.connector

from utils.gmail_api import send_email as gmail_send_email


recovery_bp = Blueprint("recovery", __name__, url_prefix="/recovery")


def _db_from_config():
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
            pass

    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _detect_balance_column(cursor):
    cursor.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    if cursor.fetchone():
        return "balance"
    cursor.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
    if cursor.fetchone():
        return "fee_balance"
    return None


def ensure_recovery_tables(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS recovery_actions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            action VARCHAR(20) NOT NULL,
            status VARCHAR(20) DEFAULT NULL,
            amount_promised DECIMAL(12,2) DEFAULT NULL,
            promise_date DATE DEFAULT NULL,
            next_follow_up DATE DEFAULT NULL,
            notes TEXT,
            created_by VARCHAR(128) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_school_student_created (school_id, student_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


@recovery_bp.route("/")
def dashboard():
    db = _db_from_config()
    cur = db.cursor(dictionary=True)
    try:
        ensure_recovery_tables(db)

        bal_col = _detect_balance_column(cur)
        if not bal_col:
            flash("No valid balance column found in 'students' table.", "error")
            return render_template("recovery.html", students=[], classes=[], selected_class="", q="", min_balance=0)

        selected_class = (request.args.get("class") or "").strip()
        q = (request.args.get("q") or "").strip()
        try:
            min_balance = float(request.args.get("min_balance") or 0)
        except Exception:
            min_balance = 0.0

        base = [
            f"SELECT s.id, s.name, s.class_name, COALESCE(s.{bal_col},0) AS balance, ra.last_action, ra.last_at",
            "FROM students s",
            "LEFT JOIN (",
            "  SELECT student_id,",
            "         MAX(created_at) AS last_at,",
            "         SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(action, ' ', COALESCE(status,'')) ORDER BY created_at DESC SEPARATOR '\n'), '\n', 1) AS last_action",
            "  FROM recovery_actions",
            "  WHERE school_id=%s",
            "  GROUP BY student_id",
            ") ra ON ra.student_id = s.id",
            "WHERE s.school_id=%s AND COALESCE(s." + bal_col + ",0) > 0",
        ]
        params: list[object] = [session.get("school_id"), session.get("school_id")]
        if selected_class:
            base.append("AND s.class_name = %s")
            params.append(selected_class)
        if q:
            like = f"%{q}%"
            base.append("AND (s.name LIKE %s OR s.admission_no LIKE %s OR s.id = %s)")
            try:
                qid = int(q)
            except Exception:
                qid = -1
            params.extend([like, like, qid])
        if min_balance and min_balance > 0:
            base.append("AND COALESCE(s." + bal_col + ",0) >= %s")
            params.append(min_balance)
        base.append("ORDER BY COALESCE(s." + bal_col + ",0) DESC, s.name ASC")

        cur.execute("\n".join(base), tuple(params))
        students = cur.fetchall() or []

        cur.execute(
            "SELECT DISTINCT class_name FROM students WHERE school_id=%s AND class_name IS NOT NULL AND class_name<>'' ORDER BY class_name",
            (session.get("school_id"),),
        )
        classes = [row[0] if not isinstance(row, dict) else row.get("class_name") for row in cur.fetchall()]
    finally:
        db.close()

    return render_template(
        "recovery.html",
        students=students,
        classes=classes,
        selected_class=selected_class,
        q=q,
        min_balance=min_balance,
    )


@recovery_bp.route("/student/<int:student_id>")
def student_detail(student_id: int):
    db = _db_from_config()
    cur = db.cursor(dictionary=True)
    try:
        ensure_recovery_tables(db)
        bal_col = _detect_balance_column(cur)
        if not bal_col:
            flash("No valid balance column found in 'students' table.", "error")
            return redirect(url_for("recovery.dashboard"))

        cur.execute(
            f"SELECT id, name, class_name, COALESCE({bal_col},0) AS balance FROM students WHERE id=%s AND school_id=%s",
            (student_id, session.get("school_id")),
        )
        student = cur.fetchone()
        if not student:
            flash("Student not found.", "error")
            return redirect(url_for("recovery.dashboard"))

        cur.execute(
            """
            SELECT id, action, status, amount_promised, promise_date, next_follow_up, notes,
                   created_by, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i') AS created_at
            FROM recovery_actions
            WHERE school_id=%s AND student_id=%s
            ORDER BY created_at DESC, id DESC
            """,
            (session.get("school_id"), student_id),
        )
        actions = cur.fetchall() or []
    finally:
        db.close()

    return render_template("recovery_student.html", student=student, actions=actions)


@recovery_bp.route("/student/<int:student_id>/log", methods=["POST"])
def log_action(student_id: int):
    action = (request.form.get("action") or "").strip() or "note"
    status = (request.form.get("status") or "").strip() or None
    notes = (request.form.get("notes") or "").strip() or None
    created_by = (request.form.get("created_by") or "").strip() or None

    try:
        amount_promised = request.form.get("amount_promised")
        amount_promised_val = Decimal(amount_promised) if amount_promised else None
    except Exception:
        amount_promised_val = None
    promise_date = (request.form.get("promise_date") or "").strip() or None
    next_follow_up = (request.form.get("next_follow_up") or "").strip() or None

    db = _db_from_config()
    cur = db.cursor()
    try:
        ensure_recovery_tables(db)
        cur.execute(
            """
            INSERT INTO recovery_actions
            (school_id, student_id, action, status, amount_promised, promise_date, next_follow_up, notes, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                session.get("school_id"),
                student_id,
                action,
                status,
                amount_promised_val,
                promise_date,
                next_follow_up,
                notes,
                created_by,
            ),
        )
        db.commit()
        flash("Recovery action logged.", "success")
    except Exception as e:
        db.rollback()
        try:
            print(f"Recovery log insert failed: {e}")
        except Exception:
            pass
        flash("Failed to log recovery action.", "error")
    finally:
        db.close()

    return redirect(url_for("recovery.student_detail", student_id=student_id))


@recovery_bp.route("/export")
def export_csv():
    db = _db_from_config()
    cur = db.cursor(dictionary=True)
    try:
        bal_col = _detect_balance_column(cur)
        if not bal_col:
            flash("No valid balance column found in 'students' table.", "error")
            return redirect(url_for("recovery.dashboard"))

        cur.execute(
            f"""
            SELECT s.id, s.name, s.class_name, COALESCE(s.{bal_col},0) AS balance,
                   ra.last_action, ra.last_at
            FROM students s
            LEFT JOIN (
                SELECT student_id,
                       MAX(created_at) AS last_at,
                       SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(action, ' ', COALESCE(status,'')) ORDER BY created_at DESC SEPARATOR '\n'), '\n', 1) AS last_action
                FROM recovery_actions WHERE school_id=%s GROUP BY student_id
            ) ra ON ra.student_id=s.id
            WHERE s.school_id=%s AND COALESCE(s.{bal_col},0) > 0
            ORDER BY COALESCE(s.{bal_col},0) DESC, s.name ASC
            """,
            (session.get("school_id"), session.get("school_id")),
        )
        rows = cur.fetchall() or []
    finally:
        db.close()

    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(["ID", "Name", "Class", "Balance", "Last Action", "Last Contacted At"])
    for r in rows:
        writer.writerow([
            r.get("id"),
            r.get("name"),
            r.get("class_name"),
            f"{float(r.get('balance') or 0):.2f}",
            r.get("last_action") or "",
            r.get("last_at") or "",
        ])
    output = si.getvalue().encode()
    return Response(output, headers={
        "Content-Type": "text/csv",
        "Content-Disposition": "attachment; filename=defaulters.csv",
    })
