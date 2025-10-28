from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, session
from flask import jsonify
from datetime import datetime
import json
import mysql.connector
from urllib.parse import urlparse

# Audit trail removed


credit_bp = Blueprint("credit", __name__, url_prefix="/credit")


def _db():
    cfg = current_app.config
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


def ensure_credit_ops_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS credit_operations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME NOT NULL,
            actor VARCHAR(100),
            student_id INT NOT NULL,
            op_type VARCHAR(32) NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            reference VARCHAR(128),
            method VARCHAR(64),
            meta TEXT,
            school_id INT NULL,
            INDEX idx_credit_ops_school_id (school_id)
        )
        """
    )
    conn.commit()


def ensure_credit_transfers_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS credit_transfers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME NOT NULL,
            actor VARCHAR(100),
            ip_addr VARCHAR(64),
            correlation_id VARCHAR(64),
            from_student_id INT NOT NULL,
            to_student_id INT NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            applied_to_balance DECIMAL(12,2) NOT NULL DEFAULT 0,
            added_to_credit DECIMAL(12,2) NOT NULL DEFAULT 0,
            reference VARCHAR(128),
            method VARCHAR(64),
            meta TEXT,
            school_id INT NULL,
            INDEX idx_credit_transfers_school_id (school_id)
        )
        """
    )
    conn.commit()


def ensure_students_credit_column(conn) -> None:
    """Ensure students table has a numeric credit column.

    Adds `credit DECIMAL(12,2) DEFAULT 0` when missing to avoid runtime errors
    on instances created without the column. Safe to call repeatedly.
    """
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'credit'")
        has = bool(cur.fetchone())
        if not has:
            cur.execute("ALTER TABLE students ADD COLUMN credit DECIMAL(12,2) DEFAULT 0")
            conn.commit()
    except Exception:
        # Non-fatal; better to proceed than crash UI
        try:
            conn.rollback()
        except Exception:
            pass


def _detect_balance_column(cur) -> str:
    cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cur.fetchone())
    return "balance" if has_balance else "fee_balance"


@credit_bp.route("/")
def credit_home():
    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_credit_ops_table(db)
        ensure_students_credit_column(db)
        # Determine correct balance column once
        bal_col = _detect_balance_column(cur)
        # Source list: students with available credit (include balance to show max applicable)
        cur.execute(
            f"SELECT id, name, class_name, COALESCE(credit,0) AS credit, COALESCE({bal_col},0) AS balance FROM students WHERE COALESCE(credit,0) > 0 AND school_id=%s ORDER BY name",
            (session.get("school_id"),),
        )
        credit_students = cur.fetchall() or []

        # Destination list: students with credit or with outstanding balance (debt)
        cur.execute(
            f"SELECT id, name, class_name, COALESCE(credit,0) AS credit, COALESCE({bal_col},0) AS balance FROM students "
            f"WHERE (COALESCE(credit,0) > 0 OR COALESCE({bal_col},0) > 0) AND school_id=%s ORDER BY name",
            (session.get("school_id"),),
        )
        transfer_targets = cur.fetchall() or []
    finally:
        db.close()
    return render_template("credit.html", credit_students=credit_students, transfer_targets=transfer_targets)


@credit_bp.route("/api/search_sources")
def search_credit_sources():
    """Live search: students with available credit (> 0). Returns id, name, class, credit, balance."""
    q = (request.args.get("q") or "").strip()
    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_students_credit_column(db)
        bal_col = _detect_balance_column(cur)
        like = f"%{q}%" if q else "%"
        cur.execute(
            f"""
            SELECT id, name, class_name, COALESCE(credit,0) AS credit, COALESCE({bal_col},0) AS balance
            FROM students
            WHERE school_id=%s
              AND COALESCE(credit,0) > 0
              AND (name LIKE %s OR admission_no LIKE %s OR class_name LIKE %s)
            ORDER BY name ASC
            LIMIT 25
            """,
            (session.get("school_id"), like, like, like),
        )
        rows = cur.fetchall() or []
        return jsonify(rows)
    finally:
        db.close()


@credit_bp.route("/api/search_targets")
def search_credit_targets():
    """Live search: destination students (has credit or has outstanding balance)."""
    q = (request.args.get("q") or "").strip()
    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_students_credit_column(db)
        bal_col = _detect_balance_column(cur)
        like = f"%{q}%" if q else "%"
        cur.execute(
            f"""
            SELECT id, name, class_name, COALESCE(credit,0) AS credit, COALESCE({bal_col},0) AS balance
            FROM students
            WHERE school_id=%s
              AND (COALESCE(credit,0) > 0 OR COALESCE({bal_col},0) > 0)
              AND (name LIKE %s OR admission_no LIKE %s OR class_name LIKE %s)
            ORDER BY name ASC
            LIMIT 25
            """,
            (session.get("school_id"), like, like, like),
        )
        rows = cur.fetchall() or []
        return jsonify(rows)
    finally:
        db.close()

@credit_bp.route("/apply", methods=["POST"])
def apply_credit():
    student_id = request.form.get("student_id", type=int)
    amount = request.form.get("amount", type=float)
    if not student_id or not amount or amount <= 0:
        flash("Provide a valid student and amount to apply.", "warning")
        return redirect(url_for("credit.credit_home"))

    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_students_credit_column(db)
        col = _detect_balance_column(cur)
        cur.execute(f"SELECT {col} AS balance, COALESCE(credit,0) AS credit, name FROM students WHERE id=%s AND school_id=%s", (student_id, session.get("school_id")))
        row = cur.fetchone()
        if not row:
            flash("Student not found.", "error")
            return redirect(url_for("credit.credit_home"))

        balance = float(row.get("balance") or 0)
        credit = float(row.get("credit") or 0)
        # Enforce hard cap: cannot apply more than available credit and outstanding debt
        max_applicable = min(credit, balance if balance > 0 else 0)
        if max_applicable <= 0:
            flash("Nothing to apply: either no credit or no outstanding balance.", "info")
            return redirect(url_for("credit.credit_home"))
        if amount > max_applicable:
            flash(
                f"Insufficient amount: available to apply is KES {max_applicable:,.2f} (credit {credit:,.2f}, debt {balance:,.2f}).",
                "warning",
            )
            return redirect(url_for("credit.credit_home"))
        to_apply = amount

        new_balance = max(balance - to_apply, 0)
        new_credit = max(credit - to_apply, 0)
        cur.execute(f"UPDATE students SET {col}=%s, credit=%s WHERE id=%s AND school_id=%s", (new_balance, new_credit, student_id, session.get("school_id")))
        db.commit()

        # audit removed

        ensure_credit_ops_table(db)
        cur2 = db.cursor()
        cur2.execute(
            "INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (datetime.utcnow(), session.get("username"), student_id, "apply", to_apply, None, None, json.dumps({"source": "manual"}), session.get("school_id")),
        )
        db.commit()

        flash(f"Applied KES {to_apply:,.2f} credit for {row.get('name')}.", "success")
    except Exception as e:
        db.rollback()
        flash(f"Error applying credit: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("credit.credit_home"))


@credit_bp.route("/refund", methods=["POST"])
def refund_credit():
    student_id = request.form.get("student_id", type=int)
    amount = request.form.get("amount", type=float)
    method = (request.form.get("method") or "").strip() or None
    reference = (request.form.get("reference") or "").strip() or None
    if not student_id or not amount or amount <= 0:
        flash("Provide a valid student and amount to refund.", "warning")
        return redirect(url_for("credit.credit_home"))

    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_students_credit_column(db)
        cur.execute("SELECT COALESCE(credit,0) AS credit, name FROM students WHERE id=%s AND school_id=%s", (student_id, session.get("school_id")))
        row = cur.fetchone()
        if not row:
            flash("Student not found.", "error")
            return redirect(url_for("credit.credit_home"))

        credit = float(row.get("credit") or 0)
        if credit <= 0:
            flash("No available credit to refund.", "info")
            return redirect(url_for("credit.credit_home"))
        if amount > credit:
            flash(
                f"Insufficient credit: available KES {credit:,.2f}, requested KES {amount:,.2f}.",
                "warning",
            )
            return redirect(url_for("credit.credit_home"))
        to_refund = amount

        new_credit = max(credit - to_refund, 0)
        cur.execute("UPDATE students SET credit=%s WHERE id=%s AND school_id=%s", (new_credit, student_id, session.get("school_id")))
        db.commit()

        # audit removed

        ensure_credit_ops_table(db)
        cur2 = db.cursor()
        cur2.execute(
            "INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (datetime.utcnow(), session.get("username"), student_id, "refund", to_refund, reference, method, json.dumps({}), session.get("school_id")),
        )
        db.commit()

        flash(f"Refunded KES {to_refund:,.2f} to {row.get('name')}.", "success")
    except Exception as e:
        db.rollback()
        flash(f"Error refunding credit: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("credit.credit_home"))


@credit_bp.route("/transfer", methods=["POST"])
def transfer_credit():
    from_id = request.form.get("from_student_id", type=int)
    to_id = request.form.get("to_student_id", type=int)
    amount = request.form.get("amount", type=float)
    if not from_id or not to_id or from_id == to_id or not amount or amount <= 0:
        flash("Provide valid students and transfer amount.", "warning")
        return redirect(url_for("credit.credit_home"))

    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_students_credit_column(db)
        col = _detect_balance_column(cur)
        # Load both students
        cur.execute("SELECT id, name, COALESCE(credit,0) AS credit FROM students WHERE id=%s AND school_id=%s", (from_id, session.get("school_id")))
        src = cur.fetchone()
        cur.execute(f"SELECT id, name, COALESCE({col},0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s AND school_id=%s", (to_id, session.get("school_id")))
        dst = cur.fetchone()
        if not src or not dst:
            flash("Student not found.", "error")
            return redirect(url_for("credit.credit_home"))

        available = float(src.get("credit") or 0)
        # Enforce strict cap: cannot transfer more than available credit
        if amount > available:
            flash(
                f"Insufficient credit: available KES {available:,.2f}, requested KES {amount:,.2f}.",
                "warning",
            )
            return redirect(url_for("credit.credit_home"))
        to_transfer = amount
        if to_transfer <= 0:
            flash("Source student has no available credit.", "info")
            return redirect(url_for("credit.credit_home"))

        # Apply to destination's balance first, surplus becomes credit.
        dst_balance = float(dst.get("balance") or 0)
        dst_credit = float(dst.get("credit") or 0)
        if dst_balance <= 0 and dst_credit <= 0:
            flash("Destination must have existing debt or credit to receive a transfer.", "warning")
            return redirect(url_for("credit.credit_home"))
        apply_to_balance = min(dst_balance, to_transfer)
        leftover = to_transfer - apply_to_balance
        new_dst_balance = max(dst_balance - apply_to_balance, 0)
        new_dst_credit = dst_credit + leftover

        new_src_credit = max(available - to_transfer, 0)

        # Update both students atomically
        cur.execute("UPDATE students SET credit=%s WHERE id=%s AND school_id=%s", (new_src_credit, from_id, session.get("school_id")))
        cur.execute(f"UPDATE students SET {col}=%s, credit=%s WHERE id=%s AND school_id=%s", (new_dst_balance, new_dst_credit, to_id, session.get("school_id")))
        db.commit()

        # Record audit trails
        ensure_credit_ops_table(db)
        ensure_credit_transfers_table(db)
        cur2 = db.cursor()
        import uuid
        corr_id = uuid.uuid4().hex
        meta = {"to_id": to_id, "applied_to_balance": apply_to_balance, "added_to_credit": leftover}
        cur2.execute(
            "INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (datetime.utcnow(), session.get("username"), from_id, "transfer", to_transfer, None, None, json.dumps({"correlation_id": corr_id, **meta}), session.get("school_id")),
        )
        cur2.execute(
            "INSERT INTO credit_transfers (ts, actor, ip_addr, correlation_id, from_student_id, to_student_id, amount, applied_to_balance, added_to_credit, reference, method, meta, school_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                datetime.utcnow(),
                session.get("username"),
                request.remote_addr,
                corr_id,
                from_id,
                to_id,
                to_transfer,
                apply_to_balance,
                leftover,
                None,
                None,
                json.dumps({"src_name": src.get("name"), "dst_name": dst.get("name")}),
                session.get("school_id"),
            ),
        )
        # Also create a visible payment record for the recipient so it appears in their history.
        try:
            cur3 = db.cursor()
            ref_note = f"From {src.get('name')} (ID {from_id}) | Applied {apply_to_balance:.2f}, Credit {leftover:.2f}"
            cur3.execute(
                "INSERT INTO payments (student_id, amount, method, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s)",
                (to_id, to_transfer, "Credit Transfer", ref_note, datetime.utcnow(), session.get("school_id")),
            )
        except Exception:
            # Non-fatal: continue even if payment note insert fails
            pass
        db.commit()

        flash(
            f"Transferred KES {to_transfer:,.2f} from {src.get('name')} to {dst.get('name')}. "
            f"Applied {apply_to_balance:,.2f} to balance; {leftover:,.2f} as credit.",
            "success",
        )
    except Exception as e:
        db.rollback()
        flash(f"Error transferring credit: {e}", "error")
    finally:
        db.close()
    return redirect(url_for("credit.credit_home"))
