from __future__ import annotations

from utils.timezone_helpers import EATDateTime as datetime

import mysql.connector
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app

from utils.db_helpers import ensure_refund_requests_table
from utils.audit import log_event
from utils.mpesa import b2c_payment, DarajaError
from routes.credit_routes import ensure_students_credit_column, ensure_credit_ops_table


refund_bp = Blueprint("refunds", __name__, url_prefix="/admin")


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


def _b2c_ready() -> bool:
    cfg = current_app.config
    initiator = (cfg.get("DARAJA_B2C_INITIATOR_NAME") or "").strip()
    credential = (cfg.get("DARAJA_B2C_SECURITY_CREDENTIAL") or "").strip()
    short_code = (cfg.get("DARAJA_B2C_SHORT_CODE") or cfg.get("DARAJA_SHORT_CODE") or "").strip()
    return bool(initiator and credential and short_code)


def _require_admin():
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return redirect(url_for("admin.login"))


@refund_bp.route("/refunds", methods=["GET"])
def refunds_home():
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    try:
        ensure_refund_requests_table(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            """
            SELECT r.*, s.name AS student_name, s.class_name
            FROM refund_requests r
            JOIN students s ON r.student_id=s.id
            WHERE r.school_id=%s
            ORDER BY r.id DESC
            LIMIT 50
            """,
            (session.get("school_id"),),
        )
        requests = cur.fetchall() or []
        cur.execute(
            "SELECT id, name, class_name, COALESCE(credit,0) AS credit FROM students WHERE school_id=%s ORDER BY name ASC",
            (session.get("school_id"),),
        )
        students = cur.fetchall() or []
    finally:
        db.close()
    return render_template("admin/refunds.html", requests=requests, students=students, b2c_enabled=_b2c_ready())


@refund_bp.route("/refunds/request", methods=["POST"])
def refund_request_create():
    guard = _require_admin()
    if guard:
        return guard
    student_id = request.form.get("student_id", type=int)
    amount = request.form.get("amount", type=float)
    method = (request.form.get("method") or "").strip()
    reference = (request.form.get("reference") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    reason = (request.form.get("reason") or "").strip()
    if not student_id or not amount or amount <= 0:
        flash("Provide a valid student and amount.", "warning")
        return redirect(url_for("refunds.refunds_home"))
    db = _db()
    try:
        ensure_refund_requests_table(db)
        ensure_students_credit_column(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT COALESCE(credit,0) AS credit FROM students WHERE id=%s AND school_id=%s",
            (student_id, session.get("school_id")),
        )
        row = cur.fetchone()
        if not row:
            flash("Student not found.", "error")
            return redirect(url_for("refunds.refunds_home"))
        credit = float(row.get("credit") or 0)
        if amount > credit:
            flash(f"Insufficient credit: available KES {credit:,.2f}.", "warning")
            return redirect(url_for("refunds.refunds_home"))
        cur.execute(
            """
            INSERT INTO refund_requests
            (school_id, student_id, amount, method, phone, reference, reason, status, requested_by, requested_at, credit_before)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                session.get("school_id"),
                student_id,
                amount,
                method or None,
                phone or None,
                reference or None,
                reason or None,
                "pending",
                session.get("username"),
                datetime.utcnow(),
                credit,
            ),
        )
        db.commit()
        flash("Refund request created.", "success")
    finally:
        db.close()
    return redirect(url_for("refunds.refunds_home"))


@refund_bp.route("/refunds/<int:req_id>/approve", methods=["POST"])
def refund_approve(req_id: int):
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    try:
        ensure_refund_requests_table(db)
        cur = db.cursor()
        cur.execute(
            "UPDATE refund_requests SET status='approved', approved_by=%s, approved_at=%s WHERE id=%s AND school_id=%s",
            (session.get("username"), datetime.utcnow(), req_id, session.get("school_id")),
        )
        db.commit()
        flash("Refund approved.", "success")
    finally:
        db.close()
    return redirect(url_for("refunds.refunds_home"))


@refund_bp.route("/refunds/<int:req_id>/reject", methods=["POST"])
def refund_reject(req_id: int):
    guard = _require_admin()
    if guard:
        return guard
    note = (request.form.get("admin_note") or "").strip()
    db = _db()
    try:
        ensure_refund_requests_table(db)
        cur = db.cursor()
        cur.execute(
            "UPDATE refund_requests SET status='rejected', admin_note=%s WHERE id=%s AND school_id=%s",
            (note or None, req_id, session.get("school_id")),
        )
        db.commit()
        flash("Refund rejected.", "info")
    finally:
        db.close()
    return redirect(url_for("refunds.refunds_home"))


@refund_bp.route("/refunds/<int:req_id>/process", methods=["POST"])
def refund_process(req_id: int):
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    cur = db.cursor(dictionary=True)
    try:
        ensure_refund_requests_table(db)
        ensure_students_credit_column(db)
        ensure_credit_ops_table(db)
        cur.execute(
            "SELECT * FROM refund_requests WHERE id=%s AND school_id=%s",
            (req_id, session.get("school_id")),
        )
        req = cur.fetchone()
        if not req or req.get("status") != "approved":
            flash("Refund request is not approved.", "warning")
            return redirect(url_for("refunds.refunds_home"))
        student_id = int(req["student_id"])
        amount = float(req.get("amount") or 0)
        method = (req.get("method") or "").strip() or "Manual refund"
        phone = (req.get("phone") or "").strip()
        reference = (req.get("reference") or "").strip()

        cur.execute(
            "SELECT COALESCE(credit,0) AS credit, name FROM students WHERE id=%s AND school_id=%s",
            (student_id, session.get("school_id")),
        )
        student = cur.fetchone()
        if not student:
            flash("Student not found.", "error")
            return redirect(url_for("refunds.refunds_home"))
        credit = float(student.get("credit") or 0)
        if amount > credit:
            flash("Insufficient credit for refund.", "warning")
            return redirect(url_for("refunds.refunds_home"))

        send_b2c = bool(phone and _b2c_ready())
        b2c_response = None
        stored_method = method
        if send_b2c:
            try:
                b2c_response = b2c_payment(phone=phone, amount=amount, remarks=reference, occasion=stored_method or None)
            except DarajaError as e:
                flash(f"M-Pesa refund failed: {e}", "error")
                return redirect(url_for("refunds.refunds_home"))
            stored_method = "M-Pesa B2C"
            reference = reference or b2c_response.get("ConversationID") or b2c_response.get("OriginatorConversationID") or b2c_response.get("TransactionID")

        new_credit = max(credit - amount, 0)
        cur.execute(
            "UPDATE students SET credit=%s WHERE id=%s AND school_id=%s",
            (new_credit, student_id, session.get("school_id")),
        )
        cur2 = db.cursor()
        meta = {"source": "refund_request", "method": stored_method}
        if phone:
            meta["phone"] = phone
        if b2c_response:
            meta["b2c_response"] = b2c_response
        import json
        cur2.execute(
            "INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                datetime.utcnow(),
                session.get("username"),
                student_id,
                "refund",
                amount,
                reference or None,
                stored_method,
                json.dumps(meta),
                session.get("school_id"),
            ),
        )
        cur.execute(
            """
            UPDATE refund_requests
            SET status='processed', processed_at=%s, credit_after=%s, reference=%s
            WHERE id=%s AND school_id=%s
            """,
            (datetime.utcnow(), new_credit, reference or None, req_id, session.get("school_id")),
        )
        db.commit()
        try:
            log_event("refund_processed", target=f"refund:{req_id}", detail=f"Student {student_id}, amount {amount}")
        except Exception:
            pass
        flash("Refund processed.", "success")
    finally:
        db.close()
    return redirect(url_for("refunds.refunds_home"))
