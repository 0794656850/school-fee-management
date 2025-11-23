from __future__ import annotations

from datetime import datetime
from flask import Blueprint, render_template, request, session, flash, redirect, url_for, current_app

from utils.db_helpers import ensure_approval_requests_table
from utils.notifications import generate_otp, hash_otp, send_otp_email, send_alert_email
from utils.document_qr import build_document_qr
from utils.audit import log_event

approval_bp = Blueprint("approval", __name__, url_prefix="/admin/approvals")


REQUEST_TYPES = [
    ("fee_write_off", "Fee write-off"),
    ("discount", "Discount approval"),
    ("credit_transfer", "Credit transfer"),
]


def _db_conn():
    try:
        from app import get_db_connection  # noqa: W0611 - lazy import

        return get_db_connection()
    except Exception:
        return None


def _require_admin_guard():
    try:
        from routes.admin_routes import _require_admin

        guard = _require_admin()
        return guard
    except Exception:
        return redirect(url_for("admin.login"))


@approval_bp.route("", methods=["GET", "POST"])
def approvals_dashboard():
    guard = _require_admin_guard()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        flash("Select a school before submitting approval requests.", "warning")
        return redirect(url_for("choose_school"))

    db = _db_conn()
    pending_request = None
    try:
        if not db:
            flash("Database unavailable.", "error")
            return redirect(url_for("admin.dashboard"))
        ensure_approval_requests_table(db)
        pending_request = _handle_request_submission(db, sid)
        requests = _fetch_requests(db, sid)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass

    return render_template(
        "admin/approvals.html",
        requests=requests,
        request_types=REQUEST_TYPES,
        pending_request=pending_request,
    )


def _fetch_requests(db, school_id):
    cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM approval_requests
        WHERE school_id=%s
        ORDER BY created_at DESC
        LIMIT 50
        """,
        (school_id,),
    )
    return cur.fetchall() or []


def _handle_request_submission(db, school_id):
    form = request.form
    otp = form.get("otp", "").strip()
    pending_id = form.get("pending_request_id", "").strip()
    if pending_id and otp:
        return _verify_pending_request(db, school_id, int(pending_id), otp)
    if request.method != "POST":
        return None
    name = (form.get("requestor_name") or "").strip()
    email = (form.get("requestor_email") or "").strip()
    req_type = (form.get("request_type") or "").strip()
    reason = (form.get("reason") or "").strip()
    amount = form.get("amount") or ""
    if not name or not email or not req_type:
        flash("Name, email, and request type are required.", "warning")
        return None
    try:
        amount_value = float(amount) if amount else None
    except Exception:
        amount_value = None
    code = generate_otp()
    otp_hash = hash_otp(code)
    now = datetime.utcnow()
    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO approval_requests
            (school_id, requestor_name, requestor_email, request_type, amount, reason, status, otp_hash, otp_requested_at, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            school_id,
            name,
            email,
            req_type,
            amount_value,
            reason,
            "otp_pending",
            otp_hash,
            now,
            now,
            now,
        ),
    )
    db.commit()
    request_id = cur.lastrowid
    sent = send_otp_email(email, code)
    if sent:
        flash(f"OTP sent to {email}. Enter it below to finalize the request.", "info")
    else:
        flash("Unable to send OTP email yet; please check email configuration.", "warning")
    log_event(
        "approval_requests",
        "otp_requested",
        detail=f"Request {request_id} created; OTP sent to {email}",
    )
    return {"id": request_id, "email": email}


def _verify_pending_request(db, school_id, request_id, otp):
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT otp_hash, status FROM approval_requests WHERE id=%s AND school_id=%s",
        (request_id, school_id),
    )
    row = cur.fetchone()
    if not row:
        flash("No matching request found.", "error")
        return None
    if row["status"] != "otp_pending":
        flash("This request is already confirmed.", "info")
        return None
    if hash_otp(otp) != (row["otp_hash"] or ""):
        flash("OTP mismatch. Please check and try again.", "error")
        return {"id": request_id}
    now = datetime.utcnow()
    cur.execute(
        """
        UPDATE approval_requests
        SET status=%s, updated_at=%s
        WHERE id=%s AND school_id=%s
        """,
        ("pending", now, request_id, school_id),
    )
    db.commit()
    flash("Approval request confirmed and now visible to approvers.", "success")
    log_event("approval_requests", "otp_verified", detail=f"Request {request_id} verified")
    return None


@approval_bp.route("/<int:request_id>/action", methods=["POST"])
def approval_action(request_id):
    guard = _require_admin_guard()
    if guard is not None:
        return guard
    sid = session.get("school_id")
    if not sid:
        return redirect(url_for("choose_school"))
    action = (request.form.get("action") or "").strip().lower()
    note = (request.form.get("admin_note") or "").strip()
    db = _db_conn()
    if not db:
        flash("Database unavailable.", "error")
        return redirect(url_for("approval.approvals_dashboard"))
    ensure_approval_requests_table(db)
    try:
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM approval_requests WHERE id=%s AND school_id=%s",
            (request_id, sid),
        )
        row = cur.fetchone()
        if not row:
            flash("Approval request not found.", "error")
            return redirect(url_for("approval.approvals_dashboard"))
        status = row["status"]
        if status not in ("pending", "approved"):
            flash("Request cannot be actioned in its current state.", "warning")
            return redirect(url_for("approval.approvals_dashboard"))
        now = datetime.utcnow()
        if action == "approve":
            qr = build_document_qr(
                "approval",
                {
                    "request_id": request_id,
                    "amount": float(row.get("amount") or 0),
                    "type": row.get("request_type") or "",
                    "school_id": sid,
                },
            )
            cur.execute(
                """
                UPDATE approval_requests
                SET status=%s, approver=%s, approved_at=%s, qr_payload=%s, admin_note=%s, updated_at=%s
                WHERE id=%s AND school_id=%s
                """,
                (
                    "approved",
                    session.get("username") or "Admin",
                    now,
                    qr,
                    note,
                    now,
                    request_id,
                    sid,
                ),
            )
            db.commit()
            flash("Request approved.", "success")
            log_event("approval_requests", "approved", detail=f"Request {request_id} approved")
            send_alert_email(
                f"Approval request {request_id} approved",
                f"Your request for {row.get('request_type')} has been approved.",
                [row.get("requestor_email") or ""],
            )
        elif action == "reject":
            cur.execute(
                """
                UPDATE approval_requests
                SET status=%s, admin_note=%s, updated_at=%s
                WHERE id=%s AND school_id=%s
                """,
                ("rejected", note, now, request_id, sid),
            )
            db.commit()
            flash("Request rejected.", "info")
            log_event("approval_requests", "rejected", detail=f"Request {request_id} rejected")
            send_alert_email(
                f"Approval request {request_id} rejected",
                f"Your request has been rejected. Note: {note}",
                [row.get("requestor_email") or ""],
            )
    finally:
        try:
            db.close()
        except Exception:
            pass
    return redirect(url_for("approval.approvals_dashboard"))
