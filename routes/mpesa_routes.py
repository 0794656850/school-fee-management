from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app, url_for, redirect, flash, session, abort
import json
import mysql.connector
from datetime import datetime

from utils.mpesa import stk_push, DarajaError, parse_callback_items
from routes.student_portal import ensure_mpesa_student_table, record_mpesa_payment_if_missing  # reuse table creator
# Pro activation is no longer auto-granted on STK callbacks.
# Keys are issued only after admin verification.
try:
    # Optional Gmail API helpers
    from utils.gmail_api import (
        send_email as gmail_send_email,
        send_email_html as gmail_send_email_html,
    )
except Exception:  # graceful fallback
    def gmail_send_email(*args, **kwargs):  # type: ignore
        return False

    def gmail_send_email_html(*args, **kwargs):  # type: ignore
        return False


mpesa_bp = Blueprint("mpesa", __name__, url_prefix="/mpesa")


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


def ensure_tables(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mpesa_payments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            merchant_request_id VARCHAR(64),
            checkout_request_id VARCHAR(64),
            result_code INT DEFAULT NULL,
            result_desc VARCHAR(255) DEFAULT NULL,
            mpesa_receipt VARCHAR(32) DEFAULT NULL,
            phone VARCHAR(32) DEFAULT NULL,
            amount DECIMAL(10,2) DEFAULT NULL,
            raw_callback TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mpesa_b2c_callbacks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            callback_type VARCHAR(32) NOT NULL,
            conversation_id VARCHAR(64) DEFAULT NULL,
            originator_conversation_id VARCHAR(64) DEFAULT NULL,
            transaction_id VARCHAR(64) DEFAULT NULL,
            result_code INT DEFAULT NULL,
            result_desc VARCHAR(255) DEFAULT NULL,
            payload LONGTEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            INDEX idx_mpesa_b2c_callback_type (callback_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()
    # Student STK tracking table (for portal payments)
    try:
        ensure_mpesa_student_table(db)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


def _record_b2c_callback(callback_type: str, payload: dict) -> None:
    db = None
    try:
        db = _db()
        ensure_tables(db)
        cur = db.cursor()
        now = datetime.now()
        result = (payload.get("Result") or {}) if isinstance(payload, dict) else {}
        cur.execute(
            """
            INSERT INTO mpesa_b2c_callbacks (callback_type, conversation_id, originator_conversation_id, transaction_id, result_code, result_desc, payload, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                callback_type,
                result.get("ConversationID"),
                result.get("OriginatorConversationID"),
                result.get("TransactionID"),
                result.get("ResultCode"),
                result.get("ResultDesc"),
                json.dumps(payload),
                now,
                now,
            ),
        )
        db.commit()
    except Exception:
        current_app.logger.exception("Failed to persist M-Pesa B2C callback")
        if db:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        if db:
            db.close()


@mpesa_bp.route("/checkout", methods=["POST"])
def checkout():
    # Requires admin session on the UI, but does not strictly guard here.
    if not session.get("user_logged_in"):
        abort(403)

    phone = (request.form.get("phone") or request.json.get("phone") if request.is_json else "").strip()
    try:
        amount = int(request.form.get("amount") or (request.json.get("amount") if request.is_json else 0) or current_app.config.get("PRO_PRICE_KES", 1500))
    except Exception:
        amount = int(current_app.config.get("PRO_PRICE_KES", 1500))

    account_ref = current_app.config.get("DARAJA_ACCOUNT_REF", "FMS-PRO")
    trans_desc = current_app.config.get("DARAJA_TRANSACTION_DESC", "Pro upgrade")
    if amount <= 0 or amount > current_app.config.get("PRO_PRICE_KES", 1500) * 50:
        flash("Invalid Pro price. Please try again.", "error")
        return redirect(url_for("admin.billing"))
    try:
        res = stk_push(phone=phone, amount=amount, account_ref=account_ref, trans_desc=trans_desc)
        # Persist initial record
        db = _db(); ensure_tables(db)
        cur = db.cursor()
        now = datetime.now()
        cur.execute(
            "INSERT INTO mpesa_payments (merchant_request_id, checkout_request_id, created_at, updated_at) VALUES (%s, %s, %s, %s)",
            (res.get("MerchantRequestID"), res.get("CheckoutRequestID"), now, now),
        )
        db.commit(); db.close()
        flash("STK push sent. Check your phone to authorize.", "info")
        return redirect(url_for("admin.billing"))
    except DarajaError as e:
        flash(f"M-Pesa error: {e}", "error")
        return redirect(url_for("admin.billing"))


@mpesa_bp.route("/callback", methods=["POST"])
def callback():
    data = request.get_json(silent=True) or {}
    body = data.get("Body", {})
    resp = body.get("stkCallback", {})
    checkout_id = resp.get("CheckoutRequestID")
    merchant_id = resp.get("MerchantRequestID")
    result_code = resp.get("ResultCode")
    result_desc = resp.get("ResultDesc")
    items = (resp.get("CallbackMetadata") or {}).get("Item", [])
    meta = parse_callback_items(items)

    # Persist/update
    db = _db(); ensure_tables(db)
    cur = db.cursor()
    now = datetime.now()
    cur.execute(
        """
        UPDATE mpesa_payments
        SET result_code=%s, result_desc=%s, mpesa_receipt=%s, phone=%s, amount=%s, raw_callback=%s, updated_at=%s
        WHERE checkout_request_id=%s
        """,
        (
            result_code, result_desc, meta.get("receipt"), meta.get("phone"), meta.get("amount"), json.dumps(data), now, checkout_id,
        ),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO mpesa_payments (merchant_request_id, checkout_request_id, result_code, result_desc, mpesa_receipt, phone, amount, raw_callback, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (merchant_id, checkout_id, result_code, result_desc, meta.get("receipt"), meta.get("phone"), meta.get("amount"), json.dumps(data), now, now),
        )
    db.commit()

    # Also update student STK records and, on success, record a student payment
    try:
        cur2 = db.cursor(dictionary=True)
        cur2.execute(
            "SELECT * FROM mpesa_student_payments WHERE checkout_request_id=%s",
            (checkout_id,),
        )
        row = cur2.fetchone()
        if row:
            # Update the mpesa_student_payments row
            cur3 = db.cursor()
            cur3.execute(
                """
                UPDATE mpesa_student_payments
                SET result_code=%s, result_desc=%s, mpesa_receipt=%s, phone=%s, amount=%s, raw_callback=%s, updated_at=%s
                WHERE checkout_request_id=%s
                """,
                (
                    result_code,
                    result_desc,
                    meta.get("receipt"),
                    meta.get("phone"),
                    meta.get("amount"),
                    json.dumps(data),
                    now,
                    checkout_id,
                ),
            )
            db.commit()
            # If success, insert into payments table for the student
            if str(result_code) == "0":
                student_id = row.get("student_id")
                school_id = row.get("school_id")
                y = row.get("year")
                t = row.get("term")
                raw_amount = meta.get("amount") or row.get("amount") or 0
                try:
                    amount_val = float(raw_amount or 0)
                except Exception:
                    amount_val = 0.0
                ref = (meta.get("receipt") or f"MP_{checkout_id}")
                payment_id = record_mpesa_payment_if_missing(
                    db=db,
                    student_id=student_id,
                    amount=amount_val,
                    reference=ref,
                    school_id=school_id,
                    year=y,
                    term=t,
                    now=now,
                )
                if payment_id:
                    # Email confirmation to guardian/student when email exists
                    try:
                        cur5 = db.cursor()
                        # Pick preferred email column
                        email_col = "email"
                        try:
                            cur5.execute("SHOW COLUMNS FROM students LIKE 'email'")
                            has_email = bool(cur5.fetchone())
                        except Exception:
                            has_email = False
                        has_parent_email = False
                        if not has_email:
                            try:
                                cur5.execute("SHOW COLUMNS FROM students LIKE 'parent_email'")
                                has_parent_email = bool(cur5.fetchone())
                            except Exception:
                                has_parent_email = False
                            if has_parent_email:
                                email_col = "parent_email"
                        # Fetch the email if column exists
                        student_email = None
                        if has_email or has_parent_email:
                            cur5.execute(f"SELECT {email_col} FROM students WHERE id=%s", (student_id,))
                            r5 = cur5.fetchone()
                            if r5 and r5[0]:
                                student_email = str(r5[0]).strip()
                        if student_email:
                            try:
                                try:
                                    receipt_url = url_for('payment_receipt', payment_id=payment_id, _external=True)
                                except Exception:
                                    receipt_url = ""
                                subject = f"Payment received - KES {float(amount_val):,.2f}"
                                brand = current_app.config.get("APP_NAME") or "School"
                                student_name = row.get("student_name") or "Student"
                                html = (
                                    f"<p>Hi,</p><p>We received your payment of <strong>KES {float(amount_val):,.2f}</strong> via M-Pesa (Ref: {ref}).</p>"
                                    f"<p>Thank you. {('View receipt: <a href=\"'+receipt_url+'\">'+receipt_url+'</a>') if receipt_url else ''}</p>"
                                    f"<p>— {brand}</p>"
                                )
                                if not gmail_send_email_html(student_email, subject, html):
                                    gmail_send_email(student_email, subject, f"Payment received KES {float(amount_val):,.2f} via M-Pesa (Ref: {ref}). " + (f"View receipt: {receipt_url}" if receipt_url else ""))
                            except Exception:
                                pass
                    except Exception:
                        pass
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass

    # IMPORTANT: Do NOT auto-activate Pro here. Admin must verify first.
    # We only persist callback data; the licensing workflow handles issuance.
    db.close()

    return jsonify({"status": "ok"})


@mpesa_bp.route("/b2c/result", methods=["POST"])
def b2c_result():
    payload = request.get_json(silent=True) or {}
    _record_b2c_callback("result", payload)
    return jsonify({"status": "ok"})


@mpesa_bp.route("/b2c/timeout", methods=["POST"])
def b2c_timeout():
    payload = request.get_json(silent=True) or {}
    _record_b2c_callback("timeout", payload)
    return jsonify({"status": "ok"})
