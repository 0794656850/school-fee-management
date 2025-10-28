from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app, url_for, redirect, flash
import json
import mysql.connector
from datetime import datetime

from utils.mpesa import stk_push, DarajaError, parse_callback_items
from utils.pro import set_license_key


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
    db.commit()


@mpesa_bp.route("/checkout", methods=["POST"])
def checkout():
    # Requires admin session on the UI, but does not strictly guard here.
    phone = (request.form.get("phone") or request.json.get("phone") if request.is_json else "").strip()
    try:
        amount = int(request.form.get("amount") or (request.json.get("amount") if request.is_json else 0) or current_app.config.get("PRO_PRICE_KES", 1500))
    except Exception:
        amount = int(current_app.config.get("PRO_PRICE_KES", 1500))

    account_ref = current_app.config.get("DARAJA_ACCOUNT_REF", "FMS-PRO")
    trans_desc = current_app.config.get("DARAJA_TRANSACTION_DESC", "Pro upgrade")
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

    # If paid, activate Pro (one-time) using receipt as unique ref
    if str(result_code) == "0" and meta.get("receipt"):
        ref = meta.get("receipt").upper()
        # Create activation record if not exists
        cur = db.cursor()
        cur.execute("SELECT id FROM pro_activations WHERE mpesa_ref=%s", (ref,))
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO pro_activations (mpesa_ref, amount, activated_at, notes) VALUES (%s,%s,%s,%s)",
                (ref, meta.get("amount"), now, f"Auto-activated via callback {checkout_id}"),
            )
            db.commit()
            # Generate license-like key and store
            import hashlib as _hashlib
            h6 = _hashlib.sha1(ref.encode("utf-8")).hexdigest()[:6].upper()
            license_key = f"CS-PRO-{ref}-{h6}"
            set_license_key(license_key)
    db.close()

    return jsonify({"status": "ok"})

