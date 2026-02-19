from __future__ import annotations

import csv
from datetime import datetime, timedelta, date
from io import StringIO
from typing import Dict, Any, Optional

import mysql.connector
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app

from utils.db_helpers import ensure_reconciliation_tables
from utils.audit import log_event


recon_bp = Blueprint("reconciliation", __name__, url_prefix="/admin")


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


def _require_admin():
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return redirect(url_for("admin.login"))


def _parse_date(value: str) -> Optional[date]:
    if not value:
        return None
    raw = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    lower = {str(k).strip().lower(): v for k, v in row.items()}
    return {
        "date": lower.get("date") or lower.get("txn_date") or lower.get("transaction_date"),
        "amount": lower.get("amount") or lower.get("value"),
        "reference": lower.get("reference") or lower.get("ref") or lower.get("receipt"),
        "phone": lower.get("phone") or lower.get("msisdn"),
        "payer_name": lower.get("payer") or lower.get("name") or lower.get("account_name"),
    }


def _auto_match_rows(db, school_id: int, import_id: int) -> int:
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM reconciliation_rows WHERE import_id=%s AND school_id=%s AND match_status='pending'",
        (import_id, school_id),
    )
    rows = cur.fetchall() or []
    matched = 0
    for r in rows:
        row_id = int(r["id"])
        amount = r.get("amount")
        ref = (r.get("reference") or "").strip()
        txn_date = r.get("txn_date")
        match_id = None
        score = 0
        if ref:
            cur.execute(
                "SELECT id, amount FROM payments WHERE school_id=%s AND LOWER(reference)=LOWER(%s) ORDER BY date DESC LIMIT 1",
                (school_id, ref),
            )
            p = cur.fetchone()
            if p and (amount is None or float(p.get("amount") or 0) == float(amount or 0)):
                match_id = int(p["id"])
                score = 100
        if not match_id and amount is not None and txn_date:
            start = txn_date - timedelta(days=3)
            end = txn_date + timedelta(days=3)
            cur.execute(
                """
                SELECT id, amount FROM payments
                WHERE school_id=%s AND date >= %s AND date <= %s AND amount=%s
                ORDER BY date DESC LIMIT 1
                """,
                (school_id, start, end, amount),
            )
            p = cur.fetchone()
            if p:
                match_id = int(p["id"])
                score = 70

        if match_id:
            cur.execute(
                """
                UPDATE reconciliation_rows
                SET match_status='matched', match_score=%s, matched_payment_id=%s, matched_at=%s
                WHERE id=%s
                """,
                (score, match_id, datetime.utcnow(), row_id),
            )
            matched += 1
    db.commit()
    return matched


def _update_import_counts(db, import_id: int, school_id: int) -> None:
    cur = db.cursor()
    cur.execute(
        "SELECT COUNT(*) AS total FROM reconciliation_rows WHERE import_id=%s AND school_id=%s",
        (import_id, school_id),
    )
    row = cur.fetchone()
    total = int(row[0]) if row else 0
    cur.execute(
        "SELECT COUNT(*) AS matched FROM reconciliation_rows WHERE import_id=%s AND school_id=%s AND match_status='matched'",
        (import_id, school_id),
    )
    row = cur.fetchone()
    matched = int(row[0]) if row else 0
    cur.execute(
        "UPDATE reconciliation_imports SET total_rows=%s, matched_rows=%s WHERE id=%s AND school_id=%s",
        (total, matched, import_id, school_id),
    )
    db.commit()


@recon_bp.route("/reconciliation", methods=["GET"])
def reconciliation_home():
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    try:
        ensure_reconciliation_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM reconciliation_imports WHERE school_id=%s ORDER BY id DESC LIMIT 25",
            (session.get("school_id"),),
        )
        imports = cur.fetchall() or []
    finally:
        db.close()
    return render_template("admin/reconciliation.html", imports=imports)


@recon_bp.route("/reconciliation/upload", methods=["POST"])
def reconciliation_upload():
    guard = _require_admin()
    if guard:
        return guard
    if "statement" not in request.files:
        flash("Upload a CSV statement file.", "warning")
        return redirect(url_for("reconciliation.reconciliation_home"))
    f = request.files["statement"]
    if not f or not f.filename:
        flash("Upload a valid CSV statement file.", "warning")
        return redirect(url_for("reconciliation.reconciliation_home"))

    db = _db()
    try:
        ensure_reconciliation_tables(db)
        content = f.read().decode("utf-8", errors="ignore")
        stream = StringIO(content)
        sample = stream.read(2048)
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.DictReader(stream, dialect=dialect)

        now = datetime.utcnow()
        cur = db.cursor()
        cur.execute(
            """
            INSERT INTO reconciliation_imports (school_id, filename, uploaded_by, status, total_rows, matched_rows, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (session.get("school_id"), f.filename, session.get("username"), "ready", 0, 0, now),
        )
        import_id = cur.lastrowid

        total_rows = 0
        for raw in reader:
            normalized = _normalize_row(raw)
            txn_date = _parse_date(str(normalized.get("date") or ""))
            try:
                amount = float(str(normalized.get("amount") or "").replace(",", ""))
            except Exception:
                amount = None
            cur.execute(
                """
                INSERT INTO reconciliation_rows
                (import_id, school_id, txn_date, amount, reference, payer_name, phone, raw_text, match_status, match_score, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    import_id,
                    session.get("school_id"),
                    txn_date,
                    amount,
                    (normalized.get("reference") or "").strip() or None,
                    (normalized.get("payer_name") or "").strip() or None,
                    (normalized.get("phone") or "").strip() or None,
                    str(raw),
                    "pending",
                    0,
                    now,
                ),
            )
            total_rows += 1
        db.commit()

        matched = _auto_match_rows(db, int(session.get("school_id")), int(import_id))
        cur.execute(
            "UPDATE reconciliation_imports SET total_rows=%s, matched_rows=%s WHERE id=%s",
            (total_rows, matched, import_id),
        )
        db.commit()
        try:
            log_event("reconciliation_import", target=f"import:{import_id}", detail=f"{total_rows} rows, {matched} matched")
        except Exception:
            pass
        flash(f"Imported {total_rows} rows. Auto-matched {matched}.", "success")
    except Exception as exc:
        flash(f"Failed to import statement: {exc}", "error")
    finally:
        try:
            db.close()
        except Exception:
            pass
    return redirect(url_for("reconciliation.reconciliation_home"))


@recon_bp.route("/reconciliation/<int:import_id>", methods=["GET"])
def reconciliation_detail(import_id: int):
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    try:
        ensure_reconciliation_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM reconciliation_imports WHERE id=%s AND school_id=%s",
            (import_id, session.get("school_id")),
        )
        info = cur.fetchone()
        if not info:
            flash("Import not found.", "warning")
            return redirect(url_for("reconciliation.reconciliation_home"))
        cur.execute(
            """
            SELECT r.*, p.amount AS payment_amount, p.reference AS payment_ref
            FROM reconciliation_rows r
            LEFT JOIN payments p ON r.matched_payment_id=p.id
            WHERE r.import_id=%s AND r.school_id=%s
            ORDER BY r.id DESC LIMIT 300
            """,
            (import_id, session.get("school_id")),
        )
        rows = cur.fetchall() or []
    finally:
        db.close()
    return render_template("admin/reconciliation_detail.html", info=info, rows=rows)


@recon_bp.route("/reconciliation/row/<int:row_id>/match", methods=["POST"])
def reconciliation_match_row(row_id: int):
    guard = _require_admin()
    if guard:
        return guard
    payment_id = request.form.get("payment_id", type=int)
    if not payment_id:
        flash("Provide a payment id to match.", "warning")
        return redirect(request.referrer or url_for("reconciliation.reconciliation_home"))
    db = _db()
    try:
        ensure_reconciliation_tables(db)
        cur = db.cursor()
        cur.execute(
            "SELECT import_id FROM reconciliation_rows WHERE id=%s AND school_id=%s",
            (row_id, session.get("school_id")),
        )
        row = cur.fetchone()
        cur.execute(
            """
            UPDATE reconciliation_rows
            SET match_status='matched', match_score=%s, matched_payment_id=%s, matched_at=%s
            WHERE id=%s AND school_id=%s
            """,
            (60, payment_id, datetime.utcnow(), row_id, session.get("school_id")),
        )
        db.commit()
        if row:
            _update_import_counts(db, int(row[0]), int(session.get("school_id")))
        flash("Row matched to payment.", "success")
    finally:
        db.close()
    return redirect(request.referrer or url_for("reconciliation.reconciliation_home"))


@recon_bp.route("/reconciliation/row/<int:row_id>/ignore", methods=["POST"])
def reconciliation_ignore_row(row_id: int):
    guard = _require_admin()
    if guard:
        return guard
    db = _db()
    try:
        ensure_reconciliation_tables(db)
        cur = db.cursor()
        cur.execute(
            "SELECT import_id FROM reconciliation_rows WHERE id=%s AND school_id=%s",
            (row_id, session.get("school_id")),
        )
        row = cur.fetchone()
        cur.execute(
            "UPDATE reconciliation_rows SET match_status='ignored', matched_at=%s WHERE id=%s AND school_id=%s",
            (datetime.utcnow(), row_id, session.get("school_id")),
        )
        db.commit()
        if row:
            _update_import_counts(db, int(row[0]), int(session.get("school_id")))
        flash("Row ignored.", "info")
    finally:
        db.close()
    return redirect(request.referrer or url_for("reconciliation.reconciliation_home"))
