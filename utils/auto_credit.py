from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from routes.credit_routes import ensure_credit_ops_table
from utils.gmail_api import send_email as _gmail_send_email, send_email_html as _gmail_send_email_html
from utils.ledger import add_entry, ensure_ledger_table
from utils.settings import get_setting


def _detect_balance_column(cur) -> str:
    cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    if cur.fetchone():
        return "balance"
    cur.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
    if cur.fetchone():
        return "fee_balance"
    return "balance"


def notify_parent_credit_applied(
    student: dict[str, Any],
    amount: float,
    year: int,
    term: int,
    portal_url: str | None,
) -> None:
    email = (student.get("parent_email") or student.get("email") or "").strip()
    if not email:
        return
    parent_name = (student.get("parent_name") or student.get("name") or "Parent").strip()
    amount_str = f"KES {amount:,.2f}"
    subject = f"{student.get('name') or 'Your child'} overpayment applied to Term {term}/{year}"
    html_body = (
        f"<p>Hi {parent_name},</p>"
        f"<p>We spotted {amount_str} of overpayment and automatically applied it to {year} Term {term} fees.</p>"
        f"<p>Visit the <a href=\"{portal_url or '#'}\">Parent Portal</a> to confirm the update.</p>"
        "<p>Best regards,<br/>School finance team.</p>"
    )
    plain_body = (
        f"Hi {parent_name},\n\n"
        f"We automatically applied {amount_str} of overpayment to {year} Term {term} fees for {student.get('name') or 'your child'}.\n"
        f"View the updates in the Parent Portal: {portal_url or 'school portal'}\n\n"
        "Best regards,\nSchool finance team"
    )
    try:
        if not _gmail_send_email_html(email, subject, html_body):
            _gmail_send_email(email, subject, plain_body)
    except Exception:
        pass


def auto_apply_credit_if_new_term(
    db,
    student: dict[str, Any],
    school_id: int,
    year: int,
    term: int,
    portal_url: str | None = None,
) -> dict[str, Any] | None:
    student_id = int(student.get("id") or 0)
    if not (student_id and school_id and year and term):
        return None
    ensure_credit_ops_table(db)
    try:
        from routes.term_routes import ensure_invoices_tables as _ensure_invoices
    except ImportError:
        _ensure_invoices = None
    if _ensure_invoices:
        _ensure_invoices(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        "SELECT meta FROM credit_operations WHERE student_id=%s AND school_id=%s AND op_type=%s",
        (student_id, school_id, "auto_apply"),
    )
    for row in cur.fetchall() or []:
        meta_raw = (row.get("meta") or "").strip()
        if not meta_raw:
            continue
        try:
            payload = json.loads(meta_raw)
        except Exception:
            continue
        if payload.get("year") == year and payload.get("term") == term:
            return None
    # Always fetch fresh balance/credit to avoid stale values from callers
    cur.execute(
        "SELECT COALESCE(balance, fee_balance, 0) AS balance, COALESCE(credit,0) AS credit FROM students WHERE id=%s AND school_id=%s",
        (student_id, school_id),
    )
    fresh = cur.fetchone() or {"balance": 0, "credit": 0}
    credit_amount = float(fresh.get("credit") or 0)
    current_balance = float(fresh.get("balance") or 0)
    if credit_amount <= 0:
        return None
    cur.execute(
        "SELECT id, total FROM invoices WHERE student_id=%s AND year=%s AND term=%s",
        (student_id, year, term),
    )
    invoice_row = cur.fetchone()
    invoice_id = int(invoice_row.get("id") or 0) if invoice_row else 0
    total = float(invoice_row.get("total") or 0) if invoice_row else 0.0
    cur.execute(
        "SELECT COALESCE(SUM(amount),0) AS paid FROM payments WHERE student_id=%s AND school_id=%s AND year=%s AND term=%s",
        (student_id, school_id, year, term),
    )
    paid_row = cur.fetchone() or {}
    paid = float(paid_row.get("paid") or 0)
    balance_col = _detect_balance_column(cur)
    # Consider both invoice math and the live balance column to capture any manual adjustments
    outstanding_invoice = max(total - paid, 0.0) if total > 0 else 0.0
    outstanding_balance = max(current_balance, 0.0)
    outstanding = max(outstanding_invoice, outstanding_balance)
    if outstanding <= 0:
        return None
    if total <= 0:
        total = outstanding
    amount_to_apply = min(credit_amount, outstanding)
    if amount_to_apply <= 0:
        return None
    payment_cur = db.cursor()
    reference = f"AUTO-CREDIT-{year}-T{term}-{student_id}"
    payment_cur.execute(
        "INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            student_id,
            amount_to_apply,
            "Auto Credit",
            term,
            year,
            reference,
            datetime.utcnow(),
            school_id,
        ),
    )
    payment_id = payment_cur.lastrowid
    balance_col = _detect_balance_column(cur)
    # Subtract against the fresh balance snapshot to keep ledger aligned
    new_balance = max(current_balance - amount_to_apply, 0.0)
    new_credit = max(credit_amount - amount_to_apply, 0.0)
    cur.execute(
        f"UPDATE students SET {balance_col}=%s, credit=%s WHERE id=%s AND school_id=%s",
        (new_balance, new_credit, student_id, school_id),
    )
    try:
        ensure_ledger_table(db)
        add_entry(
            db,
            school_id=int(school_id),
            student_id=int(student_id),
            entry_type="credit",
            amount=float(amount_to_apply),
            ref=reference,
            description=f"Auto-applied overpayment to Term {term}/{year}",
            link_type="payment",
            link_id=int(payment_id),
        )
    except Exception:
        pass
    meta_payload = json.dumps(
        {"year": year, "term": term, "invoice_id": invoice_id, "payment_id": payment_id}
    )
    cur.execute(
        """
        INSERT INTO credit_operations (ts, actor, student_id, op_type, amount, reference, method, meta, school_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            datetime.utcnow(),
            "system",
            student_id,
            "auto_apply",
            amount_to_apply,
            reference,
            "Auto Credit",
            meta_payload,
            school_id,
        ),
    )
    db.commit()
    notify_parent_credit_applied(student, amount_to_apply, year, term, portal_url)
    return {
        "message": f"KES {amount_to_apply:,.2f} of your overpayment has been auto-applied to {year} Term {term}.",
        "amount": amount_to_apply,
        "new_balance": new_balance,
        "new_credit": new_credit,
        "year": year,
        "term": term,
    }


def auto_apply_credit_for_school(
    db,
    school_id: int,
    year: int,
    term: int,
    portal_url: str | None = None,
) -> list[dict[str, Any]]:
    notices: list[dict[str, Any]] = []
    try:
        from routes.term_routes import ensure_invoices_tables as _ensure_invoices
    except ImportError:
        _ensure_invoices = None
    if _ensure_invoices:
        _ensure_invoices(db)
    cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, name, class_name, COALESCE(credit,0) AS credit,
               COALESCE(balance, fee_balance, 0) AS balance,
               parent_email, parent_name
        FROM students
        WHERE school_id=%s AND COALESCE(credit,0) > 0
        """,
        (school_id,),
    )
    students = cur.fetchall() or []
    for student in students:
        note = auto_apply_credit_if_new_term(db, student, school_id, year, term, portal_url)
        if note:
            notices.append(note)
    return notices
