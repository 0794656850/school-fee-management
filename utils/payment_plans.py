from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Tuple, List


def _add_months(d: date, months: int) -> date:
    year = d.year + ((d.month - 1 + months) // 12)
    month = ((d.month - 1 + months) % 12) + 1
    # Clamp day to last day of target month
    day = min(d.day, _last_day_of_month(year, month))
    return date(year, month, day)


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def create_payment_plan(
    db,
    school_id: int,
    student_id: int,
    total_amount: float,
    installments: int,
    frequency: str,
    start_date: Optional[date],
    year: Optional[int],
    term: Optional[int],
    created_by: Optional[str],
) -> int:
    if installments < 1:
        installments = 1
    freq = (frequency or "monthly").lower().strip()
    if freq not in ("monthly", "weekly", "biweekly"):
        freq = "monthly"
    start = start_date or date.today()
    now = datetime.utcnow()

    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO payment_plans
        (school_id, student_id, year, term, total_amount, installments, frequency, start_date, status, created_by, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            school_id,
            student_id,
            year,
            term,
            total_amount,
            installments,
            freq,
            start,
            "active",
            created_by,
            now,
            now,
        ),
    )
    plan_id = cur.lastrowid

    # Build items
    per = round(float(total_amount or 0) / installments, 2)
    items: List[Tuple[date, float]] = []
    for i in range(installments):
        if freq == "weekly":
            due = start + timedelta(days=7 * i)
        elif freq == "biweekly":
            due = start + timedelta(days=14 * i)
        else:
            due = _add_months(start, i)
        amt = per
        if i == installments - 1:
            # adjust last installment for rounding
            amt = round(float(total_amount or 0) - (per * (installments - 1)), 2)
        items.append((due, amt))

    for due, amt in items:
        cur.execute(
            """
            INSERT INTO payment_plan_items
            (plan_id, due_date, amount, status, paid_amount, created_at)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (plan_id, due, amt, "pending", 0, now),
        )
    db.commit()
    return int(plan_id)


def get_active_plan_id(db, school_id: int, student_id: int, year: Optional[int], term: Optional[int]) -> Optional[int]:
    cur = db.cursor(dictionary=True)
    if year and term in (1, 2, 3):
        cur.execute(
            """
            SELECT id FROM payment_plans
            WHERE school_id=%s AND student_id=%s AND status='active' AND year=%s AND term=%s
            ORDER BY id DESC LIMIT 1
            """,
            (school_id, student_id, year, term),
        )
    else:
        cur.execute(
            """
            SELECT id FROM payment_plans
            WHERE school_id=%s AND student_id=%s AND status='active'
            ORDER BY id DESC LIMIT 1
            """,
            (school_id, student_id),
        )
    row = cur.fetchone()
    return int(row["id"]) if row else None


def apply_payment_to_plan(
    db,
    school_id: int,
    student_id: int,
    payment_id: int,
    amount: float,
    payment_date: Optional[date],
    year: Optional[int],
    term: Optional[int],
) -> None:
    plan_id = get_active_plan_id(db, school_id, student_id, year, term)
    if not plan_id:
        return
    cur = db.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, amount, paid_amount, status
        FROM payment_plan_items
        WHERE plan_id=%s AND status IN ('pending','partial')
        ORDER BY due_date ASC, id ASC
        """,
        (plan_id,),
    )
    items = cur.fetchall() or []
    remaining = float(amount or 0)
    now = datetime.utcnow()
    for item in items:
        if remaining <= 0:
            break
        item_id = int(item["id"])
        item_amount = float(item.get("amount") or 0)
        paid_amount = float(item.get("paid_amount") or 0)
        due_left = max(item_amount - paid_amount, 0)
        if due_left <= 0:
            continue
        apply_amt = min(due_left, remaining)
        new_paid = paid_amount + apply_amt
        new_status = "paid" if new_paid >= item_amount else "partial"
        cur.execute(
            """
            UPDATE payment_plan_items
            SET paid_amount=%s, status=%s, paid_at=%s, payment_id=%s
            WHERE id=%s
            """,
            (new_paid, new_status, payment_date or now.date(), payment_id, item_id),
        )
        remaining -= apply_amt

    # If all items paid, close plan
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM payment_plan_items WHERE plan_id=%s AND status <> 'paid'",
        (plan_id,),
    )
    pending = int((cur.fetchone() or {}).get("cnt", 0))
    if pending == 0:
        cur.execute("UPDATE payment_plans SET status='completed', updated_at=%s WHERE id=%s", (now, plan_id))
    else:
        cur.execute("UPDATE payment_plans SET updated_at=%s WHERE id=%s", (now, plan_id))
    db.commit()
