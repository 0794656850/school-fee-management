import argparse
import os
import sys
import random
from datetime import datetime, timedelta, date

# Ensure project root is importable when running from scripts/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app import get_db_connection  # type: ignore
from utils.tenant import slugify_code  # type: ignore
from routes.term_routes import ensure_term_fees_table, ensure_payments_term_columns  # type: ignore


def infer_term_for_date(d: date) -> int:
    m = d.month
    if 1 <= m <= 4:
        return 1
    if 5 <= m <= 8:
        return 2
    return 3


def seed_term_fees_and_payments(
    school: str,
    flat_fee: float = 12000.0,
    pay_ratio: float = 0.6,
    pay_min_frac: float = 0.3,
    pay_max_frac: float = 0.85,
    batch_size: int = 1000,
) -> dict:
    """Seed legacy flat term fees and random payments for a school.

    - Upserts a flat fee for all students for the current year/term into term_fees
      and increases students' balances by the delta from any existing fee.
    - Inserts payments for ~pay_ratio of students with an amount between
      pay_min_frac .. pay_max_frac of the flat fee and reduces their balances.
    """
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    # Ensure required tables/columns exist
    ensure_term_fees_table(conn)
    ensure_payments_term_columns(conn)

    # Resolve school id
    code = slugify_code(school)
    cur.execute("SELECT id, name, code FROM schools WHERE code=%s", (code,))
    srow = cur.fetchone()
    if not srow:
        raise SystemExit(f"School '{school}' not found (code={code}). Create it first.")
    school_id = int(srow["id"]) if isinstance(srow, dict) else int(srow[0])

    # Determine balance column
    cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cur.fetchone())
    bal_col = "balance" if has_balance else "fee_balance"

    # Academic context
    today = date.today()
    year = today.year
    term = infer_term_for_date(today)

    # Fetch students for school
    cur.execute(f"SELECT id, COALESCE({bal_col},0) AS bal FROM students WHERE school_id=%s", (school_id,))
    students = cur.fetchall() or []
    student_ids = [int(r["id"]) if isinstance(r, dict) else int(r[0]) for r in students]
    if not student_ids:
        return {"school_id": school_id, "students": 0, "fees_upserted": 0, "payments": 0}

    # Fetch existing flat fees for those students this term
    ph = ",".join(["%s"] * len(student_ids))
    cur.execute(
        f"SELECT student_id, fee_amount FROM term_fees WHERE year=%s AND term=%s AND student_id IN ({ph})",
        (year, term, *student_ids),
    )
    existing_map = {int(r["student_id"]): float(r.get("fee_amount") or 0) for r in (cur.fetchall() or [])}

    # Upsert fees and adjust balances
    upd = conn.cursor()
    fee_rows = 0
    total_delta = 0.0
    batch = []
    upd_bal = []
    for sid in student_ids:
        prev_amt = existing_map.get(sid, 0.0)
        # small class-level variance to look realistic
        jitter = random.uniform(-0.1, 0.1)  # +/-10%
        fee_amt = max(0.0, round(flat_fee * (1.0 + jitter), 2))
        delta = fee_amt - prev_amt
        batch.append((sid, year, term, fee_amt))
        if abs(delta) > 0.005:
            upd_bal.append((delta, sid, school_id))
            total_delta += delta
        if len(batch) >= batch_size:
            upd.executemany(
                "INSERT INTO term_fees (student_id, year, term, fee_amount, school_id) VALUES (%s,%s,%s,%s,%s)"
                " ON DUPLICATE KEY UPDATE fee_amount=VALUES(fee_amount), school_id=VALUES(school_id)",
                [(sid, year, term, amt, school_id) for (sid, _, _, amt) in batch],
            )
            if upd_bal:
                upd.executemany(
                    f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                    upd_bal,
                )
            conn.commit()
            fee_rows += len(batch)
            batch.clear()
            upd_bal.clear()
    if batch:
        upd.executemany(
            "INSERT INTO term_fees (student_id, year, term, fee_amount, school_id) VALUES (%s,%s,%s,%s,%s)"
            " ON DUPLICATE KEY UPDATE fee_amount=VALUES(fee_amount), school_id=VALUES(school_id)",
            [(sid, year, term, amt, school_id) for (sid, _, _, amt) in batch],
        )
        if upd_bal:
            upd.executemany(
                f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                upd_bal,
            )
        conn.commit()
        fee_rows += len(batch)
        batch.clear()
        upd_bal.clear()

    # Seed payments for a random subset
    ensure_payments_term_columns(conn)
    pay_methods = ["M-Pesa", "Cash", "Bank"]
    will_pay = set(random.sample(student_ids, int(len(student_ids) * float(pay_ratio))))

    # Dates within last ~45 days to now
    def random_date_within_term() -> datetime:
        days_back = random.randint(0, 45)
        dt = datetime.now() - timedelta(days=days_back)
        return dt.replace(hour=random.randint(8, 17), minute=random.randint(0, 59), second=random.randint(0, 59))

    pay_rows = []
    bal_updates = []
    for sid in student_ids:
        if sid not in will_pay:
            continue
        # Payment between x%..y% of flat fee
        frac = random.uniform(pay_min_frac, pay_max_frac)
        # Keep modest jitter to avoid identical amounts
        amt = round(max(10.0, flat_fee * frac * random.uniform(0.95, 1.05)), 2)
        dt = random_date_within_term()
        ref = f"CG-{year}T{term}-{sid:06d}-{random.randint(100,999)}"
        method = random.choice(pay_methods)
        pay_rows.append((sid, amt, method, term, year, ref, dt, school_id))
        bal_updates.append((-amt, sid, school_id))  # subtract from balance
        if len(pay_rows) >= batch_size:
            upd.executemany(
                "INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                pay_rows,
            )
            if bal_updates:
                upd.executemany(
                    f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                    bal_updates,
                )
            conn.commit()
            pay_rows.clear()
            bal_updates.clear()
    if pay_rows:
        upd.executemany(
            "INSERT INTO payments (student_id, amount, method, term, year, reference, date, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            pay_rows,
        )
        if bal_updates:
            upd.executemany(
                f"UPDATE students SET {bal_col} = COALESCE({bal_col},0) + %s WHERE id=%s AND school_id=%s",
                bal_updates,
            )
        conn.commit()
    upd.close()
    cur.close()
    conn.close()

    return {
        "school_id": school_id,
        "students": len(student_ids),
        "fees_upserted": fee_rows,
        "balances_delta": round(total_delta, 2),
        "payments": int(len(will_pay)),
    }


def main():
    p = argparse.ArgumentParser(description="Seed term fees + payments for a school")
    p.add_argument("--school", required=True, help="School name or code (e.g., 'Chuka Girls')")
    p.add_argument("--flat", type=float, default=12000.0, help="Flat fee base amount per student (default 12000)")
    p.add_argument("--ratio", type=float, default=0.6, help="Ratio of students to receive a payment (0..1)")
    p.add_argument("--min", dest="pmin", type=float, default=0.3, help="Min payment fraction of fee (default 0.3)")
    p.add_argument("--max", dest="pmax", type=float, default=0.85, help="Max payment fraction of fee (default 0.85)")
    p.add_argument("--batch", type=int, default=1000, help="Batch size for executemany (default 1000)")
    args = p.parse_args()

    stats = seed_term_fees_and_payments(
        school=args.school,
        flat_fee=args.flat,
        pay_ratio=args.ratio,
        pay_min_frac=args.pmin,
        pay_max_frac=args.pmax,
        batch_size=args.batch,
    )
    print(
        f"Seeded fees+payments: school_id={stats['school_id']} students={stats['students']} "
        f"fees_upserted={stats['fees_upserted']} payments~={stats['payments']} balances_delta={stats['balances_delta']}"
    )


if __name__ == "__main__":
    main()

