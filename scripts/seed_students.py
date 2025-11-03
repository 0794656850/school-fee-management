import argparse
import random
import string
from datetime import datetime
from typing import List, Tuple, Set
import os
import sys

# Ensure project root is importable when running from scripts/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Reuse the app's DB connection util and credit column helper
from app import get_db_connection
from routes.credit_routes import ensure_students_credit_column
from utils.tenant import get_or_create_school, slugify_code


FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Paul", "Ashley",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
]

CLASSES = [
    # Adjust to your school's naming if needed
    *(f"Class {i}" for i in range(1, 13)),
    "Kindergarten", "Pre-Unit", "Playgroup",
]


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_class() -> str:
    return random.choice(CLASSES)


def random_phone() -> str:
    # Kenyan-style by default (+2547XXXXXXXX), but keep it simple if phone column exists
    prefix = random.choice(["+2547", "+2541", "+25411", "+25410"])  # modern + Safaricom/Airtel ranges
    rest = "".join(random.choice(string.digits) for _ in range(8))
    return prefix + rest


def random_balance() -> float:
    # Skew towards smaller balances but allow some bigger values
    buckets = [0, 0, 0, 0, 0, 250, 500, 1000, 2500, 5000, 10000, 20000]
    val = float(random.choice(buckets))
    if val == 0 and random.random() < 0.2:
        # 20% of zeros become a small non-zero balance
        val = round(random.uniform(50, 500), 2)
    return round(val, 2)


def load_existing_admission_numbers(cur, school_id: int | None, has_school: bool) -> Set[str]:
    cur.execute("SHOW COLUMNS FROM students LIKE 'admission_no'")
    if not cur.fetchone():
        # Fallback: older schemas might use reg_no/regNo
        cur.execute("SHOW COLUMNS FROM students LIKE 'reg_no'")
        if cur.fetchone():
            if has_school and school_id:
                cur.execute("SELECT LOWER(reg_no) FROM students WHERE school_id=%s", (school_id,))
            else:
                cur.execute("SELECT LOWER(reg_no) FROM students")
        else:
            cur.execute("SHOW COLUMNS FROM students LIKE 'regNo'")
            if cur.fetchone():
                if has_school and school_id:
                    cur.execute("SELECT LOWER(regNo) FROM students WHERE school_id=%s", (school_id,))
                else:
                    cur.execute("SELECT LOWER(regNo) FROM students")
            else:
                return set()
    else:
        if has_school and school_id:
            cur.execute("SELECT LOWER(admission_no) FROM students WHERE school_id=%s", (school_id,))
        else:
            cur.execute("SELECT LOWER(admission_no) FROM students")
    rows = cur.fetchall() or []
    return {list(r.values())[0] if isinstance(r, dict) else (r[0] if r else "") for r in rows}


def next_admission_number(existing: Set[str], start_index: int) -> str:
    # Format ADM000001 style; pick next available number
    idx = start_index
    while True:
        adm = f"ADM{idx:06d}"
        if adm.lower() not in existing:
            return adm
        idx += 1


def detect_schema(cur):
    cur.execute("SHOW COLUMNS FROM students LIKE 'phone'")
    has_phone = bool(cur.fetchone())
    cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
    has_balance = bool(cur.fetchone())
    cur.execute("SHOW COLUMNS FROM students LIKE 'fee_balance'")
    has_fee_balance = bool(cur.fetchone())
    cur.execute("SHOW COLUMNS FROM students LIKE 'school_id'")
    has_school = bool(cur.fetchone())
    # Ensure credit column exists for consistency with the app
    return has_phone, has_balance, has_fee_balance, has_school


def build_insert_sql(has_phone: bool, has_balance: bool, has_fee_balance: bool, has_school: bool) -> tuple[str, int, bool]:
    """Return (SQL, param_count_without_school, with_balance_col) where param_count excludes credit/auto defaults.

    If has_school=True, caller appends one extra parameter (school_id) to the tuple per row and we extend the SQL
    with ", school_id) VALUES (..., %s)" appropriately.
    """
    with_balance = False
    if has_balance:
        with_balance = True
        if has_phone:
            base_cols = "name, admission_no, class_name, phone, balance, credit"
            placeholders = "%s, %s, %s, %s, %s, 0"
            param_count = 5
        else:
            base_cols = "name, admission_no, class_name, balance, credit"
            placeholders = "%s, %s, %s, %s, 0"
            param_count = 4
    elif has_fee_balance:
        with_balance = True
        if has_phone:
            base_cols = "name, admission_no, class_name, phone, fee_balance, credit"
            placeholders = "%s, %s, %s, %s, %s, 0"
            param_count = 5
        else:
            base_cols = "name, admission_no, class_name, fee_balance, credit"
            placeholders = "%s, %s, %s, %s, 0"
            param_count = 4
    else:
        base_cols = "name, admission_no, class_name"
        placeholders = "%s, %s, %s"
        param_count = 3

    if has_school:
        sql = f"INSERT INTO students ({base_cols}, school_id) VALUES ({placeholders}, %s)"
    else:
        sql = f"INSERT INTO students ({base_cols}) VALUES ({placeholders})"
    return sql, param_count, with_balance


def seed_students(count: int = 2000, batch_size: int = 500, school: str | None = None) -> int:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    # Ensure optional credit column exists for compatibility with app features
    ensure_students_credit_column(conn)

    has_phone, has_balance, has_fee_balance, has_school = detect_schema(cur)

    # Resolve or create target school if requested
    school_id = None
    if school:
        try:
            code = slugify_code(school)
            school_id = get_or_create_school(conn, code=code, name=school)
        except Exception:
            school_id = None

    sql, param_count, _ = build_insert_sql(has_phone, has_balance, has_fee_balance, has_school)

    existing = load_existing_admission_numbers(cur, school_id, has_school)

    # Attempt to start indices past current count (scoped by school if present)
    if has_school and school_id:
        cur.execute("SELECT COUNT(*) AS c FROM students WHERE school_id=%s", (school_id,))
    else:
        cur.execute("SELECT COUNT(*) AS c FROM students")
    start_idx = (cur.fetchone() or {}).get("c", 0) + 1

    to_insert: list[tuple] = []
    created = 0
    next_idx = start_idx

    for _ in range(count):
        name = random_name()
        klass = random_class()
        adm = next_admission_number(existing, next_idx)
        # move index forward for next search to keep perf good
        next_idx = int(adm.replace("ADM", "")) + 1
        existing.add(adm.lower())

        bal = random_balance()
        phone = random_phone()

        if param_count == 5:
            # includes phone and a balance variant
            if has_balance:
                params = (name, adm, klass, phone, bal)
            else:
                params = (name, adm, klass, phone, bal)
        elif param_count == 4:
            # excludes phone; includes a balance variant
            params = (name, adm, klass, bal)
        else:
            params = (name, adm, klass)

        # Append school id if supported
        if has_school:
            params = (*params, school_id)

        to_insert.append(params)

        # Flush in batches
        if len(to_insert) >= batch_size:
            cur.executemany(sql, to_insert)
            conn.commit()
            created += len(to_insert)
            to_insert.clear()

    # Flush any remainder
    if to_insert:
        cur.executemany(sql, to_insert)
        conn.commit()
        created += len(to_insert)

    cur.close()
    conn.close()
    return created


def main():
    parser = argparse.ArgumentParser(description="Seed mock students into the system.")
    parser.add_argument("--count", type=int, default=2000, help="How many students to add (default: 2000)")
    parser.add_argument("--batch", type=int, default=500, help="Batch size for bulk insert (default: 500)")
    parser.add_argument("--school", type=str, default=None, help="School name or code to target (creates if missing)")
    args = parser.parse_args()

    total = seed_students(count=args.count, batch_size=args.batch, school=args.school)
    print(f"Inserted {total} mock students.")


if __name__ == "__main__":
    main()
