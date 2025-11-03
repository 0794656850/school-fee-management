from __future__ import annotations

import re
from typing import Optional, Sequence
from datetime import date

from utils.settings import ensure_school_settings_table, set_school_setting
from utils.security import hash_password
from utils.users import (
    ensure_user_tables,
    get_user_by_username,
    create_user,
    ensure_school_user,
)


def ensure_schools_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schools (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(64) NOT NULL UNIQUE,
            name VARCHAR(128) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()
    # Add progressive columns used by multi-tenant flows
    try:
        cur.execute("SHOW COLUMNS FROM schools LIKE 'first_login_at'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE schools ADD COLUMN first_login_at DATETIME NULL AFTER created_at")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
    # Optional human-friendly registration number per school
    try:
        cur.execute("SHOW COLUMNS FROM schools LIKE 'registration_no'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE schools ADD COLUMN registration_no VARCHAR(30) NULL")
            try:
                cur.execute("CREATE UNIQUE INDEX uq_school_registration_no ON schools(registration_no)")
            except Exception:
                pass
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_school_id_column(conn, table: str) -> None:
    """Ensure a `school_id` column and index exist on the given table.

    Safe to call repeatedly; creates the column if missing and adds an index.
    """
    cur = conn.cursor()
    cur.execute(f"SHOW COLUMNS FROM {table} LIKE 'school_id'")
    has = bool(cur.fetchone())
    if not has:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN school_id INT NULL")
        try:
            cur.execute(
                f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_school_id FOREIGN KEY (school_id) REFERENCES schools(id) ON DELETE SET NULL"
            )
        except Exception:
            # If FK add fails (e.g., table type), ignore; index still helps scoping
            pass
        try:
            cur.execute(f"CREATE INDEX idx_{table}_school_id ON {table} (school_id)")
        except Exception:
            # Index may already exist
            pass
        conn.commit()


def ensure_school_id_columns(conn, tables: Sequence[str]) -> None:
    ensure_schools_table(conn)
    for t in tables:
        try:
            ensure_school_id_column(conn, t)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass


def slugify_code(name_or_code: str) -> str:
    code = name_or_code.strip().lower()
    code = re.sub(r"[^a-z0-9]+", "-", code).strip("-")
    # enforce a minimal code
    return code or "school"


def get_or_create_school(conn, code: str, name: Optional[str] = None) -> Optional[int]:
    """Return school id for code; create if missing using provided name or code as name.

    Returns the numeric id or None on failure.
    """
    ensure_schools_table(conn)
    cur = conn.cursor()
    cur.execute("SELECT id FROM schools WHERE code=%s", (code,))
    row = cur.fetchone()
    if row:
        return int(row[0]) if not isinstance(row, dict) else int(row.get("id"))
    if not name:
        name = code
    cur.execute("INSERT INTO schools (code, name) VALUES (%s, %s)", (code, name))
    conn.commit()
    return int(cur.lastrowid)


def bootstrap_new_school(conn, school_id: int, name: str, code: Optional[str] = None) -> None:
    """Seed initial data for a newly created school.

    - Create basic per-school settings (name and placeholders for contact info).
    - Seed current year's three terms in academic_terms with school_id.
    Safe to call multiple times (idempotent inserts by checking existing rows).
    """
    # Seed school settings
    try:
        ensure_school_settings_table(conn)
        set_school_setting("SCHOOL_NAME", name or (code or "School"), school_id=school_id)
        # Placeholders; can be edited later in UI
        for k in ("SCHOOL_ADDRESS", "SCHOOL_PHONE", "SCHOOL_EMAIL", "SCHOOL_WEBSITE"):
            set_school_setting(k, None, school_id=school_id)
        # Default per-school login credentials for a new school
        # Username defaults to 'user'; password '9133' stored hashed.
        set_school_setting("APP_LOGIN_USERNAME", "user", school_id=school_id)
        try:
            set_school_setting("APP_LOGIN_PASSWORD", hash_password("9133"), school_id=school_id)
        except Exception:
            set_school_setting("APP_LOGIN_PASSWORD", "9133", school_id=school_id)
        # Seed first owner user mapped to this school (align with legacy credentials)
        try:
            ensure_user_tables(conn)
            existing = get_user_by_username(conn, "user")
            if existing:
                uid = int(existing["id"]) if isinstance(existing, dict) else int(existing[0])
            else:
                uid = create_user(conn, "user", None, hash_password("9133"))
            ensure_school_user(conn, uid, school_id, role="owner")
        except Exception:
            # Non-fatal; admin can create users later
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    # Seed academic terms for the school if none exist
    try:
        cur = conn.cursor()
        # Ensure table exists
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS academic_terms (
                id INT AUTO_INCREMENT PRIMARY KEY,
                year INT NOT NULL,
                term TINYINT NOT NULL,
                label VARCHAR(64),
                start_date DATE,
                end_date DATE,
                is_current TINYINT(1) DEFAULT 0,
                UNIQUE KEY uq_year_term (year, term)
            )
            """
        )
        conn.commit()
        # Ensure school_id column exists (if not already)
        try:
            cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'school_id'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE academic_terms ADD COLUMN school_id INT NULL")
                try:
                    cur.execute("CREATE INDEX idx_academic_terms_school ON academic_terms(school_id)")
                except Exception:
                    pass
                conn.commit()
        except Exception:
            pass

        # If this school has no terms, seed them
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM academic_terms WHERE school_id=%s", (school_id,))
        count = (cur.fetchone() or [0])[0]
        if int(count or 0) == 0:
            today = date.today()
            y = today.year
            # Simple month-based inference
            m = today.month
            current_term = 1 if 1 <= m <= 4 else (2 if 5 <= m <= 8 else 3)
            seed_rows = [
                (y, 1, "Term 1", f"{y}-01-03", f"{y}-04-15", 1 if current_term == 1 else 0),
                (y, 2, "Term 2", f"{y}-05-05", f"{y}-08-15", 1 if current_term == 2 else 0),
                (y, 3, "Term 3", f"{y}-09-01", f"{y}-11-30", 1 if current_term == 3 else 0),
            ]
            ins = conn.cursor()
            for yy, t, lbl, s, e, is_cur in seed_rows:
                try:
                    ins.execute(
                        "INSERT IGNORE INTO academic_terms(year, term, label, start_date, end_date, is_current, school_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (yy, t, lbl, s, e, is_cur, school_id),
                    )
                except Exception:
                    # On unique conflict due to legacy uniq key, continue
                    pass
            conn.commit()
            # Persist initial current term/year for this school in settings
            try:
                from utils.settings import set_school_setting
                set_school_setting("CURRENT_YEAR", str(y), school_id=school_id)
                set_school_setting("CURRENT_TERM", str(current_term), school_id=school_id)
            except Exception:
                pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_unique_indices_per_school(conn) -> None:
    """Best-effort: enforce/index per-school uniqueness where appropriate.

    - academic_terms: prefer UNIQUE (school_id, year, term). Try to drop legacy uq_year_term.
    - students: add index/unique on (school_id, admission_no) if column exists.
    """
    cur = conn.cursor()
    # academic_terms unique scope adjustment
    try:
        cur.execute("SHOW COLUMNS FROM academic_terms LIKE 'school_id'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE academic_terms ADD COLUMN school_id INT NULL")
            try:
                cur.execute("CREATE INDEX idx_academic_terms_school ON academic_terms(school_id)")
            except Exception:
                pass
            conn.commit()
        # Drop legacy unique if present
        try:
            cur.execute("SHOW INDEX FROM academic_terms WHERE Key_name='uq_year_term'")
            if cur.fetchone():
                try:
                    cur.execute("ALTER TABLE academic_terms DROP INDEX uq_year_term")
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass
        # Create composite unique including school_id
        try:
            cur.execute("SHOW INDEX FROM academic_terms WHERE Key_name='uq_school_year_term'")
            if not cur.fetchone():
                cur.execute("CREATE UNIQUE INDEX uq_school_year_term ON academic_terms(school_id, year, term)")
                conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    # Performance indices for large datasets (search and analytics)
    try:
        # Students: name, class and balance lookups within a school
        try:
            cur.execute("SHOW INDEX FROM students WHERE Key_name='idx_students_school_name'")
            if not cur.fetchone():
                cur.execute("CREATE INDEX idx_students_school_name ON students(school_id, name)")
                conn.commit()
        except Exception:
            pass
        try:
            cur.execute("SHOW INDEX FROM students WHERE Key_name='idx_students_school_class'")
            if not cur.fetchone():
                cur.execute("CREATE INDEX idx_students_school_class ON students(school_id, class_name)")
                conn.commit()
        except Exception:
            pass
        # Balance/fee_balance index for debtor lists
        try:
            cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
            has_bal = bool(cur.fetchone())
            if has_bal:
                cur.execute("SHOW INDEX FROM students WHERE Key_name='idx_students_school_balance'")
                if not cur.fetchone():
                    cur.execute("CREATE INDEX idx_students_school_balance ON students(school_id, balance)")
                    conn.commit()
            else:
                cur.execute("SHOW INDEX FROM students WHERE Key_name='idx_students_school_feebalance'")
                if not cur.fetchone():
                    cur.execute("CREATE INDEX idx_students_school_feebalance ON students(school_id, fee_balance)")
                    conn.commit()
        except Exception:
            pass
        # Payments: common analytics filters
        try:
            cur.execute("SHOW INDEX FROM payments WHERE Key_name='idx_pay_school_date'")
            if not cur.fetchone():
                cur.execute("CREATE INDEX idx_pay_school_date ON payments(school_id, date)")
                conn.commit()
        except Exception:
            pass
        try:
            cur.execute("SHOW INDEX FROM payments WHERE Key_name='idx_pay_school_year_term'")
            if not cur.fetchone():
                cur.execute("CREATE INDEX idx_pay_school_year_term ON payments(school_id, year, term)")
                conn.commit()
        except Exception:
            pass
        try:
            cur.execute("SHOW INDEX FROM payments WHERE Key_name='idx_pay_school_method'")
            if not cur.fetchone():
                cur.execute("CREATE INDEX idx_pay_school_method ON payments(school_id, method)")
                conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def ensure_perf_indices(conn) -> None:
    """Compatibility wrapper for older imports."""
    ensure_unique_indices_per_school(conn)


def ensure_fulltext_students(conn) -> None:
    """Create an optional FULLTEXT index on students(name, admission_no).

    Safe to call repeatedly; silently skips if not supported or already exists.
    """
    try:
        cur = conn.cursor()
        # Check if a FULLTEXT index already exists on (name, admission_no)
        try:
            cur.execute(
                "SHOW INDEX FROM students WHERE Index_type='FULLTEXT' AND (Column_name='name' OR Column_name='admission_no')"
            )
            if cur.fetchone():
                return
        except Exception:
            # SHOW INDEX not supported or table missing; abort quietly
            return

        # Attempt to create a combined FULLTEXT index
        try:
            cur.execute("CREATE FULLTEXT INDEX ft_students_name_adm ON students(name, admission_no)")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    except Exception:
        # Non-fatal: environments without privileges/engine support
        try:
            conn.rollback()
        except Exception:
            pass

    # students composite index/unique on (school_id, admission_no) if column exists
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'admission_no'")
        has_adm = bool(cur.fetchone())
        if has_adm:
            # Try unique first; if fails (due to dupes), fall back to non-unique index
            try:
                cur.execute("SHOW INDEX FROM students WHERE Key_name='uq_school_admno'")
                if not cur.fetchone():
                    cur.execute("CREATE UNIQUE INDEX uq_school_admno ON students(school_id, admission_no)")
                    conn.commit()
            except Exception:
                try:
                    cur.execute("SHOW INDEX FROM students WHERE Key_name='idx_school_admno'")
                    if not cur.fetchone():
                        cur.execute("CREATE INDEX idx_school_admno ON students(school_id, admission_no)")
                        conn.commit()
                except Exception:
                    pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
