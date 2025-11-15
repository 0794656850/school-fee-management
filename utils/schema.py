from __future__ import annotations

from typing import Tuple


def get_admission_select_and_column(cur) -> Tuple[str, str]:
    """Return a tuple (select_expr, where_column) for the students admission number.

    Tries columns in this order: admission_no, regNo, reg_no.
    select_expr always aliases to 'regNo' for downstream compatibility.
    """
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'admission_no'")
        if cur.fetchone():
            return ("admission_no AS regNo", "admission_no")
    except Exception:
        pass
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'regNo'")
        if cur.fetchone():
            return ("regNo AS regNo", "regNo")
    except Exception:
        pass
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'reg_no'")
        if cur.fetchone():
            return ("reg_no AS regNo", "reg_no")
    except Exception:
        pass
    # Fallback to NULL if none exist to avoid hard failures in SELECT lists
    return ("NULL AS regNo", "admission_no")
