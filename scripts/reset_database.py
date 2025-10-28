"""
Danger: Clears ALL data in the configured MySQL database.

Usage:
  python scripts/reset_database.py

This will TRUNCATE all tables in the configured database. It disables
foreign key checks during the operation so child tables don't block.

Only run this if you intend to remove all existing schools, students,
payments, settings, and logs to start fresh.
"""

from __future__ import annotations

import sys
import os
from typing import List


def main() -> int:
    try:
        # Ensure project root is on sys.path so `import app` works when run from scripts/
        here = os.path.dirname(os.path.abspath(__file__))
        root = os.path.abspath(os.path.join(here, os.pardir))
        if root not in sys.path:
            sys.path.insert(0, root)
        # Reuse the app's DB connection helper so we respect .env / config
        from app import get_db_connection  # type: ignore
    except Exception as e:
        print(f"Failed to import app.get_db_connection: {e}")
        return 2

    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"Could not connect to DB: {e}")
        return 3

    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    rows = cur.fetchall() or []
    tables: List[str] = []
    for r in rows:
        try:
            # MySQL returns tuples like ('table_name',)
            t = r[0]
        except Exception:
            t = str(r)
        if not t:
            continue
        tables.append(t)

    if not tables:
        print("No tables found; nothing to clear.")
        conn.close()
        return 0

    print("About to TRUNCATE tables (foreign_key_checks disabled):")
    for t in tables:
        print(f" - {t}")

    # Simple interactive guard (can be bypassed via ASSUME_YES)
    assume_yes = (os.environ.get("ASSUME_YES", "").strip().upper() in ("1", "YES", "TRUE"))
    if sys.stdin.isatty() and not assume_yes:
        try:
            ans = input("Type 'YES' to proceed: ").strip()
        except EOFError:
            ans = ""
        if ans != "YES":
            print("Aborted.")
            conn.close()
            return 1

    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in tables:
            try:
                cur.execute(f"TRUNCATE TABLE `{t}`")
            except Exception as te:
                print(f"Failed to truncate {t}: {te}")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("All tables truncated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
