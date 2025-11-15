"""
Wipe only database data (no files) for the Fee Management System.

- Connects using app.get_db_connection() honoring .env and Config.
- TRUNCATEs all application tables while preserving schema and migrations.
- Skips Alembic and SQLAlchemy internal tables by default.

Usage:
  python scripts/wipe_database_only.py --dry-run
  python scripts/wipe_database_only.py --force "DELETE ALL"

Optional:
  --include-billing   Include billing/license tables in wipe.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable, List, Set


def _log(*a):
    print("[wipe-db]", *a)


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Truncate MySQL tables (DB-only)")
    p.add_argument("--dry-run", action="store_true", help="Preview actions only")
    p.add_argument("--force", metavar="CONFIRM", help="Type 'DELETE ALL' to confirm", nargs="?")
    p.add_argument("--include-billing", action="store_true", help="Also truncate billing/license tables")
    return p.parse_args()


def _iter_tables(conn) -> List[str]:
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    rows = cur.fetchall() or []
    names: List[str] = []
    for r in rows:
        if isinstance(r, (list, tuple)) and r:
            names.append(str(r[0]))
        elif isinstance(r, dict):
            names.extend([str(v) for v in r.values()])
    return sorted(set(names))


def _truncate_all(conn, *, dry: bool, include_billing: bool) -> None:
    skip: Set[str] = {
        "alembic_version",
        # common sqlalchemy tables if any
    }
    billing_like = {"license_requests", "license_keys"}
    names = [n for n in _iter_tables(conn) if n not in skip]
    if not include_billing:
        names = [n for n in names if n not in billing_like]
    if not names:
        _log("No tables to truncate")
        return
    _log(f"Tables to truncate ({len(names)}):", ", ".join(names))
    if dry:
        for n in names:
            _log("TRUNCATE", n)
        return
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
    except Exception:
        pass
    for n in names:
        try:
            cur.execute(f"TRUNCATE TABLE `{n}`")
            _log("Truncated", n)
        except Exception as e:
            _log("Failed to truncate", n, ":", e)
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
    except Exception:
        pass
    try:
        conn.commit()
    except Exception:
        pass


def main() -> int:
    args = _args()
    dry = bool(args.dry_run)
    if not dry and args.force != "DELETE ALL":
        _log("Refusing to run without --force 'DELETE ALL'")
        return 2

    # Use the app's DB connection helper
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, here)
    try:
        from app import get_db_connection  # type: ignore
        conn = get_db_connection()
    except Exception as e:
        _log("DB connection failed:", e)
        return 3

    try:
        _truncate_all(conn, dry=dry, include_billing=bool(args.include_billing))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    _log("DONE" if not dry else "DRY-RUN COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

