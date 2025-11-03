"""
Maintenance utilities for Fee Management System.

Usage examples (PowerShell/CMD):

  python scripts/maintenance.py ensure-email-columns
  python scripts/maintenance.py backfill-email --from parent_email

This script connects using SQLALCHEMY_DATABASE_URI or DB_* env vars.
"""

from __future__ import annotations

import os
import sys
import argparse
import mysql.connector
from urllib.parse import urlparse


def _db_from_env():
    uri = os.environ.get("SQLALCHEMY_DATABASE_URI", "")
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")
    if uri:
        try:
            parsed = urlparse(uri)
            if parsed.scheme.startswith("mysql"):
                if parsed.hostname:
                    host = parsed.hostname
                if parsed.username:
                    user = parsed.username
                if parsed.password:
                    password = parsed.password
                if parsed.path and len(parsed.path) > 1:
                    database = parsed.path.lstrip("/")
        except Exception:
            pass
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def ensure_email_columns(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM students LIKE 'email'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE students ADD COLUMN email VARCHAR(190) NULL")
        cur.execute("SHOW COLUMNS FROM students LIKE 'parent_email'")
        if not cur.fetchone():
            try:
                cur.execute("ALTER TABLE students ADD COLUMN parent_email VARCHAR(190) NULL")
            except Exception:
                pass
        try:
            cur.execute("CREATE INDEX idx_students_email ON students(email)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_students_parent_email ON students(parent_email)")
        except Exception:
            pass
        conn.commit()
        print("OK: ensured email columns and indexes on students")
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def backfill_email(conn, source_col: str = "parent_email") -> int:
    if source_col not in ("parent_email", "email"):
        raise ValueError("source_col must be 'parent_email' or 'email'")
    cur = conn.cursor()
    # Ensure columns first
    ensure_email_columns(conn)
    # Copy values where email is NULL and source has data
    cur.execute(
        f"UPDATE students SET email = {source_col} WHERE (email IS NULL OR email='') AND {source_col} IS NOT NULL AND {source_col} <> ''"
    )
    affected = cur.rowcount or 0
    conn.commit()
    return int(affected)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Maintenance tools")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ensure-email-columns", help="Add email/parent_email columns + indexes if missing")
    bp = sub.add_parser("backfill-email", help="Copy from parent_email to email where email is empty")
    bp.add_argument("--from", dest="src", choices=["parent_email", "email"], default="parent_email")

    args = ap.parse_args(argv)
    conn = _db_from_env()
    try:
        if args.cmd == "ensure-email-columns":
            ensure_email_columns(conn)
            return 0
        if args.cmd == "backfill-email":
            n = backfill_email(conn, source_col=args.src)
            print(f"OK: backfilled {n} row(s)")
            return 0
        ap.error("Unknown command")
        return 2
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

