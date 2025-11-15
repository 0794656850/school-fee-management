"""
Wipe all profiles and all data from the Fee Management System.

What it does:
- TRUNCATEs all MySQL tables in the configured database.
- Deletes files under static/media, static/uploads, uploads/, and instance/.

Safety:
- Requires --force and a confirmation phrase to run.
- Supports --dry-run to preview actions.

Usage:
  python scripts/wipe_all_data.py --dry-run
  python scripts/wipe_all_data.py --force "DELETE ALL"
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from typing import Iterable, List


def _log(*a):
    print("[wipe]", *a)


def _confirm_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Wipe all data (DB + files)")
    p.add_argument("--force", metavar="CONFIRM", help="Type 'DELETE ALL' to confirm irreversible wipe")
    p.add_argument("--dry-run", action="store_true", help="Show what would be deleted, without deleting")
    return p.parse_args()


def _iter_tables(conn) -> List[str]:
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    rows = cur.fetchall() or []
    # Rows are tuples like (table_name,) possibly with dict cursor
    names: List[str] = []
    for r in rows:
        if isinstance(r, (list, tuple)) and r:
            names.append(str(r[0]))
        elif isinstance(r, dict):
            # MySQL dict cursor usually returns a single key
            names.extend([str(v) for v in r.values()])
    return sorted(set(names))


def _truncate_all_tables(conn, dry: bool = False) -> None:
    names = _iter_tables(conn)
    if not names:
        _log("No tables found; skipping DB wipe")
        return
    _log(f"Found {len(names)} tables")
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


def _rm_tree(path: str, dry: bool = False, keep: Iterable[str] = ()) -> None:
    if not os.path.exists(path):
        return
    # Remove children; preserve base dir and any keep-listed leaf names
    for root, dirs, files in os.walk(path):
        # Delete files
        for f in files:
            leaf = os.path.basename(f)
            if leaf in keep:
                continue
            full = os.path.join(root, f)
            if dry:
                _log("DELETE FILE", full)
            else:
                try:
                    os.remove(full)
                except Exception as e:
                    _log("Failed to delete", full, ":", e)
        # Delete empty dirs bottom-up in a second pass
    if not dry:
        # Remove empty directories after file deletion
        for root, dirs, files in os.walk(path, topdown=False):
            for d in dirs:
                full = os.path.join(root, d)
                try:
                    if not os.listdir(full):
                        os.rmdir(full)
                except Exception:
                    pass


def _wipe_files(dry: bool = False) -> None:
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    targets = [
        os.path.join(here, "static", "media"),
        os.path.join(here, "static", "uploads"),
        os.path.join(here, "uploads"),
        os.path.join(here, "instance"),
    ]
    for t in targets:
        if os.path.isdir(t):
            _log("Purging", t)
            _rm_tree(t, dry=dry, keep={".gitignore"})
        else:
            _log("Skip (missing)", t)
    # Remove common token/credentials if present (opt-in)
    for leaf in ("token.json",):
        p = os.path.join(here, leaf)
        if os.path.isfile(p):
            if dry:
                _log("DELETE FILE", p)
            else:
                try:
                    os.remove(p)
                except Exception as e:
                    _log("Failed to delete", p, ":", e)


def main() -> int:
    args = _confirm_args()
    dry = bool(args.dry_run)
    if not dry:
        if args.force != "DELETE ALL":
            _log("Refusing to run without --force 'DELETE ALL'")
            return 2
    # Connect to DB using the app's config logic
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from app import get_db_connection  # type: ignore
        conn = get_db_connection()
    except Exception as e:
        _log("DB connection failed:", e)
        conn = None

    if conn is not None:
        _log("Wiping database tables…")
        _truncate_all_tables(conn, dry=dry)
        try:
            conn.close()
        except Exception:
            pass
    else:
        _log("Skipping DB wipe (no connection)")

    _log("Wiping file storage…")
    _wipe_files(dry=dry)
    _log("DONE" if not dry else "DRY-RUN COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

