from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import session


def _connect():
    try:
        from app import get_db_connection  # noqa: W0611 - avoid circular import at module load
        return get_db_connection()
    except Exception:
        return None


def ensure_audit_table(db=None) -> None:
    close = False
    if db is None:
        db = _connect()
        close = True
    if db is None:
        return
    try:
        cursor = db.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                school_id INT NULL,
                user_id INT NULL,
                username VARCHAR(150) NULL,
                user_role VARCHAR(64) NULL,
                action VARCHAR(100) NOT NULL,
                target VARCHAR(100) NULL,
                detail TEXT NULL,
                created_at DATETIME NOT NULL,
                INDEX idx_audit_school (school_id),
                INDEX idx_audit_action (action)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        db.commit()
    finally:
        if close:
            try:
                db.close()
            except Exception:
                pass


def log_event(action: str, target: str | None = None, detail: str | None = None, db=None) -> None:
    now = datetime.utcnow()
    close = False
    if db is None:
        db = _connect()
        close = True
    if db is None:
        return
    try:
        ensure_audit_table(db)
        cursor = db.cursor()
        school_id = session.get("school_id")
        user_id = session.get("user_id")
        username = session.get("username")
        user_role = session.get("role") or session.get("user_role")
        cursor.execute(
            """
            INSERT INTO audit_logs (school_id, user_id, username, user_role, action, target, detail, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                school_id,
                user_id,
                username,
                user_role,
                action,
                target,
                detail,
                now,
            ),
        )
        db.commit()
    finally:
        if close:
            try:
                db.close()
            except Exception:
                pass


def fetch_audit_logs(school_id: int | None = None, limit: int = 50) -> List[Dict[str, Any]]:
    db = _connect()
    if db is None:
        return []
    try:
        ensure_audit_table(db)
        cursor = db.cursor(dictionary=True)
        if school_id:
            cursor.execute(
                """
                SELECT * FROM audit_logs
                WHERE school_id=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (school_id, limit),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM audit_logs
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
        return cursor.fetchall() or []
    finally:
        try:
            db.close()
        except Exception:
            pass
