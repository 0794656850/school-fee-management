from __future__ import annotations

import json
from typing import Optional, Any, Dict


def ensure_audit_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            school_id INT NULL,
            user VARCHAR(120) NULL,
            action VARCHAR(64) NOT NULL,
            entity_type VARCHAR(64) NULL,
            entity_id BIGINT NULL,
            meta JSON NULL,
            INDEX idx_school_ts (school_id, ts),
            INDEX idx_entity (entity_type, entity_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()


def log_event(
    conn,
    school_id: Optional[int],
    user: Optional[str],
    action: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        ensure_audit_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO audit_log (school_id, user, action, entity_type, entity_id, meta)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (school_id, user, action, entity_type, entity_id, json.dumps(meta or {})),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

