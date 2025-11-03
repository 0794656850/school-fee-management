from __future__ import annotations

import json
import hashlib
from functools import wraps
from typing import Optional, Any, Dict, Callable

try:
    # Flask types are optional; functions still work without them
    from flask import request, g
except Exception:  # pragma: no cover
    request = None  # type: ignore
    g = None  # type: ignore


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
            -- Premium fields
            ip VARCHAR(45) NULL,
            user_agent VARCHAR(255) NULL,
            route VARCHAR(200) NULL,
            method VARCHAR(10) NULL,
            status_code INT NULL,
            severity ENUM('info','warning','error','critical') NOT NULL DEFAULT 'info',
            request_id VARCHAR(64) NULL,
            sig CHAR(64) NULL,
            INDEX idx_school_ts (school_id, ts),
            INDEX idx_entity (entity_type, entity_id),
            INDEX idx_action (action),
            INDEX idx_sig (sig)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()
    # Backfill columns if table existed before (best-effort ALTERs)
    try:
        for col_sql in [
            "ADD COLUMN ip VARCHAR(45) NULL",
            "ADD COLUMN user_agent VARCHAR(255) NULL",
            "ADD COLUMN route VARCHAR(200) NULL",
            "ADD COLUMN method VARCHAR(10) NULL",
            "ADD COLUMN status_code INT NULL",
            "ADD COLUMN severity ENUM('info','warning','error','critical') NOT NULL DEFAULT 'info'",
            "ADD COLUMN request_id VARCHAR(64) NULL",
            "ADD COLUMN sig CHAR(64) NULL",
        ]:
            try:
                cur.execute(f"ALTER TABLE audit_log {col_sql}")
                conn.commit()
            except Exception:
                try: conn.rollback()
                except Exception: pass
    except Exception:
        pass


def _compute_sig(prev: str, payload: Dict[str, Any]) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    h = hashlib.sha256()
    h.update((prev or "").encode("utf-8"))
    h.update(data)
    return h.hexdigest()


def _latest_sig(conn) -> str:
    try:
        cur = conn.cursor()
        cur.execute("SELECT sig FROM audit_log WHERE sig IS NOT NULL ORDER BY id DESC LIMIT 1")
        r = cur.fetchone()
        if not r:
            return ""
        return (r[0] if not isinstance(r, dict) else r.get("sig")) or ""
    except Exception:
        return ""


def log_event(
    conn,
    school_id: Optional[int],
    user: Optional[str],
    action: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
    *,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    route: Optional[str] = None,
    method: Optional[str] = None,
    status_code: Optional[int] = None,
    severity: str = 'info',
    request_id: Optional[str] = None,
) -> None:
    """Premium audit event with tamper-evident signature chain.

    Stores extra fields (ip, user_agent, route, method, status_code, severity, request_id)
    and computes a SHA-256 chain signature in `sig`.
    """
    try:
        ensure_audit_table(conn)
        cur = conn.cursor()
        payload = {
            "school_id": school_id,
            "user": user,
            "action": action,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "meta": meta or {},
            "ip": ip,
            "user_agent": user_agent,
            "route": route,
            "method": method,
            "status_code": status_code,
            "severity": severity,
            "request_id": request_id,
        }
        prev = _latest_sig(conn)
        sig = _compute_sig(prev, payload)
        cur.execute(
            """
            INSERT INTO audit_log (school_id, user, action, entity_type, entity_id, meta, ip, user_agent, route, method, status_code, severity, request_id, sig)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                school_id,
                user,
                action,
                entity_type,
                entity_id,
                json.dumps(meta or {}),
                ip,
                user_agent,
                route,
                method,
                status_code,
                severity,
                request_id,
                sig,
            ),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def log_event_from_request(conn, action: str, entity_type: Optional[str] = None, entity_id: Optional[int] = None, meta: Optional[Dict[str, Any]] = None, severity: str = 'info') -> None:
    """Helper to log using Flask request/session context if available."""
    try:
        sid = None
        username = None
        rid = None
        if request is not None:
            try:
                from flask import session  # lazy
                sid = session.get("school_id")
                username = session.get("username") or session.get("admin_user")
            except Exception:
                pass
            ip = request.headers.get('X-Forwarded-For', request.remote_addr)
            ua = request.headers.get('User-Agent')
            route = request.path
            method = request.method
            # request id from g if set by middleware
            if g is not None:
                rid = getattr(g, 'request_id', None)
            log_event(
                conn,
                sid,
                username,
                action,
                entity_type,
                entity_id,
                meta,
                ip=ip,
                user_agent=ua,
                route=route,
                method=method,
                status_code=None,
                severity=severity,
                request_id=rid,
            )
        else:
            log_event(conn, None, None, action, entity_type, entity_id, meta, severity=severity)
    except Exception:
        pass


def audit(action: str, entity_type: Optional[str] = None, id_param: Optional[str] = None):
    """Decorator for Flask views to record audit events on success and errors.

    - action: a short action label, e.g., 'student.create', 'payment.add'
    - entity_type: optional entity type string
    - id_param: optional request arg/form key that contains the entity id
    """
    def _outer(fn: Callable):
        @wraps(fn)
        def _inner(*args, **kwargs):
            entity_id = None
            try:
                if id_param and request is not None:
                    entity_id = request.values.get(id_param)
            except Exception:
                entity_id = None
            try:
                resp = fn(*args, **kwargs)
                try:
                    from utils.settings import _db as _get_conn
                    conn = _get_conn()
                    try:
                        log_event_from_request(conn, action=action, entity_type=entity_type, entity_id=entity_id, meta={"ok": True})
                    finally:
                        conn.close()
                except Exception:
                    pass
                return resp
            except Exception as e:
                try:
                    from utils.settings import _db as _get_conn
                    conn = _get_conn()
                    try:
                        log_event_from_request(conn, action=action, entity_type=entity_type, entity_id=entity_id, meta={"ok": False, "error": str(e)}, severity='error')
                    finally:
                        conn.close()
                except Exception:
                    pass
                raise
        return _inner
    return _outer


def verify_chain(conn) -> bool:
    """Verify the integrity of the audit log chain.

    Returns True if all signatures match; False otherwise.
    """
    try:
        ensure_audit_table(conn)
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, school_id, user, action, entity_type, entity_id, meta, ip, user_agent, route, method, status_code, severity, request_id, sig FROM audit_log ORDER BY id ASC")
        prev = ""
        for row in cur.fetchall() or []:
            payload = {k: row.get(k) for k in [
                'school_id','user','action','entity_type','entity_id','meta','ip','user_agent','route','method','status_code','severity','request_id']}
            # meta may be string if DB returns raw
            meta = payload.get('meta')
            if isinstance(meta, str):
                try:
                    payload['meta'] = json.loads(meta)
                except Exception:
                    payload['meta'] = {}
            expect = _compute_sig(prev, payload)
            if (row.get('sig') or '') != expect:
                return False
            prev = expect
        return True
    except Exception:
        return False
