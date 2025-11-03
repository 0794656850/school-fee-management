from __future__ import annotations

from flask import Blueprint, request, jsonify, session, Response, redirect, url_for, flash
import csv
from io import StringIO

from utils.audit import ensure_audit_table, verify_chain
from utils.monetization import plan_status as _plan_status


audit_bp = Blueprint("audit", __name__, url_prefix="/admin/audit")


def _db():
    from flask import current_app
    import mysql.connector
    from urllib.parse import urlparse
    cfg = current_app.config
    host = "localhost"; user = "root"; password = ""; database = "school_fee_db"
    uri = cfg.get("SQLALCHEMY_DATABASE_URI", "")
    if uri and uri.startswith("mysql"):
        try:
            parsed = urlparse(uri)
            host = parsed.hostname or host
            user = parsed.username or user
            password = parsed.password or password
            if parsed.path and len(parsed.path) > 1:
                database = parsed.path.lstrip("/")
        except Exception:
            pass
    import os
    host = os.environ.get("DB_HOST", host)
    user = os.environ.get("DB_USER", user)
    password = os.environ.get("DB_PASSWORD", password)
    database = os.environ.get("DB_NAME", database)
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _require_admin():
    # Allow global admin OR a logged-in school user with admin/owner role
    if session.get("is_admin"):
        return None
    if session.get("user_logged_in") and session.get("role") in ("owner", "admin"):
        return None
    return jsonify({"ok": False, "error": "forbidden"}), 403


@audit_bp.route("/api/logs", methods=["GET"])
def api_logs():
    guard = _require_admin()
    if guard is not None:
        return guard
    db = _db()
    try:
        ensure_audit_table(db)
        cur = db.cursor(dictionary=True)
        params = []
        where = []
        school_id = session.get("school_id")
        if school_id:
            where.append("school_id=%s")
            params.append(school_id)
        action = request.args.get("action")
        if action:
            where.append("action=%s")
            params.append(action)
        entity_type = request.args.get("entity_type")
        if entity_type:
            where.append("entity_type=%s")
            params.append(entity_type)
        q = request.args.get("q")
        if q:
            where.append("(user LIKE %s OR route LIKE %s)")
            like = f"%{q}%"; params.extend([like, like])
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        limit = min(max(int(request.args.get("limit", 200)), 1), 1000)
        cur.execute(
            f"SELECT id, ts, school_id, user, action, entity_type, entity_id, meta, ip, user_agent, route, method, status_code, severity, request_id, sig FROM audit_log {where_sql} ORDER BY id DESC LIMIT {limit}",
            tuple(params),
        )
        rows = cur.fetchall() or []
        # Attempt to parse meta JSON if string
        for r in rows:
            m = r.get("meta")
            if isinstance(m, str):
                try:
                    import json
                    r["meta"] = json.loads(m)
                except Exception:
                    pass
        return jsonify({"ok": True, "logs": rows})
    finally:
        db.close()


@audit_bp.route("/api/export.csv", methods=["GET"])
def export_csv():
    guard = _require_admin()
    if guard is not None:
        return guard
    # Premium guard: require non-FREE plan
    sid = session.get("school_id")
    try:
        status = _plan_status(int(sid)) if sid else {"plan_code": "FREE"}
    except Exception:
        status = {"plan_code": "FREE"}
    if status.get("plan_code") == "FREE":
        flash("Upgrade to Pro to export audit logs.", "warning")
        return redirect(url_for("monetization.index"))
    db = _db()
    try:
        ensure_audit_table(db)
        cur = db.cursor()
        school_id = session.get("school_id")
        params = []
        where = []
        if school_id:
            where.append("school_id=%s"); params.append(school_id)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        cur.execute(f"SELECT id, ts, school_id, user, action, entity_type, entity_id, meta, ip, user_agent, route, method, status_code, severity, request_id, sig FROM audit_log {where_sql} ORDER BY id DESC LIMIT 5000", tuple(params))
        rows = cur.fetchall() or []
        # CSV build
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["id","ts","school_id","user","action","entity_type","entity_id","meta","ip","user_agent","route","method","status_code","severity","request_id","sig"])
        for r in rows:
            writer.writerow(list(r))
        return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=audit_logs.csv'})
    finally:
        db.close()


@audit_bp.route("/api/verify", methods=["GET"])
def api_verify():
    guard = _require_admin()
    if guard is not None:
        return guard
    db = _db()
    try:
        ok = verify_chain(db)
        return jsonify({"ok": True, "integrity": bool(ok)})
    finally:
        db.close()
