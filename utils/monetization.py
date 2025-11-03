from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Any

import mysql.connector
from flask import current_app


def _db():
    cfg = current_app.config
    from urllib.parse import urlparse
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


def _ensure_tables(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_plans (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            plan_code VARCHAR(20) NOT NULL DEFAULT 'FREE',
            expires_at DATETIME NULL,
            grace_days INT NOT NULL DEFAULT 7,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            activated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_school_plans_school_id (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            feature_code VARCHAR(50) NOT NULL,
            is_enabled TINYINT(1) NOT NULL DEFAULT 0,
            UNIQUE KEY uq_school_feature (school_id, feature_code),
            INDEX idx_school_features_school_id (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def feature_enabled(school_id: int, code: str) -> bool:
    db = _db()
    try:
        _ensure_tables(db)
        cur = db.cursor()
        cur.execute(
            "SELECT 1 FROM school_features WHERE school_id=%s AND feature_code=%s AND is_enabled=1 LIMIT 1",
            (school_id, code),
        )
        return cur.fetchone() is not None
    finally:
        db.close()


def plan_status(school_id: int) -> Dict[str, Any]:
    db = _db()
    try:
        _ensure_tables(db)
        cur = db.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM school_plans WHERE school_id=%s AND is_active=1 ORDER BY id DESC LIMIT 1",
            (school_id,),
        )
        plan = cur.fetchone()
        if not plan:
            return {"active": False, "expired": True, "plan_code": "FREE", "expires_at": None}
        expired = False
        in_grace = False
        expires_at = plan.get("expires_at")
        if expires_at:
            expired = datetime.utcnow() > (expires_at if isinstance(expires_at, datetime) else expires_at)
            if expired:
                grace = (expires_at if isinstance(expires_at, datetime) else expires_at) + timedelta(days=int(plan.get("grace_days", 7)))
                in_grace = datetime.utcnow() <= grace
        return {
            "active": (not expired) or in_grace,
            "expired": expired,
            "in_grace": in_grace,
            "plan_code": plan.get("plan_code", "FREE"),
            "expires_at": expires_at,
        }
    finally:
        db.close()

