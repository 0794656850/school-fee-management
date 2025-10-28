from __future__ import annotations

import os
from typing import Optional, Dict

import mysql.connector
from flask import current_app, session
from urllib.parse import urlparse


def _db():
    cfg = getattr(current_app, "config", {})
    uri = cfg.get("SQLALCHEMY_DATABASE_URI", "") if isinstance(cfg, dict) else ""

    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "school_fee_db")

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
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def ensure_app_settings_table(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            `key` VARCHAR(100) NOT NULL UNIQUE,
            `value` TEXT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_school_settings_table(db):
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_settings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            `key` VARCHAR(100) NOT NULL,
            `value` TEXT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_school_key (school_id, `key`),
            KEY idx_school (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        db = _db()
        try:
            # Prefer school-specific value when a school context exists
            try:
                sid = session.get("school_id")
            except Exception:
                sid = None

            if sid:
                try:
                    ensure_school_settings_table(db)
                    cur = db.cursor()
                    cur.execute(
                        "SELECT `value` FROM school_settings WHERE school_id=%s AND `key`=%s LIMIT 1",
                        (sid, key),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return str(row[0])
                except Exception:
                    # fall through to global
                    pass

            ensure_app_settings_table(db)
            cur = db.cursor()
            cur.execute("SELECT `value` FROM app_settings WHERE `key`=%s LIMIT 1", (key,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return str(row[0])
            return default
        finally:
            db.close()
    except Exception:
        return default


def set_setting(key: str, value: Optional[str]) -> None:
    db = _db()
    try:
        ensure_app_settings_table(db)
        cur = db.cursor()
        cur.execute(
            "INSERT INTO app_settings(`key`, `value`) VALUES(%s,%s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)",
            (key, value),
        )
        db.commit()
    finally:
        db.close()


def set_school_setting(key: str, value: Optional[str], school_id: Optional[int] = None) -> None:
    db = _db()
    try:
        ensure_school_settings_table(db)
        cur = db.cursor()
        sid = school_id
        if sid is None:
            try:
                sid = session.get("school_id")
            except Exception:
                sid = None
        if not sid:
            # no-op if no school context
            return
        cur.execute(
            """
            INSERT INTO school_settings(school_id, `key`, `value`)
            VALUES(%s,%s,%s)
            ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)
            """,
            (sid, key, value),
        )
        db.commit()
    finally:
        db.close()


def get_settings(keys: list[str]) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    if not keys:
        return out
    try:
        db = _db()
        try:
            ensure_app_settings_table(db)
            placeholders = ",".join(["%s"] * len(keys))
            cur = db.cursor()
            cur.execute(f"SELECT `key`, `value` FROM app_settings WHERE `key` IN ({placeholders})", tuple(keys))
            rows = cur.fetchall() or []
            for k in keys:
                out[k] = None
            for k, v in rows:
                out[str(k)] = None if v is None else str(v)
            return out
        finally:
            db.close()
    except Exception:
        for k in keys:
            out[k] = None
        return out
