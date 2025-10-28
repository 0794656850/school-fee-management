from __future__ import annotations

from typing import Optional, Tuple, List, Dict


def ensure_user_tables(conn) -> None:
    cur = conn.cursor()
    # Core users table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(64) NOT NULL UNIQUE,
            email VARCHAR(120) NULL,
            password_hash VARCHAR(255) NOT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # School membership and role mapping
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS school_users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            user_id INT NOT NULL,
            role ENUM('owner','admin','accountant','bursar','staff','viewer') NOT NULL DEFAULT 'staff',
            UNIQUE KEY uq_school_user (school_id, user_id),
            KEY idx_school (school_id),
            KEY idx_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()
    # Best-effort: widen enum if table exists from an older version
    try:
        cur.execute("SHOW COLUMNS FROM school_users LIKE 'role'")
        r = cur.fetchone()
        # Attempt ALTER to include new roles (MySQL requires full enum list)
        cur.execute(
            "ALTER TABLE school_users MODIFY COLUMN role ENUM('owner','admin','accountant','bursar','staff','viewer') NOT NULL DEFAULT 'staff'"
        )
        conn.commit()
    except Exception:
        try: conn.rollback()
        except Exception: pass


def get_user_by_username(conn, username: str) -> Optional[Dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username=%s LIMIT 1", (username,))
    row = cur.fetchone()
    return row


def get_user_school_role(conn, user_id: int, school_id: int) -> Optional[str]:
    cur = conn.cursor()
    cur.execute(
        "SELECT role FROM school_users WHERE user_id=%s AND school_id=%s",
        (user_id, school_id),
    )
    r = cur.fetchone()
    if not r:
        return None
    return (r[0] if not isinstance(r, dict) else r.get("role")) or None


def count_school_users(conn, school_id: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM school_users WHERE school_id=%s", (school_id,))
    c = cur.fetchone()
    try:
        return int(c[0])
    except Exception:
        try:
            return int(c.get("COUNT(*)", 0))  # type: ignore
        except Exception:
            return 0


def list_school_users(conn, school_id: int) -> List[Dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT su.id AS map_id, u.id, u.username, u.email, u.is_active, su.role
        FROM school_users su
        JOIN users u ON u.id = su.user_id
        WHERE su.school_id=%s
        ORDER BY FIELD(su.role,'owner','admin','staff','viewer'), u.username
        """,
        (school_id,),
    )
    return cur.fetchall() or []


def create_user(conn, username: str, email: Optional[str], password_hash: str) -> int:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (username, email, password_hash) VALUES (%s,%s,%s)",
        (username, email, password_hash),
    )
    conn.commit()
    return int(cur.lastrowid)


def ensure_school_user(conn, user_id: int, school_id: int, role: str = 'staff') -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO school_users (school_id, user_id, role)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE role=VALUES(role)
        """,
        (school_id, user_id, role),
    )
    conn.commit()


def set_user_password(conn, user_id: int, password_hash: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash=%s WHERE id=%s", (password_hash, user_id))
    conn.commit()


def set_user_active(conn, user_id: int, active: bool) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_active=%s WHERE id=%s", (1 if active else 0, user_id))
    conn.commit()
