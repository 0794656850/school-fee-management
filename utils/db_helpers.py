from __future__ import annotations

from datetime import datetime
from mysql.connector.connection_cext import CMySQLConnection  # type: ignore


def ensure_approval_requests_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS approval_requests (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            requestor_name VARCHAR(150) NOT NULL,
            requestor_email VARCHAR(255) NOT NULL,
            request_type VARCHAR(80) NOT NULL,
            amount DECIMAL(12,2) NULL,
            reason TEXT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'otp_pending',
            otp_hash VARCHAR(128) NULL,
            otp_requested_at DATETIME NULL,
            approver VARCHAR(150) NULL,
            approved_at DATETIME NULL,
            qr_payload TEXT NULL,
            admin_note TEXT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            INDEX idx_approval_school (school_id),
            INDEX idx_approval_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_guardian_receipts_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guardian_receipts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            guardian_name VARCHAR(150) NULL,
            guardian_email VARCHAR(255) NULL,
            guardian_phone VARCHAR(64) NULL,
            description TEXT NULL,
            file_path VARCHAR(255) NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            verified_by VARCHAR(150) NULL,
            verified_at DATETIME NULL,
            payment_date DATE NULL,
            amount DECIMAL(12,2) NULL,
            bank_name VARCHAR(128) NULL,
            notes TEXT NULL,
            admin_note TEXT NULL,
            rejection_reason TEXT NULL,
            analysis TEXT NULL,
            invoice_id INT NULL,
            payment_id INT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()
    extra_columns = {
        "payment_date": "DATE NULL",
        "amount": "DECIMAL(12,2) NULL",
        "bank_name": "VARCHAR(128) NULL",
        "notes": "TEXT NULL",
        "admin_note": "TEXT NULL",
        "rejection_reason": "TEXT NULL",
        "analysis": "TEXT NULL",
        "invoice_id": "INT NULL",
        "payment_id": "INT NULL",
    }
    for column, definition in extra_columns.items():
        try:
            cur.execute("SHOW COLUMNS FROM guardian_receipts LIKE %s", (column,))
            if cur.fetchone():
                continue
            cur.execute(f"ALTER TABLE guardian_receipts ADD COLUMN {column} {definition}")
        except Exception:
            pass
    try:
        cur.execute("ALTER TABLE guardian_receipts ADD INDEX IF NOT EXISTS idx_school_status (school_id, status)")
    except Exception:
        try:
            cur.execute("ALTER TABLE guardian_receipts ADD INDEX idx_school_status (school_id, status)")
        except Exception:
            pass
    try:
        cur.execute("ALTER TABLE guardian_receipts ADD INDEX IF NOT EXISTS idx_student_status (student_id, status)")
    except Exception:
        try:
            cur.execute("ALTER TABLE guardian_receipts ADD INDEX idx_student_status (student_id, status)")
        except Exception:
            pass
    try:
        cur.execute("ALTER TABLE guardian_receipts ADD INDEX IF NOT EXISTS idx_payment_status (status)")
    except Exception:
        try:
            cur.execute("ALTER TABLE guardian_receipts ADD INDEX idx_payment_status (status)")
        except Exception:
            pass
    db.commit()


def ensure_profile_deletion_requests_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS profile_deletion_requests (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NULL,
            student_id INT NULL,
            guardian_name VARCHAR(150) NULL,
            guardian_email VARCHAR(255) NULL,
            guardian_phone VARCHAR(64) NULL,
            reason TEXT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            processed_at DATETIME NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_profile_delete_school (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_parent_portal_columns(db: CMySQLConnection) -> None:
    cur = db.cursor()
    columns = {
        "parent_portal_archived": "TINYINT(1) NOT NULL DEFAULT 0",
        "parent_portal_archived_at": "DATETIME NULL",
        "parent_email_verified": "TINYINT(1) NOT NULL DEFAULT 0",
        "parent_email_verified_at": "DATETIME NULL",
    }
    for name, definition in columns.items():
        try:
            cur.execute("SHOW COLUMNS FROM students LIKE %s", (name,))
            if cur.fetchone():
                continue
            cur.execute(f"ALTER TABLE students ADD COLUMN {name} {definition}")
        except Exception:
            pass
    db.commit()


def ensure_bank_link_sessions_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bank_link_sessions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            student_id INT NOT NULL,
            school_id INT NOT NULL,
            bank_name VARCHAR(128) NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'disconnected',
            connect_code VARCHAR(64) NULL,
            connected_at DATETIME NULL,
            last_payment_id INT NULL,
            last_payment_amount DECIMAL(12,2) NULL,
            last_payment_at DATETIME NULL,
            last_payment_reference VARCHAR(128) NULL,
            metadata TEXT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uq_bank_link_student_school (student_id, school_id),
            INDEX idx_bank_link_school (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()
