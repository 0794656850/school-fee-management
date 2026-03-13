from __future__ import annotations

from utils.timezone_helpers import EATDateTime as datetime
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
        "parent_name": "VARCHAR(150) NULL",
        "parent_email": "VARCHAR(255) NULL",
        "parent_phone": "VARCHAR(64) NULL",
        "parent_preferred_channel": "VARCHAR(16) NOT NULL DEFAULT 'auto'",
        "parent_comm_opt_in": "TINYINT(1) NOT NULL DEFAULT 1",
        "parent_email_opt_in": "TINYINT(1) NOT NULL DEFAULT 1",
        "parent_sms_opt_in": "TINYINT(1) NOT NULL DEFAULT 1",
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


def ensure_reminder_messages_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reminder_messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            guardian_email VARCHAR(255) NULL,
            guardian_phone VARCHAR(64) NULL,
            channel VARCHAR(24) NOT NULL DEFAULT 'email',
            subject VARCHAR(255) NULL,
            body TEXT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'sent',
            sent_by VARCHAR(150) NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_reminder_school (school_id),
            INDEX idx_reminder_student (student_id),
            INDEX idx_reminder_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # Backfill missing columns for older schemas.
    try:
        def _has_col(name: str) -> bool:
            cur.execute("SHOW COLUMNS FROM reminder_messages LIKE %s", (name,))
            return bool(cur.fetchone())

        if not _has_col("guardian_email"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN guardian_email VARCHAR(255) NULL")
        if not _has_col("guardian_phone"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN guardian_phone VARCHAR(64) NULL")
        if not _has_col("channel"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN channel VARCHAR(24) NOT NULL DEFAULT 'email'")
        if not _has_col("subject"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN subject VARCHAR(255) NULL")
        if not _has_col("body"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN body TEXT NULL")
        if not _has_col("status"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN status VARCHAR(24) NOT NULL DEFAULT 'sent'")
        if not _has_col("sent_by"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN sent_by VARCHAR(150) NULL")
        if not _has_col("created_at"):
            cur.execute("ALTER TABLE reminder_messages ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
        try:
            cur.execute("CREATE INDEX idx_reminder_school ON reminder_messages (school_id)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_reminder_student ON reminder_messages (student_id)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_reminder_status ON reminder_messages (status)")
        except Exception:
            pass
    except Exception:
        pass
    db.commit()


def ensure_reminder_reads_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reminder_reads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reminder_id INT NOT NULL,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            read_at DATETIME NOT NULL,
            UNIQUE KEY uq_reminder_read (reminder_id, student_id),
            INDEX idx_read_school (school_id),
            INDEX idx_read_student (student_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # Backfill missing columns for older schemas.
    try:
        def _has_col(name: str) -> bool:
            cur.execute("SHOW COLUMNS FROM reminder_reads LIKE %s", (name,))
            return bool(cur.fetchone())

        if not _has_col("school_id"):
            cur.execute("ALTER TABLE reminder_reads ADD COLUMN school_id INT NOT NULL DEFAULT 0")
        if not _has_col("student_id"):
            cur.execute("ALTER TABLE reminder_reads ADD COLUMN student_id INT NOT NULL DEFAULT 0")
        if not _has_col("read_at"):
            cur.execute("ALTER TABLE reminder_reads ADD COLUMN read_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
        try:
            cur.execute("CREATE INDEX idx_read_school ON reminder_reads (school_id)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX idx_read_student ON reminder_reads (student_id)")
        except Exception:
            pass
    except Exception:
        pass
    db.commit()


def ensure_guardian_push_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guardian_push_subscriptions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            endpoint TEXT NOT NULL,
            p256dh VARCHAR(255) NOT NULL,
            auth VARCHAR(255) NOT NULL,
            user_agent VARCHAR(255) NULL,
            created_at DATETIME NOT NULL,
            last_seen_at DATETIME NULL,
            UNIQUE KEY uq_guardian_push (student_id, p256dh),
            INDEX idx_guardian_push_school (school_id),
            INDEX idx_guardian_push_student (student_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_reconciliation_tables(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_imports (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            filename VARCHAR(255) NOT NULL,
            uploaded_by VARCHAR(150) NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'ready',
            total_rows INT NOT NULL DEFAULT 0,
            matched_rows INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            INDEX idx_recon_import_school (school_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_rows (
            id INT AUTO_INCREMENT PRIMARY KEY,
            import_id INT NOT NULL,
            school_id INT NOT NULL,
            txn_date DATE NULL,
            amount DECIMAL(12,2) NULL,
            reference VARCHAR(128) NULL,
            payer_name VARCHAR(150) NULL,
            phone VARCHAR(64) NULL,
            raw_text TEXT NULL,
            match_status VARCHAR(24) NOT NULL DEFAULT 'pending',
            match_score INT NOT NULL DEFAULT 0,
            matched_payment_id INT NULL,
            matched_at DATETIME NULL,
            notes TEXT NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_recon_import (import_id),
            INDEX idx_recon_school (school_id),
            INDEX idx_recon_match (match_status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_payment_plan_tables(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_plans (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            year INT NULL,
            term TINYINT NULL,
            total_amount DECIMAL(12,2) NOT NULL,
            installments INT NOT NULL DEFAULT 1,
            frequency VARCHAR(24) NOT NULL DEFAULT 'monthly',
            start_date DATE NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'active',
            created_by VARCHAR(150) NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            INDEX idx_plan_school (school_id),
            INDEX idx_plan_student (student_id),
            INDEX idx_plan_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_plan_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            plan_id INT NOT NULL,
            due_date DATE NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            paid_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
            paid_at DATETIME NULL,
            payment_id INT NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_plan_item_plan (plan_id),
            INDEX idx_plan_item_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def ensure_refund_requests_table(db: CMySQLConnection) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS refund_requests (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            method VARCHAR(64) NULL,
            phone VARCHAR(64) NULL,
            reference VARCHAR(128) NULL,
            reason TEXT NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            requested_by VARCHAR(150) NULL,
            requested_at DATETIME NOT NULL,
            approved_by VARCHAR(150) NULL,
            approved_at DATETIME NULL,
            processed_at DATETIME NULL,
            admin_note TEXT NULL,
            payment_id INT NULL,
            credit_before DECIMAL(12,2) NULL,
            credit_after DECIMAL(12,2) NULL,
            INDEX idx_refund_school (school_id),
            INDEX idx_refund_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()
