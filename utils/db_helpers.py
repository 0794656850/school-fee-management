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
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()
