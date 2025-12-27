from __future__ import annotations

from datetime import datetime


def ensure_payment_sources_tables(db) -> None:
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_sources (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            source_type VARCHAR(64) NOT NULL,
            source_ref VARCHAR(64) NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            amount DECIMAL(12,2) NULL,
            raw_text TEXT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            INDEX idx_payment_sources_school (school_id),
            INDEX idx_payment_sources_student (student_id),
            INDEX idx_payment_sources_status (status),
            INDEX idx_payment_sources_ref (source_ref)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payment_status_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            receipt_id INT NULL,
            status VARCHAR(32) NOT NULL,
            actor VARCHAR(150) NULL,
            note TEXT NULL,
            created_at DATETIME NOT NULL,
            INDEX idx_payment_history_school (school_id),
            INDEX idx_payment_history_student (student_id),
            INDEX idx_payment_history_receipt (receipt_id),
            INDEX idx_payment_history_status (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    db.commit()


def record_payment_source(
    *,
    db,
    school_id: int,
    student_id: int,
    source_type: str,
    source_ref: str | None,
    status: str,
    amount: float | None,
    raw_text: str | None,
) -> int | None:
    if not (db and school_id and student_id and source_type):
        return None
    ensure_payment_sources_tables(db)
    cur = db.cursor()
    now = datetime.utcnow()
    cur.execute(
        """
        INSERT INTO payment_sources
            (school_id, student_id, source_type, source_ref, status, amount, raw_text, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            school_id,
            student_id,
            source_type,
            source_ref,
            status,
            amount,
            raw_text,
            now,
            now,
        ),
    )
    return cur.lastrowid


def update_payment_source_status(*, db, source_ref: str, status: str) -> None:
    if not (db and source_ref and status):
        return
    ensure_payment_sources_tables(db)
    cur = db.cursor()
    cur.execute(
        "UPDATE payment_sources SET status=%s, updated_at=%s WHERE source_ref=%s",
        (status, datetime.utcnow(), source_ref),
    )


def log_payment_status(
    *,
    db,
    school_id: int,
    student_id: int,
    receipt_id: int | None,
    status: str,
    actor: str | None,
    note: str | None,
) -> None:
    if not (db and school_id and student_id and status):
        return
    ensure_payment_sources_tables(db)
    cur = db.cursor()
    cur.execute(
        """
        INSERT INTO payment_status_history
            (school_id, student_id, receipt_id, status, actor, note, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            school_id,
            student_id,
            receipt_id,
            status,
            actor,
            note,
            datetime.utcnow(),
        ),
    )
