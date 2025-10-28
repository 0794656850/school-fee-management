from __future__ import annotations

from typing import Optional


def ensure_ledger_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ledger_entries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT NOT NULL,
            student_id INT NOT NULL,
            ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            entry_type ENUM('debit','credit') NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            ref VARCHAR(64) NULL,
            description VARCHAR(255) NULL,
            link_type VARCHAR(32) NULL,
            link_id BIGINT NULL,
            INDEX idx_school_student (school_id, student_id, ts),
            INDEX idx_link (link_type, link_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    conn.commit()


def add_entry(
    conn,
    school_id: int,
    student_id: int,
    entry_type: str,
    amount: float,
    ref: Optional[str] = None,
    description: Optional[str] = None,
    link_type: Optional[str] = None,
    link_id: Optional[int] = None,
) -> None:
    ensure_ledger_table(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO ledger_entries (school_id, student_id, entry_type, amount, ref, description, link_type, link_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (school_id, student_id, entry_type, amount, ref, description, link_type, link_id),
    )
    conn.commit()

