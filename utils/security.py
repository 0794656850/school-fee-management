from __future__ import annotations

from typing import Optional
from werkzeug.security import generate_password_hash, check_password_hash


def hash_password(plain: str, method: str = "pbkdf2:sha256", salt_length: int = 16) -> str:
    plain = (plain or "").strip()
    return generate_password_hash(plain, method=method, salt_length=salt_length)


def is_hashed(value: Optional[str]) -> bool:
    if not value:
        return False
    v = str(value)
    # Werkzeug hashes usually start with method prefix like 'pbkdf2:sha256:'
    return v.startswith("pbkdf2:") or v.startswith("scrypt:") or v.startswith("sha256:")


def verify_password(stored_value: str, candidate: str) -> bool:
    """Verify a stored password value which may be hashed or plain text.

    - If stored_value looks hashed, use check_password_hash.
    - Otherwise, fall back to plain string equality.
    """
    if is_hashed(stored_value):
        try:
            return check_password_hash(stored_value, candidate or "")
        except Exception:
            return False
    # Plain text fallback
    return (stored_value or "") == (candidate or "")

