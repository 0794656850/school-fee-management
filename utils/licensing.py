import hmac
import json
import base64
import hashlib
import os
from datetime import datetime

# Read secret key from environment or fallback
LICENSE_SECRET = os.getenv("LICENSE_SECRET", "my_super_secret_license_key_2025")


def _b64u_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b'=').decode()


def _b64u_decode(s: str) -> bytes:
    s = s or ""
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())


def generate_key(school_uid: str, plan_code: str = "PREMIUM", features=None, expires_at: str = "2026-12-31T23:59:59Z") -> str:
    if features is None:
        features = [
            "reports_advanced",
            "bulk_messaging",
            "ai_assistant",
            "multi_term",
            "templates_custom",
        ]
    payload = {
        "school_uid": str(school_uid),
        "plan_code": plan_code,
        "features": features,
        "expires_at": expires_at,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(LICENSE_SECRET.encode(), raw, hashlib.sha256).digest()
    return f"{_b64u_encode(raw)}.{_b64u_encode(sig)}"


def verify_key(token: str):
    try:
        raw_b64, sig_b64 = (token or "").split(".")
        raw = _b64u_decode(raw_b64)
        sig = _b64u_decode(sig_b64)
        expect = hmac.new(LICENSE_SECRET.encode(), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expect):
            return False, {"error": "BAD_SIGNATURE"}
        payload = json.loads(raw.decode())
        return True, payload
    except Exception as e:
        return False, {"error": str(e)}

