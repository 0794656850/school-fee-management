#!/usr/bin/env python3
"""
License Key Generator for CS Fee Management System
Author: Akillius Vincent
Project: CS Fee Management System
"""

import hmac
import json
import base64
import hashlib
from datetime import datetime

# Change this SECRET once and keep it private.
# Read from environment to avoid hardcoding secrets in the repo.
import os
LICENSE_SECRET = os.environ.get("LICENSE_SECRET", "")

if not LICENSE_SECRET:
    # Prompt the operator to enter a secret at runtime to avoid committing it
    try:
        LICENSE_SECRET = input("Enter LICENSE_SECRET (not saved to disk): ").strip()
    except Exception:
        LICENSE_SECRET = ""
    if not LICENSE_SECRET:
        raise SystemExit("LICENSE_SECRET is required. Set env or input interactively.")


def b64u_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def generate_license(school_uid, plan_code="PREMIUM", features=None, expires_at="2026-12-31T23:59:59Z"):
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
    token = f"{b64u_encode(raw)}.{b64u_encode(sig)}"
    return token


if __name__ == "__main__":
    print("🧾 CS Fee Management System License Generator")
    print("-------------------------------------------")
    school_uid = input("Enter School UID (use the school's code from DB): ").strip()
    plan = (input("Enter plan (FREE or PREMIUM): ").strip() or "PREMIUM").upper()
    expiry = (input("Enter expiry date (YYYY-MM-DD, default 2026-12-31): ").strip() or "2026-12-31")

    features = [
        "reports_advanced",
        "bulk_messaging",
        "ai_assistant",
        "multi_term",
        "templates_custom",
    ]

    expires_at = f"{expiry}T23:59:59Z"
    key = generate_license(school_uid, plan, features, expires_at)

    print("\n✅ LICENSE KEY GENERATED:")
    print(key)
    print("\nPaste this key in the Billing & Upgrade page (enter it as an M-Pesa reference) to activate the license.")
