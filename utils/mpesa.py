from __future__ import annotations

import base64
import json
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import RequestException, SSLError, ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout
from flask import current_app, url_for
from utils.settings import get_setting
import os


class DarajaError(Exception):
    pass


def _cfg(key: str, default: str = "") -> str:
    try:
        val = (current_app.config.get(key) or "").strip()
        if val:
            return val
        # Fallback to DB-backed settings
        db_val = get_setting(key, None)
        if db_val is not None and str(db_val).strip():
            return str(db_val).strip()
        return default
    except Exception:
        return default


def _stub_enabled() -> bool:
    try:
        v = os.environ.get("DARAJA_STUB") or os.environ.get("MPESA_STUB") or os.environ.get("FREE_MODE")
        return bool(str(v or "").strip()) and str(v or "").strip().lower() not in {"0", "false", "no"}
    except Exception:
        return False


def _base_url() -> str:
    env = _cfg("DARAJA_ENV", "sandbox").lower()
    return "https://api.safaricom.co.ke" if env == "production" else "https://sandbox.safaricom.co.ke"


def get_access_token(timeout: int = 15) -> str:
    if _stub_enabled():
        return "stub-token"
    key = _cfg("DARAJA_CONSUMER_KEY")
    secret = _cfg("DARAJA_CONSUMER_SECRET")
    if not key or not secret:
        raise DarajaError("Daraja consumer key/secret not configured")
    token_url = f"{_base_url()}/oauth/v1/generate?grant_type=client_credentials"
    auth = (key, secret)
    try:
        r = requests.get(token_url, auth=auth, timeout=timeout)
    except (RequestsTimeout, SSLError, RequestsConnectionError, RequestException) as e:
        err = (
            "Network/SSL error contacting Daraja token endpoint: "
            f"{type(e).__name__}: {e}"
        )
        raise DarajaError(err)
    if r.status_code != 200:
        raise DarajaError(f"Auth failed: {r.status_code} {r.text}")
    try:
        data = r.json()
    except ValueError:
        raise DarajaError("Auth failed: non-JSON response from Daraja")
    token = data.get("access_token") or ""
    if not token:
        raise DarajaError("Auth failed: access_token missing in response")
    return token


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def _password(short_code: str, passkey: str, ts: str) -> str:
    raw = f"{short_code}{passkey}{ts}".encode("utf-8")
    return base64.b64encode(raw).decode("utf-8")


def normalize_msisdn(phone: str) -> str:
    p = (phone or "").strip()
    if p.startswith("+"):
        p = p[1:]
    if p.startswith("0"):
        p = "254" + p[1:]
    if p.startswith("254"):
        return p
    # Fallback: digits only, take last 9 with 254 prefix
    digits = "".join(ch for ch in p if ch.isdigit())
    if len(digits) >= 9:
        return "254" + digits[-9:]
    return p


def stk_push(phone: str, amount: int, account_ref: Optional[str] = None, trans_desc: Optional[str] = None, callback_url: Optional[str] = None, timeout: int = 20) -> Dict[str, Any]:
    if _stub_enabled():
        # Simulate an immediate, successful STK push initiation
        return {
            "ResponseCode": "0",
            "ResponseDescription": "Success. Request accepted for processing",
            "MerchantRequestID": f"MR{int(time.time())}",
            "CheckoutRequestID": f"CR{int(time.time()*1000)}",
            "CustomerMessage": "Success. Request accepted for processing"
        }
    short_code = _cfg("DARAJA_SHORT_CODE")
    passkey = _cfg("DARAJA_PASSKEY")
    if not short_code or not passkey:
        raise DarajaError("Daraja ShortCode/Passkey not configured")

    token = get_access_token()
    ts = _timestamp()
    # Resolve and validate callback URL. Daraja rejects private/localhost URLs.
    try:
        cb = (callback_url or _cfg("DARAJA_CALLBACK_URL") or url_for("mpesa.callback", _external=True) or "").strip()
        # Basic normalization: prefer https when available
        if cb.startswith("http://") and (_cfg("DARAJA_ENV", "sandbox").lower() != "sandbox"):
            cb = cb.replace("http://", "https://", 1)
        from urllib.parse import urlparse
        pr = urlparse(cb)
        host = (pr.hostname or "").lower()
        if (not pr.scheme) or (not host):
            raise DarajaError("Invalid CallBackURL. Set DARAJA_CALLBACK_URL to your public https URL e.g. https://your-domain/mpesa/callback")
        if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
            raise DarajaError("Invalid CallBackURL (localhost). Use a public URL (ngrok/Cloudflare) to /mpesa/callback and set DARAJA_CALLBACK_URL")
    except DarajaError:
        raise
    except Exception:
        raise DarajaError("Failed to resolve CallBackURL. Set DARAJA_CALLBACK_URL to your public https URL")

    payload = {
        "BusinessShortCode": short_code,
        "Password": _password(short_code, passkey, ts),
        "Timestamp": ts,
        "TransactionType": "CustomerPayBillOnline",
        "Amount": int(amount),
        "PartyA": normalize_msisdn(phone),
        "PartyB": short_code,
        "PhoneNumber": normalize_msisdn(phone),
        "CallBackURL": cb,
        "AccountReference": account_ref or _cfg("DARAJA_ACCOUNT_REF", "FMS-PRO"),
        "TransactionDesc": trans_desc or _cfg("DARAJA_TRANSACTION_DESC", "Pro upgrade"),
    }
    url = f"{_base_url()}/mpesa/stkpush/v1/processrequest"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except (RequestsTimeout, SSLError, RequestsConnectionError, RequestException) as e:
        err = (
            "Network/SSL error during STK push: "
            f"{type(e).__name__}: {e}"
        )
        raise DarajaError(err)
    if r.status_code != 200:
        raise DarajaError(f"STK push failed: {r.status_code} {r.text}")
    try:
        return r.json()
    except ValueError:
        raise DarajaError("STK push failed: non-JSON response from Daraja")


def parse_callback_items(items: list[dict]) -> dict:
    out: Dict[str, Any] = {}
    for it in items or []:
        name = it.get("Name")
        val = it.get("Value") or it.get("Value", None)
        if name and "Receipt" in name:
            out["receipt"] = val
        elif name == "Amount":
            out["amount"] = val
        elif name in ("PhoneNumber", "MSISDN"):
            out["phone"] = str(val)
        elif name == "TransactionDate":
            out["transaction_date"] = str(val)
        elif name == "Balance":
            out["balance"] = val
        elif name == "MpesaReceiptNumber":
            out["receipt"] = val
    return out
