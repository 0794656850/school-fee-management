from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests
from flask import current_app, url_for
from requests.exceptions import RequestException, SSLError, ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout
from utils.settings import get_setting


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


def _resolve_callback_url(callback_url: Optional[str] = None) -> str:
    cb = (callback_url or _cfg("DARAJA_CALLBACK_URL") or "").strip()
    if not cb:
        raise DarajaError(
            "DARAJA_CALLBACK_URL is not set. Provide your public HTTPS callback (e.g. https://your-ngrok-url/mpesa/callback)."
        )
    pr = urlparse(cb)
    scheme = (pr.scheme or "").lower()
    host = (pr.hostname or "").lower()
    if scheme != "https" or not host:
        raise DarajaError("CallBackURL must be a public HTTPS endpoint ending in /mpesa/callback.")
    if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
        raise DarajaError("CallBackURL must not target a localhost or private host.")
    return cb


def _resolve_b2c_urls() -> tuple[str, str]:
    result = (current_app.config.get("DARAJA_B2C_RESULT_URL") or "").strip()
    timeout = (current_app.config.get("DARAJA_B2C_TIMEOUT_URL") or "").strip()
    if not result or not timeout:
        try:
            if not result:
                result = url_for("mpesa.b2c_result", _external=True)
            if not timeout:
                timeout = url_for("mpesa.b2c_timeout", _external=True)
        except RuntimeError:
            # Fallback to empty; validation will raise later if missing
            pass
    return result, timeout


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
    cb = _resolve_callback_url(callback_url)

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


def b2c_payment(phone: str, amount: float, remarks: Optional[str] = None, occasion: Optional[str] = None, command: Optional[str] = None, timeout: int = 20) -> Dict[str, Any]:
    if _stub_enabled():
        return {
            "ResponseCode": "0",
            "ResponseDescription": "Success. Payment request accepted",
            "ConversationID": f"C{int(time.time())}",
            "OriginatorConversationID": f"OC{int(time.time())}",
            "TransactionID": f"T{int(time.time()*1000)}",
        }
    initiator = _cfg("DARAJA_B2C_INITIATOR_NAME")
    security = _cfg("DARAJA_B2C_SECURITY_CREDENTIAL")
    short_code = _cfg("DARAJA_B2C_SHORT_CODE") or _cfg("DARAJA_SHORT_CODE")
    if not initiator or not security:
        raise DarajaError("Daraja B2C initiator and security credential are not configured")
    if not short_code:
        raise DarajaError("Daraja B2C short code is not configured")

    amount_int = int(round(amount))
    if amount_int <= 0:
        raise DarajaError("Amount must be greater than zero")

    result_url, timeout_url = _resolve_b2c_urls()
    if not result_url or not timeout_url:
        raise DarajaError("Daraja B2C result/timeout URLs are not configured")

    token = get_access_token()
    payload = {
        "InitiatorName": initiator,
        "SecurityCredential": security,
        "CommandID": command or _cfg("DARAJA_B2C_COMMAND", "BusinessPayment"),
        "Amount": amount_int,
        "PartyA": short_code,
        "PartyB": normalize_msisdn(phone),
        "Remarks": remarks or occasion or _cfg("DARAJA_B2C_OCCASION", "Credit refund"),
        "QueueTimeOutURL": timeout_url,
        "ResultURL": result_url,
        "Occasion": occasion or _cfg("DARAJA_B2C_OCCASION", "Credit refund"),
    }
    url = f"{_base_url()}/mpesa/b2c/v1/paymentrequest"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
    except (RequestsTimeout, SSLError, RequestsConnectionError, RequestException) as e:
        err = (
            "Network/SSL error during Daraja B2C payout: "
            f"{type(e).__name__}: {e}"
        )
        raise DarajaError(err)
    if r.status_code != 200:
        raise DarajaError(f"Daraja B2C payout failed: {r.status_code} {r.text}")
    try:
        return r.json()
    except ValueError:
        raise DarajaError("Daraja B2C payout failed: non-JSON response from Daraja")


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
