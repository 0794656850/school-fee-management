from __future__ import annotations

from typing import List

import requests
from requests.exceptions import RequestException


NGROK_TUNNELS_ENDPOINT = "http://127.0.0.1:4040/api/tunnels"
NGROK_ERROR_MSG = "Ngrok not detected - start ngrok first."


class NgrokError(Exception):
    """Raised when the ngrok tunnel information cannot be resolved."""


def _normalize_public_url(url: str) -> str:
    return url.rstrip("/") if url else ""


def detect_ngrok_https_url(timeout: float = 2.0) -> str:
    """Return the first https ngrok public URL or raise NgrokError."""
    try:
        resp = requests.get(NGROK_TUNNELS_ENDPOINT, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except (RequestException, ValueError):
        raise NgrokError(NGROK_ERROR_MSG)
    tunnels: List[dict] = data.get("tunnels") or []
    for tunnel in tunnels:
        public_url = (tunnel.get("public_url") or "").strip()
        proto = (tunnel.get("proto") or "").strip().lower()
        if public_url and public_url.lower().startswith("https://") and proto == "https":
            return _normalize_public_url(public_url)
    # Fallback: accept any https tunnel even if proto key missing
    for tunnel in tunnels:
        public_url = (tunnel.get("public_url") or "").strip()
        if public_url.lower().startswith("https://"):
            return _normalize_public_url(public_url)
    raise NgrokError(NGROK_ERROR_MSG)
