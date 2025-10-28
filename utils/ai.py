import os
import json
import time
import random
import threading
from typing import List, Dict, Any, Tuple

import requests


DEFAULT_MODEL = os.environ.get("AI_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))

# Simple, in‑process rate limiting across callers in this process
_last_call_ts: float | None = None
_rate_lock = threading.Lock()


def _respect_min_interval():
    """Sleep to respect OPENAI_RPM or OPENAI_MIN_INTERVAL if configured.

    - If OPENAI_RPM is set (requests per minute), derive a minimum interval.
    - Otherwise, if OPENAI_MIN_INTERVAL seconds is set, use that directly.
    """
    global _last_call_ts
    rpm = os.environ.get("OPENAI_RPM")
    min_interval_env = os.environ.get("OPENAI_MIN_INTERVAL")
    min_interval = None
    try:
        if rpm:
            r = float(rpm)
            if r > 0:
                min_interval = max(0.0, 60.0 / r)
        elif min_interval_env:
            min_interval = max(0.0, float(min_interval_env))
    except Exception:
        # Ignore malformed values
        min_interval = None

    if min_interval is None:
        return

    with _rate_lock:
        now = time.time()
        if _last_call_ts is None:
            _last_call_ts = now
            return
        elapsed = now - _last_call_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        _last_call_ts = time.time()


def _parse_retry_after(headers: dict, default_delay: float) -> float:
    """Parse retry delay from headers, preferring standard Retry-After.

    Falls back to OpenAI x-ratelimit-reset-* headers, which may return values
    like '12ms' or '1s'. Returns a delay in seconds.
    """
    ra = headers.get("Retry-After")
    if ra:
        try:
            return max(default_delay, float(ra))
        except Exception:
            pass

    # Try OpenAI specific headers
    for key in (
        "x-ratelimit-reset-requests",
        "x-ratelimit-reset-tokens",
        "x-ratelimit-reset-requests-remaining",
        "x-ratelimit-reset-tokens-remaining",
    ):
        val = headers.get(key)
        if not val:
            continue
        try:
            s = str(val).strip().lower()
            if s.endswith("ms"):
                return max(default_delay, float(s[:-2]) / 1000.0)
            if s.endswith("s"):
                return max(default_delay, float(s[:-1]))
            # Raw seconds
            return max(default_delay, float(s))
        except Exception:
            continue
    return default_delay


def ai_is_configured() -> bool:
    """Return True if an API key is available for AI calls."""
    return bool(os.environ.get("OPENAI_API_KEY") or os.environ.get("AZURE_OPENAI_API_KEY"))


def _openai_chat(messages: List[Dict[str, str]], model: str = DEFAULT_MODEL, temperature: float = 0.2) -> str:
    """Minimal OpenAI Chat API call using requests to avoid hard dependency.

    Supports either the public OpenAI API (OPENAI_API_KEY, OPENAI_BASE_URL optional)
    or Azure OpenAI (AZURE_OPENAI_* envs). Returns assistant content text.
    """
    # Azure OpenAI configuration
    azure_key = os.environ.get("AZURE_OPENAI_API_KEY")
    if azure_key:
        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
        deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", model)
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01")
        url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"
        headers = {"Content-Type": "application/json", "api-key": azure_key}
        payload = {"messages": messages, "temperature": temperature}
    else:
        # Public OpenAI
        key = os.environ.get("OPENAI_API_KEY")
        base = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        url = f"{base}/chat/completions"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
        payload = {"model": model, "messages": messages, "temperature": temperature}

    # Simple retry with exponential backoff for rate limits and transient errors
    max_retries = int(os.environ.get("OPENAI_MAX_RETRIES", "5"))
    backoff_base = float(os.environ.get("OPENAI_BACKOFF_BASE", "0.5"))
    timeout = int(os.environ.get("OPENAI_TIMEOUT", "60"))

    last_err: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            # Honor configured per-call pacing to avoid 429s proactively
            _respect_min_interval()
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
            # 2xx fast-path
            if 200 <= resp.status_code < 300:
                data = resp.json()
                return data.get("choices", [{}])[0].get("message", {}).get("content", "")

            # Handle rate limit and transient server errors with retry
            if resp.status_code in (429, 500, 502, 503, 504):
                # Respect Retry-After or x-ratelimit-reset-* if provided
                delay = _parse_retry_after(resp.headers, backoff_base * (2 ** attempt))
                # Add a little jitter
                delay += random.uniform(0, 0.25)

                if attempt < max_retries:
                    time.sleep(delay)
                    continue

            # Non-retryable or exhausted retries
            resp.raise_for_status()
            # If raise_for_status didn't raise, fall through to parse
            data = resp.json()
            return data.get("choices", [{}])[0].get("message", {}).get("content", "")
        except requests.RequestException as e:
            last_err = e
            if attempt < max_retries:
                delay = backoff_base * (2 ** attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
                continue
            break

    # If we get here, we failed after retries
    if last_err:
        raise last_err
    raise RuntimeError("AI request failed after retries")


def classify_intent(query: str) -> Tuple[str, Dict[str, Any]]:
    """Classify a user's query into a known intent with extracted entities.

    Falls back to simple heuristics if no AI key configured.
    Known intents:
      - student_balance
      - top_debtors
      - analytics_summary
      - generate_reminder
    """
    system = {
        "role": "system",
        "content": (
            "You are an intent classifier for a School Fee Management app. "
            "Return STRICT JSON with keys: intent, entities. "
            "intents: student_balance, top_debtors, analytics_summary, generate_reminder. "
            "Entities may include: student_name, admission_no, count."
        ),
    }
    user = {"role": "user", "content": query}

    if ai_is_configured():
        try:
            out = _openai_chat([system, user])
            # Attempt to find JSON in the output.
            start = out.find("{")
            end = out.rfind("}")
            if start != -1 and end != -1 and end > start:
                obj = json.loads(out[start : end + 1])
                return obj.get("intent", "unknown"), obj.get("entities", {})
        except Exception:
            pass

    q = query.lower()
    # Heuristic fallback
    if any(k in q for k in ["balance", "owing", "due", "fees for"]):
        return "student_balance", {}
    if any(k in q for k in ["top debt", "highest balance", "debtors"]):
        # Extract a small integer if mentioned
        import re

        m = re.search(r"top\s+(\d{1,2})", q)
        cnt = int(m.group(1)) if m else 5
        return "top_debtors", {"count": cnt}
    if any(k in q for k in ["analytics", "summary", "totals", "collection"]):
        return "analytics_summary", {}
    if any(k in q for k in ["remind", "notice", "notify", "compose message"]):
        return "generate_reminder", {}
    return "unknown", {}


def answer_with_ai(context: str, question: str) -> str:
    """General QA using AI on provided context text. If not configured, return a helpful note."""
    if not ai_is_configured():
        return (
            "AI not configured. Set OPENAI_API_KEY or AZURE_OPENAI_API_KEY to enable intelligent answers."
        )
    system = {
        "role": "system",
        "content": (
            "You are a helpful assistant specialized in school fees. "
            "Use the provided context to answer succinctly."
        ),
    }
    messages = [system, {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}]
    try:
        return _openai_chat(messages)
    except Exception as e:
        return f"AI error: {e}"
