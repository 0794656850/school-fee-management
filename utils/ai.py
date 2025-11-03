import os
import json
import time
import random
import threading
from typing import List, Dict, Any, Tuple

import requests
import numpy as np
import pickle
from pathlib import Path


DEFAULT_MODEL = os.environ.get(
    "AI_MODEL",
    os.environ.get("GEMINI_MODEL", os.environ.get("VERTEX_GEMINI_MODEL", "gemini-1.5-flash")),
)

# Optional local LLM fallback (Hugging Face transformers)
try:  # optional imports; only used when Vertex/OpenAI are not configured
    from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline  # type: ignore
except Exception:  # pragma: no cover
    AutoTokenizer = None  # type: ignore
    AutoModelForCausalLM = None  # type: ignore
    pipeline = None  # type: ignore
try:
    import torch  # type: ignore
except Exception:  # pragma: no cover
    torch = None  # type: ignore

LOCAL_LLM_MODEL = os.environ.get("LOCAL_LLM_MODEL", "TinyLlama/TinyLlama-1.1B-Chat-v1.0")
LOCAL_LLM_MAX_NEW_TOKENS = int(os.environ.get("LOCAL_LLM_MAX_NEW_TOKENS", "256"))
LOCAL_LLM_TEMPERATURE = float(os.environ.get("LOCAL_LLM_TEMPERATURE", "0.5"))

# Simple, in‑process rate limiting across callers in this process
_last_call_ts: float | None = None
_rate_lock = threading.Lock()


def _respect_min_interval():
    """Sleep to respect AI_RPM or AI_MIN_INTERVAL if configured.

    - If AI_RPM is set (requests per minute), derive a minimum interval.
    - Otherwise, if AI_MIN_INTERVAL seconds is set, use that directly.
    """
    global _last_call_ts
    rpm = os.environ.get("AI_RPM")
    min_interval_env = os.environ.get("AI_MIN_INTERVAL")
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


def _global_rate_gate() -> None:
    """Cross-process rate gating based on MySQL when AI_RPM is set.

    - Ensures we do not exceed AI_RPM requests per minute across all app processes.
    - Uses a short-lived MySQL advisory lock to serialize the check-insert logic.
    - Falls back silently if DB is unavailable or env not set.
    """
    rpm_env = os.environ.get("AI_RPM")
    try:
        rpm = float(rpm_env) if rpm_env else None
    except Exception:
        rpm = None
    if not rpm or rpm <= 0:
        return

    # Lazy import DB helper to avoid circular imports at module import
    try:
        from utils.settings import _db as _get_conn
    except Exception:
        return

    try:
        db = _get_conn()
    except Exception:
        return

    try:
        cur = db.cursor()
        # Create table once
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_rate_events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_at DATETIME NOT NULL,
                KEY idx_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        db.commit()

        # Acquire advisory lock to serialize rate checks
        try:
            cur.execute("SELECT GET_LOCK('ai_rate_gate', 10)")
            _ = cur.fetchone()
        except Exception:
            # If we cannot acquire a lock, still attempt a best-effort gate
            pass

        # Prune old entries (>90s)
        try:
            cur.execute("DELETE FROM ai_rate_events WHERE created_at < (NOW() - INTERVAL 90 SECOND)")
            db.commit()
        except Exception:
            pass

        # Loop until we can proceed under the RPM window
        while True:
            cur.execute("SELECT COUNT(*) FROM ai_rate_events WHERE created_at > (NOW() - INTERVAL 60 SECOND)")
            count = int(cur.fetchone()[0])
            if count < rpm:
                # Record this request and proceed
                cur.execute("INSERT INTO ai_rate_events(created_at) VALUES (NOW())")
                db.commit()
                break
            # Compute minimal wait until oldest event drops out of the 60s window
            try:
                cur.execute(
                    "SELECT TIMESTAMPDIFF(SECOND, MIN(created_at), NOW()) AS age FROM ai_rate_events WHERE created_at > (NOW() - INTERVAL 60 SECOND)"
                )
                row = cur.fetchone()
                age = int(row[0]) if row and row[0] is not None else 0
                sleep_for = max(1.0, 60.0 - age) + random.uniform(0, 0.25)
            except Exception:
                sleep_for = 1.0 + random.uniform(0, 0.25)
            time.sleep(min(sleep_for, 5.0))
    finally:
        try:
            # Release advisory lock if held
            try:
                cur.execute("SELECT RELEASE_LOCK('ai_rate_gate')")
            except Exception:
                pass
            db.close()
        except Exception:
            pass


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


def _get_setting_db(key: str) -> str | None:
    """Best-effort read of a setting from DB without creating import cycles.

    Falls back silently on any failure (DB unavailable, import error, etc.).
    """
    try:
        # Lazy import to avoid circular imports at module import time
        from utils.settings import get_setting as _get
        val = _get(key, None)
        if val is None:
            return None
        s = str(val).strip()
        return s or None
    except Exception:
        return None


def _read_service_account_project(path: str | None) -> str | None:
    """Best‑effort read of project_id from a service account JSON file."""
    if not path:
        return None
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            s = (data.get("project_id") or "").strip()
            return s or None
    except Exception:
        return None
    return None


def _resolve_gcp_project() -> str | None:
    """Resolve a GCP project ID from env, service account, or ADC.

    Order of precedence:
      1) VERTEX_PROJECT_ID env
      2) GOOGLE_CLOUD_PROJECT env
      3) project_id from service_account.json or GOOGLE_APPLICATIONS_CREDENTIALS
      4) google.auth.default() inferred project
    """
    # 1/2: environment
    try:
        env_project = (
            os.environ.get("VERTEX_PROJECT_ID")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
        )
        if env_project:
            s = str(env_project).strip()
            if s:
                return s
    except Exception:
        # Continue to other resolution strategies
        pass

    # 3: service account JSON on disk
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path and os.path.exists("service_account.json"):
        sa_path = "service_account.json"
    p = _read_service_account_project(sa_path)
    if p:
        return p

    # 4: ADC via google.auth.default()
    try:  # pragma: no cover - optional dep
        import google.auth  # type: ignore

        _, proj = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])  # type: ignore
        if proj:
            return str(proj)
    except Exception:
        pass
    return None


def _has_vertex_config() -> bool:
    """Best-effort check whether Vertex AI is likely configured.

    Signals True if any of the following look present:
      - `VERTEX_PROJECT_ID` env
      - `GOOGLE_CLOUD_PROJECT` env
      - `GOOGLE_APPLICATION_CREDENTIALS` file exists
      - `service_account.json` exists in repo root
    This is heuristic by design; actual init may still fail at runtime.
    """
    try:
        if os.environ.get("VERTEX_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT"):
            return True
        sa = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if sa and os.path.exists(sa):
            return True
        if os.path.exists("service_account.json"):
            return True
        # As a last resort, if ADC can produce a project, consider it configured
        try:  # pragma: no cover - optional dep
            import google.auth  # type: ignore

            _creds, proj = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])  # type: ignore
            if proj:
                return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def ai_is_configured() -> bool:
    """Return True if Vertex AI appears configured."""
    return _has_vertex_config()


def ai_provider() -> str:
    """Return which AI provider is configured: 'vertex' or 'none'."""
    try:
        return "vertex" if _has_vertex_config() else "none"
    except Exception:
        return "none"


def _vertex_generate(messages: List[Dict[str, str]]) -> str:
    """Generate text using Vertex AI Gemini models.

    This function initializes Vertex AI using either ADC or a service
    account pointed to by `GOOGLE_APPLICATION_CREDENTIALS` (or a local
    `service_account.json`), then calls a Gemini model to produce output.
    """
    try:
        # Lazy imports so environments without Vertex deps still work
        from google.oauth2 import service_account  # type: ignore
        import vertexai  # type: ignore
        from vertexai.generative_models import GenerativeModel  # type: ignore
    except Exception as e:  # pragma: no cover - optional dependency
        raise RuntimeError("Vertex AI libraries not installed. Add google-cloud-aiplatform.") from e

    # Project and region
    project = _resolve_gcp_project()
    location = os.environ.get("VERTEX_LOCATION", "us-central1")

    # Credentials: prefer explicit service account path; otherwise ADC
    creds = None
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path and os.path.exists("service_account.json"):
        sa_path = "service_account.json"
    if sa_path and os.path.exists(sa_path):
        try:
            creds = service_account.Credentials.from_service_account_file(sa_path)
            # If project not explicitly provided, try load from file
            if not project:
                project = _read_service_account_project(sa_path) or project
        except Exception:
            creds = None

    # Init Vertex AI (credentials may be None -> ADC)
    try:
        if not project:
            raise RuntimeError(
                "Missing GCP project. Set VERTEX_PROJECT_ID or GOOGLE_CLOUD_PROJECT, "
                "or ensure service_account.json has project_id, or configure ADC via gcloud."
            )
        vertexai.init(project=project, location=location, credentials=creds)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Vertex AI: {e}")

    g_model = (
        os.environ.get("VERTEX_GEMINI_MODEL")
        or os.environ.get("GEMINI_MODEL")
        or "gemini-1.5-flash"
    )
    if str(g_model).strip().lower() == "gemini-pro":
        g_model = "gemini-1.5-flash"

    # Flatten chat messages into a single prompt (stateless usage)
    parts: List[str] = []
    for m in messages or []:
        role = (m.get("role") or "user").capitalize()
        parts.append(f"{role}: {(m.get('content') or '')}\n")
    prompt = "".join(parts).strip()

    try:
        model_obj = GenerativeModel(g_model)
        resp = model_obj.generate_content(prompt)
        text = getattr(resp, "text", None)
        if text:
            return text
        # Fallback path for older SDKs
        try:
            return (
                (resp.candidates or [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            ) or ""
        except Exception:
            return ""
    except Exception as e:
        raise RuntimeError(f"Vertex AI error: {e}")


def _openai_chat(messages: List[Dict[str, str]], model: str = DEFAULT_MODEL, temperature: float = 0.2) -> str:
    """Unified chat: prefer Vertex/Gemini, else Azure/OpenAI REST.

    - If Vertex AI appears configured, call Gemini via Vertex first.
    - Else if GOOGLE_API_KEY present, call Gemini via google-generativeai.
    - Else call Azure OpenAI or OpenAI Chat Completions.
    """
    # Normalize model: callers may pass None/"" to mean default
    model = model or DEFAULT_MODEL

    # Vertex AI (Gemini) preferred if available
    if _has_vertex_config():
        try:
            return _vertex_generate(messages)
        except Exception:
            # fall through to other providers
            pass

    # Google Gemini first if configured (env or DB)
    gkey = os.environ.get("GOOGLE_API_KEY") or _get_setting_db("GOOGLE_API_KEY")
    if gkey:
        try:
            import google.generativeai as genai  # type: ignore
            genai.configure(api_key=gkey)
            # Prefer modern Gemini defaults; fall back to legacy name
            g_model = os.environ.get("GEMINI_MODEL") or _get_setting_db("GEMINI_MODEL") or "gemini-1.5-flash"
            if str(g_model).strip().lower() == "gemini-pro":
                g_model = "gemini-1.5-flash"
            parts: List[str] = []
            for m in messages or []:
                role = (m.get("role") or "user").capitalize()
                parts.append(f"{role}: {(m.get('content') or '')}\n")
            prompt = "".join(parts).strip()
            model_obj = genai.GenerativeModel(g_model)
            resp = model_obj.generate_content(prompt)
            text = getattr(resp, "text", None)
            if text:
                return text
            try:
                return (resp.candidates or [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "") or ""
            except Exception:
                return ""
        except Exception:
            # On any Gemini failure, continue to other providers
            pass
    # Azure OpenAI configuration (env first, then DB)
    azure_key = os.environ.get("AZURE_OPENAI_API_KEY") or _get_setting_db("AZURE_OPENAI_API_KEY")
    if azure_key:
        endpoint = (os.environ.get("AZURE_OPENAI_ENDPOINT") or _get_setting_db("AZURE_OPENAI_ENDPOINT") or "").rstrip("/")
        deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT") or _get_setting_db("AZURE_OPENAI_DEPLOYMENT") or model
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION") or _get_setting_db("AZURE_OPENAI_API_VERSION") or "2024-06-01"
        url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"
        headers = {"Content-Type": "application/json", "api-key": azure_key}
        payload = {"messages": messages, "temperature": temperature}
    else:
        # Public OpenAI (env first, then DB)
        key = os.environ.get("OPENAI_API_KEY") or _get_setting_db("OPENAI_API_KEY")
        base = (os.environ.get("OPENAI_BASE_URL") or _get_setting_db("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
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
            # Honor configured pacing to avoid 429s proactively (in-process + cross-process)
            _respect_min_interval()
            _global_rate_gate()
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
            "AI not configured. Configure Vertex AI (service account + VERTEX_PROJECT_ID) to enable intelligent answers."
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


# ---- Lightweight local RAG over project index ----
_RAG_CACHE = {
    'loaded': False,
    'ok': False,
    'texts': None,
    'metas': None,
    'nn': None,
    'embed_model': None,
    # Optional secondary user-provided index under instance/ai_user
    'user_ok': False,
    'user_texts': None,
    'user_metas': None,
    'user_nn': None,
}


def _rag_try_load() -> bool:
    if _RAG_CACHE['loaded']:
        return bool(_RAG_CACHE['ok'])
    _RAG_CACHE['loaded'] = True
    try:
        repo_root = Path(__file__).resolve().parents[1]
        out_dir = repo_root / 'instance' / 'ai'
        texts_path = out_dir / 'texts.jsonl'
        meta_path = out_dir / 'meta.json'
        nn_path = out_dir / 'nn.pkl'
        if not (texts_path.exists() and meta_path.exists() and nn_path.exists()):
            _RAG_CACHE['ok'] = False
            return False
        texts: list[str] = []
        with open(texts_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    texts.append(rec.get('text', ''))
                except Exception:
                    continue
        with open(meta_path, 'r', encoding='utf-8') as f:
            metas = json.load(f)
        with open(nn_path, 'rb') as f:
            nn = pickle.load(f)
        # Lazy import to avoid cold start cost if unused
        from sentence_transformers import SentenceTransformer
        embed_model = SentenceTransformer('all-MiniLM-L6-v2')
        _RAG_CACHE.update({'ok': True, 'texts': texts, 'metas': metas, 'nn': nn, 'embed_model': embed_model})

        # Optionally load user-provided index
        user_dir = repo_root / 'instance' / 'ai_user'
        u_texts = user_dir / 'texts.jsonl'
        u_meta = user_dir / 'meta.json'
        u_nn = user_dir / 'nn.pkl'
        if u_texts.exists() and u_meta.exists() and u_nn.exists():
            try:
                texts2: list[str] = []
                with open(u_texts, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                            texts2.append(rec.get('text', ''))
                        except Exception:
                            continue
                with open(u_meta, 'r', encoding='utf-8') as f:
                    metas2 = json.load(f)
                with open(u_nn, 'rb') as f:
                    nn2 = pickle.load(f)
                _RAG_CACHE.update({'user_ok': True, 'user_texts': texts2, 'user_metas': metas2, 'user_nn': nn2})
            except Exception:
                _RAG_CACHE['user_ok'] = False
        return True
    except Exception:
        _RAG_CACHE['ok'] = False
        return False


def _rag_retrieve(question: str, k: int = 6) -> list[dict]:
    if not _rag_try_load():
        return []
    texts = _RAG_CACHE['texts']
    metas = _RAG_CACHE['metas']
    nn = _RAG_CACHE['nn']
    model = _RAG_CACHE['embed_model']
    all_hits: list[dict] = []
    try:
        q_emb = model.encode([question], convert_to_numpy=True, normalize_embeddings=True)
        if texts and nn is not None:
            dists, idxs = nn.kneighbors(q_emb, n_neighbors=min(k, len(texts)))
            idxs = idxs[0].tolist(); dists = dists[0].tolist()
            for i, d in zip(idxs, dists):
                all_hits.append({'text': texts[i], 'meta': metas[i], 'distance': float(d)})
        if _RAG_CACHE.get('user_ok') and _RAG_CACHE.get('user_texts') and _RAG_CACHE.get('user_nn') is not None:
            u_texts = _RAG_CACHE['user_texts']
            u_metas = _RAG_CACHE['user_metas']
            u_nn = _RAG_CACHE['user_nn']
            d2, i2 = u_nn.kneighbors(q_emb, n_neighbors=min(k, len(u_texts)))
            i2 = i2[0].tolist(); d2 = d2[0].tolist()
            for i, d in zip(i2, d2):
                all_hits.append({'text': u_texts[i], 'meta': u_metas[i], 'distance': float(d)})
        # sort by distance and keep top k
        all_hits.sort(key=lambda h: h.get('distance', 1.0))
        return all_hits[:k]
    except Exception:
        return []


def rag_status() -> dict:
    """Return a brief status of available knowledge indexes and chunk counts."""
    # Force a load attempt without raising
    try:
        _rag_try_load()
    except Exception:
        pass
    return {
        'project_index': {
            'available': bool(_RAG_CACHE.get('ok')),
            'chunks': (len(_RAG_CACHE.get('texts') or []) if _RAG_CACHE.get('ok') else 0),
        },
        'user_index': {
            'available': bool(_RAG_CACHE.get('user_ok')),
            'chunks': (len(_RAG_CACHE.get('user_texts') or []) if _RAG_CACHE.get('user_ok') else 0),
        },
    }


def answer_with_ai_rag(question: str) -> str:
    """RAG-augmented answer using local index if available; otherwise, general totals fallback."""
    hits = _rag_retrieve(question, k=6)
    if not hits:
        # No index; provide generic guidance
        return answer_with_ai("", question)
    ctx_parts = []
    for h in hits:
        path = h.get('meta', {}).get('path')
        start = h.get('meta', {}).get('start_line')
        ctx_parts.append(f"[Source: {path}:{start}]\n{h.get('text','')}")
    context = "\n\n".join(ctx_parts)
    if not ai_is_configured():
        # Try local LLM fallback (transformers). If unavailable, return context.
        try:
            ans = _local_llm_answer(question, context)
            if ans and ans.strip():
                return ans
        except Exception:
            pass
        return "AI not configured. Top context:\n\n" + context
    system = {
        'role': 'system',
        'content': (
            'You are a helpful assistant for a school fee management system. '
            'Answer ONLY using the provided context. If unknown, say you do not know.'
        ),
    }
    user = {'role': 'user', 'content': f"Context:\n{context}\n\nQuestion: {question}"}
    try:
        return _openai_chat([system, user])
    except Exception as e:
        return f"AI error: {e}"


# ---- General chat (ChatGPT-like) ----
def chat_anything(history: List[Dict[str, str]], temperature: float = 0.7, model: str = DEFAULT_MODEL) -> str:
    """General-purpose chat using provided history (list of {role, content}).

    - With configured AI, calls Vertex AI for a response.
    - Without AI configured, falls back to showing top local context (RAG) for the latest user question.
    """
    if not ai_is_configured():
        # Fallback: RAG + local LLM if available
        question = ""
        try:
            for m in reversed(history or []):
                if (m or {}).get('role') == 'user':
                    question = (m or {}).get('content') or ''
                    break
            if not question and history:
                question = (history[-1] or {}).get('content') or ''
        except Exception:
            question = ""

        hits = _rag_retrieve(question or "", k=6)
        ctx_parts: List[str] = []
        for h in hits or []:
            meta = h.get('meta', {}) or {}
            path = meta.get('path')
            start = meta.get('start_line')
            ctx_parts.append(f"[Source: {path}:{start}]\n{h.get('text','')}")
        context = "\n\n".join(ctx_parts)
        # Try local generation; if fails, return context
        try:
            ans = _local_llm_answer(question or "", context)
            if ans and ans.strip():
                return ans
        except Exception:
            pass
        return ("AI not configured. Top context:\n\n" + context) if context else (
            "AI not configured. Configure Vertex AI (service account + VERTEX_PROJECT_ID) to enable chat.")

    # Normalize model: callers may pass None/"" to mean default
    model = model or DEFAULT_MODEL

    system_prompt = (
        "You are a helpful AI assistant for a school fee management system. "
        "You can also answer general real-world questions clearly and concisely. "
        "When questions involve data from the system, be precise and avoid guessing. "
        "If you are missing information, ask for clarification briefly."
    )
    messages = [{"role": "system", "content": system_prompt}] + history
    try:
        return _openai_chat(messages, model=model, temperature=temperature)
    except Exception as e:
        return f"AI error: {e}"


def _openai_chat_stream(messages: List[Dict[str, str]], model: str = DEFAULT_MODEL, temperature: float = 0.7):
    """Yield assistant text deltas, preferring Vertex/Gemini streaming.

    Falls back to google-generativeai, then Azure/OpenAI streaming API.
    """
    # Normalize model: callers may pass None/"" to mean default
    model = model or DEFAULT_MODEL

    # Vertex AI streaming if configured
    if _has_vertex_config():
        try:
            import vertexai  # type: ignore
            from vertexai.generative_models import GenerativeModel  # type: ignore
            from google.oauth2 import service_account  # type: ignore

            project = _resolve_gcp_project()
            location = os.environ.get("VERTEX_LOCATION", "us-central1")
            creds = None
            sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if not sa_path and os.path.exists("service_account.json"):
                sa_path = "service_account.json"
            if sa_path and os.path.exists(sa_path):
                try:
                    creds = service_account.Credentials.from_service_account_file(sa_path)
                except Exception:
                    creds = None
            if not project:
                raise RuntimeError(
                    "Missing GCP project. Set VERTEX_PROJECT_ID or GOOGLE_CLOUD_PROJECT, "
                    "or ensure service_account.json has project_id, or configure ADC via gcloud."
                )
            vertexai.init(project=project, location=location, credentials=creds)

            g_model = (
                os.environ.get("VERTEX_GEMINI_MODEL")
                or os.environ.get("GEMINI_MODEL")
                or "gemini-1.5-flash"
            )
            if str(g_model).strip().lower() == "gemini-pro":
                g_model = "gemini-1.5-flash"
            parts: List[str] = []
            for m in messages or []:
                role = (m.get("role") or "user").capitalize()
                parts.append(f"{role}: {(m.get('content') or '')}\n")
            prompt = "".join(parts).strip()

            model_obj = GenerativeModel(g_model)
            resp = model_obj.generate_content(prompt, stream=True)
            for chunk in resp:
                try:
                    delta = getattr(chunk, "text", None)
                    if delta:
                        yield delta
                except Exception:
                    continue
            return
        except Exception:
            # fall through to other providers
            pass

    # Google Gemini streaming if configured
    gkey = os.environ.get("GOOGLE_API_KEY") or _get_setting_db("GOOGLE_API_KEY")
    if gkey:
        try:
            import google.generativeai as genai  # type: ignore
            genai.configure(api_key=gkey)
            # Prefer modern Gemini defaults; fall back to legacy name
            g_model = os.environ.get("GEMINI_MODEL") or _get_setting_db("GEMINI_MODEL") or "gemini-1.5-flash"
            if str(g_model).strip().lower() == "gemini-pro":
                g_model = "gemini-1.5-flash"
            parts: List[str] = []
            for m in messages or []:
                role = (m.get("role") or "user").capitalize()
                parts.append(f"{role}: {(m.get('content') or '')}\n")
            prompt = "".join(parts).strip()
            model_obj = genai.GenerativeModel(g_model)
            resp = model_obj.generate_content(prompt, stream=True)
            for chunk in resp:
                try:
                    delta = getattr(chunk, "text", None)
                    if delta:
                        yield delta
                except Exception:
                    continue
            return
        except Exception:
            # fall through to OpenAI/Azure streaming
            pass
    # Azure OpenAI configuration (env first, then DB)
    azure_key = os.environ.get("AZURE_OPENAI_API_KEY") or _get_setting_db("AZURE_OPENAI_API_KEY")
    if azure_key:
        endpoint = (os.environ.get("AZURE_OPENAI_ENDPOINT") or _get_setting_db("AZURE_OPENAI_ENDPOINT") or "").rstrip("/")
        deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT") or _get_setting_db("AZURE_OPENAI_DEPLOYMENT") or model
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION") or _get_setting_db("AZURE_OPENAI_API_VERSION") or "2024-06-01"
        url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"
        headers = {"Content-Type": "application/json", "api-key": azure_key}
        payload = {"messages": messages, "temperature": temperature, "stream": True}
    else:
        key = os.environ.get("OPENAI_API_KEY") or _get_setting_db("OPENAI_API_KEY")
        base = (os.environ.get("OPENAI_BASE_URL") or _get_setting_db("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        url = f"{base}/chat/completions"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
        payload = {"model": model, "messages": messages, "temperature": temperature, "stream": True}

    timeout = int(os.environ.get("OPENAI_TIMEOUT", "60"))

    # Pacing gates (in-process + cross-process) before opening stream
    _respect_min_interval()
    _global_rate_gate()

    with requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout, stream=True) as resp:
        resp.raise_for_status()
        for raw in resp.iter_lines(decode_unicode=True):
            if not raw:
                continue
            # Expect lines like: "data: {json}" or "data: [DONE]"
            line = raw.strip()
            if line.startswith('data:'):
                data = line[len('data:'):].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                    for choice in obj.get('choices', []):
                        delta = ((choice.get('delta') or {}).get('content'))
                        if delta:
                            yield delta
                except Exception:
                    # Best-effort: if parsing fails, skip this chunk
                    continue


def chat_anything_stream(history: List[Dict[str, str]], temperature: float = 0.7, model: str = DEFAULT_MODEL):
    """Stream ChatGPT-like response deltas for the given history.

    Yields plain text deltas suitable for SSE.
    """
    if not ai_is_configured():
        # Provide a single-chunk local fallback if possible
        try:
            question = ""
            for m in reversed(history or []):
                if (m or {}).get('role') == 'user':
                    question = (m or {}).get('content') or ''
                    break
            hits = _rag_retrieve(question or "", k=6)
            ctx_parts = []
            for h in hits or []:
                meta = h.get('meta', {}) or {}
                path = meta.get('path')
                start = meta.get('start_line')
                ctx_parts.append(f"[Source: {path}:{start}]\n{h.get('text','')}")
            context = "\n\n".join(ctx_parts)
            ans = _local_llm_answer(question or "", context)
            if ans and ans.strip():
                yield ans
                return
        except Exception:
            pass
        yield "AI not configured. Build the index (scripts/ai_index.py) or enable Vertex AI."
        return
    system_prompt = (
        "You are a helpful AI assistant for a school fee management system. "
        "You can also answer general real-world questions clearly and concisely. "
        "When questions involve data from the system, be precise and avoid guessing. "
        "If you are missing information, ask for clarification briefly."
    )
    messages = [{"role": "system", "content": system_prompt}] + history
    for chunk in _openai_chat_stream(messages, model=model, temperature=temperature):
        yield chunk


# ---- Vertex-only overrides ----
def _openai_chat(messages: List[Dict[str, str]], model: str = DEFAULT_MODEL, temperature: float = 0.2) -> str:  # type: ignore[override]
    """Vertex-only implementation (kept name for compatibility)."""
    try:
        _respect_min_interval()
        _global_rate_gate()
    except Exception:
        pass
    return _vertex_generate(messages)


def _openai_chat_stream(messages: List[Dict[str, str]], model: str = DEFAULT_MODEL, temperature: float = 0.7):  # type: ignore[override]
    """Vertex-only streaming implementation (kept name for compatibility)."""
    try:
        _respect_min_interval()
        _global_rate_gate()
    except Exception:
        pass

    try:
        import vertexai  # type: ignore
        from vertexai.generative_models import GenerativeModel  # type: ignore
        from google.oauth2 import service_account  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Vertex AI libraries not installed. Add google-cloud-aiplatform.") from e

    project = os.environ.get("VERTEX_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("VERTEX_LOCATION", "us-central1")
    creds = None
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path and os.path.exists("service_account.json"):
        sa_path = "service_account.json"
    if sa_path and os.path.exists(sa_path):
        try:
            creds = service_account.Credentials.from_service_account_file(sa_path)
        except Exception:
            creds = None
    if not project:
        raise RuntimeError(
            "Missing GCP project. Set VERTEX_PROJECT_ID or GOOGLE_CLOUD_PROJECT, "
            "or ensure service_account.json has project_id, or configure ADC via gcloud."
        )
    vertexai.init(project=project, location=location, credentials=creds)

    g_model = (
        os.environ.get("VERTEX_GEMINI_MODEL")
        or os.environ.get("GEMINI_MODEL")
        or "gemini-1.5-flash"
    )
    if str(g_model).strip().lower() == "gemini-pro":
        g_model = "gemini-1.5-flash"
    parts: List[str] = []
    for m in messages or []:
        role = (m.get("role") or "user").capitalize()
        parts.append(f"{role}: {(m.get('content') or '')}\n")
    prompt = "".join(parts).strip()

    model_obj = GenerativeModel(g_model)
    resp = model_obj.generate_content(prompt, stream=True)
    for chunk in resp:
        try:
            delta = getattr(chunk, "text", None)
            if delta:
                yield delta
        except Exception:
            continue


# ---- Local HF generation fallback ----
_LOCAL_PIPELINE = None


def _local_llm_answer(question: str, context: str) -> str:
    """Generate an answer using a local transformers model, grounded by context.

    If transformers/torch are not installed, raises or returns empty string.
    """
    global _LOCAL_PIPELINE
    if AutoTokenizer is None or AutoModelForCausalLM is None or pipeline is None:
        return ""
    # Lazy init pipeline
    if _LOCAL_PIPELINE is None:
        device_index = -1
        try:
            if torch is not None and hasattr(torch, "cuda") and torch.cuda.is_available():
                device_index = 0
        except Exception:
            device_index = -1
        tok = AutoTokenizer.from_pretrained(LOCAL_LLM_MODEL, use_fast=True, trust_remote_code=True)
        mdl = AutoModelForCausalLM.from_pretrained(LOCAL_LLM_MODEL, trust_remote_code=True)
        _LOCAL_PIPELINE = pipeline("text-generation", model=mdl, tokenizer=tok, device=device_index)

    system = (
        "You are a helpful assistant for a school fee management system. "
        "Answer ONLY using the provided context. If unknown, say you do not know."
    )
    prompt = (
        f"System: {system}\n"
        f"Context:\n{context}\n\n"
        f"User: {question}\nAssistant:"
    )
    try:
        out = _LOCAL_PIPELINE(
            prompt,
            max_new_tokens=LOCAL_LLM_MAX_NEW_TOKENS,
            do_sample=True,
            temperature=LOCAL_LLM_TEMPERATURE,
            top_p=0.9,
            pad_token_id=_LOCAL_PIPELINE.tokenizer.eos_token_id,
            eos_token_id=_LOCAL_PIPELINE.tokenizer.eos_token_id,
            num_return_sequences=1,
        )
        text = out[0]["generated_text"]
        # Return only the assistant continuation
        return (text.split("Assistant:")[-1] or "").strip()
    except Exception:
        return ""
