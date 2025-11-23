from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ai_engine.vector_store import VectorStore


def _format_context(hits: List[Dict]) -> str:
    parts: List[str] = []
    for h in hits:
        meta = h.get("meta", {}) or {}
        path = meta.get("path")
        start = meta.get("start_line")
        parts.append(f"[Source: {path}:{start}]\n{h.get('text','')}")
    return "\n\n".join(parts)


def retrieve_context(query: str, k: int = 6) -> List[Dict[str, Any]]:
    vs = VectorStore()
    return vs.query(query, k=k)


def answer_query(query: str, model: Optional[str] = None) -> Dict[str, Any]:
    """RAG-style answer using context + system LLM.

    Returns a dict: {"answer": str, "context": [...], "provider": str}
    """
    hits = retrieve_context(query, k=6)
    context_text = _format_context(hits)
    # Prefer existing utils.ai pipeline for provider selection and keys
    try:
        from utils.ai import ai_is_configured, _openai_chat, ai_provider
    except Exception:  # pragma: no cover
        ai_is_configured = lambda: False  # type: ignore
        _openai_chat = None  # type: ignore
        ai_provider = lambda: "none"  # type: ignore

    provider = "none"
    try:
        provider = ai_provider()
    except Exception:
        provider = "none"

    if not ai_is_configured() or _openai_chat is None:
        # Try local transformers fallback via utils.ai if available
        try:
            from utils.ai import _local_llm_answer  # type: ignore
            if hits and _local_llm_answer:
                ans = _local_llm_answer(query, context_text)
                if ans and ans.strip():
                    return {"answer": ans, "context": hits, "provider": provider}
        except Exception:
            pass
        return {
            "answer": ("AI not configured. Top retrieved context:\n\n" + context_text) if hits else "AI not configured and no index available.",
            "context": hits,
            "provider": provider,
        }

    system = {
        "role": "system",
        "content": (
            "You are the SmartEduPay Fee Management system assistant. "
            "Answer ONLY using the provided context. If unknown, say you do not know."
        ),
    }
    user = {"role": "user", "content": f"Context:\n{context_text}\n\nQuestion: {query}"}
    try:
        answer = _openai_chat([system, user], model=model or None, temperature=0.2)
    except Exception as e:
        answer = f"AI error: {e}"
    return {"answer": answer, "context": hits, "provider": provider}


# --- Optional action routing (safe defaults) ---
def list_students_with_pending_balance(school_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return students whose balance indicates unpaid fees.

    Tries multiple schemas: columns may be `balance` or `fee_balance`.
    """
    try:
        from utils.settings import _db as _get_conn
    except Exception:
        return []
    try:
        db = _get_conn()
    except Exception:
        return []
    try:
        cur = db.cursor(dictionary=True)
        # Determine balance column
        cur.execute("SHOW COLUMNS FROM students LIKE 'balance'")
        has_balance = bool(cur.fetchone())
        bal_col = "balance" if has_balance else "fee_balance"
        if school_id:
            q = f"SELECT id, name, admission_no AS regNo, class_name, {bal_col} AS balance FROM students WHERE {bal_col} > 0 AND school_id=%s ORDER BY {bal_col} DESC"
            cur.execute(q, (school_id,))
        else:
            q = f"SELECT id, name, admission_no AS regNo, class_name, {bal_col} AS balance FROM students WHERE {bal_col} > 0 ORDER BY {bal_col} DESC"
            cur.execute(q)
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        try:
            db.close()
        except Exception:
            pass


def monthly_revenue_report(year: Optional[int] = None, school_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Compute revenue per month from `payments` table, supporting schema variations."""
    try:
        from utils.settings import _db as _get_conn
    except Exception:
        return []
    try:
        db = _get_conn()
    except Exception:
        return []
    try:
        cur = db.cursor(dictionary=True)
        # Try to detect column names
        amount_expr = "COALESCE(amount, amount_paid)"
        date_col = "payment_date"
        # If payment_date not present, try created_at
        try:
            cur.execute("SHOW COLUMNS FROM payments LIKE 'payment_date'")
            if not cur.fetchone():
                date_col = "created_at"
        except Exception:
            pass
        where = []
        params: List[Any] = []
        if year:
            where.append(f"YEAR({date_col})=%s")
            params.append(year)
        if school_id:
            try:
                cur.execute("SHOW COLUMNS FROM payments LIKE 'school_id'")
                if cur.fetchone():
                    where.append("school_id=%s")
                    params.append(school_id)
            except Exception:
                pass
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT YEAR({date_col}) AS year, MONTH({date_col}) AS month,
                   SUM({amount_expr}) AS total_amount, COUNT(*) AS tx_count
            FROM payments
            {where_sql}
            GROUP BY YEAR({date_col}), MONTH({date_col})
            ORDER BY YEAR({date_col}), MONTH({date_col})
            """,
            tuple(params),
        )
        return cur.fetchall() or []
    except Exception:
        return []
    finally:
        try:
            db.close()
        except Exception:
            pass


def handle_query(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry for /ai/query requests.

    Supports two modes:
    - RAG question answering (default)
    - Optional action execution for specific intents when `allow_actions` is true
    """
    question = (payload or {}).get("question") or ""
    allow_actions = bool((payload or {}).get("allow_actions"))
    model = (payload or {}).get("model")
    school_id = (payload or {}).get("school_id")

    # Simple intent routing
    qlow = question.lower()
    if allow_actions and ("pending" in qlow and "student" in qlow):
        data = list_students_with_pending_balance(school_id=school_id)
        return {
            "answer": f"Found {len(data)} students with pending balances.",
            "data": data,
            "intent": "list_pending_students",
        }
    if allow_actions and ("monthly" in qlow and ("revenue" in qlow or "payments" in qlow)):
        data = monthly_revenue_report(school_id=school_id)
        return {
            "answer": "Generated monthly revenue summary.",
            "data": data,
            "intent": "monthly_revenue",
        }

    # Default to RAG answer
    return answer_query(question, model=model)

