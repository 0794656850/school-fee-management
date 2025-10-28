import os
import json
import pickle
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.neighbors import NearestNeighbors


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "instance" / "ai"


def load_index():
    with open(OUT_DIR / "meta.json", "r", encoding="utf-8") as f:
        metas = json.load(f)
    texts = []
    with open(OUT_DIR / "texts.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            texts.append(rec["text"])
    embeddings = np.load(OUT_DIR / "embeddings.npy")
    with open(OUT_DIR / "nn.pkl", "rb") as f:
        nn: NearestNeighbors = pickle.load(f)
    return texts, metas, embeddings, nn


def retrieve(query: str, model: SentenceTransformer, texts, metas, nn: NearestNeighbors, k: int = 6):
    q_emb = model.encode([query], convert_to_numpy=True, normalize_embeddings=True)
    dists, idxs = nn.kneighbors(q_emb, n_neighbors=min(k, len(texts)))
    idxs = idxs[0].tolist()
    dists = dists[0].tolist()
    results = []
    for i, d in zip(idxs, dists):
        results.append({
            "text": texts[i],
            "meta": metas[i],
            "distance": float(d),
        })
    return results


SYSTEM_PROMPT = (
    "You are a helpful assistant for a school fee management system. "
    "Answer clearly and precisely using the provided context. "
    "If the answer is not in context, say you don’t know. "
    "Cite file paths and start line numbers from context where relevant."
)


def format_context(snippets: List[Dict]) -> str:
    out = []
    for s in snippets:
        path = s["meta"]["path"]
        start = s["meta"]["start_line"]
        out.append(f"[Source: {path}:{start}]\n{s['text']}")
    return "\n\n".join(out)


def call_openai(prompt: str, model: str = None) -> str:
    # Lazy import to avoid hard dependency if not used
    try:
        from openai import OpenAI
        import openai
    except Exception as e:
        raise RuntimeError("openai package not installed. Add 'openai' to requirements.txt") from e

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set in environment.")
    max_retries = int(os.getenv("OPENAI_MAX_RETRIES", "5"))
    timeout = float(os.getenv("OPENAI_TIMEOUT", "60"))
    backoff_base = float(os.getenv("OPENAI_BACKOFF_BASE", "0.5"))
    client = OpenAI(api_key=api_key, max_retries=max_retries, timeout=timeout)
    model = model or os.getenv("OPENAI_MODEL", os.getenv("AI_MODEL", "gpt-4o-mini"))

    last_err = None
    for attempt in range(max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                import time as _t, random as _r
                # Respect Retry-After on 429 if available
                delay = 0.5 * (2 ** attempt) + _r.uniform(0, 0.5)
                try:
                    # Handle OpenAI v1 exceptions with response headers
                    if isinstance(e, getattr(openai, "RateLimitError", tuple())) or (
                        hasattr(e, "status_code") and getattr(e, "status_code", None) == 429
                    ) or (
                        hasattr(e, "response") and getattr(getattr(e, "response", None), "status_code", None) == 429
                    ):
                        resp = getattr(e, "response", None)
                        headers = getattr(resp, "headers", {}) or {}
                        ra = headers.get("Retry-After") or headers.get("retry-after")
                        if ra:
                            try:
                                delay = max(backoff_base, float(ra))
                            except Exception:
                                pass
                except Exception:
                    pass
                _t.sleep(delay)
                continue
            break
    raise last_err if last_err else RuntimeError("OpenAI call failed")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ask questions against the project knowledge base")
    parser.add_argument("question", type=str, nargs="+", help="Your question")
    parser.add_argument("--k", type=int, default=6, help="Top-k contexts")
    parser.add_argument("--model", type=str, default=None, help="OpenAI model (default env AI_MODEL or gpt-4o-mini)")
    args = parser.parse_args()

    question = " ".join(args.question)

    texts, metas, embeddings, nn = load_index()
    embed_model = SentenceTransformer("all-MiniLM-L6-v2")
    hits = retrieve(question, embed_model, texts, metas, nn, k=args.k)

    context = format_context(hits)
    prompt = (
        "Context:\n" + context +
        "\n\nQuestion: " + question +
        "\n\nInstructions: Answer using only the context above. "
        "If unknown or outside scope, say you don’t know."
    )

    try:
        answer = call_openai(prompt, model=args.model)
        print("\n=== Answer ===\n")
        print(answer)
    except Exception as e:
        print("\n[warning] Could not call OpenAI:", str(e))
        print("\nTop retrieved context (set OPENAI_API_KEY to enable answers):\n")
        print(context)


if __name__ == "__main__":
    main()
