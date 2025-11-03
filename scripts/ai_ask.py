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


def call_vertex(prompt: str, model: str | None = None) -> str:
    # Lazy import to avoid hard dependency if not used
    from google.oauth2 import service_account
    import vertexai
    from vertexai.generative_models import GenerativeModel

    sa = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or str(Path("service_account.json").resolve())
    creds = None
    if os.path.exists(sa):
        try:
            creds = service_account.Credentials.from_service_account_file(sa)
        except Exception:
            creds = None

    project = os.environ.get("VERTEX_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or ""
    location = os.environ.get("VERTEX_LOCATION", "us-central1")
    vertexai.init(project=project, location=location, credentials=creds)

    model_name = model or os.getenv("VERTEX_GEMINI_MODEL") or os.getenv("GEMINI_MODEL") or "gemini-1.5-flash"
    if model_name.strip().lower() == "gemini-pro":
        model_name = "gemini-1.5-flash"
    gen = GenerativeModel(model_name)
    resp = gen.generate_content(prompt)
    return getattr(resp, "text", None) or (resp.candidates[0].content.parts[0].text if getattr(resp, "candidates", None) else "")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ask questions against the project knowledge base")
    parser.add_argument("question", type=str, nargs="+", help="Your question")
    parser.add_argument("--k", type=int, default=6, help="Top-k contexts")
    parser.add_argument("--model", type=str, default=None, help="Vertex Gemini model (default env VERTEX_GEMINI_MODEL)")
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
        answer = call_vertex(prompt, model=args.model)
        print("\n=== Answer ===\n")
        print(answer)
    except Exception as e:
        print("\n[warning] Could not call Vertex AI:", str(e))
        print("\nTop retrieved context (configure Vertex credentials to enable answers):\n")
        print(context)


if __name__ == "__main__":
    main()
