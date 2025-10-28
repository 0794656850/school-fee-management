import os
import re
import json
import pickle
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np

from sentence_transformers import SentenceTransformer
from sklearn.neighbors import NearestNeighbors


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "instance" / "ai"
OUT_DIR.mkdir(parents=True, exist_ok=True)


INCLUDE_EXTS = {
    ".py", ".md", ".txt", ".html", ".jinja", ".jinja2", ".sql", ".ini", ".cfg",
}

EXCLUDE_DIRS = {".git", "venv", "__pycache__", "node_modules", ".pytest_cache", ".vscode", "instance/ai"}


def should_include(path: Path) -> bool:
    if path.is_dir():
        return False
    rel = path.relative_to(REPO_ROOT)
    parts = set(rel.parts)
    # quick path exclusion
    for ex in EXCLUDE_DIRS:
        if ex in str(rel).replace("\\", "/"):
            return False
    return path.suffix.lower() in INCLUDE_EXTS


def discover_files() -> List[Path]:
    files: List[Path] = []
    for root, dirs, filenames in os.walk(REPO_ROOT):
        # prune excluded dirs
        pruned = []
        for d in list(dirs):
            rp = Path(root) / d
            rel = rp.resolve().relative_to(REPO_ROOT)
            if any(ex in str(rel).replace("\\", "/") for ex in EXCLUDE_DIRS):
                continue
            pruned.append(d)
        dirs[:] = pruned
        for fn in filenames:
            p = Path(root) / fn
            if should_include(p):
                files.append(p)
    return files


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def chunk_by_lines(text: str, max_lines: int = 80, overlap: int = 10) -> List[Tuple[int, str]]:
    """Split text into overlapping chunks by lines. Returns list of (start_line, chunk_text)."""
    lines = text.splitlines()
    chunks: List[Tuple[int, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        start = i
        end = min(i + max_lines, n)
        chunk = "\n".join(lines[start:end])
        chunks.append((start + 1, chunk))  # 1-based line number
        if end == n:
            break
        i = end - overlap
        if i <= start:
            i = end
    return chunks


def build_corpus(files: List[Path]) -> Tuple[List[str], List[Dict]]:
    texts: List[str] = []
    metas: List[Dict] = []
    for p in files:
        rel = str(p.relative_to(REPO_ROOT)).replace("\\", "/")
        content = read_text(p)
        if not content.strip():
            continue
        for start_line, chunk in chunk_by_lines(content):
            texts.append(chunk)
            metas.append({
                "path": rel,
                "start_line": start_line,
            })
    return texts, metas


def main():
    print("[ai_index] Discovering files...")
    files = discover_files()
    files.sort()
    print(f"[ai_index] Found {len(files)} files to index")

    print("[ai_index] Building text corpus (chunking)...")
    texts, metas = build_corpus(files)
    print(f"[ai_index] Built {len(texts)} chunks")

    print("[ai_index] Loading embedding model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    print("[ai_index] Computing embeddings...")
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True)

    print("[ai_index] Fitting nearest neighbors index (cosine)...")
    nn = NearestNeighbors(n_neighbors=8, metric="cosine")
    nn.fit(embeddings)

    # Persist artifacts
    print("[ai_index] Saving artifacts...")
    np.save(OUT_DIR / "embeddings.npy", embeddings)
    with open(OUT_DIR / "texts.jsonl", "w", encoding="utf-8") as f:
        for i, t in enumerate(texts):
            rec = {"id": i, "text": t}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    with open(OUT_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(metas, f, ensure_ascii=False, indent=2)
    with open(OUT_DIR / "nn.pkl", "wb") as f:
        pickle.dump(nn, f)

    print(f"[ai_index] Done. Saved to {OUT_DIR}")


if __name__ == "__main__":
    main()

