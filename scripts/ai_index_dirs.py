import os
import argparse
import json
import pickle
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.neighbors import NearestNeighbors


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "instance" / "ai_user"
OUT_DIR.mkdir(parents=True, exist_ok=True)


INCLUDE_EXTS = {
    ".txt", ".md", ".pdf", ".html", ".csv", ".json", ".py", ".ini", ".cfg",
}

EXCLUDE_DIR_NAMES = {".git", "venv", "__pycache__", "node_modules", ".pytest_cache", ".vscode"}


def discover_files(roots: List[Path]) -> List[Path]:
    files: List[Path] = []
    seen = set()
    for root in roots:
        if not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            # prune excluded directory names
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIR_NAMES]
            for fn in filenames:
                p = Path(dirpath) / fn
                # basic include rules
                if p.suffix.lower() in INCLUDE_EXTS:
                    rp = p.resolve()
                    if rp not in seen:
                        seen.add(rp)
                        files.append(rp)
    return files


def read_text(path: Path) -> str:
    try:
        # naive text read; for pdf/others, rely on plain text layer or skip
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def chunk_by_lines(text: str, max_lines: int = 80, overlap: int = 10) -> List[Tuple[int, str]]:
    lines = text.splitlines()
    chunks: List[Tuple[int, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        start = i
        end = min(i + max_lines, n)
        chunk = "\n".join(lines[start:end])
        chunks.append((start + 1, chunk))
        if end == n:
            break
        i = end - overlap
        if i <= start:
            i = end
    return chunks


def build_corpus(files: List[Path], roots: List[Path]) -> Tuple[List[str], List[Dict]]:
    texts: List[str] = []
    metas: List[Dict] = []
    for p in files:
        # store path relative to first matching root for readability
        rel = None
        for r in roots:
            try:
                rel = str(p.relative_to(r))
                break
            except Exception:
                continue
        rel = rel or str(p)
        content = read_text(p)
        if not content.strip():
            continue
        for start_line, chunk in chunk_by_lines(content):
            texts.append(chunk)
            metas.append({
                "path": rel.replace("\\", "/"),
                "start_line": start_line,
            })
    return texts, metas


def main():
    parser = argparse.ArgumentParser(description="Index user-provided folders into the AI knowledge base")
    parser.add_argument("--paths", type=str, required=True, help="Folders to index; separate multiple with ';'")
    args = parser.parse_args()

    raw = args.paths or ""
    parts = [s.strip() for s in raw.split(";") if s.strip()]
    roots = [Path(p) for p in parts]
    if not roots:
        print("[ai_index_dirs] No valid paths provided.")
        return

    print("[ai_index_dirs] Discovering files...")
    files = discover_files(roots)
    files.sort()
    print(f"[ai_index_dirs] Found {len(files)} files to index")

    print("[ai_index_dirs] Building text corpus (chunking)...")
    texts, metas = build_corpus(files, roots)
    print(f"[ai_index_dirs] Built {len(texts)} chunks")

    if not texts:
        print("[ai_index_dirs] Nothing to index.")
        return

    print("[ai_index_dirs] Loading embedding model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer("all-MiniLM-L6-v2")

    print("[ai_index_dirs] Computing embeddings...")
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True)

    print("[ai_index_dirs] Fitting nearest neighbors index (cosine)...")
    nn = NearestNeighbors(n_neighbors=8, metric="cosine")
    nn.fit(embeddings)

    print("[ai_index_dirs] Saving artifacts...")
    np.save(OUT_DIR / "embeddings.npy", embeddings)
    with open(OUT_DIR / "texts.jsonl", "w", encoding="utf-8") as f:
        for i, t in enumerate(texts):
            rec = {"id": i, "text": t}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    with open(OUT_DIR / "meta.json", "w", encoding="utf-8") as f:
        json.dump(metas, f, ensure_ascii=False, indent=2)
    with open(OUT_DIR / "nn.pkl", "wb") as f:
        pickle.dump(nn, f)

    print(f"[ai_index_dirs] Done. Saved to {OUT_DIR}")


if __name__ == "__main__":
    main()

