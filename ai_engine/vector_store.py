import json
import os
import pickle
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
from sklearn.neighbors import NearestNeighbors

try:
    from sentence_transformers import SentenceTransformer
except Exception as _e:  # pragma: no cover
    SentenceTransformer = None  # type: ignore


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "instance" / "ai"


def _ensure_out_dir() -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUT_DIR


class VectorStore:
    """Lightweight vector store for project knowledge.

    Artifacts are compatible with `scripts/ai_index.py` and utils.ai RAG loader:
      - embeddings.npy
      - texts.jsonl (id, text)
      - meta.json (list of {path, start_line})
      - nn.pkl (sklearn NearestNeighbors)
    """

    def __init__(self, out_dir: Optional[Path] = None):
        self.out_dir = out_dir or _ensure_out_dir()
        self._embeddings: Optional[np.ndarray] = None
        self._texts: Optional[List[str]] = None
        self._metas: Optional[List[Dict]] = None
        self._nn: Optional[NearestNeighbors] = None
        self._embed_model = None

    def _load_embed_model(self):
        if self._embed_model is None:
            if SentenceTransformer is None:
                raise RuntimeError("sentence-transformers not installed. Add to requirements.txt")
            self._embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        return self._embed_model

    def load(self) -> bool:
        """Load artifacts if present."""
        texts_path = self.out_dir / "texts.jsonl"
        meta_path = self.out_dir / "meta.json"
        emb_path = self.out_dir / "embeddings.npy"
        nn_path = self.out_dir / "nn.pkl"
        if not (texts_path.exists() and meta_path.exists() and emb_path.exists() and nn_path.exists()):
            return False
        texts: List[str] = []
        with texts_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                texts.append(rec.get("text", ""))
        with meta_path.open("r", encoding="utf-8") as f:
            metas = json.load(f)
        try:
            embeddings = np.load(emb_path)
        except Exception:
            embeddings = None
        try:
            with nn_path.open("rb") as f:
                nn = pickle.load(f)
        except Exception:
            nn = None
        self._texts, self._metas, self._embeddings, self._nn = texts, metas, embeddings, nn
        return bool(self._texts and self._metas and self._embeddings is not None and self._nn is not None)

    def save(self, texts: List[str], metas: List[Dict], embeddings: np.ndarray, nn: NearestNeighbors) -> None:
        out = _ensure_out_dir()
        np.save(out / "embeddings.npy", embeddings)
        with (out / "texts.jsonl").open("w", encoding="utf-8") as f:
            for i, t in enumerate(texts):
                f.write(json.dumps({"id": i, "text": t}, ensure_ascii=False) + "\n")
        with (out / "meta.json").open("w", encoding="utf-8") as f:
            json.dump(metas, f, ensure_ascii=False, indent=2)
        with (out / "nn.pkl").open("wb") as f:
            pickle.dump(nn, f)
        # cache in-memory
        self._texts, self._metas, self._embeddings, self._nn = texts, metas, embeddings, nn

    def build_embeddings(self, texts: List[str]) -> np.ndarray:
        model = self._load_embed_model()
        return model.encode(texts, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

    def fit_nn(self, embeddings: np.ndarray, n_neighbors: int = 8) -> NearestNeighbors:
        nn = NearestNeighbors(n_neighbors=min(n_neighbors, len(embeddings)), metric="cosine")
        nn.fit(embeddings)
        return nn

    def query(self, question: str, k: int = 6) -> List[Dict]:
        """Retrieve top-k chunks with metadata.

        Returns list of dicts with keys: text, meta, distance
        """
        if not self.load():
            return []
        if not self._texts or not self._metas or self._nn is None:
            return []
        model = self._load_embed_model()
        q = model.encode([question], convert_to_numpy=True, normalize_embeddings=True)
        try:
            distances, idxs = self._nn.kneighbors(q, n_neighbors=min(k, len(self._texts)))
        except Exception:
            return []
        out: List[Dict] = []
        for d, i in zip(distances[0].tolist(), idxs[0].tolist()):
            out.append({
                "text": self._texts[i],
                "meta": self._metas[i],
                "distance": float(d),
            })
        return out

