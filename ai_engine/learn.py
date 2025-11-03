import os
import re
import json
import pickle
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

from ai_engine.vector_store import VectorStore, REPO_ROOT, _ensure_out_dir


INCLUDE_EXTS = {
    ".py", ".md", ".txt", ".html", ".jinja", ".jinja2", ".sql", ".ini", ".cfg",
}
EXCLUDE_DIRS = {".git", "venv", "__pycache__", "node_modules", ".pytest_cache", ".vscode", "instance/ai", "instance/ai_user"}


def _should_include(path: Path) -> bool:
    if path.is_dir():
        return False
    rel = str(path.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
    for ex in EXCLUDE_DIRS:
        if ex in rel:
            return False
    return path.suffix.lower() in INCLUDE_EXTS


def discover_files() -> List[Path]:
    files: List[Path] = []
    for root, dirs, filenames in os.walk(REPO_ROOT):
        # prune excluded dirs
        pruned = []
        for d in list(dirs):
            rp = Path(root) / d
            rel = str(rp.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
            if any(ex in rel for ex in EXCLUDE_DIRS):
                continue
            pruned.append(d)
        dirs[:] = pruned
        for fn in filenames:
            p = Path(root) / fn
            if _should_include(p):
                files.append(p)
    return files


def read_text(path: Path) -> str:
    try:
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


def build_corpus(files: List[Path]) -> Tuple[List[str], List[Dict]]:
    texts: List[str] = []
    metas: List[Dict] = []
    for p in files:
        rel = str(p.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
        content = read_text(p)
        if not content.strip():
            continue
        for start_line, chunk in chunk_by_lines(content):
            texts.append(chunk)
            metas.append({"path": rel, "start_line": start_line})
    return texts, metas


def build_knowledge_graph(files: List[Path]) -> Dict:
    """Extract a coarse knowledge graph of modules, routes, models, and relations."""
    graph = {
        "modules": {},  # module -> {functions: [], classes: []}
        "routes": [],   # {module, path, methods, func}
        "models": [],   # {name, table, fields, relations}
        "entities": set(),
        "edges": [],    # (src, rel, dst)
    }

    def add_entity(name: str):
        graph["entities"].add(name)

    # Parse python modules
    for p in files:
        if p.suffix != ".py":
            continue
        mod = str(p.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
        text = read_text(p)
        funcs = re.findall(r"^def\s+([a-zA-Z_][a-zA-Z0-9_]*)\(.*\):", text, flags=re.M)
        classes = re.findall(r"^class\s+([A-Za-z_][A-Za-z0-9_]*)\(.*\):", text, flags=re.M)
        graph["modules"][mod] = {"functions": funcs, "classes": classes}
        for f in funcs:
            add_entity(f"func:{mod}:{f}")
        for c in classes:
            add_entity(f"class:{mod}:{c}")

        # Flask routes via Blueprint.route
        for m in re.finditer(r"@\s*([a-zA-Z_][a-zA-Z0-9_]*)\.route\(\s*([\'\"][^\)\'\"]+[\'\"])\s*(?:,\s*methods=\[([^\]]+)\])?\)", text):
            bp, path_lit, methods = m.groups()
            try:
                route_path = json.loads(path_lit)
            except Exception:
                route_path = path_lit.strip("\"'")
            # next def is handler
            after = text[m.end():]
            func_m = re.search(r"^def\s+([a-zA-Z_][a-zA-Z0-9_]*)\(", after, flags=re.M)
            func_name = func_m.group(1) if func_m else ""
            methods_list: List[str] = []
            if methods:
                methods_list = [t.strip().strip("\"'") for t in methods.split(',')]
            graph["routes"].append({"module": mod, "blueprint": bp, "path": route_path, "methods": methods_list, "func": func_name})
            add_entity(f"route:{route_path}")
            if func_name:
                graph["edges"].append((f"route:{route_path}", "handled_by", f"func:{mod}:{func_name}"))

        # SQLAlchemy models (simple scan)
        if mod.endswith("models.py"):
            for cls_m in re.finditer(r"class\s+([A-Za-z_][A-Za-z0-9_]*)\(db\.Model\):\s+__tablename__\s*=\s*'([^']+)'([\s\S]*?)(?=\nclass\s|\Z)", text, flags=re.M):
                name, table, body = cls_m.groups()
                fields = re.findall(r"\n\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*db\.Column\(([^\)]*)\)", body)
                rels = re.findall(r"db\.relationship\(\s*'([^']+)'\s*,\s*backref='([^']+)'", body)
                graph["models"].append({
                    "name": name,
                    "table": table,
                    "fields": [f"{fname}:{ftype}" for fname, ftype in fields],
                    "relations": [{"target": t, "backref": b} for t, b in rels],
                })
                add_entity(f"model:{name}")
                add_entity(f"table:{table}")
                graph["edges"].append((f"model:{name}", "maps_to", f"table:{table}"))
                for t, _b in rels:
                    graph["edges"].append((f"model:{name}", "relates_to", f"model:{t}"))

    # Serialize entities
    graph["entities"] = sorted(graph["entities"])  # type: ignore
    return graph


def learn() -> Dict[str, int]:
    files = discover_files()
    files.sort()
    texts, metas = build_corpus(files)

    store = VectorStore()
    embeddings = store.build_embeddings(texts)
    nn = store.fit_nn(embeddings)
    store.save(texts, metas, embeddings, nn)

    # Build and save knowledge graph
    kg = build_knowledge_graph(files)
    out = _ensure_out_dir()
    with (out / "knowledge_graph.json").open("w", encoding="utf-8") as f:
        json.dump(kg, f, ensure_ascii=False, indent=2)

    return {"files": len(files), "chunks": len(texts)}


def main():
    stats = learn()
    print(f"[ai_engine.learn] Indexed {stats['files']} files into {stats['chunks']} chunks")


if __name__ == "__main__":
    main()

