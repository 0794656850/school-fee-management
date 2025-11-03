AI Assistant (Project-Aware)

This adds a lightweight Retrieval-Augmented Generation (RAG) assistant that answers questions using the contents of this repository (code, templates, docs, configs).

Features
- Indexes relevant files into embeddings with `sentence-transformers`.
- Fast retrieval via scikit-learn cosine nearest neighbors (cross-platform).
- Uses Vertex AI (Gemini) for grounded answers by default.
- Cites file paths and line numbers for traceability.

Setup
1) Install Python dependencies (in your venv):
   - `pip install -r requirements.txt`

2) Configure Vertex AI (recommended)
   - Create a service account with Vertex AI permissions.
   - Place it at `service_account.json` in the repo root, or set `GOOGLE_APPLICATION_CREDENTIALS` to its path.
   - Set `VERTEX_PROJECT_ID` and optional `VERTEX_LOCATION` (default `us-central1`).

3) Build the index:
   - `python scripts/ai_index.py`
   - Outputs artifacts to `instance/ai/`.

4) Ask a question:
   - `python scripts/ai_ask.py "How does M-Pesa STK push work here?"`
   - Optional: `--k 8` to retrieve more chunks, `--model gemini-1.5-flash` to override model.

Notes
- The index includes .py, .md, .txt, .html/.jinja templates, .sql, .ini/.cfg by default. It skips `venv`, `.git`, caches, and `instance/ai`.
- If your project changes, re-run `python scripts/ai_index.py` to refresh the index.
- To use a different embedding model, change `all-MiniLM-L6-v2` in both scripts.
- If you want a local model, you can adapt `scripts/ai_ask.py` to call your provider; the retrieval remains the same.

Optional Fine-Tuning
- You can log Q&A pairs from your admin/support flows and build a supervised fine-tuning dataset later.
- Start with a JSONL format like: `{ "messages": [{"role":"system","content":"..."},{"role":"user","content":"..."},{"role":"assistant","content":"..."}] }` and follow your provider’s fine-tune docs.

Teach It Your System
- Put concise, authoritative product docs in `docs/` so they are indexed. A starter file `docs/SYSTEM_OVERVIEW.md` summarizes branding, navigation, endpoints, and capabilities.
- After updating documentation, rebuild the index: `python scripts/ai_index.py`.
- In-app, visit `/ai` and ask questions like “What does Credit Ops do?” or “Where do I manage invoices?”
