# Local AI Assistant (Runs on Your PC)

This is a minimal, offline-friendly assistant you can run locally with open models via Hugging Face `transformers`.

## 1) Install Python 3.11+
- Download from: https://www.python.org/downloads/
- Verify in CMD/PowerShell:

```
python --version
```

## 2) Create a virtual environment (recommended)
```
python -m venv .venv
.\.venv\Scripts\activate
```

## 3) Install required libraries
```
pip install --upgrade pip
pip install torch transformers sentencepiece safetensors
```

Notes:
- `torch` is the core tensor library (CPU-only is fine for small models).
- `transformers` loads open models.
- `sentencepiece` enables tokenization for many models.
- `safetensors` avoids pickle-based model formats and speeds up loading.

## 4) Choose a small, free model
For CPU-only PCs, start small to keep it responsive:
- TinyLlama: `TinyLlama/TinyLlama-1.1B-Chat-v1.0` (good starter)
- Phi-2: `microsoft/phi-2` (may require license acceptance on Hugging Face)
- OPT 1.3B: `facebook/opt-1.3b` (basic, not chat-tuned)

The first run will download weights automatically. For truly offline use, run once while online.

## 5) Run the assistant
From repo root (or this folder):
```
python local_ai_assistant\run_local_assistant.py --model TinyLlama/TinyLlama-1.1B-Chat-v1.0
```
Common flags:
- `--max-new-tokens 256` control response length
- `--temperature 0.7` sampling randomness
- `--top-p 0.9` nucleus sampling

Exit the chat with `exit` or `quit`.

## 6) Tips and alternatives
- If you have a GPU + CUDA, the script will auto-detect and use it.
- Larger models (e.g., 7B) need more RAM/VRAM; start small.
- Alternative stack: GPT4All app or Python SDK with GGUF models for very easy CPU inference.

---

Troubleshooting
- SSL/cert issues on first model download: update `pip` and `certifi`.
- Slow responses on CPU: reduce `--max-new-tokens`, choose a smaller model, or try GPU.
- Some models require `trust_remote_code=True`; the script falls back to enable it if needed.
