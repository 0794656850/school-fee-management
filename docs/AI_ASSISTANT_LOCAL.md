Local AI Assistant Fallback (No Vertex)

Overview
- The in-app assistant can answer questions about your system using a local model when Vertex/Gemini is not configured.
- It uses your repository index (RAG) for grounded answers.

Requirements
- Install dependencies (already in requirements.txt):
  - transformers, torch, sentencepiece, safetensors
- Build the knowledge index once (downloads embeddings):
  - `python scripts/ai_index.py`

Optional Environment Variables
- `LOCAL_LLM_MODEL` (default: TinyLlama/TinyLlama-1.1B-Chat-v1.0)
- `LOCAL_LLM_MAX_NEW_TOKENS` (default: 256)
- `LOCAL_LLM_TEMPERATURE` (default: 0.5)

How it works
- When cloud AI isn’t configured, the backend retrieves top-k project chunks and prompts a local transformers model to generate an answer constrained to that context.
- If the local model isn’t available, it returns the top retrieved context so you can still find the relevant files.

Delete Chat
- On `/ai`, select a chat, click Delete. The backend deletes the chat and its messages (foreign key cascade).

Troubleshooting
- If downloads fail, ensure network access for the first run to fetch model weights.
- For slow CPUs, keep TinyLlama and reduce tokens. If you have CUDA, it will auto-use the GPU.
