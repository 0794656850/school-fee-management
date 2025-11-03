import os
from flask import Blueprint, request, jsonify

try:
    import google.generativeai as genai  # type: ignore
except Exception as e:  # pragma: no cover
    genai = None  # Defer import error until first request


gemini_bp = Blueprint("gemini", __name__)


def _ensure_configured():
    if genai is None:
        raise RuntimeError("google-generativeai package not installed. pip install google-generativeai")
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY not set in environment")
    # Configure once per process (idempotent)
    genai.configure(api_key=api_key)


@gemini_bp.route("/chat", methods=["POST"])
def gemini_chat():
    data = request.get_json(silent=True) or {}
    user_input = (data.get("message") or "").strip()
    if not user_input:
        return jsonify({"error": "Message is required"}), 400

    try:
        _ensure_configured()
        # Allow override via env; default to a modern Gemini model
        model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        if model_name.strip().lower() == "gemini-pro":
            model_name = "gemini-1.5-flash"
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(user_input)
        text = getattr(response, "text", None)
        if not text and hasattr(response, "candidates"):
            try:
                # Fallback parse for older SDK variants
                text = (response.candidates or [{}])[0].get("content", {}).get("parts", [{}])[0].get("text")
            except Exception:
                text = None
        return jsonify({"reply": text or ""})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
