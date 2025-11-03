"""
Simple Vertex AI initialization and Gemini text generation using a service account.

Usage:
  python scripts/vertex_gemini_text.py [optional prompt]

Environment (optional):
  GOOGLE_APPLICATION_CREDENTIALS  Path to service_account.json
  VERTEX_PROJECT_ID               GCP project ID (defaults to JSON's project_id)
  VERTEX_LOCATION                 Region (defaults to 'us-central1')
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional

from google.oauth2 import service_account
from google.cloud import aiplatform

# The Vertex AI generative models live under the aiplatform package as `vertexai`
import vertexai
from vertexai.generative_models import GenerativeModel


def _default_sa_path() -> str:
    return os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", os.path.abspath("service_account.json"))


def _load_project_from_json(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("project_id")
    except Exception:
        return None


def main() -> int:
    prompt = " ".join(sys.argv[1:]).strip() or "Write a short, friendly greeting."

    sa_path = _default_sa_path()
    if not os.path.exists(sa_path):
        print(f"Service account file not found: {sa_path}", file=sys.stderr)
        print("Set GOOGLE_APPLICATION_CREDENTIALS or place service_account.json in repo root.", file=sys.stderr)
        return 2

    creds = service_account.Credentials.from_service_account_file(sa_path)

    project = os.environ.get("VERTEX_PROJECT_ID") or _load_project_from_json(sa_path) or "MyAIProject1"
    location = os.environ.get("VERTEX_LOCATION", "us-central1")

    # Initialize both high-level (vertexai) and low-level (aiplatform) clients
    vertexai.init(project=project, location=location, credentials=creds)
    aiplatform.init(project=project, location=location, credentials=creds)
    print("Vertex AI initialized successfully!")

    # Choose a Gemini model available in Vertex AI
    # Common choices: "gemini-1.5-flash", "gemini-1.5-pro"
    model_name_candidates = [
        "gemini-1.5-flash",
        "gemini-1.5-pro",
        "gemini-1.0-pro",
    ]

    last_err: Optional[Exception] = None
    for name in model_name_candidates:
        try:
            model = GenerativeModel(name)
            response = model.generate_content(prompt)
            text = getattr(response, "text", None) or (response.candidates[0].content.parts[0].text if getattr(response, "candidates", None) else "")
            print("\n--- Gemini Response ---\n")
            print(text.strip())
            return 0
        except Exception as e:
            last_err = e
            continue

    print("Failed to generate with Gemini via Vertex AI.")
    if last_err:
        print(f"Last error: {last_err}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

