import os
import json

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def main() -> None:
    print("VERTEX_PROJECT_ID:", os.environ.get("VERTEX_PROJECT_ID"))
    print("GOOGLE_CLOUD_PROJECT:", os.environ.get("GOOGLE_CLOUD_PROJECT"))
    print("VERTEX_LOCATION:", os.environ.get("VERTEX_LOCATION"))
    sa = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    print("GOOGLE_APPLICATION_CREDENTIALS:", sa)
    print("Creds file exists:", os.path.exists(sa))
    try:
        import vertexai  # type: ignore
        from vertexai.generative_models import GenerativeModel  # type: ignore
        from google.oauth2 import service_account  # type: ignore

        creds = None
        if sa and os.path.exists(sa):
            creds = service_account.Credentials.from_service_account_file(sa)
        project = os.environ.get("VERTEX_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        location = os.environ.get("VERTEX_LOCATION", "us-central1")
        print("Initializing Vertex...", {"project": project, "location": location})
        vertexai.init(project=project, location=location, credentials=creds)
        print("Vertex init OK")
        model = GenerativeModel(os.environ.get("VERTEX_GEMINI_MODEL", "gemini-1.5-flash"))
        r = model.generate_content("Say 'hi' in one word.")
        print("Model response:", getattr(r, "text", None))
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    main()

