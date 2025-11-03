from flask import Blueprint, redirect, request, session, url_for, flash
import os

from google_auth_oauthlib.flow import Flow

# Reuse scopes and token storage path from the Gmail util
from utils.gmail_api import SCOPES, _token_path


gmail_oauth_bp = Blueprint("gmail_oauth", __name__)


def _credentials_file() -> str:
    return os.path.abspath(os.environ.get("GMAIL_CREDENTIALS_JSON", "credentials.json"))


def _redirect_uri() -> str:
    """Return the OAuth redirect URI.

    Priority:
    1) Explicit env var `GMAIL_REDIRECT_URI`
    2) Derive from the current request host using url_for(..., _external=True)
    3) Fallback to previous default for local dev
    """
    env_uri = os.environ.get("GMAIL_REDIRECT_URI", "").strip()
    if env_uri:
        return env_uri
    # Try read the first redirect_uri from credentials.json to avoid host mismatches
    try:
        import json
        with open(_credentials_file(), "r", encoding="utf-8") as f:
            data = json.load(f)
        # Support both 'web' and 'installed' styles; prefer web
        cfg = data.get("web") or data.get("installed") or {}
        uris = cfg.get("redirect_uris") or []
        if isinstance(uris, list) and uris:
            return uris[0]
    except Exception:
        pass
    try:
        # Use current request host/port to avoid mismatches like 127.0.0.1 vs localhost
        # Scheme follows Flask preference/env (http for local unless overridden)
        scheme = os.environ.get("PREFERRED_URL_SCHEME", "http")
        return url_for("gmail_oauth.oauth2callback", _external=True, _scheme=scheme)
    except Exception:
        # Safe fallback for local development
        return "http://127.0.0.1:5000/oauth2callback"


@gmail_oauth_bp.route("/gmail/authorize")
def gmail_authorize():
    cred_file = _credentials_file()
    if not os.path.exists(cred_file):
        flash(f"credentials.json not found at {cred_file}", "error")
        return redirect(url_for("reminders.reminders_home"))

    flow = Flow.from_client_secrets_file(
        cred_file,
        scopes=SCOPES,
        redirect_uri=_redirect_uri(),
    )

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    session["gmail_oauth_state"] = state
    return redirect(authorization_url)


@gmail_oauth_bp.route("/oauth2callback")
def oauth2callback():
    cred_file = _credentials_file()
    state = session.get("gmail_oauth_state")
    if not os.path.exists(cred_file) or not state:
        flash("OAuth state missing or credentials.json not found.", "error")
        return redirect(url_for("reminders.reminders_home"))

    try:
        flow = Flow.from_client_secrets_file(
            cred_file,
            scopes=SCOPES,
            state=state,
            redirect_uri=_redirect_uri(),
        )
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        # Persist token for Gmail API helper to reuse
        token_file = _token_path()
        os.makedirs(os.path.dirname(token_file) or ".", exist_ok=True)
        with open(token_file, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

        flash("Gmail authorization completed successfully.", "success")
        return redirect(url_for("reminders.reminders_home"))
    except Exception as e:
        # Provide a friendly message and guidance when OAuth exchange fails
        msg = str(e)
        # Common causes: redirect_uri mismatch, missing state due to cookie issues, or revoked client
        hint = (
            "OAuth failed. Check that your Authorized redirect URI matches this app "
            "and that your session cookies are being sent (disable SESSION_COOKIE_SECURE for HTTP)."
        )
        try:
            print(f"[oauth2callback] Token exchange error: {e}")
        except Exception:
            pass
        flash(f"Gmail authorization error: {msg}. {hint}", "error")
        return redirect(url_for("reminders.reminders_home"))
