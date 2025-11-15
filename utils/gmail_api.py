import os
import base64
from typing import Optional

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# Only send permission
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _credentials_path() -> str:
    return os.path.abspath(os.environ.get("GMAIL_CREDENTIALS_JSON", "credentials.json"))


def _token_path() -> str:
    return os.path.abspath(os.environ.get("GMAIL_TOKEN_JSON", "token.json"))


def _get_creds() -> Optional["Credentials"]:
    creds = None
    token_file = _token_path()
    cred_file = _credentials_path()

    # Import Google auth libs lazily to avoid hard dependency at module import
    try:
        from google.oauth2.credentials import Credentials  # type: ignore
        from google.auth.transport.requests import Request  # type: ignore
    except Exception:
        # Libraries not installed; caller will see None and treat as unavailable
        return None

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # Refresh or run local server flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as exc:
                try:
                    os.remove(token_file)
                except Exception:
                    pass
                try:
                    print(f"Gmail token refresh failed, removing {token_file}: {exc}")
                except Exception:
                    pass
                return None
        else:
            # In a Flask app, use the web OAuth flow handled by routes/gmail_oauth_routes.py
            # to avoid dynamic localhost port redirects that cause redirect_uri_mismatch.
            # Guide the caller to start the authorization flow via the app route.
            if not os.path.exists(cred_file):
                # No client creds present; cannot proceed
                return None
            # In app flow, we use /gmail/authorize; return None to indicate not ready
            return None
        # Save token for next runs
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    return creds


def has_valid_token() -> bool:
    """Return True when Gmail credentials can be loaded."""
    try:
        return _get_creds() is not None
    except Exception:
        return False


def authenticate_gmail():
    """
    Authenticate and return a Gmail API service client.

    - Uses `credentials.json` for OAuth 2.0 client credentials
    - Stores/refreshes tokens in `token.json`
    - Scope limited to gmail.send
    Returns a Gmail service on success, or None on failure.
    """
    try:
        creds = _get_creds()
        if creds is None:
            return None
        try:
            from googleapiclient.discovery import build  # type: ignore
        except Exception:
            return None
        return build("gmail", "v1", credentials=creds)
    except Exception as e:
        try:
            print(f"Failed to authenticate Gmail: {e}")
        except Exception:
            pass
        return None


def send_email(to: str, subject: str, body: str) -> bool:
    """
    Send an email using the Gmail API and OAuth2 user credentials.

    - Reads client credentials from `credentials.json` (or env GMAIL_CREDENTIALS_JSON)
    - Caches user token in `token.json` (or env GMAIL_TOKEN_JSON)
    - Scope: gmail.send
    Returns True on success, False on failure.
    """
    try:
        service = authenticate_gmail()
        if service is None:
            return False

        message = MIMEText(body or "")
        message["to"] = to
        message["subject"] = subject or ""
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True
    except Exception as e:
        try:
            print(f"Email send failed: {e}")
        except Exception:
            pass
        return False


def send_email_html(to: str, subject: str, html_body: str) -> bool:
    """
    Send an HTML email using Gmail API OAuth2.

    Falls back to the same authentication flow as send_email, but constructs
    a MIME message with text/html content so modern, styled receipts render
    properly in clients.
    """
    try:
        service = authenticate_gmail()
        if service is None:
            return False

        msg = MIMEMultipart('alternative')
        msg['to'] = to
        msg['subject'] = subject or ""
        # Only HTML part (most clients handle this fine); callers may add a plain
        # text fallback if needed.
        part_html = MIMEText(html_body or "", 'html', 'utf-8')
        msg.attach(part_html)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True
    except Exception as e:
        try:
            print(f"HTML email send failed: {e}")
        except Exception:
            pass
        return False
