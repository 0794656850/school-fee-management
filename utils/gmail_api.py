import os
import base64
from typing import Optional

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Only send permission
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _credentials_path() -> str:
    return os.path.abspath(os.environ.get("GMAIL_CREDENTIALS_JSON", "credentials.json"))


def _token_path() -> str:
    return os.path.abspath(os.environ.get("GMAIL_TOKEN_JSON", "token.json"))


def _get_creds() -> Optional[Credentials]:
    creds = None
    token_file = _token_path()
    cred_file = _credentials_path()

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # Refresh or run local server flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # In a Flask app, use the web OAuth flow handled by routes/gmail_oauth_routes.py
            # to avoid dynamic localhost port redirects that cause redirect_uri_mismatch.
            # Guide the caller to start the authorization flow via the app route.
            if not os.path.exists(cred_file):
                raise FileNotFoundError(
                    f"Gmail credentials file not found at {cred_file}. Place your OAuth2 client JSON there."
                )
            raise RuntimeError(
                "Gmail not authorized. Visit /gmail/authorize in your browser "
                "to connect a Google account. Ensure the Authorized redirect URI "
                "in Google Cloud matches your app, e.g., http://127.0.0.1:5000/oauth2callback "
                "(or set GMAIL_REDIRECT_URI)."
            )
        # Save token for next runs
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    return creds


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
    except HttpError as e:
        # Surface Gmail API errors for visibility
        try:
            print(f"Gmail API error: {e}")
        except Exception:
            pass
        return False
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
    except HttpError as e:
        try:
            print(f"Gmail API error: {e}")
        except Exception:
            pass
        return False
    except Exception as e:
        try:
            print(f"HTML email send failed: {e}")
        except Exception:
            pass
        return False
