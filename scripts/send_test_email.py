import os
import sys

# Allow running from repo root
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.gmail_api import send_email, _token_path, _credentials_path  # type: ignore


def main():
    to = os.environ.get("TEST_EMAIL_TO") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not to:
        print("Usage: TEST_EMAIL_TO=you@example.com python scripts/send_test_email.py [optional-to]")
        sys.exit(2)
    print(f"credentials.json: {_credentials_path()}")
    print(f"token.json:       {_token_path()} (must exist after /gmail/authorize)")
    ok = send_email(to, "Gmail API test", "This is a test from Fee Management System.")
    print("Result:", "OK" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

