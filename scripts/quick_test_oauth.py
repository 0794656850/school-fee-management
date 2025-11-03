import os
from flask import Flask

# Local dev env toggles
os.environ.setdefault("SESSION_COOKIE_SECURE", "0")
os.environ.setdefault("PREFERRED_URL_SCHEME", "http")
os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
os.environ.setdefault("GMAIL_REDIRECT_URI", "http://127.0.0.1:5000/oauth2callback")

import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))); from routes.gmail_oauth_routes import gmail_oauth_bp

app = Flask(__name__)
app.secret_key = "test"
app.register_blueprint(gmail_oauth_bp)

with app.test_client() as c:
    r = c.get("/gmail/authorize", follow_redirects=False)
    print("status:", r.status_code)
    loc = r.headers.get("Location", "")
    print("location_has_google:", "accounts.google.com" in loc)
    print("location:", loc[:160])
