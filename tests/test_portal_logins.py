import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app


def test_pages_render():
    app.testing = True
    with app.test_client() as c:
        assert c.get('/auth/login').status_code == 200
        assert c.get('/g/login').status_code == 200
        assert c.get('/s/login').status_code == 200


def test_guardian_dashboard_requires_token():
    app.testing = True
    with app.test_client() as c:
        r = c.get('/guardian_dashboard')
        assert r.status_code in (301, 302)
        assert '/g/login' in (r.headers.get('Location') or '')
