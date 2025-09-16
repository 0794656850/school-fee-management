import pytest
from flask import Flask, session, url_for
from utils import admin_required

@pytest.fixture
def app():
    app = Flask(__name__)
    app.secret_key = "test_secret"

    @app.route('/protected')
    @admin_required
    def protected():
        return "Admin Access"

    return app

@pytest.fixture
def client(app):
    return app.test_client()

def test_admin_required_redirects_if_not_logged_in(client):
    response = client.get('/protected', follow_redirects=False)
    assert response.status_code == 302
    assert '/login' in response.headers['Location']

def test_admin_required_allows_access_if_logged_in(app, client):
    with app.test_request_context():
        with client.session_transaction() as sess:
            sess['admin_logged_in'] = True
        response = client.get('/protected')
        assert response.status_code == 200
        assert b"Admin Access" in response.data