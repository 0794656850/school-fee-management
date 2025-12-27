import os
import sys

from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import app
from utils.security import verify_password


class FakeCursor:
    def __init__(self, fetchone_result=None):
        self.fetchone_result = fetchone_result
        self.exec_calls: list[tuple[str, tuple | None]] = []

    def execute(self, query, params=None):
        self.exec_calls.append((query, params))

    def fetchone(self):
        result = self.fetchone_result
        self.fetchone_result = None
        return result


class FakeConnection:
    """A lightweight stand-in for the MySQL connection used by forgot_password_simple."""

    def __init__(self, school_row, email_row):
        self.school_row = school_row
        self.email_row = email_row
        self.committed = False
        self.closed = False
        self.cursors: list[FakeCursor] = []

    def cursor(self, dictionary=False):
        cursor = FakeCursor(self.school_row if dictionary else self.email_row)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_forgot_password_simple_sends_new_password_email():
    app.testing = True
    fake_db = FakeConnection(
        {"id": 73, "name": "Example Academy"},
        ("admin@example.org",),
    )
    with patch("app.get_db_connection", return_value=fake_db), patch(
        "utils.gmail_api.send_email_html"
    ) as mock_send_html, patch("utils.gmail_api.send_email", return_value=False), patch(
        "secrets.choice", return_value="X"
    ), patch("routes.auth_routes.set_setting") as mock_set_setting:
        mock_send_html.return_value = True
        with app.test_client() as client:
            response = client.post("/auth/forgot/simple", data={"school_code": "example-academy"})
    assert response.status_code == 302
    location = response.headers["Location"]
    assert location.split("?")[0] == "/auth/login"
    assert fake_db.committed
    assert fake_db.closed
    assert mock_send_html.call_count == 1
    args = mock_send_html.call_args[0]
    assert args[0] == "admin@example.org"
    assert args[1] == "Your New School Admin Password"
    assert "Your new login password is:" in args[2]
    assert "XXXXXXXXXX" in args[2]
    email_cursor = next(
        (
            cursor
            for cursor in fake_db.cursors
            if any(call[0].startswith("INSERT INTO school_settings") for call in cursor.exec_calls)
        ),
        None,
    )
    assert email_cursor is not None
    insert_call = next(
        (call for call in email_cursor.exec_calls if call[1] and len(call[1]) == 2),
        None,
    )
    assert insert_call is not None
    insert_params = insert_call[1]
    assert insert_params is not None
    assert insert_params[0] == 73
    hashed_value = insert_params[1]
    assert verify_password(hashed_value, "XXXXXXXXXX")
    assert mock_set_setting.called
    set_setting_args = mock_set_setting.call_args[0]
    assert set_setting_args[0] == "ADMIN_PASSWORD"
    assert verify_password(set_setting_args[1], "XXXXXXXXXX")
