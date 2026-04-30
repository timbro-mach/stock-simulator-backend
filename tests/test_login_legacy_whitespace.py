import importlib
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture()
def app_client(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("APP_BASE_URL", "https://example.com")
    if "msal" not in sys.modules:
        sys.modules["msal"] = types.SimpleNamespace(ConfidentialClientApplication=object)
    if "app" in sys.modules:
        del sys.modules["app"]
    app_module = importlib.import_module("app")
    app_module.app.config["TESTING"] = True
    with app_module.app.app_context():
        app_module.db.drop_all()
        app_module.db.create_all()
    return app_module.app.test_client(), app_module


def _seed_legacy_user(app_module, username, email, password="StrongPass!234"):
    with app_module.app.app_context():
        user = app_module.User(username=username, email=email)
        user.set_password(password)
        app_module.db.session.add(user)
        app_module.db.session.commit()


def test_login_succeeds_when_stored_username_has_trailing_whitespace(app_client):
    client, app_module = app_client
    _seed_legacy_user(app_module, username="Gordon Montgomery ", email="gordon@example.com")

    resp = client.post("/login", json={"username": "Gordon Montgomery", "password": "StrongPass!234"})
    assert resp.status_code == 200, resp.get_json()


def test_login_succeeds_when_stored_username_has_leading_whitespace_and_mixed_case(app_client):
    client, app_module = app_client
    _seed_legacy_user(app_module, username=" Gordon Montgomery", email="gordon2@example.com")

    resp = client.post("/login", json={"username": "gordon montgomery", "password": "StrongPass!234"})
    assert resp.status_code == 200, resp.get_json()


def test_login_by_email_succeeds_when_stored_email_has_whitespace(app_client):
    client, app_module = app_client
    _seed_legacy_user(app_module, username="gordonm", email=" gordon3@example.com ")

    resp = client.post("/login", json={"username": "gordon3@example.com", "password": "StrongPass!234"})
    assert resp.status_code == 200, resp.get_json()


def test_forgot_password_finds_user_with_whitespace_email(app_client, monkeypatch):
    client, app_module = app_client
    _seed_legacy_user(app_module, username="gordonm2", email=" gordon4@example.com ")
    monkeypatch.setattr(app_module, "generate_reset_token", lambda: "rawtoken")
    monkeypatch.setattr(app_module, "send_reset_email", lambda *args, **kwargs: None)

    resp = client.post("/api/auth/forgot-password", json={"email": "gordon4@example.com"})
    assert resp.status_code == 200

    with app_module.app.app_context():
        token = app_module.PasswordResetToken.query.first()
        assert token is not None, "expected a reset token to be issued for the legacy whitespace email"
