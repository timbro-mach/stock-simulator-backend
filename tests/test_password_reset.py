import importlib
import sys
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
    monkeypatch.setenv("SMTP_HOST", "")
    if "app" in sys.modules:
        del sys.modules["app"]
    app_module = importlib.import_module("app")
    app_module.app.config["TESTING"] = True
    with app_module.app.app_context():
        app_module.db.drop_all()
        app_module.db.create_all()
    return app_module.app.test_client(), app_module


def create_user(app_module, email="user@example.com", password="StrongPass!234"):
    with app_module.app.app_context():
        user = app_module.User(username="tester", email=email)
        user.set_password(password)
        app_module.db.session.add(user)
        app_module.db.session.commit()
        return user.id


def test_forgot_password_returns_generic_message_for_unknown_email(app_client):
    client, _ = app_client
    response = client.post("/api/auth/forgot-password", json={"email": "missing@example.com"})
    assert response.status_code == 200
    assert response.get_json()["message"] == "If an account exists for that email, we sent a reset link."


def test_token_is_stored_hashed(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)
    monkeypatch.setattr(app_module, "generate_reset_token", lambda: "rawtoken")
    monkeypatch.setattr(app_module, "send_reset_email", lambda *args, **kwargs: None)

    response = client.post("/api/auth/forgot-password", json={"email": "user@example.com"})
    assert response.status_code == 200

    with app_module.app.app_context():
        token_record = app_module.PasswordResetToken.query.first()
        assert token_record is not None
        assert token_record.token_hash != "rawtoken"
        assert token_record.token_hash == app_module.hash_value("rawtoken")


def test_reset_password_rejects_invalid_token(app_client):
    client, _ = app_client
    response = client.post("/api/auth/reset-password", json={"token": "bad", "newPassword": "NewPass!23456"})
    assert response.status_code == 400


def test_reset_password_rejects_expired_token(app_client):
    client, app_module = app_client
    user_id = create_user(app_module, email="expired@example.com")

    with app_module.app.app_context():
        token = app_module.PasswordResetToken(
            user_id=user_id,
            token_hash=app_module.hash_value("expiredtoken"),
            expires_at=app_module.datetime.utcnow() - app_module.timedelta(minutes=1),
        )
        app_module.db.session.add(token)
        app_module.db.session.commit()

    response = client.post("/api/auth/reset-password", json={"token": "expiredtoken", "newPassword": "NewPass!23456"})
    assert response.status_code == 400


def test_successful_reset_updates_password_and_invalidates_token(app_client):
    client, app_module = app_client
    user_id = create_user(app_module, email="reset@example.com", password="OldPass!23456")

    with app_module.app.app_context():
        token = app_module.PasswordResetToken(
            user_id=user_id,
            token_hash=app_module.hash_value("validtoken"),
            expires_at=app_module.datetime.utcnow() + app_module.timedelta(minutes=30),
        )
        app_module.db.session.add(token)
        app_module.db.session.commit()
        token_id = token.id

    response = client.post("/api/auth/reset-password", json={"token": "validtoken", "newPassword": "NewPass!23456"})
    assert response.status_code == 200

    with app_module.app.app_context():
        updated_user = app_module.db.session.get(app_module.User, user_id)
        updated_token = app_module.db.session.get(app_module.PasswordResetToken, token_id)
        assert updated_user.check_password("NewPass!23456")
        assert updated_token.used_at is not None


def test_rate_limiter_blocks_after_threshold(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)
    counter = {"value": 0}
    def token_factory():
        counter["value"] += 1
        return f"ratetoken{counter['value']}"
    monkeypatch.setattr(app_module, "generate_reset_token", token_factory)
    monkeypatch.setattr(app_module, "send_reset_email", lambda *args, **kwargs: None)

    for _ in range(app_module.PASSWORD_RESET_RATE_LIMIT_IP):
        response = client.post("/api/auth/forgot-password", json={"email": "user@example.com"})
        assert response.status_code == 200

    response = client.post("/api/auth/forgot-password", json={"email": "user@example.com"})
    assert response.status_code == 200

    with app_module.app.app_context():
        token_count = app_module.PasswordResetToken.query.count()
        assert token_count == app_module.PASSWORD_RESET_RATE_LIMIT_EMAIL
