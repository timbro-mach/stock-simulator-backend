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


def create_user(app_module, username, email, password="StrongPass!234", is_admin=False):
    with app_module.app.app_context():
        user = app_module.User(username=username, email=email)
        user.set_password(password)
        user.is_admin = is_admin
        app_module.db.session.add(user)
        app_module.db.session.commit()


def _create_competition(client, username="organizer", name="Date Cup", start="2026-01-01", end="2026-02-01"):
    payload = {"username": username, "competition_name": name}
    if start is not None:
        payload["start_date"] = start
    if end is not None:
        payload["end_date"] = end
    resp = client.post("/competition/create", json=payload)
    assert resp.status_code == 200
    return resp.get_json()["competition_code"]


def test_organizer_can_update_competition_dates(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": code,
            "start_date": "2026-03-01",
            "end_date": "2026-04-01",
        },
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert "updated successfully" in body["message"]
    assert body["competition"]["start_date"].startswith("2026-03-01")
    assert body["competition"]["end_date"].startswith("2026-04-01")

    lookup = client.get(f"/competition/by_code/{code}").get_json()
    assert lookup["start_date"].startswith("2026-03-01")
    assert lookup["end_date"].startswith("2026-04-01")


def test_update_dates_supports_competition_id(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)
    comp_id = client.get(f"/competition/by_code/{code}").get_json()["id"]

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_id": comp_id,
            "end_date": "2026-05-15",
        },
    )
    assert resp.status_code == 200
    assert resp.get_json()["competition"]["end_date"].startswith("2026-05-15")


def test_update_dates_allows_partial_update(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": code,
            "start_date": "2026-01-15",
        },
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["competition"]["start_date"].startswith("2026-01-15")
    assert body["competition"]["end_date"].startswith("2026-02-01")


def test_update_dates_allows_clearing_dates(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": code,
            "start_date": None,
            "end_date": None,
        },
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["competition"]["start_date"] is None
    assert body["competition"]["end_date"] is None


def test_update_dates_rejects_non_organizer(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    create_user(app_module, username="student", email="student@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "student",
            "competition_code": code,
            "start_date": "2026-03-01",
            "end_date": "2026-04-01",
        },
    )
    assert resp.status_code == 403


def test_admin_can_update_any_competition_dates(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    create_user(app_module, username="siteadmin", email="siteadmin@example.com", is_admin=True)
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "siteadmin",
            "competition_code": code,
            "start_date": "2026-06-01",
            "end_date": "2026-07-01",
        },
    )
    assert resp.status_code == 200
    assert resp.get_json()["competition"]["start_date"].startswith("2026-06-01")


def test_update_dates_rejects_end_before_start(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": code,
            "start_date": "2026-05-01",
            "end_date": "2026-04-01",
        },
    )
    assert resp.status_code == 400
    assert "end_date" in resp.get_json()["message"]


def test_update_dates_rejects_invalid_format(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": code,
            "start_date": "03/01/2026",
        },
    )
    assert resp.status_code == 400


def test_update_dates_requires_at_least_one_field(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")
    code = _create_competition(client)

    resp = client.post(
        "/competition/update_dates",
        json={"username": "organizer", "competition_code": code},
    )
    assert resp.status_code == 400


def test_update_dates_returns_404_for_unknown_competition(app_client):
    client, app_module = app_client
    create_user(app_module, username="organizer", email="organizer@example.com")

    resp = client.post(
        "/competition/update_dates",
        json={
            "username": "organizer",
            "competition_code": "does-not-exist",
            "start_date": "2026-03-01",
        },
    )
    assert resp.status_code == 404
