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


def test_competitions_list_includes_competition_identity_fields(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_resp = client.post(
        "/competition/create",
        json={"username": "teacher", "competition_name": "Identity Cup"},
    )
    assert create_resp.status_code == 200
    created_code = create_resp.get_json()["competition_code"]

    list_resp = client.get("/competitions")
    assert list_resp.status_code == 200
    payload = list_resp.get_json()
    entry = next(item for item in payload if item["code"] == created_code)
    assert entry["id"] > 0
    assert entry["code"] == created_code
    assert entry["competition_id"] == entry["id"]
    assert entry["competition_code"] == created_code


def test_competition_lookup_by_code_returns_primary_key_id(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_resp = client.post(
        "/competition/create",
        json={"username": "teacher", "competition_name": "Lookup Cup"},
    )
    assert create_resp.status_code == 200
    created_code = create_resp.get_json()["competition_code"]

    lookup_resp = client.get(f"/competition/by_code/{created_code}")
    assert lookup_resp.status_code == 200
    payload = lookup_resp.get_json()
    assert payload["id"] > 0
    assert payload["competition_id"] == payload["id"]
    assert payload["code"] == created_code
    assert payload["competition_code"] == created_code


def test_competition_lookup_by_code_matches_competitions_shape(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_resp = client.post(
        "/competition/create",
        json={
            "username": "teacher",
            "competition_name": "Shape Cup",
            "curriculumEnabled": True,
            "curriculumWeeks": 2,
            "curriculumStartDate": "2026-03-01",
            "curriculumEndDate": "2026-03-15",
        },
    )
    assert create_resp.status_code == 200
    created_code = create_resp.get_json()["competition_code"]

    list_resp = client.get("/competitions")
    assert list_resp.status_code == 200
    list_payload = list_resp.get_json()
    list_entry = next(item for item in list_payload if item["code"] == created_code)

    lookup_resp = client.get(f"/competition/by_code/{created_code}")
    assert lookup_resp.status_code == 200
    lookup_payload = lookup_resp.get_json()

    assert lookup_payload == list_entry
    for required_field in (
        "id",
        "competition_id",
        "code",
        "competition_code",
        "name",
        "competition_name",
    ):
        assert required_field in lookup_payload
    assert lookup_payload["id"] is not None
    assert lookup_payload["competition_id"] == lookup_payload["id"]


def test_competition_lookup_by_code_returns_404_when_not_found(app_client):
    client, _ = app_client

    lookup_resp = client.get("/competition/by_code/not-a-real-code")
    assert lookup_resp.status_code == 404
    assert lookup_resp.get_json()["message"] == "Competition not found"


def test_admin_competitions_payload_includes_id_and_curriculum_metadata(app_client):
    client, app_module = app_client
    create_user(app_module, username="admin", email="admin@example.com", is_admin=True)
    create_resp = client.post(
        "/competition/create",
        json={
            "username": "admin",
            "competition_name": "Admin Curriculum Cup",
            "curriculumEnabled": True,
            "curriculumWeeks": 4,
            "curriculumStartDate": "2026-01-01",
            "curriculumEndDate": "2026-02-01",
        },
    )
    assert create_resp.status_code == 200

    admin_resp = client.get("/admin/competitions", query_string={"admin_username": "admin"})
    assert admin_resp.status_code == 200
    payload = admin_resp.get_json()
    row = next(item for item in payload if item["name"] == "Admin Curriculum Cup")
    assert row["id"] > 0
    assert row["curriculum_enabled"] is True
    assert row["curriculumEnabled"] is True


def test_normal_competition_join_behavior_unchanged(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_resp = client.post(
        "/competition/create",
        json={"username": "teacher", "competition_name": "Join Cup"},
    )
    assert create_resp.status_code == 200
    comp_code = create_resp.get_json()["competition_code"]

    join_resp = client.post(
        "/competition/join",
        json={"username": "student", "competition_code": comp_code},
    )
    assert join_resp.status_code == 200
    assert "Successfully joined" in join_resp.get_json()["message"]
