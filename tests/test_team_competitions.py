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


def create_user(app_module, username="tester", email="user@example.com", password="StrongPass!234"):
    with app_module.app.app_context():
        user = app_module.User(username=username, email=email)
        user.set_password(password)
        app_module.db.session.add(user)
        app_module.db.session.commit()


def test_competition_team_join_accepts_team_id_alias_and_user_payload_includes_unified_object(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)

    with app_module.app.app_context():
        user = app_module.User.query.filter_by(username="tester").first()
        team = app_module.Team(name="Wolves", created_by=user.id)
        app_module.db.session.add(team)
        app_module.db.session.flush()
        app_module.db.session.add(app_module.TeamMember(team_id=team.id, user_id=user.id))
        comp = app_module.Competition(code="ABC123", name="Spring Cup", created_by=user.id)
        app_module.db.session.add(comp)
        app_module.db.session.commit()

        team_id = team.id

    join_resp = client.post(
        "/competition/team/join",
        json={"username": "tester", "team_id": team_id, "competition_code": "ABC123"},
    )
    assert join_resp.status_code == 200

    monkeypatch.setattr(app_module, "get_current_and_prev_close", lambda symbol: (100.0, 99.0))

    user_resp = client.get("/user", query_string={"username": "tester"})
    assert user_resp.status_code == 200
    payload = user_resp.get_json()

    assert len(payload["team_competitions"]) == 1
    entry = payload["team_competitions"][0]
    assert entry["team_name"] == "Wolves"
    assert entry["team_competition"] == {
        "team": {"id": team_id, "name": "Wolves"},
        "competition": {"code": "ABC123", "name": "Spring Cup"},
    }


def test_login_and_user_skip_orphaned_team_records_and_keep_competition_accounts(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module, username="legacy", email="legacy@example.com")

    with app_module.app.app_context():
        user = app_module.User.query.filter_by(username="legacy").first()
        comp = app_module.Competition(code="LEG123", name="Legacy Comp", created_by=user.id)
        app_module.db.session.add(comp)
        app_module.db.session.flush()
        app_module.db.session.add(app_module.CompetitionMember(user_id=user.id, competition_id=comp.id, cash_balance=100000))

        # orphaned legacy team/member rows should not break login or /user responses
        team = app_module.Team(name="Temp Team", created_by=user.id)
        app_module.db.session.add(team)
        app_module.db.session.flush()
        orphan_team_id = team.id
        app_module.db.session.add(app_module.TeamMember(team_id=orphan_team_id, user_id=user.id))
        app_module.db.session.add(app_module.CompetitionTeam(competition_id=comp.id, team_id=orphan_team_id, cash_balance=100000))
        app_module.db.session.commit()

        # delete the team row to emulate legacy/orphaned data
        app_module.db.session.delete(team)
        app_module.db.session.commit()

    monkeypatch.setattr(app_module, "get_current_and_prev_close", lambda symbol: (100.0, 99.0))

    login_resp = client.post("/login", json={"username": "legacy", "password": "StrongPass!234"})
    assert login_resp.status_code == 200
    login_payload = login_resp.get_json()
    assert len(login_payload["competition_accounts"]) == 1
    assert login_payload["competition_accounts"][0]["competition_code"] == "LEG123"
    assert login_payload["competition_accounts"][0]["account_type"] == "competition"
    assert login_payload["team_competitions"] == []

    user_resp = client.get("/user", query_string={"username": "legacy"})
    assert user_resp.status_code == 200
    payload = user_resp.get_json()
    assert len(payload["competition_accounts"]) == 1
    assert payload["competition_accounts"][0]["competition_code"] == "LEG123"
    assert payload["competition_accounts"][0]["account_type"] == "competition"
    assert payload["team_competitions"] == []
