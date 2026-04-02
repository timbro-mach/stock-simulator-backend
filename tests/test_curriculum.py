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


def create_user(app_module, username="teacher", email="teacher@example.com", password="StrongPass!234"):
    with app_module.app.app_context():
        user = app_module.User(username=username, email=email)
        user.set_password(password)
        app_module.db.session.add(user)
        app_module.db.session.commit()


def _create_competition(client, payload):
    return client.post("/competition/create", json=payload)


def _assert_curriculum_records_exist(app_module, competition_name, expected_weeks):
    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name=competition_name).first()
        assert comp is not None
        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        assert curriculum is not None
        assert curriculum.enabled is True
        assert curriculum.total_weeks == expected_weeks

        modules = (
            app_module.CurriculumModule.query
            .filter_by(curriculum_id=curriculum.id)
            .order_by(app_module.CurriculumModule.week_number.asc())
            .all()
        )
        assert len(modules) == expected_weeks

        module_ids = [m.id for m in modules]
        assignments = (
            app_module.CurriculumAssignment.query
            .filter(app_module.CurriculumAssignment.module_id.in_(module_ids))
            .all()
        )
        assert assignments
        assert any(a.type == "exam" for a in assignments)
        return comp.id


def test_competition_creation_without_curriculum_remains_unchanged(app_client):
    client, app_module = app_client
    create_user(app_module)

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "No Curriculum Cup",
        "start_date": "2026-01-01",
        "end_date": "2026-03-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="No Curriculum Cup").first()
        assert comp is not None
        assert app_module.Curriculum.query.filter_by(competition_id=comp.id).first() is None


def test_competition_creation_with_curriculum_creates_linked_records(app_client):
    client, app_module = app_client
    create_user(app_module)

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Curriculum Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 6,
        "curriculumStartDate": "2026-02-01",
        "curriculumEndDate": "2026-03-15",
    })
    assert resp.status_code == 200

    competition_id = _assert_curriculum_records_exist(
        app_module,
        competition_name="Curriculum Cup",
        expected_weeks=6,
    )
    summary_resp = client.get(f"/curriculum/competition/{competition_id}")
    assert summary_resp.status_code == 200
    modules_resp = client.get(f"/curriculum/competition/{competition_id}/modules")
    assert modules_resp.status_code == 200


def test_competition_creation_accepts_snake_case_curriculum_fields_and_summary_lookup(app_client):
    client, app_module = app_client
    create_user(app_module)

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Snake Case Curriculum Cup",
        "curriculum_enabled": True,
        "curriculum_weeks": 5,
        "curriculum_start_date": "2026-02-01",
        "curriculum_end_date": "2026-03-15",
    })
    assert resp.status_code == 200

    competition_id = _assert_curriculum_records_exist(
        app_module,
        competition_name="Snake Case Curriculum Cup",
        expected_weeks=5,
    )

    summary_resp = client.get(f"/curriculum/competition/{competition_id}")
    assert summary_resp.status_code == 200
    assert summary_resp.get_json()["enabled"] is True
    modules_resp = client.get(f"/curriculum/competition/{competition_id}/modules")
    assert modules_resp.status_code == 200


@pytest.mark.parametrize("weeks", [6, 8, 12])
def test_variable_week_curriculum_generation(app_client, weeks):
    client, app_module = app_client
    create_user(app_module)

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": f"Weeks {weeks}",
        "curriculumEnabled": True,
        "curriculumWeeks": weeks,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-04-30",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name=f"Weeks {weeks}").first()
        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        modules = app_module.CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(app_module.CurriculumModule.week_number.asc()).all()
        assert len(modules) == weeks
        assert modules[0].unlock_date <= modules[0].due_date
        assert modules[-1].due_date <= curriculum.end_date


def test_quiz_submission_auto_grades_correctly_and_grade_summary(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Grading Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 6,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-03-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Grading Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        competition_id = comp.id
        student_id = student.id
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()

        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        first_module = app_module.CurriculumModule.query.filter_by(curriculum_id=curriculum.id, week_number=1).first()
        quiz = app_module.CurriculumAssignment.query.filter_by(module_id=first_module.id, type="quiz").first()
        answer_key = quiz.answer_key_json["questions"]

    submit_resp = client.post(
        f"/curriculum/assignments/{quiz.id}/submissions",
        json={
            "username": "student",
            "answers": answer_key,
        },
    )
    assert submit_resp.status_code == 200
    submit_payload = submit_resp.get_json()
    assert submit_payload["score"] == 20
    assert submit_payload["percentage"] == 100

    grades_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert grades_resp.status_code == 200
    grades_payload = grades_resp.get_json()
    assert grades_payload["totalPointsPossible"] > 0
    assert grades_payload["totalPointsEarned"] >= 20
    assert grades_payload["letterGrade"] in {"A", "B", "C", "D", "F"}


def test_curriculum_grades_requires_auth_and_permissions(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_user(app_module, username="outsider", email="outsider@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Auth Check Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-15",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Auth Check Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()
        competition_id = comp.id
        student_id = student.id

    missing_auth_resp = client.get(f"/curriculum/competition/{competition_id}/grades/{student_id}")
    assert missing_auth_resp.status_code == 401

    unknown_user_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "ghost"},
    )
    assert unknown_user_resp.status_code == 401

    forbidden_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "outsider"},
    )
    assert forbidden_resp.status_code == 403


def test_curriculum_grades_accepts_member_account_id_and_checks_membership(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_user(app_module, username="nonmember", email="nonmember@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Grade Lookup Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-15",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Grade Lookup Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        nonmember = app_module.User.query.filter_by(username="nonmember").first()
        member = app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000)
        app_module.db.session.add(member)
        app_module.db.session.commit()
        member_account_id = member.id
        competition_id = comp.id
        student_id = student.id
        nonmember_id = nonmember.id

    member_id_resp = client.get(
        f"/curriculum/competition/{member_account_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert member_id_resp.status_code == 200
    assert member_id_resp.get_json()["competitionId"] == competition_id

    nonmember_resp = client.get(
        f"/curriculum/competition/{member_account_id}/grades/{nonmember_id}",
        query_string={"username": "nonmember"},
    )
    assert nonmember_resp.status_code == 404


def test_curriculum_endpoints_do_not_interfere_with_simulator_endpoints(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Isolation Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 8,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-03-15",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Isolation Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        competition_code = comp.code
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)
    trade_resp = client.post(
        "/competition/buy",
        json={"username": "student", "competition_code": competition_code, "symbol": "AAPL", "quantity": 1},
    )
    assert trade_resp.status_code == 200
    assert trade_resp.get_json()["message"] == "Competition buy successful"


def test_curriculum_modules_include_lesson_content_and_rich_assignments(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Lesson Content Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Lesson Content Cup").first()
        competition_id = comp.id

    modules_resp = client.get(f"/curriculum/competition/{competition_id}/modules")
    assert modules_resp.status_code == 200
    modules = modules_resp.get_json()
    assert modules
    first_module = modules[0]
    assert first_module["lessonContent"]
    assert len(first_module["lessonContent"]) > 300
    quiz = next(a for a in first_module["assignments"] if a["type"] == "quiz")
    assert len(quiz["content"]["questions"]) == 20
    assert len(quiz["answer_key_json"]["questions"]) == 20
    written = next(a for a in first_module["assignments"] if a["type"] == "assignment")
    assert len(written["content"]["questions"]) == 2
    assert all(len(q["sections"]) >= 3 for q in written["content"]["questions"])


def test_instructor_can_list_submissions_and_manually_grade(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Manual Grade Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 3,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Manual Grade Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()

        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        first_module = app_module.CurriculumModule.query.filter_by(curriculum_id=curriculum.id, week_number=1).first()
        assignment = app_module.CurriculumAssignment.query.filter_by(module_id=first_module.id, type="assignment").first()
        competition_id = comp.id

    submit_resp = client.post(
        f"/curriculum/assignments/{assignment.id}/submissions",
        json={"username": "student", "answers": {"a1": {"a": "test"}, "a2": {"a": "12.5%"}}},
    )
    assert submit_resp.status_code == 200
    assert submit_resp.get_json()["autoGraded"] is False

    list_resp = client.get(
        f"/curriculum/assignments/{assignment.id}/submissions",
        query_string={"username": "teacher"},
    )
    assert list_resp.status_code == 200
    list_payload = list_resp.get_json()
    assert list_payload["totalSubmissions"] == 1
    submission_id = list_payload["submissions"][0]["submissionId"]

    grade_resp = client.post(
        f"/curriculum/submissions/{submission_id}/grade",
        json={"username": "teacher", "question_1_score": 9, "question_2_score": 8, "feedback": "Good use of diversification rationale."},
    )
    assert grade_resp.status_code == 200
    assert grade_resp.get_json()["score"] == 17
    assert grade_resp.get_json()["question1Score"] == 9
    assert grade_resp.get_json()["question2Score"] == 8

    overview_resp = client.get(
        f"/curriculum/competition/{competition_id}/instructor-overview",
        query_string={"username": "teacher"},
    )
    assert overview_resp.status_code == 200
    overview_payload = overview_resp.get_json()
    assert "recentSubmissions" in overview_payload
    assert "writtenAssignmentSubmissions" in overview_payload


def test_module_grade_breakdown_includes_trade_participation_and_competition_gradebook(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Trade Participation Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 2,
        "curriculumStartDate": "2026-03-15",
        "curriculumEndDate": "2026-05-15",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Trade Participation Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()
        competition_id = comp.id
        competition_code = comp.code
        student_id = student.id

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)
    trade_resp = client.post(
        "/competition/buy",
        json={"username": "student", "competition_id": competition_id, "competition_code": competition_code, "symbol": "AAPL", "quantity": 1},
    )
    assert trade_resp.status_code == 200

    grades_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert grades_resp.status_code == 200
    module_grades = grades_resp.get_json()["moduleGrades"]
    assert module_grades
    assert module_grades[0]["tradeParticipation"]["tradeCompleted"] is True
    assert module_grades[0]["tradeParticipation"]["tradePoints"] == 10

    competition_gradebook_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades",
        query_string={"username": "teacher"},
    )
    assert competition_gradebook_resp.status_code == 200
    assert competition_gradebook_resp.get_json()["studentCount"] == 1


def test_curriculum_summary_accepts_competition_member_account_id(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Member Id Lookup Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 6,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-03-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Member Id Lookup Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        member = app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000)
        app_module.db.session.add(member)
        app_module.db.session.commit()
        member_account_id = member.id
        competition_id = comp.id

    summary_resp = client.get(f"/curriculum/competition/{member_account_id}")
    assert summary_resp.status_code == 200
    assert summary_resp.get_json()["competitionId"] == competition_id


@pytest.mark.parametrize(
    "competition_name,payload",
    [
        (
            "Collision Camel Case Curriculum Cup",
            {
                "curriculumEnabled": True,
                "curriculumWeeks": 6,
                "curriculumStartDate": "2026-01-01",
                "curriculumEndDate": "2026-03-01",
            },
        ),
        (
            "Collision Snake Case Curriculum Cup",
            {
                "curriculum_enabled": True,
                "curriculum_weeks": 6,
                "curriculum_start_date": "2026-01-01",
                "curriculum_end_date": "2026-03-01",
            },
        ),
    ],
)
def test_curriculum_endpoints_resolve_member_account_id_when_ids_collide(app_client, competition_name, payload):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    normal_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Collision Baseline Normal Cup",
        "start_date": "2026-01-01",
        "end_date": "2026-03-01",
    })
    assert normal_resp.status_code == 200

    curriculum_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": competition_name,
        **payload,
    })
    assert curriculum_resp.status_code == 200

    curriculum_competition_id = _assert_curriculum_records_exist(
        app_module,
        competition_name=competition_name,
        expected_weeks=6,
    )

    with app_module.app.app_context():
        student = app_module.User.query.filter_by(username="student").first()
        member = app_module.CompetitionMember(
            competition_id=curriculum_competition_id,
            user_id=student.id,
            cash_balance=100000,
        )
        app_module.db.session.add(member)
        app_module.db.session.commit()
        member_account_id = member.id

    summary_resp = client.get(f"/curriculum/competition/{member_account_id}")
    assert summary_resp.status_code == 200
    assert summary_resp.get_json()["competitionId"] == curriculum_competition_id

    modules_resp = client.get(f"/curriculum/competition/{member_account_id}/modules")
    assert modules_resp.status_code == 200
    assert len(modules_resp.get_json()) == 6


def test_admin_delete_competition_handles_curriculum_and_normal_competitions(app_client):
    client, app_module = app_client
    create_user(app_module, username="admin", email="admin@example.com")
    create_user(app_module, username="teacher", email="teacher@example.com")

    with app_module.app.app_context():
        admin = app_module.User.query.filter_by(username="admin").first()
        admin.is_admin = True
        app_module.db.session.commit()

    curriculum_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Admin Delete Curriculum Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-01",
    })
    assert curriculum_resp.status_code == 200

    normal_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Admin Delete Normal Cup",
        "start_date": "2026-01-01",
        "end_date": "2026-02-01",
    })
    assert normal_resp.status_code == 200

    with app_module.app.app_context():
        curriculum_comp = app_module.Competition.query.filter_by(name="Admin Delete Curriculum Cup").first()
        normal_comp = app_module.Competition.query.filter_by(name="Admin Delete Normal Cup").first()
        assert app_module.Curriculum.query.filter_by(competition_id=curriculum_comp.id).first() is not None
        curriculum_code = curriculum_comp.code
        normal_code = normal_comp.code

    delete_curriculum_resp = client.post(
        "/admin/delete_competition",
        json={"username": "admin", "competition_code": curriculum_code},
    )
    assert delete_curriculum_resp.status_code == 200

    delete_normal_resp = client.post(
        "/admin/delete_competition",
        json={"username": "admin", "competition_code": normal_code},
    )
    assert delete_normal_resp.status_code == 200

    with app_module.app.app_context():
        assert app_module.Competition.query.filter_by(code=curriculum_code).first() is None
        assert app_module.Curriculum.query.filter_by(competition_id=curriculum_comp.id).first() is None
        assert app_module.Competition.query.filter_by(code=normal_code).first() is None
