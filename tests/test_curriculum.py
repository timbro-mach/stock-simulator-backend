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


def _setup_teacher_dashboard_case(client, app_module, competition_name="Teacher Dashboard Cup"):
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_user(app_module, username="outsider", email="outsider@example.com")

    resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": competition_name,
        "curriculumEnabled": True,
        "curriculumWeeks": 3,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-01",
    })
    assert resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name=competition_name).first()
        student = app_module.User.query.filter_by(username="student").first()
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()
        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        first_module = app_module.CurriculumModule.query.filter_by(curriculum_id=curriculum.id, week_number=1).first()
        quiz = app_module.CurriculumAssignment.query.filter_by(module_id=first_module.id, type="quiz").first()
        written = app_module.CurriculumAssignment.query.filter_by(module_id=first_module.id, type="assignment").first()
        return comp.id, comp.code, student.id, quiz.id, written.id


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
            "competition_id": competition_id,
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


def test_curriculum_grades_prefers_requester_membership_when_member_id_collides_with_competition_id(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_user(app_module, username="dummy", email="dummy@example.com")

    target_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Collision Target Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-15",
    })
    assert target_resp.status_code == 200

    other_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Collision Other Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 4,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-15",
    })
    assert other_resp.status_code == 200

    with app_module.app.app_context():
        target_comp = app_module.Competition.query.filter_by(name="Collision Target Cup").first()
        other_comp = app_module.Competition.query.filter_by(name="Collision Other Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        dummy = app_module.User.query.filter_by(username="dummy").first()

        app_module.db.session.add(app_module.CompetitionMember(
            competition_id=target_comp.id,
            user_id=dummy.id,
            cash_balance=100000,
        ))
        app_module.db.session.commit()

        student_member = app_module.CompetitionMember(
            competition_id=target_comp.id,
            user_id=student.id,
            cash_balance=100000,
        )
        app_module.db.session.add(student_member)
        app_module.db.session.commit()

        assert student_member.id == other_comp.id

        colliding_member_id = student_member.id
        expected_competition_id = target_comp.id
        student_id = student.id

    resp = client.get(
        f"/curriculum/competition/{colliding_member_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["competitionId"] == expected_competition_id


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
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": {"a": "test"}, "a2": {"a": "12.5%"}}},
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


def test_teacher_dashboard_roster_and_student_detail_endpoints(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, quiz_id, written_id = _setup_teacher_dashboard_case(client, app_module)

    with app_module.app.app_context():
        quiz = app_module.db.session.get(app_module.CurriculumAssignment, quiz_id)
        answer_key = quiz.answer_key_json["questions"]

    quiz_submit_resp = client.post(
        f"/curriculum/assignments/{quiz_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": answer_key},
    )
    assert quiz_submit_resp.status_code == 200
    assert quiz_submit_resp.get_json()["status"] == "graded"

    written_submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"q1": "analysis", "q2": "reflection"}},
    )
    assert written_submit_resp.status_code == 200
    assert written_submit_resp.get_json()["status"] == "pending_grade"

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    roster_payload = roster_resp.get_json()
    assert roster_payload["roster"]
    row = roster_payload["roster"][0]
    assert row["userId"] == student_id
    assert row["completedQuizzes"] == 1
    assert row["completedAssignments"] == 1

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    assert detail_resp.status_code == 200
    detail_payload = detail_resp.get_json()
    assert detail_payload["student"]["userId"] == student_id
    assignment_item = next(item for item in detail_payload["items"] if item["assignmentType"] == "assignment")
    assert assignment_item["gradingStatus"] == "pending_grade"
    assert assignment_item["isManuallyGradable"] is True
    assert assignment_item["submissionContent"]


def test_teacher_manual_grade_updates_summary_and_trade_blotter_endpoint(app_client, monkeypatch):
    client, app_module = app_client
    competition_id, competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Teacher Grade And Trades Cup"
    )

    written_submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"q1": "alpha", "q2": "beta"}},
    )
    assert written_submit_resp.status_code == 200

    list_resp = client.get(
        f"/curriculum/assignments/{written_id}/submissions",
        query_string={"username": "teacher"},
    )
    submission_id = list_resp.get_json()["submissions"][0]["submissionId"]

    pre_detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    pre_assignment_item = next(item for item in pre_detail_resp.get_json()["items"] if item["assignmentType"] == "assignment")
    assert pre_assignment_item["gradingStatus"] == "pending_grade"
    assert pre_assignment_item["pointsEarned"] == 0.0

    grade_resp = client.post(
        f"/curriculum/submissions/{submission_id}/grade",
        json={"username": "teacher", "score": 18, "feedback": "Strong submission", "rubric_notes": "Clear structure"},
    )
    assert grade_resp.status_code == 200
    grade_payload = grade_resp.get_json()
    assert grade_payload["score"] == 18
    assert grade_payload["status"] == "graded"
    assert grade_payload["gradingStatus"] == "graded"
    assert grade_payload["isManuallyGradable"] is True
    assert grade_payload["gradeSummary"]["totalPointsEarned"] >= 18

    post_detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    post_assignment_item = next(item for item in post_detail_resp.get_json()["items"] if item["assignmentType"] == "assignment")
    assert post_assignment_item["gradingStatus"] == "graded"
    assert post_assignment_item["pointsEarned"] == 18.0
    assert post_assignment_item["rubricNotes"] == "Clear structure"

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)
    trade_resp = client.post(
        "/competition/buy",
        json={"username": "student", "competition_id": competition_id, "competition_code": competition_code, "symbol": "AAPL", "quantity": 1},
    )
    assert trade_resp.status_code == 200

    trades_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}/trades",
        query_string={"username": "teacher"},
    )
    assert trades_resp.status_code == 200
    trades = trades_resp.get_json()["trades"]
    assert trades
    assert trades[0]["symbol"] == "AAPL"
    assert trades[0]["side"] == "buy"


def test_teacher_question_grade_endpoint_updates_submission_totals(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Question Grade Totals Cup"
    )
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": "x", "a2": "y"}},
    )
    assert submit_resp.status_code == 200

    with app_module.app.app_context():
        sub = app_module.CurriculumSubmission.query.filter_by(
            assignment_id=written_id,
            user_id=student_id,
        ).first()
        submission_id = sub.id

    grade_resp = client.post(
        f"/teacher/submissions/{submission_id}/question-grades",
        json={
            "username": "teacher",
            "grades": [
                {"questionId": "a1", "pointsAwarded": 8, "pointsPossible": 10, "feedback": "Good"},
                {"questionId": "a2", "pointsAwarded": 7, "pointsPossible": 10, "feedback": "Solid"},
            ],
            "finalFeedback": "Overall good work",
        },
    )
    assert grade_resp.status_code == 200
    payload = grade_resp.get_json()
    assert payload["pointsEarned"] == 15
    assert payload["pointsPossible"] == 20
    assert payload["percentage"] == 75.0
    assert payload["status"] == "graded"
    assert len(payload["questionGrades"]) == 2


def test_student_and_teacher_grade_views_match_after_manual_question_grading(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Grade Sync Cup"
    )
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": "x", "a2": "y"}},
    )
    assert submit_resp.status_code == 200

    with app_module.app.app_context():
        sub = app_module.CurriculumSubmission.query.filter_by(assignment_id=written_id, user_id=student_id).first()
        submission_id = sub.id

    grade_resp = client.post(
        f"/teacher/submissions/{submission_id}/question-grades",
        json={
            "username": "teacher",
            "grades": [{"questionId": "a1", "pointsAwarded": 9, "pointsPossible": 10}],
            "finalFeedback": "Partial but graded",
        },
    )
    assert grade_resp.status_code == 200

    student_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    teacher_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    assert student_resp.status_code == 200
    assert teacher_resp.status_code == 200
    student_payload = student_resp.get_json()
    teacher_payload = teacher_resp.get_json()
    assert student_payload["totalPointsEarned"] == teacher_payload["gradeSummary"]["totalPointsEarned"]
    assert student_payload["totalPointsPossible"] == teacher_payload["gradeSummary"]["totalPointsPossible"]
    assert student_payload["percentage"] == teacher_payload["gradeSummary"]["curriculumPercentage"]


def test_instructor_alias_endpoints_return_consistent_non_zero_grade_summaries_after_grading(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Instructor Alias Grade Sync Cup"
    )
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": "x", "a2": "y"}},
    )
    assert submit_resp.status_code == 200

    with app_module.app.app_context():
        sub = app_module.CurriculumSubmission.query.filter_by(assignment_id=written_id, user_id=student_id).first()
        submission_id = sub.id

    grade_resp = client.post(
        f"/curriculum/submissions/{submission_id}/grade",
        json={"username": "teacher", "score": 17, "feedback": "Good effort"},
    )
    assert grade_resp.status_code == 200
    assert grade_resp.get_json()["status"] == "graded"

    student_grades_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    instructor_student_resp = client.get(
        f"/curriculum/competition/{competition_id}/instructor/students/{student_id}",
        query_string={"username": "teacher"},
    )
    instructor_roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/instructor/roster",
        query_string={"username": "teacher"},
    )

    assert student_grades_resp.status_code == 200
    assert instructor_student_resp.status_code == 200
    assert instructor_roster_resp.status_code == 200

    student_payload = student_grades_resp.get_json()
    detail_payload = instructor_student_resp.get_json()
    roster_payload = instructor_roster_resp.get_json()
    roster_row = next(row for row in roster_payload["roster"] if row["userId"] == student_id)

    assert student_payload["totalPointsEarned"] == 17.0
    assert student_payload["totalPointsPossible"] == 20.0
    assert student_payload["percentage"] == 85.0
    assert student_payload["letterGrade"] == "B"
    assert student_payload["gradeSummaryByModule"]

    assert detail_payload["gradeSummary"]["totalPointsEarned"] == 17.0
    assert detail_payload["gradeSummary"]["totalPointsPossible"] == 20.0
    assert detail_payload["gradeSummary"]["percentage"] == 85.0
    assert detail_payload["gradeSummary"]["letterGrade"] == "B"
    assert detail_payload["gradeSummaryByModule"]

    assert roster_row["totalPointsEarned"] == 17.0
    assert roster_row["totalPointsPossible"] == 20.0
    assert roster_row["percentage"] == 85.0
    assert roster_row["letterGrade"] == "B"
    assert roster_row["gradeSummaryByModule"]


def test_grade_summary_counts_graded_quiz_even_when_module_unlock_is_future(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")

    create_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Future Unlock Grade Sync Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 3,
        "curriculumStartDate": "2099-01-01",
        "curriculumEndDate": "2099-02-01",
    })
    assert create_resp.status_code == 200

    with app_module.app.app_context():
        comp = app_module.Competition.query.filter_by(name="Future Unlock Grade Sync Cup").first()
        student = app_module.User.query.filter_by(username="student").first()
        app_module.db.session.add(app_module.CompetitionMember(competition_id=comp.id, user_id=student.id, cash_balance=100000))
        app_module.db.session.commit()
        curriculum = app_module.Curriculum.query.filter_by(competition_id=comp.id).first()
        first_module = app_module.CurriculumModule.query.filter_by(curriculum_id=curriculum.id, week_number=1).first()
        quiz = app_module.CurriculumAssignment.query.filter_by(module_id=first_module.id, type="quiz").first()
        answers = dict((quiz.answer_key_json or {}).get("questions", {}))
        competition_id = comp.id
        student_id = student.id
        quiz_id = quiz.id

    submit_resp = client.post(
        f"/curriculum/assignments/{quiz_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": answers},
    )
    assert submit_resp.status_code == 200
    assert submit_resp.get_json()["status"] == "graded"

    student_grades = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert student_grades.status_code == 200
    student_payload = student_grades.get_json()
    assert student_payload["totalPointsEarned"] == 20.0
    assert student_payload["totalPointsPossible"] == 20.0
    assert student_payload["percentage"] == 100.0
    assert student_payload["completedItems"] == 1
    assert student_payload["totalItems"] >= 1
    assert student_payload["progressPercentage"] > 0
    assert student_payload["gradeSummaryByModule"]

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    roster_row = next(row for row in roster_resp.get_json()["roster"] if row["userId"] == student_id)
    assert roster_row["totalPointsEarned"] == 20.0
    assert roster_row["totalPointsPossible"] == 20.0
    assert roster_row["curriculumPercentage"] == 100.0
    assert roster_row["progressPercentage"] > 0


def test_partial_grading_marks_assignment_as_graded(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Partial Rule Cup"
    )
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": "x", "a2": "y"}},
    )
    assert submit_resp.status_code == 200
    with app_module.app.app_context():
        sub = app_module.CurriculumSubmission.query.filter_by(assignment_id=written_id, user_id=student_id).first()
        submission_id = sub.id

    grade_resp = client.post(
        f"/teacher/submissions/{submission_id}/question-grades",
        json={"username": "teacher", "grades": [{"questionId": "a1", "pointsAwarded": 5, "pointsPossible": 10}]},
    )
    assert grade_resp.status_code == 200
    assert grade_resp.get_json()["status"] == "graded"

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    assignment_item = next(item for item in detail_resp.get_json()["items"] if item["assignmentType"] == "assignment")
    assert assignment_item["gradingStatus"] == "graded"


def test_null_safe_percentages_when_no_graded_work_exists(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, _written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Null Safe Grades Cup"
    )
    student_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    teacher_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    assert student_resp.status_code == 200
    assert teacher_resp.status_code == 200
    assert student_resp.get_json()["percentage"] is None
    assert teacher_resp.get_json()["gradeSummary"]["curriculumPercentage"] is None


def test_regrading_upsert_replaces_question_scores_and_recalculates(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Regrade Upsert Cup"
    )
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"a1": "x", "a2": "y"}},
    )
    assert submit_resp.status_code == 200
    with app_module.app.app_context():
        sub = app_module.CurriculumSubmission.query.filter_by(assignment_id=written_id, user_id=student_id).first()
        submission_id = sub.id

    first_grade = client.post(
        f"/teacher/submissions/{submission_id}/question-grades",
        json={"username": "teacher", "grades": [{"questionId": "a1", "pointsAwarded": 8, "pointsPossible": 10}]},
    )
    assert first_grade.status_code == 200
    assert first_grade.get_json()["pointsEarned"] == 8

    regrade = client.post(
        f"/teacher/submissions/{submission_id}/question-grades",
        json={"username": "teacher", "grades": [{"questionId": "a1", "pointsAwarded": 10, "pointsPossible": 10}]},
    )
    assert regrade.status_code == 200
    payload = regrade.get_json()
    assert payload["pointsEarned"] == 10
    assert payload["percentage"] == 100.0


def test_grade_summary_uses_released_graded_scope_and_95_percent_quiz_case(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, quiz_id, _written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Released Scope Cup"
    )
    with app_module.app.app_context():
        quiz = app_module.db.session.get(app_module.CurriculumAssignment, quiz_id)
        answer_items = list(quiz.answer_key_json["questions"].items())
        mostly_correct_answers = {
            qid: ("__wrong__" if idx == 0 else expected)
            for idx, (qid, expected) in enumerate(answer_items)
        }

    submit_resp = client.post(
        f"/curriculum/assignments/{quiz_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": mostly_correct_answers},
    )
    assert submit_resp.status_code == 200
    assert submit_resp.get_json()["score"] == 19

    student_grade_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert student_grade_resp.status_code == 200
    student_grade_payload = student_grade_resp.get_json()
    assert student_grade_payload["totalPointsEarned"] == 19.0
    assert student_grade_payload["totalPointsPossible"] == 20.0
    assert student_grade_payload["percentage"] == 95.0
    assert student_grade_payload["grade_summary_overall"]["percentage"] == 95.0
    assert student_grade_payload["grade_summary_by_module"]

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    roster_row = next(row for row in roster_resp.get_json()["roster"] if row["userId"] == student_id)
    assert roster_row["curriculumPercentage"] == 95.0
    assert roster_row["totalPointsEarned"] == 19.0
    assert roster_row["totalPointsPossible"] == 20.0
    assert roster_row["grade_summary_by_module"]

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    assert detail_resp.status_code == 200
    detail_payload = detail_resp.get_json()
    assert detail_payload["gradeSummary"]["curriculumPercentage"] == 95.0
    assert detail_payload["grade_summary_by_module"] == roster_row["grade_summary_by_module"]


def test_pending_written_submission_exposes_manual_grading_metadata_and_transitions(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Pending Metadata Cup"
    )

    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"q1": "alpha", "q2": "beta"}},
    )
    assert submit_resp.status_code == 200
    submit_payload = submit_resp.get_json()
    assert submit_payload["status"] == "pending_grade"
    assert submit_payload["isManuallyGradable"] is True
    submission_id = submit_payload["submissionId"]

    detail_before = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    ).get_json()
    before_item = next(item for item in detail_before["items"] if item["assignmentType"] == "assignment")
    assert before_item["gradingStatus"] == "pending_grade"
    assert before_item["submissionId"] == submission_id

    grade_resp = client.post(
        f"/curriculum/submissions/{submission_id}/grade",
        json={
            "username": "teacher",
            "score": 16,
            "feedback": "Solid analysis",
            "rubric_notes": "Meets expectations",
        },
    )
    assert grade_resp.status_code == 200
    grade_payload = grade_resp.get_json()
    assert grade_payload["gradingStatus"] == "graded"

    detail_after = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    ).get_json()
    after_item = next(item for item in detail_after["items"] if item["assignmentType"] == "assignment")
    assert after_item["gradingStatus"] == "graded"
    assert after_item["pointsEarned"] == 16.0


def test_student_cannot_access_teacher_dashboard_routes(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Teacher Auth Guard Cup"
    )

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "student"},
    )
    assert roster_resp.status_code == 403

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "student"},
    )
    assert detail_resp.status_code == 403

    list_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": {"q1": "x", "q2": "y"}},
    )
    submission_id = client.get(
        f"/curriculum/assignments/{written_id}/submissions",
        query_string={"username": "teacher"},
    ).get_json()["submissions"][0]["submissionId"]
    assert list_resp.status_code == 200

    forbidden_grade_resp = client.post(
        f"/curriculum/submissions/{submission_id}/grade",
        json={"username": "student", "score": 10},
    )
    assert forbidden_grade_resp.status_code == 403


def test_competition_creator_can_access_teacher_endpoints_with_member_account_id(app_client):
    client, app_module = app_client
    competition_id, _competition_code, _student_id, _quiz_id, _written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Teacher Creator Access Cup"
    )
    with app_module.app.app_context():
        teacher = app_module.User.query.filter_by(username="teacher").first()
        teacher_member = app_module.CompetitionMember.query.filter_by(
            competition_id=competition_id,
            user_id=teacher.id,
        ).first()
        if not teacher_member:
            teacher_member = app_module.CompetitionMember(
                competition_id=competition_id,
                user_id=teacher.id,
                cash_balance=100000,
            )
            app_module.db.session.add(teacher_member)
            app_module.db.session.commit()
        teacher_member_account_id = teacher_member.id

    roster_resp = client.get(
        f"/curriculum/competition/{teacher_member_account_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    assert roster_resp.get_json()["competitionId"] == competition_id
    assert roster_resp.get_json()["is_instructor_for_competition"] is True


def test_teacher_roster_prefers_competition_id_when_member_id_collides(app_client):
    client, app_module = app_client
    create_user(app_module, username="teacher", email="teacher@example.com")
    create_user(app_module, username="student", email="student@example.com")
    create_user(app_module, username="other", email="other@example.com")

    baseline_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Roster Collision Baseline Cup",
        "start_date": "2026-01-01",
        "end_date": "2026-03-01",
    })
    assert baseline_resp.status_code == 200

    target_resp = _create_competition(client, {
        "username": "teacher",
        "competition_name": "Roster Collision Target Cup",
        "curriculumEnabled": True,
        "curriculumWeeks": 3,
        "curriculumStartDate": "2026-01-01",
        "curriculumEndDate": "2026-02-01",
    })
    assert target_resp.status_code == 200

    with app_module.app.app_context():
        baseline_comp = app_module.Competition.query.filter_by(name="Roster Collision Baseline Cup").first()
        target_comp = app_module.Competition.query.filter_by(name="Roster Collision Target Cup").first()
        teacher = app_module.User.query.filter_by(username="teacher").first()
        student = app_module.User.query.filter_by(username="student").first()
        other = app_module.User.query.filter_by(username="other").first()

        baseline_member_1 = app_module.CompetitionMember(
            competition_id=baseline_comp.id,
            user_id=student.id,
            cash_balance=100000,
        )
        baseline_member_2 = app_module.CompetitionMember(
            competition_id=baseline_comp.id,
            user_id=other.id,
            cash_balance=100000,
        )
        target_member = app_module.CompetitionMember(
            competition_id=target_comp.id,
            user_id=student.id,
            cash_balance=100000,
        )
        app_module.db.session.add_all([baseline_member_1, baseline_member_2, target_member])
        app_module.db.session.commit()

        assert baseline_member_2.id == target_comp.id
        target_comp_id = target_comp.id
        student_id = student.id

    roster_resp = client.get(
        f"/curriculum/competition/{target_comp_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    payload = roster_resp.get_json()
    assert payload["competitionId"] == target_comp_id
    assert len(payload["roster"]) == 1
    assert payload["roster"][0]["userId"] == student_id


def test_quiz_submission_array_format_auto_grades_and_surfaces_in_teacher_views(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, quiz_id, _written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Teacher Quiz Visibility Cup"
    )
    with app_module.app.app_context():
        quiz = app_module.db.session.get(app_module.CurriculumAssignment, quiz_id)
        answer_rows = [{"questionId": qid, "selectedChoice": expected} for qid, expected in quiz.answer_key_json["questions"].items()]

    submit_resp = client.post(
        f"/curriculum/assignments/{quiz_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": answer_rows},
    )
    assert submit_resp.status_code == 200
    payload = submit_resp.get_json()
    assert payload["status"] == "graded"
    assert payload["score"] == payload["pointsPossible"]

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    row = next(r for r in roster_resp.get_json()["roster"] if r["userId"] == student_id)
    assert row["completedQuizzes"] == 1
    assert row["latestQuizSubmission"]["gradingStatus"] == "graded"

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    quiz_item = next(item for item in detail_resp.get_json()["items"] if item["assignmentType"] == "quiz")
    assert quiz_item["pointsEarned"] > 0
    assert quiz_item["gradingStatus"] == "graded"


def test_multipart_written_submission_persists_and_visible_in_teacher_detail(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, _quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Multipart Written Cup"
    )
    multipart_answers = [
        {
            "questionId": "q1",
            "parts": [
                {"partId": "thesis", "response": "Diversification lowers idiosyncratic risk."},
                {"partId": "support", "response": "Correlation matters for portfolio volatility."},
            ],
        },
        {"questionId": "q2", "response": "I would rebalance monthly based on drawdown limits."},
    ]
    submit_resp = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={"username": "student", "competition_id": competition_id, "answers": multipart_answers},
    )
    assert submit_resp.status_code == 200
    assert submit_resp.get_json()["status"] == "pending_grade"

    detail_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/students/{student_id}",
        query_string={"username": "teacher"},
    )
    written_item = next(item for item in detail_resp.get_json()["items"] if item["assignmentType"] == "assignment")
    assert written_item["gradingStatus"] == "pending_grade"
    assert isinstance(written_item["submissionContent"], dict)
    assert written_item["submissionContent"]["answers"][0]["parts"][0]["response"].startswith("Diversification")


def test_legacy_quiz_without_answer_key_counts_toward_grade_summary(app_client):
    client, app_module = app_client
    competition_id, _competition_code, student_id, quiz_id, written_id = _setup_teacher_dashboard_case(
        client, app_module, competition_name="Legacy Quiz Keyless Cup"
    )

    with app_module.app.app_context():
        quiz = app_module.db.session.get(app_module.CurriculumAssignment, quiz_id)
        quiz.answer_key_json = None
        app_module.db.session.commit()

    quiz_submit = client.post(
        f"/curriculum/assignments/{quiz_id}/submissions",
        json={
            "username": "student",
            "competition_id": competition_id,
            "answers": [{"questionId": "q1", "selectedChoice": "Incorrect"}],
        },
    )
    assert quiz_submit.status_code == 200

    written_submit = client.post(
        f"/curriculum/assignments/{written_id}/submissions",
        json={
            "username": "student",
            "competition_id": competition_id,
            "answers": [{"questionId": "q1", "response": "Short answer"}],
        },
    )
    assert written_submit.status_code == 200

    roster_resp = client.get(
        f"/curriculum/competition/{competition_id}/teacher/roster",
        query_string={"username": "teacher"},
    )
    assert roster_resp.status_code == 200
    row = next(r for r in roster_resp.get_json()["roster"] if r["userId"] == student_id)
    assert row["completedQuizzes"] == 1
    assert row["completedAssignments"] == 1
    assert row["totalPointsPossible"] > 0
    assert row["curriculumPercentage"] == 0.0

    student_summary_resp = client.get(
        f"/curriculum/competition/{competition_id}/grades/{student_id}",
        query_string={"username": "student"},
    )
    assert student_summary_resp.status_code == 200
    summary_payload = student_summary_resp.get_json()
    assert summary_payload["totalPointsPossible"] > 0
