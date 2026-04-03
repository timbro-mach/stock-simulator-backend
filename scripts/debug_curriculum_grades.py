"""Diagnostic helper for curriculum grading gaps.

Usage examples:
  DATABASE_URL=sqlite:///stock_simulator.db python scripts/debug_curriculum_grades.py --competition-id 12 --username student1
  DATABASE_URL=sqlite:///stock_simulator.db python scripts/debug_curriculum_grades.py --competition-code ABC123 --user-id 45
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", os.getenv("DATABASE_URL", "sqlite:///local.db"))

from app import (  # noqa: E402
    app,
    db,
    Competition,
    User,
    Curriculum,
    CurriculumModule,
    CurriculumAssignment,
    CurriculumSubmission,
    SubmissionQuestionGrade,
    _compute_grade_summary,
    _evaluate_assignment_for_gradebook,
)


def _resolve_competition(args):
    if args.competition_id:
        return db.session.get(Competition, args.competition_id)
    if args.competition_code:
        return Competition.query.filter_by(code=args.competition_code).first()
    return None


def _resolve_user(args):
    if args.user_id:
        return db.session.get(User, args.user_id)
    if args.username:
        return User.query.filter_by(username=args.username).first()
    return None


def _fmt(value):
    if value is None:
        return "None"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def run_debug(args):
    with app.app_context():
        competition = _resolve_competition(args)
        user = _resolve_user(args)

        if not competition:
            raise SystemExit("Competition not found. Provide --competition-id or --competition-code.")
        if not user:
            raise SystemExit("User not found. Provide --user-id or --username.")

        curriculum = Curriculum.query.filter_by(competition_id=competition.id, enabled=True).first()
        print(f"competition id={competition.id} code={competition.code} name={competition.name}")
        print(f"user id={user.id} username={user.username} email={user.email}")

        if not curriculum:
            print("\n[CRITICAL] No enabled curriculum found for competition.")
            return

        print(
            f"curriculum id={curriculum.id} enabled={curriculum.enabled} "
            f"start={_fmt(curriculum.start_date)} end={_fmt(curriculum.end_date)} total_weeks={curriculum.total_weeks}"
        )

        modules = (
            CurriculumModule.query
            .filter_by(curriculum_id=curriculum.id)
            .order_by(CurriculumModule.week_number.asc())
            .all()
        )
        module_ids = [m.id for m in modules]
        assignments = CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).all() if module_ids else []
        assignments_by_module = {}
        for assignment in assignments:
            assignments_by_module.setdefault(assignment.module_id, []).append(assignment)

        submissions = CurriculumSubmission.query.filter(
            CurriculumSubmission.user_id == user.id,
            CurriculumSubmission.assignment_id.in_([a.id for a in assignments]) if assignments else False,
        ).all() if assignments else []
        submission_by_assignment = {s.assignment_id: s for s in submissions}

        print(f"\nmodules={len(modules)} assignments={len(assignments)} submissions={len(submissions)}")

        now = datetime.utcnow()
        for module in modules:
            released = module.unlock_date <= now
            print(
                f"\n[module week={module.week_number} id={module.id}] "
                f"unlock={_fmt(module.unlock_date)} due={_fmt(module.due_date)} released={released}"
            )
            for assignment in assignments_by_module.get(module.id, []):
                sub = submission_by_assignment.get(assignment.id)
                eval_row = _evaluate_assignment_for_gradebook(assignment, sub)
                question_rows = SubmissionQuestionGrade.query.filter_by(submission_id=sub.id).all() if sub else []
                has_answer_key = bool((assignment.answer_key_json or {}).get("questions"))
                print(
                    "  - "
                    f"assignment_id={assignment.id} type={assignment.type} title={assignment.title!r} points={assignment.points} "
                    f"has_answer_key={has_answer_key}"
                )
                print(
                    "    "
                    f"submission_id={_fmt(sub.id if sub else None)} submission_competition_id={_fmt(sub.competition_id if sub else None)} "
                    f"auto_graded={_fmt(sub.auto_graded if sub else None)} graded_at={_fmt(sub.graded_at if sub else None)} "
                    f"assignment_total_score={_fmt(sub.assignment_total_score if sub else None)} score={_fmt(sub.score if sub else None)} "
                    f"percentage={_fmt(sub.percentage if sub else None)}"
                )
                print(
                    "    "
                    f"gradebook_status={eval_row['status']} points_earned={eval_row['pointsEarned']} "
                    f"points_possible={eval_row['pointsPossible']} question_grade_rows={len(question_rows)}"
                )
                if sub and sub.competition_id != competition.id:
                    print("    [WARNING] Submission competition_id does not match requested competition.")

        summary = _compute_grade_summary(competition.id, user.id)
        if not summary:
            print("\n[CRITICAL] _compute_grade_summary returned None.")
            return

        overall = summary.get("grade_summary_overall", {})
        print("\n[summary]")
        print(
            "  "
            f"points_earned={overall.get('points_earned')} points_possible={overall.get('points_possible')} "
            f"percentage={overall.get('percentage')} completed_items={overall.get('completed_items')} "
            f"total_items={overall.get('total_items')} progress={overall.get('progress_percentage')}"
        )

        module_rows = summary.get("grade_summary_by_module", [])
        for row in module_rows:
            print(
                "  "
                f"module_id={row.get('module_id')} week={row.get('week_number')} status={row.get('status')} "
                f"points={row.get('pointsEarned')}/{row.get('pointsPossible')}"
            )


def parse_args():
    parser = argparse.ArgumentParser(description="Debug curriculum grading for one student in one competition.")
    parser.add_argument("--competition-id", type=int)
    parser.add_argument("--competition-code")
    parser.add_argument("--user-id", type=int)
    parser.add_argument("--username")
    return parser.parse_args()


if __name__ == "__main__":
    run_debug(parse_args())
