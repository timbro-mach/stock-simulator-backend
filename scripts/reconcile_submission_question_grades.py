"""One-time reconciliation for submission_question_grades.

Usage:
  DATABASE_URL=sqlite:///local.db python scripts/reconcile_submission_question_grades.py
"""

import os
from datetime import datetime

os.environ.setdefault("DATABASE_URL", os.getenv("DATABASE_URL", "sqlite:///local.db"))

from app import (  # noqa: E402
    app,
    db,
    CurriculumAssignment,
    CurriculumSubmission,
    SubmissionQuestionGrade,
)


def reconcile():
    repaired_rows = 0
    recomputed_submissions = 0
    with app.app_context():
        submissions = CurriculumSubmission.query.all()
        for sub in submissions:
            assignment = db.session.get(CurriculumAssignment, sub.assignment_id)
            if not assignment or assignment.type not in ("assignment", "written_assignment"):
                continue

            existing = SubmissionQuestionGrade.query.filter_by(submission_id=sub.id).count()
            if existing == 0 and sub.assignment_total_score is not None:
                possible = float(assignment.points or 0.0)
                row = SubmissionQuestionGrade(
                    submission_id=sub.id,
                    question_id="legacy_total",
                    points_awarded=float(sub.assignment_total_score or 0.0),
                    points_possible=possible,
                    feedback=(sub.feedback_json or {}).get("instructorComment"),
                    graded_by=sub.graded_by_user_id or sub.user_id,
                    graded_at=sub.graded_at or datetime.utcnow(),
                )
                db.session.add(row)
                repaired_rows += 1

            rows = SubmissionQuestionGrade.query.filter_by(submission_id=sub.id).all()
            if rows:
                earned = round(sum(float(r.points_awarded or 0.0) for r in rows), 2)
                possible = round(sum(float(r.points_possible or 0.0) for r in rows), 2)
                sub.assignment_total_score = earned
                sub.score = earned
                sub.percentage = round((earned / possible * 100.0), 2) if possible else 0.0
                recomputed_submissions += 1

        db.session.commit()
        print(
            f"reconciliation_complete repaired_question_grade_rows={repaired_rows} "
            f"recomputed_submissions={recomputed_submissions}"
        )


if __name__ == "__main__":
    reconcile()
