"""Move a competition's cumulative Final Exam out of the last curriculum week
into its own dedicated week, without disturbing student work.

Background
----------
``generate_curriculum_for_competition`` attaches the cumulative final exam as an
``exam``-type ``CurriculumAssignment`` on the *last* curriculum module (the week
where ``week == total_weeks``). For cohorts that want the final exam to stand on
its own, that means it shows up *inside* the last content week (e.g. Week 7).

This one-off migration detaches that exam assignment and re-parents it onto a new
module appended after the curriculum modules (e.g. Week 8: "Final Exam"). The new
week opens when the previous last module is due (i.e. once the curriculum modules
are finished) and is due one week later; the curriculum end date is extended so
the exam has its own window. No modules, assignments, or submissions are deleted,
so all student work is preserved.

The script is idempotent: if the exam already lives on its own dedicated week it
reports "nothing to do" and exits. It runs as a dry-run by default; pass
``--apply`` to persist the changes.

Usage
-----
    python scripts/move_final_exam_to_own_week.py --competition b3ab1983
    python scripts/move_final_exam_to_own_week.py --competition b3ab1983 --apply

``--competition`` accepts either the competition code (preferred) or, as a
fallback, an exact competition name.
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    app,
    db,
    Competition,
    Curriculum,
    CurriculumModule,
    CurriculumAssignment,
)

# Number of days the final exam stays open after the curriculum modules finish.
EXAM_WINDOW_DAYS = 7


def _resolve_competition(identifier):
    """Resolve a competition by code first, then by exact name."""
    competition = Competition.query.filter_by(code=identifier).first()
    if competition:
        return competition
    return Competition.query.filter_by(name=identifier).first()


def migrate(identifier, apply_changes):
    competition = _resolve_competition(identifier)
    if not competition:
        print(f"ERROR: no competition found for '{identifier}' (tried code, then name).")
        return 1

    print(f"Competition: {competition.name!r} (code={competition.code}, id={competition.id})")

    curriculum = Curriculum.query.filter_by(competition_id=competition.id).first()
    if not curriculum:
        print("ERROR: this competition has no curriculum.")
        return 1

    modules = (
        CurriculumModule.query.filter_by(curriculum_id=curriculum.id)
        .order_by(CurriculumModule.week_number.asc())
        .all()
    )
    if not modules:
        print("ERROR: curriculum has no modules.")
        return 1

    exams = (
        CurriculumAssignment.query.filter(
            CurriculumAssignment.module_id.in_([m.id for m in modules]),
            CurriculumAssignment.type == "exam",
        )
        .all()
    )
    if not exams:
        print("ERROR: no exam-type assignment found in this curriculum; nothing to move.")
        return 1
    if len(exams) > 1:
        print(f"ERROR: expected exactly one exam assignment, found {len(exams)}. Aborting.")
        return 1

    exam = exams[0]
    modules_by_id = {m.id: m for m in modules}
    host_module = modules_by_id[exam.module_id]
    siblings = CurriculumAssignment.query.filter_by(module_id=host_module.id).all()

    # Idempotency: the exam already sits on a dedicated week (no other assignments).
    if len(siblings) == 1:
        print(
            f"Nothing to do: the final exam already lives on its own week "
            f"({host_module.title!r}, week {host_module.week_number})."
        )
        return 0

    max_week = max(m.week_number for m in modules)
    new_week = max_week + 1
    unlock_date = host_module.due_date
    due_date = host_module.due_date + timedelta(days=EXAM_WINDOW_DAYS)
    new_end_date = max(curriculum.end_date, due_date)

    print()
    print("Planned changes:")
    print(f"  - Detach exam {exam.title!r} (id={exam.id}) from {host_module.title!r} "
          f"(week {host_module.week_number}).")
    print(f"  - Create new module 'Week {new_week}: Final Exam' (week_number={new_week}).")
    print(f"      unlock_date = {unlock_date.isoformat()} (when the curriculum modules finish)")
    print(f"      due_date    = {due_date.isoformat()} (+{EXAM_WINDOW_DAYS} days)")
    print(f"      prerequisite_module_id = {host_module.id} (unlocks after the last content week)")
    print(f"  - Re-parent the exam assignment onto the new module.")
    print(f"  - curriculum.total_weeks: {curriculum.total_weeks} -> {new_week}")
    print(f"  - curriculum.end_date:    {curriculum.end_date.isoformat()} -> {new_end_date.isoformat()}")
    print(f"  - Student submissions left untouched: {exam.title!r} keeps assignment id {exam.id}.")
    print()

    if not apply_changes:
        print("Dry run only. Re-run with --apply to persist these changes.")
        return 0

    new_module = CurriculumModule(
        curriculum_id=curriculum.id,
        week_number=new_week,
        title=f"Week {new_week}: Final Exam",
        description=(
            "Cumulative final exam covering every curriculum module. This week "
            "opens once the curriculum modules are finished."
        ),
        lesson_content=(
            "The final exam is a cumulative, multiple-choice assessment spanning "
            "all prior modules. Review each week's eText before you begin."
        ),
        unlock_date=unlock_date,
        due_date=due_date,
        prerequisite_module_id=host_module.id,
        passing_threshold=host_module.passing_threshold,
    )
    db.session.add(new_module)
    db.session.flush()  # assign new_module.id

    exam.module_id = new_module.id
    curriculum.total_weeks = new_week
    curriculum.end_date = new_end_date

    db.session.commit()
    print(f"Done. Final exam moved to module id {new_module.id} (week {new_week}).")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--competition",
        required=True,
        help="Competition code (preferred) or exact name.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the changes. Without this flag the script is a dry run.",
    )
    args = parser.parse_args()

    with app.app_context():
        return migrate(args.competition, args.apply)


if __name__ == "__main__":
    sys.exit(main())
