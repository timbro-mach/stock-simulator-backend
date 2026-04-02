# Curriculum Teacher Dashboard API Contract (April 2026)

## Instructor permission signal

For competition/account payloads used by dashboard flows (`/login`, `/user`, `/competitions`, `/competition/by_code/:code`, `/featured_competitions`, teacher curriculum endpoints), backend now returns:

- `competition_id` (canonical competition table ID).
- `competitionId` (camelCase alias where already used by existing endpoint).
- `is_instructor_for_competition` (boolean).

`is_instructor_for_competition` is true when the requesting user is any of:

1. competition creator (`competition.created_by`), or
2. an admin (`user.is_admin`).

## Submission request schema

`POST /curriculum/assignments/:assignmentId/submissions`

Required fields:

- `username: string`
- `competition_id: number` (member-account/team-account IDs are accepted and resolved to canonical competition ID)
- `answers: object | array`

Accepted `answers` formats:

### Quiz/exam

- map style: `{ "<questionId>": "<selectedValue>" }`
- array style:
  - `[{ "questionId": "...", "selectedChoice": "..." }]`
  - alias keys accepted for selected choice: `selected`, `answer`, `value`.

### Written assignment

- single response per question:
  - `[{ "questionId": "...", "response": "..." }]`
- multipart:
  - `[{ "questionId": "...", "parts": [{ "partId": "...", "response": "..." }] }]`
- map style is also accepted and normalized.

Stored canonical shape in `curriculum_submission.answers_json`:

```json
{
  "answers": [
    { "questionId": "q1", "selectedChoice": "B" },
    { "questionId": "q2", "response": "..." },
    { "questionId": "q3", "parts": [{ "partId": "a", "response": "..." }] }
  ]
}
```

## Submission response schema

`200 OK` payload contains normalized grading fields:

- `competitionId`, `competition_id`
- `assignmentId`, `userId`
- `submissionId`
- `answers` (canonical stored submission content)
- `score`
- `pointsEarned`
- `pointsPossible`
- `percentage`
- `status` (`graded`, `submitted`, or `pending_grade`)
- `autoGraded`
- `isManuallyGradable`
- `submittedAt`
- `feedback`

Quiz/exam behavior:

- If answer key exists, submission is auto-graded immediately and returns `status: "graded"`.
- If no answer key exists, submission is accepted and returns `status: "submitted"` with feedback indicating `gradingMode: "no_answer_key"`.

Written assignment behavior:

- Stored with full structured content (including `parts`).
- Returns `status: "pending_grade"` with manual grading metadata.

## Grade summary aggregation contract

All student/teacher curriculum-grade surfaces now use the same aggregation scope:

- Scope: **released modules only** (`module.unlock_date <= now`) and **graded items only** (auto-graded quiz/exam submissions + manually graded written submissions).
- Pending manual grades (`pending_grade`) are visible in item rows but excluded from denominator until graded.
- Future modules are excluded from denominator.

Every grade summary payload includes:

- `grade_summary_overall`
  - `scope` (`released_modules_graded_items`)
  - `points_earned`
  - `points_possible`
  - `percentage`
  - `letter`
- `grade_summary_by_module[]`
  - `module_id`
  - `week`
  - `points_earned`
  - `points_possible`
  - `percentage`
  - `letter`
  - `status` (`graded`, `pending_grade`, `not_started`)

Also available in camelCase aliases: `gradeSummaryOverall`, `gradeSummaryByModule`.

## Manual grading endpoint contract

`POST /curriculum/submissions/:submissionId/grade`

Required:

- `username: string` (must be competition instructor/admin)
- `score: number` **or** (`question_1_score` + `question_2_score`)

Optional:

- `feedback: string`
- `rubric_notes: string`
- `comments: string`
- `percentage: number`

Returns:

- updated submission row (`submissionId`, `gradingStatus: "graded"`, `score`, `pointsEarned`, `pointsPossible`, `feedback`, `rubricNotes`, etc.)
- `gradeSummary` (updated student summary after grade write)

## Validation errors (4xx)

`422 Unprocessable Entity` for malformed submissions:

- missing `competition_id`
- non-integer `competition_id`
- assignment/competition mismatch
- invalid `answers` shape (missing `questionId`, missing `selectedChoice` for quiz, invalid/missing `parts[].response`, etc.)

## Migration note

No DB schema migration is required for this change. Existing JSON and dict-shaped submissions remain readable; all new writes are normalized to the canonical `{"answers":[...]}` shape.
