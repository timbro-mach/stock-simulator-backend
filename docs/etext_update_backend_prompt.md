# Backend Prompt Template: Update eText for Specific Curriculum Modules

Use the following prompt when asking the backend/code agent to implement support for updating eText (`lesson_content`) for selected modules.

## Recommended Prompt

```
Please implement backend support for authoring/updating eText content for specific curriculum modules.

Context:
- `CurriculumModule` already stores eText in `lesson_content`.
- We can fetch modules via `GET /curriculum/competition/<competition_id>/modules`.
- Curriculum is currently generated via `POST /curriculum/competition/<competition_id>/generate`.

Goal:
Add a secure instructor-only endpoint to update `lesson_content` for one or more modules in an existing competition curriculum, without regenerating the entire curriculum.

Requirements:
1. Add endpoint:
   - `PATCH /curriculum/competition/<int:competition_id>/modules/lesson-content`
2. Request body:
   - `username` (required; instructor)
   - `updates` (required array), each item:
     - `moduleId` (required int)
     - `lessonContent` (required string)
3. Behavior:
   - Validate competition exists and curriculum is enabled.
   - Validate requester is instructor for that competition.
   - Validate each module belongs to the competition's curriculum.
   - Trim/normalize content and reject empty `lessonContent`.
   - Update only `lesson_content` (do not alter assignments, dates, titles).
   - Commit once after all valid updates.
4. Response:
   - 200 with summary:
     - `updatedCount`
     - `updatedModuleIds`
   - Use 4xx with clear `message` for validation/auth failures.
5. Tests:
   - Instructor can update multiple modules successfully.
   - Non-instructor gets 403.
   - Invalid moduleId (not in curriculum) returns 400.
   - Empty `lessonContent` returns 400.
6. Keep API style consistent with existing curriculum routes in `app.py`.

Also return an example `curl` request in the PR description showing how I can pass module eText that I provide.
```

## Example Payload You Can Fill In

```json
{
  "username": "teacher_username",
  "updates": [
    {
      "moduleId": 101,
      "lessonContent": "## Week 1 eText: ...your provided text..."
    },
    {
      "moduleId": 102,
      "lessonContent": "## Week 2 eText: ...your provided text..."
    }
  ]
}
```
