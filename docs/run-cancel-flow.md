# Run Create + Cancel Flow (DB → Storage → API → UI)

This document describes how creating a run and canceling a run flows through the system.

## Components

- **Database**: SQLite file at `instance/sp.db` by default (overridable via `SP_DB_PATH`).
- **Storage layer**: `src/storage/db.py`, `src/storage/runs.py`.
- **API layer (Flask)**: `src/app.py`.
- **UI**: `src/templates/index.html` + `src/static/run_controls.js`.

## Data model (SQLite)

Schema is initialized on-demand via `init_schema()`:

- Table: `runs`
  - `run_id` (TEXT, primary key)
  - `status` (TEXT, not null)
  - `created_at` (TEXT, not null, ISO-8601 UTC)

## Run statuses used here

- `queued`: run exists and is waiting to start
- `running`: run is in-progress (not fully implemented yet; included for cancel guardrails)
- `canceled`: run has been canceled and should not do further work

## Storage layer flow

### Create run (`create_run`)

File: `src/storage/runs.py`

1. Generate a UUID `run_id`.
2. Set initial `status = "queued"`.
3. Set `created_at = datetime.now(timezone.utc).isoformat()`.
4. Open a SQLite connection (`open_connection(db_path)`), initialize schema, insert the row, commit.
5. Return a dict: `{run_id, status, created_at}`.

### Cancel run (`cancel_run`)

File: `src/storage/runs.py`

Cancellation is an **atomic, guarded update**:

1. Run `UPDATE runs SET status = "canceled" WHERE run_id = ? AND status IN ("queued", "running")`.
2. If the update modified a row:
   - Fetch the run row and return it.
3. If the update modified no rows:
   - If `run_id` does not exist: raise `RunNotFoundError`.
   - If `run_id` exists but status is not in (`queued`, `running`): raise `RunNotCancelableError(current_status)`.

This cleanly separates:

- **404** cases (unknown run) vs
- **409** cases (known run, but not cancelable from current status).

## API layer flow

File: `src/app.py`

### Create run endpoint

- Route: `POST /api/v1/runs`
- Calls: `storage.runs.create_run(SP_DB_PATH)`
- Success response:
  - HTTP 200
  - `{ "ok": true, "data": { "run_id": "...", "status": "queued", "created_at": "..." } }`
- Failure response:
  - HTTP 500 with `{ ok: false, error: { code: "RUN_CREATE_FAILED", message: "..." } }`

### Cancel run endpoint

- Route: `POST /api/v1/runs/<run_id>/cancel`
- Calls: `storage.runs.cancel_run(SP_DB_PATH, run_id)`
- Success response:
  - HTTP 200
  - `{ "ok": true, "data": { "run_id": "...", "status": "canceled", "created_at": "..." } }`
- Not found:
  - HTTP 404
  - `{ "ok": false, "error": { "code": "RUN_NOT_FOUND", "message": "Run not found." } }`
- Not cancelable:
  - HTTP 409
  - `{ "ok": false, "error": { "code": "RUN_NOT_CANCELABLE", "message": "Run is not cancelable.", "details": { "current_status": "..." } } }`
- Unexpected error:
  - HTTP 500
  - `{ "ok": false, "error": { "code": "RUN_CANCEL_FAILED", "message": "Failed to cancel run." } }`

## UI flow

Files: `src/templates/index.html`, `src/static/run_controls.js`

### New run

1. User clicks **New run**.
2. UI sends `POST /api/v1/runs`.
3. On success:
   - UI stores `currentRunId`.
   - UI updates the Run ID and Status fields.
   - **Cancel run** becomes enabled.
4. On failure:
   - UI displays an error message (rendered via `textContent`).

### Cancel run

1. User clicks **Cancel run**.
2. UI sends `POST /api/v1/runs/<run_id>/cancel`.
3. On success:
   - UI updates status to **Canceled** (styled prominently).
   - **Cancel run** becomes disabled for that run.
4. On failure:
   - UI displays the API error message (and the API may also provide `details.current_status` for debugging).

## How to validate quickly

- Unit tests:
  - `.\.venv\Scripts\python.exe -m unittest discover -s tests`
- Manual sanity:
  1. Start the app, open `/`.
  2. Click **New run**, observe `Queued`.
  3. Click **Cancel run**, observe `Canceled` and the button disables.

