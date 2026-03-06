# Story 1.3 Flow — Cancel a Run

## Purpose

Allow canceling a run with a durable, guarded state transition in SQLite, surfaced via API and UI.

## Flow (UI → API → DB)

1. User clicks **New run** (UI).
2. UI calls `POST /api/v1/runs` and stores the returned `run_id`.
3. User clicks **Cancel run** (UI).
4. UI calls `POST /api/v1/runs/<run_id>/cancel`.
5. API route in `src/app.py` calls `storage.runs.cancel_run(db_path, run_id)`.
6. `src/storage/runs.py` performs an atomic guarded update:
   - `queued|running` → `canceled`
7. API returns:
   - `200` on success
   - `404 RUN_NOT_FOUND` if run_id doesn’t exist
   - `409 RUN_NOT_CANCELABLE` if run exists but can’t be canceled (includes `error.details.current_status`)
8. UI shows “Canceled” and disables cancel for that run.

## Key files

- API: `src/app.py`
- Storage: `src/storage/runs.py`
- UI: `src/templates/index.html`, `src/static/run_controls.js`
- Tests: `tests/test_runs_api.py`, `tests/test_app.py`

## More detail

- `docs/run-cancel-flow.md`

