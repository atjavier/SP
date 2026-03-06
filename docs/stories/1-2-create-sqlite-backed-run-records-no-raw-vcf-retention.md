# Story 1.2 Flow — Create a Run (SQLite-backed)

## Purpose

Create a durable run record in SQLite (no raw VCF storage in this story) and return it via a JSON API.

## Flow (API → DB)

1. Client calls `POST /api/v1/runs`.
2. Route handler in `src/app.py` calls `storage.runs.create_run(db_path)`.
3. `src/storage/runs.py` opens SQLite via `src/storage/db.py` helpers.
4. Schema is initialized if needed (`runs` table).
5. A row is inserted into `runs` with:
   - `run_id` (UUID)
   - `status` (`queued` as of Story 1.3 alignment)
   - `created_at` (ISO-8601 UTC)
6. API returns `{ ok: true, data: { run_id, status, created_at } }`.

## Data

- SQLite file default: `<repo_root>\instance\sp.db` (configurable via `SP_DB_PATH`).
- Table: `runs`.

## Key files

- API: `src/app.py`
- Storage: `src/storage/db.py`, `src/storage/runs.py`
- Tests: `tests/test_runs_api.py`

