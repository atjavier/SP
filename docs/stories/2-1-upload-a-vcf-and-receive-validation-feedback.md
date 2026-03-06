# Story 2.1 Flow — Upload VCF + Validation Feedback

## Purpose

Upload (or replace) a run’s VCF file, validate it immediately, and show errors/warnings in the UI before any pipeline execution.

Raw VCF is stored only as a **temporary run attachment** on disk (not in SQLite).

## Flow (UI → API → filesystem + DB → UI)

1. User clicks **New run**.
   - UI calls `POST /api/v1/runs` and stores `run_id` (in `localStorage` for refresh persistence).
2. User selects a `.vcf` or `.vcf.gz` file and clicks **Upload & Validate**.
3. UI sends multipart form data to:
   - `POST /api/v1/runs/<run_id>/vcf` with `vcf_file`.
4. API route in `src/app.py` calls `storage.run_inputs.store_run_vcf(db_path, run_id, file_storage=...)`.
5. Storage layer:
   - writes the file under `instance/uploads/<run_id>/input.vcf` (or `input.vcf.gz`)
   - runs `src/vcf_validation.py` to produce `{ ok, errors, warnings }`
   - upserts `run_inputs` row in SQLite with filename + uploaded_at + validation JSON (no raw VCF blobs)
6. API returns `{ ok: true, data: { run_id, ..., validation: { ok, errors, warnings } } }`.
7. UI renders:
   - a success/warning/error banner
   - error list (blocking) and warning list (non-blocking)
8. If user uploads a different file again, the stored attachment is replaced and validation results update.

## Key files

- API: `src/app.py`
- Validation: `src/vcf_validation.py`
- Storage: `src/storage/run_inputs.py`, `src/storage/db.py`
- UI: `src/templates/index.html`, `src/static/upload_controls.js`, `src/static/run_controls.js`
- Tests: `tests/test_vcf_validation.py`, `tests/test_vcf_upload_api.py`, `tests/test_app.py`

## Notes

- The UI can reload the latest validation on refresh via `GET /api/v1/runs/<run_id>/vcf`.
- Upload size is capped by `SP_MAX_UPLOAD_BYTES` (default: 50 MiB). Requests over the limit return `413` with `UPLOAD_TOO_LARGE`.
