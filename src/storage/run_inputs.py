import json
import os
from datetime import datetime, timezone

from storage.db import init_schema, open_connection
from storage.runs import get_run
from vcf_validation import validate_vcf_path


class RunInputRunNotFoundError(Exception):
    pass


def _uploads_root_for_db(db_path: str) -> str:
    if db_path == ":memory:":
        raise ValueError("run input attachments require a filesystem-backed database path")
    instance_dir = os.path.dirname(os.path.abspath(db_path))
    return os.path.join(instance_dir, "uploads")


def _run_upload_dir(db_path: str, run_id: str) -> str:
    return os.path.join(_uploads_root_for_db(db_path), run_id)


def _stored_filename(original_filename: str) -> str:
    lowered = (original_filename or "").lower()
    if lowered.endswith(".vcf.gz"):
        return "input.vcf.gz"
    return "input.vcf"


def store_run_vcf(db_path: str, run_id: str, *, file_storage) -> dict:
    run = get_run(db_path, run_id)
    if not run:
        raise RunInputRunNotFoundError("Run not found.")

    original_filename = getattr(file_storage, "filename", "") or ""
    stored_filename = _stored_filename(original_filename)

    upload_dir = _run_upload_dir(db_path, run_id)
    os.makedirs(upload_dir, exist_ok=True)

    for entry in os.listdir(upload_dir):
        try:
            os.remove(os.path.join(upload_dir, entry))
        except OSError:
            pass

    stored_path = os.path.join(upload_dir, stored_filename)
    file_storage.save(stored_path)

    validation = validate_vcf_path(stored_path)
    uploaded_at = datetime.now(timezone.utc).isoformat()

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute(
            """
            INSERT INTO run_inputs (
              run_id,
              original_filename,
              stored_filename,
              uploaded_at,
              validation_ok,
              validation_errors_json,
              validation_warnings_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
              original_filename = excluded.original_filename,
              stored_filename = excluded.stored_filename,
              uploaded_at = excluded.uploaded_at,
              validation_ok = excluded.validation_ok,
              validation_errors_json = excluded.validation_errors_json,
              validation_warnings_json = excluded.validation_warnings_json
            """,
            (
                run_id,
                original_filename,
                stored_filename,
                uploaded_at,
                1 if validation.get("ok") else 0,
                json.dumps(validation.get("errors", [])),
                json.dumps(validation.get("warnings", [])),
            ),
        )
        conn.commit()

    return {
        "run_id": run_id,
        "original_filename": original_filename,
        "stored_filename": stored_filename,
        "uploaded_at": uploaded_at,
        "validation": validation,
    }


def get_run_input(db_path: str, run_id: str) -> dict | None:
    with open_connection(db_path) as conn:
        init_schema(conn)
        row = conn.execute(
            """
            SELECT original_filename, stored_filename, uploaded_at, validation_ok,
                   validation_errors_json, validation_warnings_json
            FROM run_inputs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    if not row:
        return None
    errors = json.loads(row[4] or "[]")
    warnings = json.loads(row[5] or "[]")
    return {
        "run_id": run_id,
        "original_filename": row[0],
        "stored_filename": row[1],
        "uploaded_at": row[2],
        "validation": {"ok": bool(row[3]), "errors": errors, "warnings": warnings},
    }
