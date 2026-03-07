import os
import secrets
from typing import Any

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import RequestEntityTooLarge


def create_app(config_overrides: dict[str, Any] | None = None) -> Flask:
    app = Flask(__name__)
    secret_key = os.environ.get("SECRET_KEY")
    if not secret_key:
        secret_key = secrets.token_hex(32)
    app.config["SECRET_KEY"] = secret_key

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_db_path = os.path.join(project_root, "instance", "sp.db")
    app.config["SP_DB_PATH"] = os.environ.get("SP_DB_PATH", default_db_path)

    default_max_upload_bytes = int(os.environ.get("SP_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
    app.config["SP_MAX_UPLOAD_BYTES"] = default_max_upload_bytes
    app.config["MAX_CONTENT_LENGTH"] = default_max_upload_bytes

    default_max_decompressed_bytes = int(
        os.environ.get("SP_MAX_VCF_DECOMPRESSED_BYTES", str(250 * 1024 * 1024))
    )
    app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"] = default_max_decompressed_bytes

    if config_overrides:
        app.config.update(config_overrides)
        if "SP_MAX_UPLOAD_BYTES" in app.config:
            app.config["MAX_CONTENT_LENGTH"] = app.config["SP_MAX_UPLOAD_BYTES"]

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_entity_too_large(_exc):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {
                        "code": "UPLOAD_TOO_LARGE",
                        "message": "Uploaded file is too large.",
                        "details": {"max_bytes": app.config.get("SP_MAX_UPLOAD_BYTES")},
                    },
                }
            ),
            413,
        )

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.post("/api/v1/runs")
    def create_run():
        from storage.runs import create_run as create_run_record

        try:
            record = create_run_record(app.config["SP_DB_PATH"])
        except Exception:
            app.logger.exception("Failed to create run record")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CREATE_FAILED",
                            "message": "Failed to create run record.",
                        },
                    }
                ),
                500,
            )
        return jsonify({"ok": True, "data": record})

    @app.get("/api/v1/runs/<run_id>")
    def get_run(run_id: str):
        from storage.runs import get_run as get_run_record

        try:
            record = get_run_record(app.config["SP_DB_PATH"], run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_FETCH_FAILED",
                            "message": "Failed to fetch run record.",
                        },
                    }
                ),
                500,
            )

        if not record:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_NOT_FOUND",
                            "message": "Run not found.",
                        },
                    }
                ),
                404,
            )

        return jsonify({"ok": True, "data": record})

    @app.post("/api/v1/runs/<run_id>/cancel")
    def cancel_run(run_id: str):
        from storage.runs import (
            RunNotCancelableError,
            RunNotFoundError,
            cancel_run as cancel_run_record,
        )

        try:
            record = cancel_run_record(app.config["SP_DB_PATH"], run_id)
        except RunNotFoundError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_NOT_FOUND",
                            "message": "Run not found.",
                        },
                    }
                ),
                404,
            )
        except RunNotCancelableError as exc:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_NOT_CANCELABLE",
                            "message": "Run is not cancelable.",
                            "details": {"current_status": exc.current_status},
                        },
                    }
                ),
                409,
            )
        except Exception:
            app.logger.exception("Failed to cancel run")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CANCEL_FAILED",
                            "message": "Failed to cancel run.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": record})

    @app.post("/api/v1/runs/<run_id>/vcf")
    def upload_vcf(run_id: str):
        from storage.run_inputs import (
            RunInputRunNotFoundError,
            store_run_vcf,
        )

        content_length = request.content_length
        max_bytes = app.config.get("SP_MAX_UPLOAD_BYTES")
        if content_length is not None and max_bytes and content_length > max_bytes:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "UPLOAD_TOO_LARGE",
                            "message": "Uploaded file is too large.",
                            "details": {"max_bytes": max_bytes, "content_length": content_length},
                        },
                    }
                ),
                413,
            )

        file_storage = request.files.get("vcf_file")
        if not file_storage:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_FILE_MISSING",
                            "message": "No VCF file was provided.",
                        },
                    }
                ),
                400,
            )

        filename = getattr(file_storage, "filename", "") or ""
        lowered = filename.lower()
        if filename and not (lowered.endswith(".vcf") or lowered.endswith(".vcf.gz")):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "UNSUPPORTED_FILE_TYPE",
                            "message": "Only .vcf and .vcf.gz files are supported.",
                        },
                    }
                ),
                400,
            )

        try:
            input_record = store_run_vcf(
                app.config["SP_DB_PATH"],
                run_id,
                file_storage=file_storage,
            )
        except RunInputRunNotFoundError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_NOT_FOUND",
                            "message": "Run not found.",
                        },
                    }
                ),
                404,
            )
        except Exception:
            app.logger.exception("Failed to upload VCF")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_UPLOAD_FAILED",
                            "message": "Failed to upload VCF.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": input_record})

    @app.get("/api/v1/runs/<run_id>/vcf")
    def get_uploaded_vcf(run_id: str):
        from storage.run_inputs import get_run_input
        from storage.runs import get_run as get_run_record

        run = get_run_record(app.config["SP_DB_PATH"], run_id)
        if not run:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_NOT_FOUND",
                            "message": "Run not found.",
                        },
                    }
                ),
                404,
            )

        input_record = get_run_input(app.config["SP_DB_PATH"], run_id)
        return jsonify({"ok": True, "data": input_record})

    @app.post("/api/v1/runs/<run_id>/parse")
    def parse_uploaded_vcf(run_id: str):
        import os as _os
        import uuid as _uuid
        from datetime import datetime, timezone

        from storage.db import connect as _connect_db
        from storage.db import init_schema as _init_schema
        from storage.run_inputs import (
            delete_run_upload_checked,
            get_run_input,
            get_run_upload_path,
        )
        from storage.runs import get_run as get_run_record
        from storage.stages import (
            get_stage,
            mark_stage_blocked,
            mark_stage_failed,
            mark_stage_running,
            mark_stage_succeeded,
        )
        from storage.variants import clear_variants_for_run
        from vcf_parser import VcfParseError, iter_vcf_snv_records

        db_path = app.config["SP_DB_PATH"]

        run = get_run_record(db_path, run_id)
        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        if run.get("status") == "canceled":
            mark_stage_blocked(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=None,
                error_code="RUN_CANCELED",
                error_message="Run is canceled and cannot be parsed.",
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CANCELED",
                            "message": "Run is canceled and cannot be parsed.",
                        },
                    }
                ),
                409,
            )

        run_input = get_run_input(db_path, run_id)
        if not run_input:
            mark_stage_blocked(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=None,
                error_code="VCF_NOT_UPLOADED",
                error_message="No VCF uploaded for this run.",
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "VCF_NOT_UPLOADED", "message": "No VCF uploaded for this run."},
                    }
                ),
                409,
            )

        if not run_input.get("validation", {}).get("ok"):
            mark_stage_blocked(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=run_input.get("uploaded_at"),
                error_code="VCF_NOT_VALIDATED",
                error_message="Uploaded VCF is not valid; fix validation errors before parsing.",
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_NOT_VALIDATED",
                            "message": "Uploaded VCF is not valid; fix validation errors before parsing.",
                        },
                    }
                ),
                409,
            )

        uploaded_at = run_input.get("uploaded_at")
        stage = get_stage(db_path, run_id, "parser")

        force = (request.args.get("force") or "").strip().lower() in {"1", "true", "yes"}
        if stage and stage.get("status") == "running":
            if force:
                if not app.config.get("TESTING"):
                    stats_note = stage.get("started_at")
                    app.logger.warning(
                        "Force-restarting parser stage for run_id=%s (previous started_at=%s)",
                        run_id,
                        stats_note,
                    )
            else:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "STAGE_RUNNING",
                                "message": "Parser stage is already running.",
                                "details": {"started_at": stage.get("started_at")},
                            },
                        }
                    ),
                    409,
                )

        if stage and stage.get("status") == "succeeded" and stage.get("input_uploaded_at") == uploaded_at:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "ALREADY_PARSED", "message": "This upload was already parsed."},
                    }
                ),
                409,
            )

        upload_path = get_run_upload_path(db_path, run_id)
        if not upload_path or not _os.path.exists(upload_path):
            mark_stage_blocked(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=uploaded_at,
                error_code="VCF_ATTACHMENT_MISSING",
                error_message="VCF attachment is missing on disk.",
                error_details={"path": upload_path},
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_ATTACHMENT_MISSING",
                            "message": "VCF attachment is missing on disk.",
                        },
                    }
                ),
                409,
            )

        stats: dict = {}
        sample: list[dict] = []
        created_at = datetime.now(timezone.utc).isoformat()
        rows: list[tuple] = []
        attempted = 0
        inserted = 0

        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN")

                mark_stage_running(
                    db_path, run_id, "parser", input_uploaded_at=uploaded_at, conn=conn, commit=False
                )
                clear_variants_for_run(db_path, run_id, conn=conn, commit=False)

                for record in iter_vcf_snv_records(
                    upload_path,
                    stats=stats,
                    sample=sample,
                    sample_limit=10,
                    max_decompressed_bytes=app.config.get("SP_MAX_VCF_DECOMPRESSED_BYTES"),
                ):
                    rows.append(
                        (
                            str(_uuid.uuid4()),
                            run_id,
                            record["chrom"],
                            int(record["pos"]),
                            record["ref"],
                            record["alt"],
                            record.get("source_line"),
                            created_at,
                        )
                    )

                    if len(rows) >= 500:
                        attempted += len(rows)
                        before = conn.total_changes
                        conn.executemany(
                            """
                            INSERT OR IGNORE INTO run_variants (
                              variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            rows,
                        )
                        inserted += conn.total_changes - before
                        rows.clear()

                if rows:
                    attempted += len(rows)
                    before = conn.total_changes
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO run_variants (
                          variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                    inserted += conn.total_changes - before

                stats["snv_records_persisted"] = inserted
                stats["duplicate_records_ignored"] = max(0, attempted - inserted)

                mark_stage_succeeded(
                    db_path,
                    run_id,
                    "parser",
                    input_uploaded_at=uploaded_at,
                    stats=stats,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()

        except VcfParseError as exc:
            try:
                conn = _connect_db(db_path)
                try:
                    _init_schema(conn)
                    conn.execute("BEGIN")
                    clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "parser",
                        input_uploaded_at=uploaded_at,
                        error_code=exc.code,
                        error_message=exc.message,
                        error_details={"line_number": exc.line_number, **(exc.details or {})},
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                app.logger.exception("Failed to persist parser failure state")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_PARSE_FAILED",
                            "message": "Failed to parse VCF.",
                            "details": {
                                "error_code": exc.code,
                                "line_number": exc.line_number,
                                **(exc.details or {}),
                            },
                        },
                    }
                ),
                422,
            )
        except Exception as exc:
            app.logger.exception("Unexpected parse failure")
            try:
                conn = _connect_db(db_path)
                try:
                    _init_schema(conn)
                    conn.execute("BEGIN")
                    clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "parser",
                        input_uploaded_at=uploaded_at,
                        error_code="UNEXPECTED_ERROR",
                        error_message="Unexpected parser failure.",
                        error_details={"reason": str(exc)},
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                app.logger.exception("Failed to persist unexpected parser failure state")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_PARSE_FAILED",
                            "message": "Failed to parse VCF.",
                        },
                    }
                ),
                500,
            )

        cleanup = delete_run_upload_checked(db_path, run_id)
        if not cleanup.get("ok"):
            try:
                conn = _connect_db(db_path)
                try:
                    _init_schema(conn)
                    conn.execute("BEGIN")
                    clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "parser",
                        input_uploaded_at=uploaded_at,
                        error_code="CLEANUP_FAILED",
                        error_message="Failed to delete uploaded VCF after parsing.",
                        error_details={"path": upload_path, **cleanup},
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                app.logger.exception("Failed to persist cleanup failure state")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_CLEANUP_FAILED",
                            "message": "Failed to delete uploaded VCF after parsing.",
                        },
                    }
                ),
                500,
            )

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "parser": {"status": "succeeded", "stats": stats},
                    "variants_sample": sample,
                },
            }
        )

    return app


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG") == "1"
    create_app().run(debug=debug)
