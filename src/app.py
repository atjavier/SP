import os
import secrets
import sys
import threading
import time
from typing import Any

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from werkzeug.exceptions import RequestEntityTooLarge


def create_app(config_overrides: dict[str, Any] | None = None) -> Flask:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if not (config_overrides and config_overrides.get("TESTING")):
        try:
            from env_file import load_env_file

            load_env_file(os.path.join(project_root, ".env"), override=False)
        except Exception:
            # never fail app startup because of local env file parsing
            pass

    app = Flask(__name__)
    secret_key = os.environ.get("SECRET_KEY")
    if not secret_key:
        secret_key = secrets.token_hex(32)
    app.config["SECRET_KEY"] = secret_key

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

    try:
        from storage.runs import recover_interrupted_runs

        recovered = recover_interrupted_runs(app.config["SP_DB_PATH"])
        if recovered.get("runs_recovered"):
            app.logger.warning(
                "Recovered interrupted runs at startup: runs=%s stages=%s",
                recovered.get("runs_recovered", 0),
                recovered.get("stages_recovered", 0),
            )
    except Exception:
        app.logger.exception("Failed to recover interrupted runs at startup")

    if app.config.get("TESTING"):
        # Unit tests should not execute external tools like Java/SnpEff even if
        # a developer has them configured in their shell environment.
        os.environ["SP_SNPEFF_ENABLED"] = "0"
        # Keep tests fully offline by default for evidence lookups.
        os.environ["SP_DBSNP_ENABLED"] = "0"
        os.environ["SP_CLINVAR_ENABLED"] = "0"
        os.environ["SP_GNOMAD_ENABLED"] = "0"
        # Unit tests should also avoid depending on a locally installed VEP.
        # Keep these settings on app config (not process-global env) and pass
        # them explicitly to the prediction stage via the orchestrator.
        test_tools_dir = os.path.join(os.path.dirname(app.config["SP_DB_PATH"]), ".test-tools")
        os.makedirs(test_tools_dir, exist_ok=True)
        vep_cache_dir = os.path.join(test_tools_dir, "vep-cache")
        os.makedirs(vep_cache_dir, exist_ok=True)
        alpha_file_path = os.path.join(test_tools_dir, "alphamissense.tsv")
        if not os.path.isfile(alpha_file_path):
            with open(alpha_file_path, "w", encoding="utf-8", newline="\n") as f:
                f.write("# deterministic test fixture\n")
        app.config["SP_TEST_PREDICTION_CONFIG"] = {
            "cmd": sys.executable,
            "script_path": os.path.join(project_root, "scripts", "mock_vep.py"),
            "cache_dir": vep_cache_dir,
            "alphamissense_file": alpha_file_path,
            "timeout_seconds": 30,
            "plugin_dir": None,
            "fasta_path": None,
            "extra_args": [],
        }

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

    @app.get("/api/v1/runs/<run_id>/stages")
    def get_run_stages(run_id: str):
        from storage.runs import get_run as get_run_record
        from storage.stages import list_pipeline_stages

        db_path = app.config["SP_DB_PATH"]

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for stage listing")
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

        try:
            stages = list_pipeline_stages(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run stages")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_STAGES_FETCH_FAILED",
                            "message": "Failed to fetch run stages.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stages": stages}})

    @app.get("/api/v1/runs/<run_id>/classifications")
    def get_run_classifications(run_id: str):
        from storage.classifications import list_classifications_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for classification listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "classification") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "classifications": [],
                    },
                }
            )

        try:
            rows = list_classifications_for_run(db_path, run_id, limit=limit)
        except Exception:
            app.logger.exception("Failed to fetch classifications for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "CLASSIFICATIONS_FETCH_FAILED",
                            "message": "Failed to fetch classification results.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "classifications": rows}})

    @app.get("/api/v1/runs/<run_id>/predictor_outputs")
    def get_run_predictor_outputs(run_id: str):
        from storage.predictor_outputs import list_predictor_outputs_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))

        predictor_key = (request.args.get("predictor_key") or "").strip() or None
        variant_id = (request.args.get("variant_id") or "").strip() or None

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for predictor outputs listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "prediction") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "predictor_outputs": [],
                    },
                }
            )

        try:
            rows = list_predictor_outputs_for_run(
                db_path,
                run_id,
                predictor_key=predictor_key,
                variant_id=variant_id,
                limit=limit,
            )
        except Exception:
            app.logger.exception("Failed to fetch predictor outputs for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "PREDICTOR_OUTPUTS_FETCH_FAILED",
                            "message": "Failed to fetch predictor outputs.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "predictor_outputs": rows}})

    @app.get("/api/v1/runs/<run_id>/pre_annotations")
    def get_run_pre_annotations(run_id: str):
        from storage.pre_annotations import list_pre_annotations_for_run_public
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        variant_id = (request.args.get("variant_id") or "").strip() or None

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for pre-annotation listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "pre_annotation") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "pre_annotations": [],
                    },
                }
            )

        try:
            rows = list_pre_annotations_for_run_public(
                db_path,
                run_id,
                limit=limit,
                variant_id=variant_id,
            )
        except Exception:
            app.logger.exception("Failed to fetch pre-annotations for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "PRE_ANNOTATIONS_FETCH_FAILED",
                            "message": "Failed to fetch pre-annotation results.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "pre_annotations": rows}})

    @app.get("/api/v1/runs/<run_id>/dbsnp_evidence")
    def get_run_dbsnp_evidence(run_id: str):
        from storage.dbsnp_evidence import list_dbsnp_evidence_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        variant_id = (request.args.get("variant_id") or "").strip() or None

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for dbSNP evidence listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "annotation") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "dbsnp_evidence": [],
                    },
                }
            )

        try:
            rows = list_dbsnp_evidence_for_run(
                db_path,
                run_id,
                variant_id=variant_id,
                limit=limit,
            )
        except Exception:
            app.logger.exception("Failed to fetch dbSNP evidence for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "DBSNP_EVIDENCE_FETCH_FAILED",
                            "message": "Failed to fetch dbSNP evidence.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "dbsnp_evidence": rows}})

    @app.get("/api/v1/runs/<run_id>/clinvar_evidence")
    def get_run_clinvar_evidence(run_id: str):
        from storage.clinvar_evidence import list_clinvar_evidence_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        variant_id = (request.args.get("variant_id") or "").strip() or None

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for ClinVar evidence listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "annotation") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "clinvar_evidence": [],
                    },
                }
            )

        try:
            rows = list_clinvar_evidence_for_run(
                db_path,
                run_id,
                variant_id=variant_id,
                limit=limit,
            )
        except Exception:
            app.logger.exception("Failed to fetch ClinVar evidence for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "CLINVAR_EVIDENCE_FETCH_FAILED",
                            "message": "Failed to fetch ClinVar evidence.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "clinvar_evidence": rows}})

    @app.get("/api/v1/runs/<run_id>/annotation_output")
    def get_run_annotation_output(run_id: str):
        from storage.run_artifacts import run_artifacts_dir
        from storage.run_inputs import get_run_input
        from storage.runs import get_run as get_run_record
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 300
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 300
        limit = max(1, min(limit, 5000))

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for annotation output listing")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        run_input = get_run_input(db_path, run_id)
        latest_uploaded_at = run_input.get("uploaded_at") if run_input else None
        stage = get_stage(db_path, run_id, "annotation") or {}

        stage_same_input = (
            bool(latest_uploaded_at)
            and stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )
        if not stage_same_input:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "output_vcf_path": None,
                        "preview_lines": [],
                        "preview_line_count": 0,
                        "truncated": False,
                    },
                }
            )

        stage_stats = stage.get("stats")
        if not isinstance(stage_stats, dict):
            stage_stats = {}

        artifacts_dir = os.path.abspath(run_artifacts_dir(db_path, run_id))
        default_path = os.path.abspath(os.path.join(artifacts_dir, "snpeff.annotated.vcf"))
        output_vcf_path = stage_stats.get("output_vcf_path") or default_path
        output_vcf_path = os.path.abspath(str(output_vcf_path))

        try:
            in_artifacts = os.path.commonpath([output_vcf_path, artifacts_dir]) == artifacts_dir
        except ValueError:
            in_artifacts = False
        if not in_artifacts:
            output_vcf_path = default_path

        if not os.path.isfile(output_vcf_path):
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage": stage or None,
                        "output_vcf_path": output_vcf_path,
                        "preview_lines": [],
                        "preview_line_count": 0,
                        "truncated": False,
                    },
                }
            )

        preview_lines: list[str] = []
        truncated = False
        try:
            with open(output_vcf_path, "r", encoding="utf-8", errors="replace") as f:
                for idx, line in enumerate(f):
                    if idx >= limit:
                        truncated = True
                        break
                    preview_lines.append(line.rstrip("\r\n"))
        except Exception:
            app.logger.exception(
                "Failed to read annotation output preview for run_id=%s path=%s",
                run_id,
                output_vcf_path,
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "ANNOTATION_OUTPUT_READ_FAILED",
                            "message": "Failed to read annotation output preview.",
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
                    "stage": stage or None,
                    "output_vcf_path": output_vcf_path,
                    "preview_lines": preview_lines,
                    "preview_line_count": len(preview_lines),
                    "truncated": truncated,
                },
            }
        )

    @app.get("/api/v1/runs/<run_id>/events")
    def stream_run_events(run_id: str):
        from sse import SseEnvelope, format_sse_comment, format_sse_event, format_sse_retry, now_iso8601
        from storage.runs import get_run as get_run_record
        from storage.stages import PIPELINE_STAGE_ORDER, list_pipeline_stages

        db_path = app.config["SP_DB_PATH"]

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run for SSE stream")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_FETCH_FAILED", "message": "Failed to fetch run record."},
                    }
                ),
                500,
            )

        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        def _snapshot_events() -> tuple[list[str], dict[str, str]]:
            event_at = now_iso8601()
            run_payload = SseEnvelope(
                run_id=run_id,
                event_at=event_at,
                data={"status": run.get("status")},
            ).to_dict()
            messages: list[str] = [format_sse_event("run_status", run_payload)]

            stages = list_pipeline_stages(db_path, run_id)
            by_name = {s.get("stage_name"): s for s in stages}
            stage_status_by_name: dict[str, str] = {}
            for stage_name in PIPELINE_STAGE_ORDER:
                stage = by_name.get(stage_name) or {"stage_name": stage_name, "status": "queued"}
                stage_status_by_name[stage_name] = stage.get("status") or "queued"
                stage_payload = SseEnvelope(
                    run_id=run_id,
                    event_at=event_at,
                    data=stage,
                ).to_dict()
                messages.append(format_sse_event("stage_status", stage_payload))

            return messages, stage_status_by_name

        def _event_stream():
            import sqlite3

            yield format_sse_retry(2000)
            snapshot_messages, stage_status_by_name = _snapshot_events()
            for msg in snapshot_messages:
                yield msg

            last_run_status = run.get("status")
            last_stage_status_by_name: dict[str, str] = dict(stage_status_by_name)
            last_heartbeat_at = time.time()

            poll_seconds = 0.5
            heartbeat_seconds = 15.0

            while True:
                try:
                    current_run = get_run_record(db_path, run_id)
                    if not current_run:
                        return

                    current_status = current_run.get("status")
                    if current_status != last_run_status:
                        last_run_status = current_status
                        payload = SseEnvelope(
                            run_id=run_id,
                            event_at=now_iso8601(),
                            data={"status": current_status},
                        ).to_dict()
                        yield format_sse_event("run_status", payload)

                    stages = list_pipeline_stages(db_path, run_id)
                    for stage in stages:
                        name = stage.get("stage_name")
                        if not name:
                            continue
                        status = stage.get("status") or "queued"
                        if last_stage_status_by_name.get(name) != status:
                            last_stage_status_by_name[name] = status
                            payload = SseEnvelope(
                                run_id=run_id,
                                event_at=now_iso8601(),
                                data=stage,
                            ).to_dict()
                            yield format_sse_event("stage_status", payload)

                    now = time.time()
                    if now - last_heartbeat_at >= heartbeat_seconds:
                        last_heartbeat_at = now
                        yield format_sse_comment("ping")

                    time.sleep(poll_seconds)
                except GeneratorExit:
                    return
                except sqlite3.OperationalError as exc:
                    if "locked" in str(exc).lower():
                        time.sleep(min(0.25, poll_seconds))
                        continue
                    app.logger.exception("SSE stream sqlite error for run_id=%s", run_id)
                    return
                except Exception:
                    app.logger.exception("SSE stream failed for run_id=%s", run_id)
                    return

        headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        return Response(stream_with_context(_event_stream()), mimetype="text/event-stream", headers=headers)

    @app.post("/api/v1/runs/<run_id>/start")
    def start_run(run_id: str):
        from pipeline.orchestrator import OrchestratorError, prepare_pipeline_start, run_pipeline
        from storage.runs import (
            AnotherRunRunningError,
            RunAlreadyRunningError,
            RunNotFoundError,
            RunNotStartableError,
            claim_run_for_execution,
            set_run_status_if_not_canceled,
        )

        db_path = app.config["SP_DB_PATH"]

        try:
            prepared = prepare_pipeline_start(db_path, run_id)
        except OrchestratorError as exc:
            error: dict[str, Any] = {"code": exc.code, "message": exc.message}
            if exc.details:
                error["details"] = exc.details
            return jsonify({"ok": False, "error": error}), exc.http_status

        started_stage = prepared.get("started_stage")
        if started_stage is None:
            run = prepared.get("run") or {}
            return jsonify(
                {
                    "ok": True,
                    "data": {"run_id": run_id, "status": run.get("status"), "started_stage": None},
                }
            )

        try:
            claim_run_for_execution(db_path, run_id)
        except AnotherRunRunningError as exc:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "ANOTHER_RUN_RUNNING",
                            "message": "Another run is currently running.",
                            "details": {"running_run_id": exc.running_run_id},
                        },
                    }
                ),
                409,
            )
        except RunAlreadyRunningError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_ALREADY_RUNNING", "message": "Run is already running."},
                    }
                ),
                409,
            )
        except RunNotStartableError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_CANCELED", "message": "Run is canceled and cannot be started."},
                    }
                ),
                409,
            )
        except RunNotFoundError:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )
        except Exception:
            app.logger.exception("Failed to start run")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_START_FAILED", "message": "Failed to start run."},
                    }
                ),
                500,
            )

        def _background_execute():
            try:
                run_pipeline(
                    db_path,
                    run_id,
                    max_decompressed_bytes=app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"],
                    logger=app.logger,
                    prediction_config=app.config.get("SP_TEST_PREDICTION_CONFIG"),
                )
            except OrchestratorError as exc:
                app.logger.warning(
                    "Pipeline orchestration stopped for run_id=%s code=%s message=%s details=%s",
                    run_id,
                    exc.code,
                    exc.message,
                    exc.details,
                )
            except Exception:
                app.logger.exception("Pipeline orchestration crashed for run_id=%s", run_id)
            finally:
                try:
                    set_run_status_if_not_canceled(db_path, run_id, "queued")
                except Exception:
                    app.logger.exception("Failed to reset run status for run_id=%s", run_id)

        threading.Thread(target=_background_execute, daemon=True).start()

        return jsonify(
            {"ok": True, "data": {"run_id": run_id, "status": "running", "started_stage": started_stage}}
        )

    @app.post("/api/v1/runs/<run_id>/stages/<stage_name>/retry")
    def retry_stage(run_id: str, stage_name: str):
        from pipeline.orchestrator import OrchestratorError, prepare_pipeline_start, run_pipeline
        from storage.run_inputs import get_run_input
        from storage.runs import (
            AnotherRunRunningError,
            RunAlreadyRunningError,
            RunNotFoundError,
            RunNotStartableError,
            claim_run_for_execution,
            get_run as get_run_record,
            set_run_status_if_not_canceled,
        )
        from storage.stages import (
            PIPELINE_STAGE_ORDER,
            StageResetRunCanceledError,
            list_pipeline_stages,
            reset_stage_and_downstream,
        )

        db_path = app.config["SP_DB_PATH"]
        normalized_stage = (stage_name or "").strip()

        if normalized_stage not in PIPELINE_STAGE_ORDER:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "STAGE_NOT_FOUND",
                            "message": "Stage not found.",
                            "details": {"stage_name": stage_name},
                        },
                    }
                ),
                404,
            )

        run = get_run_record(db_path, run_id)
        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        if run.get("status") == "canceled":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_CANCELED", "message": "Run is canceled and cannot be retried."},
                    }
                ),
                409,
            )

        stages = list_pipeline_stages(db_path, run_id)
        by_name = {stage.get("stage_name"): stage for stage in stages if stage.get("stage_name")}
        current_stage = by_name.get(normalized_stage) or {}
        current_status = current_stage.get("status") or "queued"

        if current_status != "failed":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "STAGE_NOT_FAILED",
                            "message": "Stage is not failed.",
                            "details": {"stage_name": normalized_stage, "current_status": current_status},
                        },
                    }
                ),
                409,
            )

        run_input = get_run_input(db_path, run_id)
        if not run_input:
            return (
                jsonify(
                    {"ok": False, "error": {"code": "VCF_NOT_UPLOADED", "message": "No VCF uploaded for this run."}}
                ),
                409,
            )
        if not run_input.get("validation", {}).get("ok"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VCF_NOT_VALIDATED",
                            "message": "Uploaded VCF is not valid; fix validation errors before retrying.",
                        },
                    }
                ),
                409,
            )

        uploaded_at = run_input.get("uploaded_at")
        stage_uploaded_at = current_stage.get("input_uploaded_at")
        if stage_uploaded_at and uploaded_at and stage_uploaded_at != uploaded_at:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "STAGE_INPUT_MISMATCH",
                            "message": "Failed stage does not match the latest uploaded input; restart from an earlier stage.",
                            "details": {
                                "stage_name": normalized_stage,
                                "stage_input_uploaded_at": stage_uploaded_at,
                                "uploaded_at": uploaded_at,
                            },
                        },
                    }
                ),
                409,
            )

        try:
            claim_run_for_execution(db_path, run_id)
        except AnotherRunRunningError as exc:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "ANOTHER_RUN_RUNNING",
                            "message": "Another run is currently running.",
                            "details": {"running_run_id": exc.running_run_id},
                        },
                    }
                ),
                409,
            )
        except RunAlreadyRunningError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_ALREADY_RUNNING", "message": "Run is already running."},
                    }
                ),
                409,
            )
        except RunNotStartableError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_CANCELED", "message": "Run is canceled and cannot be retried."},
                    }
                ),
                409,
            )
        except RunNotFoundError:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )
        except Exception:
            app.logger.exception("Failed to claim run for retry")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_START_FAILED", "message": "Failed to start retry."},
                    }
                ),
                500,
            )

        try:
            reset_stage_and_downstream(db_path, run_id, normalized_stage)
        except StageResetRunCanceledError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_CANCELED", "message": "Run is canceled and cannot be retried."},
                    }
                ),
                409,
            )
        except Exception:
            app.logger.exception("Failed to reset stages for retry (run_id=%s stage=%s)", run_id, normalized_stage)
            try:
                set_run_status_if_not_canceled(db_path, run_id, "queued")
            except Exception:
                app.logger.exception("Failed to reset run status after retry reset failure (run_id=%s)", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "STAGE_RESET_FAILED",
                            "message": "Failed to reset stages for retry.",
                        },
                    }
                ),
                500,
            )

        start_index = PIPELINE_STAGE_ORDER.index(normalized_stage)
        reset_stages = list(PIPELINE_STAGE_ORDER[start_index:])
        preserved_stages: list[str] = []
        stages_after = list_pipeline_stages(db_path, run_id)
        by_name_after = {
            stage.get("stage_name"): stage for stage in stages_after if stage.get("stage_name")
        }
        for name in PIPELINE_STAGE_ORDER[:start_index]:
            stage = by_name_after.get(name) or {}
            if stage.get("status") == "succeeded" and stage.get("input_uploaded_at") == uploaded_at:
                preserved_stages.append(name)

        try:
            prepared = prepare_pipeline_start(db_path, run_id)
        except OrchestratorError as exc:
            try:
                set_run_status_if_not_canceled(db_path, run_id, "queued")
            except Exception:
                app.logger.exception("Failed to reset run status after retry prepare failure (run_id=%s)", run_id)
            error: dict[str, Any] = {"code": exc.code, "message": exc.message}
            if exc.details:
                error["details"] = exc.details
            return jsonify({"ok": False, "error": error}), exc.http_status

        started_stage = prepared.get("started_stage")
        if started_stage is None:
            try:
                set_run_status_if_not_canceled(db_path, run_id, "queued")
            except Exception:
                app.logger.exception("Failed to reset run status after no-op retry (run_id=%s)", run_id)
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "stage_name": normalized_stage,
                        "preserved_stages": preserved_stages,
                        "reset_stages": reset_stages,
                        "status": run.get("status"),
                        "started_stage": None,
                    },
                }
            )

        def _background_execute():
            try:
                run_pipeline(
                    db_path,
                    run_id,
                    max_decompressed_bytes=app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"],
                    logger=app.logger,
                    prediction_config=app.config.get("SP_TEST_PREDICTION_CONFIG"),
                )
            except OrchestratorError as exc:
                app.logger.warning(
                    "Pipeline orchestration stopped for run_id=%s code=%s message=%s details=%s",
                    run_id,
                    exc.code,
                    exc.message,
                    exc.details,
                )
            except Exception:
                app.logger.exception("Pipeline orchestration crashed for run_id=%s", run_id)
            finally:
                try:
                    set_run_status_if_not_canceled(db_path, run_id, "queued")
                except Exception:
                    app.logger.exception("Failed to reset run status for run_id=%s", run_id)

        threading.Thread(target=_background_execute, daemon=True).start()

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "stage_name": normalized_stage,
                    "preserved_stages": preserved_stages,
                    "reset_stages": reset_stages,
                    "status": "running",
                    "started_stage": started_stage,
                },
            }
        )

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
        from storage.runs import get_run as get_run_record

        db_path = app.config["SP_DB_PATH"]

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for VCF upload")
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

        if run.get("status") == "running":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_RUNNING",
                            "message": "Run is currently running. Cancel it before uploading a new file.",
                        },
                    }
                ),
                409,
            )

        if run.get("status") == "canceled":
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CANCELED",
                            "message": "Run is canceled. Create a new run to upload another file.",
                        },
                    }
                ),
                409,
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
                db_path,
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
        from storage.run_inputs import get_run_input, get_run_upload_path
        from storage.runs import get_run as get_run_record
        from storage.stages import (
            get_stage,
            mark_stage_canceled,
            mark_stage_failed,
        )
        from pipeline.parser_stage import StageExecutionError, run_parser_stage

        db_path = app.config["SP_DB_PATH"]

        run = get_run_record(db_path, run_id)
        if not run:
            return (
                jsonify({"ok": False, "error": {"code": "RUN_NOT_FOUND", "message": "Run not found."}}),
                404,
            )

        if run.get("status") == "canceled":
            mark_stage_canceled(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=None,
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
            mark_stage_failed(
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
            mark_stage_failed(
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

        upload_path = get_run_upload_path(db_path, run_id)
        try:
            result = run_parser_stage(
                db_path,
                run_id,
                uploaded_at=uploaded_at,
                upload_path=upload_path,
                max_decompressed_bytes=app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"],
                logger=app.logger,
                force=force,
            )
        except StageExecutionError as exc:
            error: dict[str, Any] = {"code": exc.code, "message": exc.message}
            if exc.details:
                error["details"] = exc.details
            return jsonify({"ok": False, "error": error}), exc.http_status

        return jsonify({"ok": True, "data": {"run_id": run_id, **result}})

    return app


if __name__ == "__main__":
    try:
        from env_file import load_env_file

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        load_env_file(os.path.join(project_root, ".env"), override=False)
    except Exception:
        pass

    debug = os.environ.get("FLASK_DEBUG") == "1"
    create_app().run(debug=debug)
