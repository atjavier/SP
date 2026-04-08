import glob
import json
import os
import secrets
import sqlite3
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
    app.config["APP_NAME"] = os.environ.get("SP_APP_NAME", "BioEvidence")
    app.config["APP_TAGLINE"] = os.environ.get(
        "SP_APP_TAGLINE",
        "Teach and trace SNV outcomes.",
    )
    if "SP_APP_SHORT_NAME" in os.environ:
        app.config["APP_SHORT_NAME"] = os.environ.get("SP_APP_SHORT_NAME")
    else:
        name_parts = [part for part in app.config["APP_NAME"].split() if part]
        if len(name_parts) > 1:
            short_name = "".join(part[0] for part in name_parts[:2]).upper()
        else:
            short_name = app.config["APP_NAME"][:2].upper()
        app.config["APP_SHORT_NAME"] = short_name

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

    @app.context_processor
    def inject_app_branding():
        return {
            "app_name": app.config.get("APP_NAME", "BioEvidence"),
            "app_tagline": app.config.get("APP_TAGLINE", "Teach and trace SNV outcomes."),
            "app_short_name": app.config.get("APP_SHORT_NAME", "BE"),
        }

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/docs")
    def docs():
        return render_template("docs.html")

    def _serialize_run_for_response(
        run: dict | None,
        *,
        run_id: str | None = None,
        status_override: str | None = None,
    ) -> dict[str, Any]:
        payload = dict(run or {})
        if run_id is not None:
            payload["run_id"] = run_id
        if status_override is not None:
            payload["status"] = status_override
        return {
            "run_id": payload.get("run_id"),
            "status": payload.get("status"),
            "created_at": payload.get("created_at"),
            "reference_build": payload.get("reference_build"),
            "annotation_evidence_policy": payload.get("annotation_evidence_policy"),
            "evidence_mode_requested": payload.get("evidence_mode_requested"),
            "evidence_mode_effective": payload.get("evidence_mode_effective"),
            "evidence_online_available": payload.get("evidence_online_available"),
            "evidence_offline_sources_configured": payload.get("evidence_offline_sources_configured") or {},
            "evidence_mode_decision_reason": payload.get("evidence_mode_decision_reason"),
            "evidence_mode_detected_at": payload.get("evidence_mode_detected_at"),
        }

    def _determine_terminal_run_status(db_path: str, run_id: str) -> str:
        """
        Keep run-level status aligned with the latest attempt outcome:
        - failed if any latest-upload stage failed
        - queued otherwise
        """
        from storage.run_inputs import get_run_input
        from storage.stages import list_pipeline_stages

        run_input = get_run_input(db_path, run_id) or {}
        latest_uploaded_at = run_input.get("uploaded_at")
        if not latest_uploaded_at:
            return "queued"

        stages = list_pipeline_stages(db_path, run_id)
        for stage in stages:
            if stage.get("input_uploaded_at") != latest_uploaded_at:
                continue
            if (stage.get("status") or "").strip().lower() == "failed":
                return "failed"
        return "queued"

    def _stage_ready_for_latest_upload(db_path: str, run_id: str, stage_name: str) -> bool:
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        run_input = get_run_input(db_path, run_id) or {}
        latest_uploaded_at = run_input.get("uploaded_at")
        if not latest_uploaded_at:
            return False
        stage = get_stage(db_path, run_id, stage_name) or {}
        return (
            stage.get("status") == "succeeded"
            and stage.get("input_uploaded_at") == latest_uploaded_at
        )

    def _build_run_logger(run_id: str):
        from run_logging import build_run_logger

        instance_dir = os.path.dirname(app.config["SP_DB_PATH"])
        return build_run_logger(run_id, instance_dir=instance_dir)

    def _finalize_run_status(
        db_path: str,
        run_id: str,
        *,
        force_failed: bool = False,
        max_attempts: int = 3,
    ) -> str:
        from storage.runs import get_run as get_run_record, set_run_status_if_not_canceled

        current = get_run_record(db_path, run_id) or {}
        was_canceled = current.get("status") == "canceled"

        if force_failed:
            terminal_status = "failed"
        else:
            terminal_status = None
            for attempt in range(1, max_attempts + 1):
                try:
                    terminal_status = _determine_terminal_run_status(db_path, run_id)
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempt >= max_attempts:
                        app.logger.exception(
                            "Failed to determine terminal run status for run_id=%s",
                            run_id,
                        )
                        break
                    time.sleep(0.1 * attempt)
                except Exception:
                    app.logger.exception(
                        "Failed to determine terminal run status for run_id=%s",
                        run_id,
                    )
                    break
            if terminal_status is None:
                # Prefer idling over a false-negative failed status when final status
                # cannot be confidently determined (for example transient DB contention).
                terminal_status = "queued"

        for attempt in range(1, max_attempts + 1):
            try:
                set_run_status_if_not_canceled(db_path, run_id, terminal_status)
                break
            except Exception:
                if attempt >= max_attempts:
                    app.logger.exception("Failed to reset run status for run_id=%s", run_id)
                    break
                # Brief backoff for transient sqlite lock contention.
                time.sleep(0.1 * attempt)
        if was_canceled:
            return "canceled"
        return terminal_status

    def _determine_final_status_for_logging(
        db_path: str,
        run_id: str,
        *,
        force_failed: bool = False,
    ) -> str:
        from storage.runs import get_run as get_run_record

        if force_failed:
            return "failed"
        run = get_run_record(db_path, run_id) or {}
        if run.get("status") == "canceled":
            return "canceled"
        try:
            return _determine_terminal_run_status(db_path, run_id)
        except Exception:
            return "queued"

    @app.post("/api/v1/runs")
    def create_run():
        from storage.runs import (
            create_run as create_run_record,
            normalize_annotation_evidence_policy,
        )

        payload = request.get_json(silent=True)
        if request.is_json:
            raw_body = request.get_data(cache=True, as_text=False) or b""
            if raw_body.strip() and payload is None:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "RUN_CREATE_INVALID",
                                "message": "Malformed JSON request body.",
                            },
                        }
                    ),
                    400,
                )
        if payload is not None and not isinstance(payload, dict):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CREATE_INVALID",
                            "message": "Request body must be a JSON object.",
                        },
                    }
                ),
                400,
            )
        requested_policy = (
            payload.get("annotation_evidence_policy")
            if isinstance(payload, dict)
            else None
        )
        normalized_policy = normalize_annotation_evidence_policy(requested_policy)
        if requested_policy is not None and normalized_policy is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_CREATE_INVALID",
                            "message": "Invalid annotation_evidence_policy.",
                            "details": {
                                "annotation_evidence_policy": requested_policy,
                                "allowed_values": ["stop", "continue"],
                            },
                        },
                    }
                ),
                400,
            )

        try:
            record = create_run_record(
                app.config["SP_DB_PATH"],
                annotation_evidence_policy=normalized_policy,
            )
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

    @app.get("/api/v1/runs/<run_id>/logs")
    def get_run_logs(run_id: str):
        from collections import deque
        from storage.runs import get_run as get_run_record

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        limit = 200
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
        limit = max(1, min(limit, 1000))

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for log listing")
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

        instance_dir = os.path.dirname(db_path)
        log_path = os.path.join(instance_dir, "logs", "runs", f"{run_id}.log")
        if not os.path.isfile(log_path):
            return jsonify({"ok": True, "data": {"run_id": run_id, "logs": []}})

        logs: list[dict] = []
        try:
            with open(log_path, "r", encoding="utf-8") as handle:
                tail = deque(handle, maxlen=limit)
            for line in tail:
                if not line.strip():
                    continue
                try:
                    logs.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        except Exception:
            app.logger.exception("Failed to read run logs for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "RUN_LOGS_FAILED", "message": "Failed to read run logs."},
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "logs": logs}})

    @app.post("/api/v1/runs/<run_id>/settings")
    def update_run_settings(run_id: str):
        from storage.runs import (
            RunNotFoundError,
            RunPolicyNotUpdatableError,
            normalize_annotation_evidence_policy,
            update_run_annotation_evidence_policy,
        )

        payload = request.get_json(silent=True)
        if request.is_json:
            raw_body = request.get_data(cache=True, as_text=False) or b""
            if raw_body.strip() and payload is None:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "RUN_SETTINGS_INVALID",
                                "message": "Malformed JSON request body.",
                            },
                        }
                    ),
                    400,
                )
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_SETTINGS_INVALID",
                            "message": "Request body must be a JSON object.",
                        },
                    }
                ),
                400,
            )

        requested_policy = payload.get("annotation_evidence_policy")
        normalized_policy = normalize_annotation_evidence_policy(requested_policy)
        if normalized_policy is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_SETTINGS_INVALID",
                            "message": "Invalid annotation_evidence_policy.",
                            "details": {
                                "annotation_evidence_policy": requested_policy,
                                "allowed_values": ["stop", "continue"],
                            },
                        },
                    }
                ),
                400,
            )

        try:
            record = update_run_annotation_evidence_policy(
                app.config["SP_DB_PATH"],
                run_id,
                annotation_evidence_policy=normalized_policy,
            )
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
        except RunPolicyNotUpdatableError as exc:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_SETTINGS_NOT_UPDATABLE",
                            "message": "Run settings cannot be updated while the run is running.",
                            "details": {"current_status": exc.current_status},
                        },
                    }
                ),
                409,
            )
        except ValueError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_SETTINGS_INVALID",
                            "message": "Invalid run settings.",
                        },
                    }
                ),
                400,
            )
        except Exception:
            app.logger.exception("Failed to update run settings")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "RUN_SETTINGS_UPDATE_FAILED",
                            "message": "Failed to update run settings.",
                        },
                    }
                ),
                500,
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
            stages = None
            for attempt in range(1, 4):
                try:
                    stages = list_pipeline_stages(db_path, run_id)
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempt >= 3:
                        raise
                    time.sleep(0.05 * attempt)
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

        return jsonify({"ok": True, "data": {"run_id": run_id, "stages": stages or []}})

    @app.get("/api/v1/runs/<run_id>/classifications")
    def get_run_classifications(run_id: str):
        from storage.classifications import count_classifications_for_run, list_classifications_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        category = (request.args.get("category") or "").strip().lower() or None
        variant_id = (request.args.get("variant_id") or "").strip() or None
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)

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
                        "limit": limit,
                        "offset": offset,
                        "total_count": 0,
                        "category": category,
                        "variant_id": variant_id,
                    },
                }
            )

        try:
            rows = list_classifications_for_run(
                db_path,
                run_id,
                limit=limit,
                offset=offset,
                category=category,
                variant_id=variant_id,
            )
            total_count = count_classifications_for_run(db_path, run_id, category=category, variant_id=variant_id)
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

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "stage": stage or None,
                    "classifications": rows,
                    "limit": limit,
                    "offset": offset,
                    "total_count": total_count,
                    "category": category,
                    "variant_id": variant_id,
                },
            }
        )

    @app.get("/api/v1/runs/<run_id>/predictor_outputs")
    def get_run_predictor_outputs(run_id: str):
        from storage.predictor_outputs import count_predictor_outputs_for_run, list_predictor_outputs_for_run
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)

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
                        "limit": limit,
                        "offset": offset,
                        "total_count": 0,
                        "predictor_key": predictor_key,
                        "variant_id": variant_id,
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
                offset=offset,
            )
            total_count = count_predictor_outputs_for_run(
                db_path,
                run_id,
                predictor_key=predictor_key,
                variant_id=variant_id,
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

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "stage": stage or None,
                    "predictor_outputs": rows,
                    "limit": limit,
                    "offset": offset,
                    "total_count": total_count,
                    "predictor_key": predictor_key,
                    "variant_id": variant_id,
                },
            }
        )

    @app.get("/api/v1/runs/<run_id>/pre_annotations")
    def get_run_pre_annotations(run_id: str):
        from storage.pre_annotations import (
            count_pre_annotations_for_run_public,
            list_pre_annotations_for_run_public,
        )
        from storage.runs import get_run as get_run_record
        from storage.run_inputs import get_run_input
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)
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
                        "limit": limit,
                        "offset": offset,
                        "total_count": 0,
                        "variant_id": variant_id,
                    },
                }
            )

        try:
            rows = list_pre_annotations_for_run_public(
                db_path,
                run_id,
                limit=limit,
                offset=offset,
                variant_id=variant_id,
            )
            total_count = count_pre_annotations_for_run_public(db_path, run_id, variant_id=variant_id)
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

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "stage": stage or None,
                    "pre_annotations": rows,
                    "limit": limit,
                    "offset": offset,
                    "total_count": total_count,
                    "variant_id": variant_id,
                },
            }
        )

    @app.get("/api/v1/runs/<run_id>/variant_summaries")
    def get_run_variant_summaries(run_id: str):
        from storage.run_inputs import get_run_input
        from storage.runs import get_run as get_run_record
        from storage.stages import get_stage, list_pipeline_stages
        from storage.variant_summaries import (
            count_variant_summaries_for_run,
            list_variant_summaries_for_run,
        )

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        completeness_raw = (request.args.get("completeness") or "").strip().lower()
        completeness = (
            completeness_raw
            if completeness_raw in {"complete", "partial", "unavailable", "failed"}
            else None
        )
        limit = 100
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 100
        limit = max(1, min(limit, 1000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for variant summary listing")
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
        stage = get_stage(db_path, run_id, "parser") or {}

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
                        "variant_summaries": [],
                        "limit": limit,
                        "offset": offset,
                        "total_count": 0,
                    },
                }
            )

        stage_statuses: dict[str, str] | None = None
        annotation_evidence_completeness = None
        if completeness:
            stages = list_pipeline_stages(db_path, run_id)
            stage_statuses = {
                stage.get("stage_name"): stage.get("status") or ""
                for stage in stages
                if stage.get("stage_name")
            }
            annotation_stage = next(
                (entry for entry in stages if entry.get("stage_name") == "annotation"),
                {},
            )
            annotation_stats = annotation_stage.get("stats")
            if isinstance(annotation_stats, dict):
                annotation_evidence_completeness = annotation_stats.get(
                    "annotation_evidence_completeness"
                )

        try:
            list_kwargs = {"limit": limit, "offset": offset}
            count_kwargs: dict[str, object] = {}
            if completeness:
                list_kwargs.update(
                    {
                        "completeness": completeness,
                        "stage_statuses": stage_statuses,
                        "annotation_evidence_completeness": annotation_evidence_completeness,
                    }
                )
                count_kwargs.update(
                    {
                        "completeness": completeness,
                        "stage_statuses": stage_statuses,
                        "annotation_evidence_completeness": annotation_evidence_completeness,
                    }
                )

            rows = list_variant_summaries_for_run(db_path, run_id, **list_kwargs)
            total_count = count_variant_summaries_for_run(db_path, run_id, **count_kwargs)
        except Exception:
            app.logger.exception("Failed to fetch variant summaries for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "VARIANT_SUMMARY_FETCH_FAILED",
                            "message": "Failed to fetch variant summaries.",
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
                    "variant_summaries": rows,
                    "limit": limit,
                    "offset": offset,
                    "total_count": total_count,
                },
            }
        )

    VALID_EVIDENCE_CLASSIFICATIONS = {"missense"}
    VALID_EVIDENCE_OUTCOMES = {"found", "not_found", "error", "all"}

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
        classification_raw = (request.args.get("classification") or "").strip().lower()
        outcome_raw = (request.args.get("outcome") or "").strip().lower()
        if classification_raw and classification_raw not in VALID_EVIDENCE_CLASSIFICATIONS:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_CLASSIFICATION_INVALID",
                            "message": "Invalid classification filter.",
                            "details": {
                                "classification": classification_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_CLASSIFICATIONS),
                            },
                        },
                    }
                ),
                400,
            )
        if outcome_raw and outcome_raw not in VALID_EVIDENCE_OUTCOMES:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_OUTCOME_INVALID",
                            "message": "Invalid evidence outcome filter.",
                            "details": {
                                "outcome": outcome_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_OUTCOMES),
                            },
                        },
                    }
                ),
                400,
            )
        classification = classification_raw or "missense"
        outcome = outcome_raw or None

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
                classification=classification,
                outcome=outcome,
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
        classification_raw = (request.args.get("classification") or "").strip().lower()
        outcome_raw = (request.args.get("outcome") or "").strip().lower()
        if classification_raw and classification_raw not in VALID_EVIDENCE_CLASSIFICATIONS:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_CLASSIFICATION_INVALID",
                            "message": "Invalid classification filter.",
                            "details": {
                                "classification": classification_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_CLASSIFICATIONS),
                            },
                        },
                    }
                ),
                400,
            )
        if outcome_raw and outcome_raw not in VALID_EVIDENCE_OUTCOMES:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_OUTCOME_INVALID",
                            "message": "Invalid evidence outcome filter.",
                            "details": {
                                "outcome": outcome_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_OUTCOMES),
                            },
                        },
                    }
                ),
                400,
            )
        classification = classification_raw or "missense"
        outcome = outcome_raw or None

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
                classification=classification,
                outcome=outcome,
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

    @app.get("/api/v1/runs/<run_id>/gnomad_evidence")
    def get_run_gnomad_evidence(run_id: str):
        from storage.gnomad_evidence import list_gnomad_evidence_for_run
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
        classification_raw = (request.args.get("classification") or "").strip().lower()
        outcome_raw = (request.args.get("outcome") or "").strip().lower()
        if classification_raw and classification_raw not in VALID_EVIDENCE_CLASSIFICATIONS:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_CLASSIFICATION_INVALID",
                            "message": "Invalid classification filter.",
                            "details": {
                                "classification": classification_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_CLASSIFICATIONS),
                            },
                        },
                    }
                ),
                400,
            )
        if outcome_raw and outcome_raw not in VALID_EVIDENCE_OUTCOMES:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "EVIDENCE_OUTCOME_INVALID",
                            "message": "Invalid evidence outcome filter.",
                            "details": {
                                "outcome": outcome_raw,
                                "allowed_values": sorted(VALID_EVIDENCE_OUTCOMES),
                            },
                        },
                    }
                ),
                400,
            )
        classification = classification_raw or "missense"
        outcome = outcome_raw or None

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for gnomAD evidence listing")
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
                        "gnomad_evidence": [],
                    },
                }
            )

        try:
            rows = list_gnomad_evidence_for_run(
                db_path,
                run_id,
                variant_id=variant_id,
                classification=classification,
                outcome=outcome,
                limit=limit,
            )
        except Exception:
            app.logger.exception("Failed to fetch gnomAD evidence for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "GNOMAD_EVIDENCE_FETCH_FAILED",
                            "message": "Failed to fetch gnomAD evidence.",
                        },
                    }
                ),
                500,
            )

        return jsonify({"ok": True, "data": {"run_id": run_id, "stage": stage or None, "gnomad_evidence": rows}})

    @app.get("/api/v1/runs/<run_id>/annotation_output")
    def get_run_annotation_output(run_id: str):
        from storage.run_artifacts import run_artifacts_dir
        from storage.run_inputs import get_run_input
        from storage.runs import get_run as get_run_record
        from storage.stages import get_stage

        db_path = app.config["SP_DB_PATH"]
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        pos_raw = (request.args.get("pos") or "").strip()
        limit = 300
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 300
        limit = max(1, min(limit, 5000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)
        pos_filter = None
        if pos_raw:
            try:
                pos_filter = int(pos_raw)
            except ValueError:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "ANNOTATION_OUTPUT_POS_INVALID",
                                "message": "Position filter must be an integer.",
                            },
                        }
                    ),
                    400,
                )

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
        data_line_count = 0
        header_line = None
        pos_text = str(pos_filter) if pos_filter is not None else None
        try:
            with open(output_vcf_path, "r", encoding="utf-8", errors="replace") as f:
                data_seen = 0
                for line in f:
                    stripped = line.rstrip("\r\n")
                    if stripped.startswith("#CHROM"):
                        header_line = stripped
                        continue
                    if stripped.startswith("#") or not stripped:
                        continue

                    if pos_text is not None:
                        cols = stripped.split("\t")
                        if len(cols) > 1 and cols[1] == pos_text:
                            if header_line and not preview_lines:
                                preview_lines.append(header_line)
                            preview_lines.append(stripped)
                            data_line_count += 1
                            if data_line_count >= limit:
                                for tail in f:
                                    tail_stripped = tail.rstrip("\r\n")
                                    if tail_stripped.startswith("#") or not tail_stripped:
                                        continue
                                    tail_cols = tail_stripped.split("\t")
                                    if len(tail_cols) > 1 and tail_cols[1] == pos_text:
                                        truncated = True
                                        break
                                break
                        continue

                    if data_seen < offset:
                        data_seen += 1
                        continue

                    if header_line and not preview_lines:
                        preview_lines.append(header_line)
                    preview_lines.append(stripped)
                    data_line_count += 1
                    data_seen += 1
                    if data_line_count >= limit:
                        for tail in f:
                            tail_stripped = tail.rstrip("\r\n")
                            if tail_stripped.startswith("#") or not tail_stripped:
                                continue
                            truncated = True
                            break
                        break
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
                    "data_line_count": data_line_count,
                    "offset": 0 if pos_text is not None else offset,
                    "pos_filter": pos_filter,
                    "truncated": truncated,
                },
            }
        )

    _BATCH_ARTIFACT_MAP = {
        "classification.input.vcf": "classification.input.batch*.vcf",
        "classification.vep.jsonl": "classification.vep.batch*.jsonl",
    }

    def _batch_artifact_candidates(artifacts_dir: str, name: str) -> list[str]:
        pattern = _BATCH_ARTIFACT_MAP.get(name)
        if not pattern:
            return []
        return sorted(glob.glob(os.path.join(artifacts_dir, pattern)))

    ARTIFACT_CATALOG = [
        {"name": "classification.input.vcf", "kind": "vcf", "stage": "classification"},
        {"name": "classification.vep.jsonl", "kind": "jsonl", "stage": "classification"},
        {"name": "prediction.input.vcf", "kind": "vcf", "stage": "prediction"},
        {"name": "prediction.vep.jsonl", "kind": "jsonl", "stage": "prediction"},
        {"name": "snpeff.annotated.vcf", "kind": "vcf", "stage": "annotation"},
    ]

    @app.get("/api/v1/runs/<run_id>/artifacts")
    def list_run_artifacts(run_id: str):
        from storage.run_artifacts import run_artifacts_dir
        from storage.runs import get_run as get_run_record

        db_path = app.config["SP_DB_PATH"]

        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for artifact listing")
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

        artifacts_dir = os.path.abspath(run_artifacts_dir(db_path, run_id))
        catalog = []
        for entry in ARTIFACT_CATALOG:
            stage_ready = _stage_ready_for_latest_upload(db_path, run_id, entry["stage"])
            artifact_path = os.path.abspath(os.path.join(artifacts_dir, entry["name"]))
            exists = stage_ready and os.path.isfile(artifact_path)
            if stage_ready and not exists:
                exists = bool(_batch_artifact_candidates(artifacts_dir, entry["name"]))
            reason = None
            if not stage_ready:
                reason = "stage_not_ready"
            elif not exists:
                reason = "not_found"
            catalog.append(
                {
                    "name": entry["name"],
                    "kind": entry["kind"],
                    "stage": entry["stage"],
                    "available": exists,
                    "reason": reason,
                }
            )

        html_stage_ready = _stage_ready_for_latest_upload(db_path, run_id, "reporting")
        html_items = []
        if os.path.isdir(artifacts_dir):
            for name in sorted(os.listdir(artifacts_dir)):
                if not name.lower().endswith(".html"):
                    continue
                artifact_path = os.path.abspath(os.path.join(artifacts_dir, name))
                exists = html_stage_ready and os.path.isfile(artifact_path)
                reason = None
                if not html_stage_ready:
                    reason = "stage_not_ready"
                elif not os.path.isfile(artifact_path):
                    reason = "not_found"
                html_items.append(
                    {
                        "name": name,
                        "kind": "html",
                        "stage": "reporting",
                        "available": exists,
                        "reason": reason,
                    }
                )

        return jsonify({"ok": True, "data": {"run_id": run_id, "artifacts": catalog + html_items}})

    @app.get("/api/v1/runs/<run_id>/artifacts/preview")
    def get_run_artifact_preview(run_id: str):
        from storage.run_artifacts import run_artifacts_dir
        from storage.runs import get_run as get_run_record

        name = (request.args.get("name") or "").strip()
        limit_raw = (request.args.get("limit") or "").strip()
        offset_raw = (request.args.get("offset") or "").strip()
        pos_raw = (request.args.get("pos") or "").strip()
        limit = 200
        if limit_raw:
            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
        limit = max(1, min(limit, 5000))
        offset = 0
        if offset_raw:
            try:
                offset = int(offset_raw)
            except ValueError:
                offset = 0
        offset = max(0, offset)
        pos_filter = None
        if pos_raw:
            try:
                pos_filter = int(pos_raw)
            except ValueError:
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "ARTIFACT_POS_INVALID",
                                "message": "Position filter must be an integer.",
                            },
                        }
                    ),
                    400,
                )

        if not name:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "ARTIFACT_NAME_REQUIRED", "message": "Artifact name is required."},
                    }
                ),
                400,
            )

        db_path = app.config["SP_DB_PATH"]
        try:
            run = get_run_record(db_path, run_id)
        except Exception:
            app.logger.exception("Failed to fetch run record for artifact preview")
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

        entry = next((item for item in ARTIFACT_CATALOG if item["name"] == name), None)
        kind = None
        stage = None
        if entry:
            kind = entry["kind"]
            stage = entry["stage"]
        elif name.lower().endswith(".html"):
            kind = "html"
            stage = "reporting"
        else:
            return (
                jsonify({"ok": False, "error": {"code": "ARTIFACT_UNKNOWN", "message": "Artifact not supported."}}),
                404,
            )

        if not _stage_ready_for_latest_upload(db_path, run_id, stage):
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "artifact": {
                            "name": name,
                            "kind": kind,
                            "available": False,
                            "reason": "stage_not_ready",
                        },
                    },
                }
            )

        artifacts_dir = os.path.abspath(run_artifacts_dir(db_path, run_id))
        artifact_path = os.path.abspath(os.path.join(artifacts_dir, name))
        if not os.path.isfile(artifact_path):
            batch_candidates = _batch_artifact_candidates(artifacts_dir, name)
            if batch_candidates:
                artifact_path = os.path.abspath(batch_candidates[0])
        try:
            in_artifacts = os.path.commonpath([artifact_path, artifacts_dir]) == artifacts_dir
        except ValueError:
            in_artifacts = False
        if not in_artifacts:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {"code": "ARTIFACT_INVALID_PATH", "message": "Invalid artifact path."},
                    }
                ),
                400,
            )

        if not os.path.isfile(artifact_path):
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        "run_id": run_id,
                        "artifact": {
                            "name": name,
                            "kind": kind,
                            "available": False,
                            "reason": "not_found",
                        },
                    },
                }
            )

        if kind == "vcf":
            preview_lines = []
            truncated = False
            data_line_count = 0
            header_line = None
            pos_text = str(pos_filter) if pos_filter is not None else None
            try:
                with open(artifact_path, "r", encoding="utf-8", errors="replace") as handle:
                    data_seen = 0
                    for line in handle:
                        stripped = line.rstrip("\r\n")
                        if stripped.startswith("#CHROM"):
                            header_line = stripped
                            continue
                        if stripped.startswith("#") or not stripped:
                            continue

                        if pos_text is not None:
                            cols = stripped.split("\t")
                            if len(cols) > 1 and cols[1] == pos_text:
                                if header_line and not preview_lines:
                                    preview_lines.append(header_line)
                                preview_lines.append(stripped)
                                data_line_count += 1
                                if data_line_count >= limit:
                                    for tail in handle:
                                        tail_stripped = tail.rstrip("\r\n")
                                        if tail_stripped.startswith("#") or not tail_stripped:
                                            continue
                                        tail_cols = tail_stripped.split("\t")
                                        if len(tail_cols) > 1 and tail_cols[1] == pos_text:
                                            truncated = True
                                            break
                                    break
                            continue

                        if data_seen < offset:
                            data_seen += 1
                            continue

                        if header_line and not preview_lines:
                            preview_lines.append(header_line)
                        preview_lines.append(stripped)
                        data_line_count += 1
                        data_seen += 1
                        if data_line_count >= limit:
                            for tail in handle:
                                tail_stripped = tail.rstrip("\r\n")
                                if tail_stripped.startswith("#") or not tail_stripped:
                                    continue
                                truncated = True
                                break
                            break
            except Exception:
                app.logger.exception("Failed to read VCF artifact preview for run_id=%s", run_id)
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "ARTIFACT_READ_FAILED",
                                "message": "Failed to read artifact preview.",
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
                        "artifact": {
                            "name": name,
                            "kind": kind,
                            "available": True,
                            "preview_lines": preview_lines,
                            "preview_line_count": len(preview_lines),
                            "data_line_count": data_line_count,
                            "offset": 0 if pos_text is not None else offset,
                            "pos_filter": pos_filter,
                            "truncated": truncated,
                        },
                    },
                }
            )

        if kind == "jsonl":
            rows = []
            truncated = False
            try:
                with open(artifact_path, "r", encoding="utf-8", errors="replace") as handle:
                    for idx, line in enumerate(handle):
                        if idx >= limit:
                            truncated = True
                            break
                        raw_line = line.strip()
                        if not raw_line:
                            continue
                        try:
                            rows.append(json.loads(raw_line))
                        except json.JSONDecodeError:
                            rows.append({"_raw": raw_line})
            except Exception:
                app.logger.exception("Failed to read JSONL artifact preview for run_id=%s", run_id)
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": {
                                "code": "ARTIFACT_READ_FAILED",
                                "message": "Failed to read artifact preview.",
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
                        "artifact": {
                            "name": name,
                            "kind": kind,
                            "available": True,
                            "rows": rows,
                            "row_count": len(rows),
                            "truncated": truncated,
                        },
                    },
                }
            )

        # html
        html_truncated = False
        html_text = ""
        max_bytes = 500000
        try:
            with open(artifact_path, "r", encoding="utf-8", errors="replace") as handle:
                html_text = handle.read(max_bytes + 1)
            if len(html_text) > max_bytes:
                html_text = html_text[:max_bytes]
                html_truncated = True
        except Exception:
            app.logger.exception("Failed to read HTML artifact preview for run_id=%s", run_id)
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": {
                            "code": "ARTIFACT_READ_FAILED",
                            "message": "Failed to read artifact preview.",
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
                    "artifact": {
                        "name": name,
                        "kind": kind,
                        "available": True,
                        "html": html_text,
                        "truncated": html_truncated,
                    },
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

        def _variant_result_count(stats: dict | None) -> int | None:
            if not isinstance(stats, dict):
                return None
            for key in (
                "variants_written",
                "variants_processed",
                "pre_annotations_persisted",
                "snv_records_persisted",
            ):
                value = stats.get(key)
                if isinstance(value, int):
                    return value
            return None

        def _snapshot_events() -> tuple[list[str], dict[str, str], dict[str, int | None]]:
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
            output_count_by_name: dict[str, int | None] = {}
            for stage_name in PIPELINE_STAGE_ORDER:
                stage = by_name.get(stage_name) or {"stage_name": stage_name, "status": "queued"}
                stage_status_by_name[stage_name] = stage.get("status") or "queued"
                output_count_by_name[stage_name] = _variant_result_count(stage.get("stats"))
                stage_payload = SseEnvelope(
                    run_id=run_id,
                    event_at=event_at,
                    data=stage,
                ).to_dict()
                messages.append(format_sse_event("stage_status", stage_payload))

            return messages, stage_status_by_name, output_count_by_name

        def _event_stream():
            import sqlite3

            yield format_sse_retry(2000)
            snapshot_messages, stage_status_by_name, output_count_by_name = _snapshot_events()
            for msg in snapshot_messages:
                yield msg

            last_run_status = run.get("status")
            last_stage_status_by_name: dict[str, str] = dict(stage_status_by_name)
            last_output_count_by_name: dict[str, int | None] = dict(output_count_by_name)
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
                        stats_count = _variant_result_count(stage.get("stats"))
                        emitted_variant_result = False
                        if stats_count is not None and stats_count != last_output_count_by_name.get(name):
                            last_output_count_by_name[name] = stats_count
                            result_payload = SseEnvelope(
                                run_id=run_id,
                                event_at=now_iso8601(),
                                data={
                                    "stage_name": name,
                                    "status": status,
                                    "variants_written": stats_count,
                                },
                            ).to_dict()
                            yield format_sse_event("variant_result", result_payload)
                            emitted_variant_result = True
                        if last_stage_status_by_name.get(name) != status:
                            last_stage_status_by_name[name] = status
                            payload = SseEnvelope(
                                run_id=run_id,
                                event_at=now_iso8601(),
                                data=stage,
                            ).to_dict()
                            yield format_sse_event("stage_status", payload)

                            if status in {"succeeded", "failed"} and not emitted_variant_result:
                                result_data = {"stage_name": name, "status": status}
                                if stats_count is not None:
                                    result_data["variants_written"] = stats_count
                                result_payload = SseEnvelope(
                                    run_id=run_id,
                                    event_at=now_iso8601(),
                                    data=result_data,
                                ).to_dict()
                                yield format_sse_event("variant_result", result_payload)

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
        run = prepared.get("run") or {}
        if started_stage is None:
            return jsonify(
                {
                    "ok": True,
                    "data": {
                        **_serialize_run_for_response(run, run_id=run_id),
                        "started_stage": None,
                    },
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

        run_logger = _build_run_logger(run_id)
        from run_logging import close_run_logger, log_run_event

        log_run_event(
            run_logger,
            "run_start",
            "Run started.",
            stage_name=started_stage,
            status="running",
        )

        def _background_execute():
            unexpected_error = False
            try:
                run_pipeline(
                    db_path,
                    run_id,
                    max_decompressed_bytes=app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"],
                    logger=run_logger,
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
                unexpected_error = True
                app.logger.exception("Pipeline orchestration crashed for run_id=%s", run_id)
            finally:
                try:
                    final_status = _determine_final_status_for_logging(
                        db_path,
                        run_id,
                        force_failed=unexpected_error,
                    )
                    log_run_event(
                        run_logger,
                        "run_finalize",
                        "Run finalized.",
                        status=final_status,
                    )
                    _finalize_run_status(db_path, run_id, force_failed=unexpected_error)
                    close_run_logger(run_logger)
                finally:
                    try:
                        from pipeline.cancel_signals import clear_run_cancel_request

                        clear_run_cancel_request(run_id)
                    except Exception:
                        app.logger.exception("Failed to clear cancel signal for run_id=%s", run_id)

        threading.Thread(target=_background_execute, daemon=True).start()

        return jsonify(
            {
                "ok": True,
                "data": {
                    **_serialize_run_for_response(run, run_id=run_id, status_override="running"),
                    "started_stage": started_stage,
                },
            }
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
                        **_serialize_run_for_response(run, run_id=run_id),
                        "started_stage": None,
                    },
                }
            )

        run_logger = _build_run_logger(run_id)
        from run_logging import close_run_logger, log_run_event

        log_run_event(
            run_logger,
            "run_start",
            "Run retry started.",
            stage_name=normalized_stage,
            status="running",
            details={"retry_stage": normalized_stage},
        )

        def _background_execute():
            unexpected_error = False
            try:
                run_pipeline(
                    db_path,
                    run_id,
                    max_decompressed_bytes=app.config["SP_MAX_VCF_DECOMPRESSED_BYTES"],
                    logger=run_logger,
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
                unexpected_error = True
                app.logger.exception("Pipeline orchestration crashed for run_id=%s", run_id)
            finally:
                try:
                    final_status = _determine_final_status_for_logging(
                        db_path,
                        run_id,
                        force_failed=unexpected_error,
                    )
                    log_run_event(
                        run_logger,
                        "run_finalize",
                        "Run finalized.",
                        status=final_status,
                    )
                    _finalize_run_status(db_path, run_id, force_failed=unexpected_error)
                    close_run_logger(run_logger)
                finally:
                    try:
                        from pipeline.cancel_signals import clear_run_cancel_request

                        clear_run_cancel_request(run_id)
                    except Exception:
                        app.logger.exception("Failed to clear cancel signal for run_id=%s", run_id)

        threading.Thread(target=_background_execute, daemon=True).start()

        return jsonify(
            {
                "ok": True,
                "data": {
                    "run_id": run_id,
                    "stage_name": normalized_stage,
                    "preserved_stages": preserved_stages,
                    "reset_stages": reset_stages,
                    **_serialize_run_for_response(run, run_id=run_id, status_override="running"),
                    "started_stage": started_stage,
                },
            }
        )

    @app.post("/api/v1/runs/<run_id>/cancel")
    def cancel_run(run_id: str):
        from pipeline.cancel_signals import request_run_cancel
        from storage.runs import (
            RunNotCancelableError,
            RunNotFoundError,
            cancel_run as cancel_run_record,
        )

        request_run_cancel(run_id)
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

        run_logger = _build_run_logger(run_id)
        from run_logging import log_run_event

        log_run_event(run_logger, "run_cancel", "Run canceled.", status="canceled")

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
