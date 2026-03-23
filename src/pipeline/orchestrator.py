from storage.run_inputs import get_run_input, get_run_upload_path
from storage.runs import get_run
from storage.stages import (
    PIPELINE_STAGE_ORDER,
    get_stage,
    list_pipeline_stages,
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)

from pipeline.parser_stage import StageExecutionError, run_parser_stage
from pipeline.pre_annotation_stage import run_pre_annotation_stage
from pipeline.classification_stage import run_classification_stage
from pipeline.prediction_stage import run_prediction_stage
from pipeline.annotation_stage import run_annotation_stage


class OrchestratorError(Exception):
    def __init__(self, http_status: int, code: str, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details or {}


def _build_reporting_stats(db_path: str, run_id: str) -> dict:
    annotation_stage = get_stage(db_path, run_id, "annotation") or {}
    annotation_stats = annotation_stage.get("stats")
    if not isinstance(annotation_stats, dict):
        return {"status": "completed"}

    reporting_stats: dict = {"status": "completed"}
    for key in (
        "annotation_evidence_completeness",
        "annotation_evidence_policy",
        "evidence_mode_requested",
        "evidence_mode_effective",
        "evidence_mode_decision_reason",
        "evidence_online_available",
        "evidence_offline_sources_configured",
        "evidence_source_completeness",
        "evidence_source_completeness_reason",
        "evidence_failed_sources",
        "evidence_complete_sources",
        "evidence_partial_sources",
        "evidence_unavailable_sources",
    ):
        value = annotation_stats.get(key)
        if value is not None:
            reporting_stats[key] = value
    return reporting_stats


def prepare_pipeline_start(db_path: str, run_id: str) -> dict:
    run = get_run(db_path, run_id)
    if not run:
        raise OrchestratorError(404, "RUN_NOT_FOUND", "Run not found.")
    if run.get("status") == "canceled":
        raise OrchestratorError(409, "RUN_CANCELED", "Run is canceled and cannot be started.")

    run_input = get_run_input(db_path, run_id)
    if not run_input:
        raise OrchestratorError(409, "VCF_NOT_UPLOADED", "No VCF uploaded for this run.")
    if not run_input.get("validation", {}).get("ok"):
        raise OrchestratorError(409, "VCF_NOT_VALIDATED", "Uploaded VCF is not valid; fix validation errors before starting.")

    uploaded_at = run_input.get("uploaded_at")
    started_stage = determine_start_stage(db_path, run_id, uploaded_at=uploaded_at)

    return {
        "run": run,
        "uploaded_at": uploaded_at,
        "started_stage": started_stage,
    }


def determine_start_stage(db_path: str, run_id: str, *, uploaded_at: str) -> str | None:
    stages = list_pipeline_stages(db_path, run_id)
    by_name = {s.get("stage_name"): s for s in stages if s.get("stage_name")}

    for stage_name in PIPELINE_STAGE_ORDER:
        stage = by_name.get(stage_name) or {}
        status = stage.get("status") or "queued"
        stage_uploaded_at = stage.get("input_uploaded_at")

        is_same_input = stage_uploaded_at == uploaded_at
        is_complete = status == "succeeded" and is_same_input

        if is_complete:
            continue

        if status in {"failed", "canceled"} and is_same_input:
            raise OrchestratorError(
                409,
                "STAGE_NOT_RESUMABLE",
                "Run has a non-resumable stage state; retry/resume is handled by a later story.",
                details={"stage_name": stage_name, "status": status},
            )

        return stage_name

    return None


def run_pipeline(
    db_path: str,
    run_id: str,
    *,
    max_decompressed_bytes: int,
    logger,
    prediction_config: dict | None = None,
) -> dict:
    from run_logging import log_run_event

    def _log_stage_start(stage_name: str) -> None:
        log_run_event(
            logger,
            "stage_start",
            f"Stage {stage_name} started.",
            stage_name=stage_name,
            status="running",
        )

    def _log_stage_success(stage_name: str) -> None:
        log_run_event(
            logger,
            "stage_success",
            f"Stage {stage_name} succeeded.",
            stage_name=stage_name,
            status="succeeded",
        )

    def _log_stage_failure(stage_name: str, code: str, message: str, details: dict | None = None) -> None:
        log_run_event(
            logger,
            "stage_failed",
            f"Stage {stage_name} failed.",
            stage_name=stage_name,
            status="failed",
            error_code=code,
            error_message=message,
            details=details or {},
        )

    prepared = prepare_pipeline_start(db_path, run_id)
    run = prepared.get("run") or {}
    annotation_evidence_policy = run.get("annotation_evidence_policy")
    uploaded_at = prepared["uploaded_at"]
    upload_path = get_run_upload_path(db_path, run_id)

    started_stage = prepared["started_stage"]
    if started_stage is None:
        return {"run_id": run_id, "started_stage": None, "executed_stages": [], "stages": list_pipeline_stages(db_path, run_id)}

    start_index = PIPELINE_STAGE_ORDER.index(started_stage)

    executed: list[str] = []
    for stage_name in PIPELINE_STAGE_ORDER[start_index:]:
        latest = get_run(db_path, run_id)
        if latest and latest.get("status") == "canceled":
            raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

        current_stage = get_stage(db_path, run_id, stage_name)
        if current_stage and current_stage.get("status") == "canceled":
            raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

        if stage_name == "parser":
            _log_stage_start(stage_name)
            try:
                run_parser_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    upload_path=upload_path,
                    max_decompressed_bytes=max_decompressed_bytes,
                    logger=logger,
                    force=False,
                )
            except StageExecutionError as exc:
                if exc.code == "ALREADY_PARSED":
                    continue
                _log_stage_failure(stage_name, exc.code, exc.message, exc.details)
                raise OrchestratorError(exc.http_status, exc.code, exc.message, details=exc.details) from None

            latest_after = get_run(db_path, run_id)
            if latest_after and latest_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            _log_stage_success(stage_name)
            executed.append(stage_name)
            continue

        if stage_name == "pre_annotation":
            _log_stage_start(stage_name)
            try:
                run_pre_annotation_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    logger=logger,
                    force=False,
                )
            except StageExecutionError as exc:
                if exc.code == "ALREADY_PRE_ANNOTATED":
                    continue
                _log_stage_failure(stage_name, exc.code, exc.message, exc.details)
                raise OrchestratorError(exc.http_status, exc.code, exc.message, details=exc.details) from None

            latest_after = get_run(db_path, run_id)
            if latest_after and latest_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            _log_stage_success(stage_name)
            executed.append(stage_name)
            continue

        if stage_name == "classification":
            _log_stage_start(stage_name)
            try:
                run_classification_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    logger=logger,
                    force=False,
                    vep_config_overrides=prediction_config,
                )
            except StageExecutionError as exc:
                if exc.code == "ALREADY_CLASSIFIED":
                    continue
                _log_stage_failure(stage_name, exc.code, exc.message, exc.details)
                raise OrchestratorError(exc.http_status, exc.code, exc.message, details=exc.details) from None

            latest_after = get_run(db_path, run_id)
            if latest_after and latest_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            _log_stage_success(stage_name)
            executed.append(stage_name)
            continue

        if stage_name == "prediction":
            _log_stage_start(stage_name)
            try:
                run_prediction_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    logger=logger,
                    force=False,
                    vep_config_overrides=prediction_config,
                )
            except StageExecutionError as exc:
                if exc.code == "ALREADY_PREDICTED":
                    continue
                _log_stage_failure(stage_name, exc.code, exc.message, exc.details)
                raise OrchestratorError(exc.http_status, exc.code, exc.message, details=exc.details) from None

            latest_after = get_run(db_path, run_id)
            if latest_after and latest_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            _log_stage_success(stage_name)
            executed.append(stage_name)
            continue

        if stage_name == "annotation":
            _log_stage_start(stage_name)
            try:
                run_annotation_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    logger=logger,
                    force=False,
                    evidence_failure_policy=annotation_evidence_policy,
                )
            except StageExecutionError as exc:
                if exc.code == "ALREADY_ANNOTATED":
                    continue
                _log_stage_failure(stage_name, exc.code, exc.message, exc.details)
                raise OrchestratorError(exc.http_status, exc.code, exc.message, details=exc.details) from None

            latest_after = get_run(db_path, run_id)
            if latest_after and latest_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            _log_stage_success(stage_name)
            executed.append(stage_name)
            continue

        try:
            _log_stage_start(stage_name)
            mark_stage_running(db_path, run_id, stage_name, input_uploaded_at=uploaded_at)

            latest_after_start = get_run(db_path, run_id)
            if latest_after_start and latest_after_start.get("status") == "canceled":
                mark_stage_canceled(db_path, run_id, stage_name, input_uploaded_at=uploaded_at)
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

            stage_stats = {"status": "completed"}
            if stage_name == "reporting":
                stage_stats = _build_reporting_stats(db_path, run_id)

            mark_stage_succeeded(
                db_path,
                run_id,
                stage_name,
                input_uploaded_at=uploaded_at,
                stats=stage_stats,
            )
            stage_after = get_stage(db_path, run_id, stage_name)
            if stage_after and stage_after.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")
            latest_after_complete = get_run(db_path, run_id)
            if latest_after_complete and latest_after_complete.get("status") == "canceled":
                raise OrchestratorError(409, "RUN_CANCELED", "Run was canceled.")

        except OrchestratorError:
            raise
        except Exception as exc:
            logger.exception("Stage execution failed: %s", stage_name)
            _log_stage_failure(
                stage_name,
                "STAGE_FAILED",
                "Stage execution failed.",
                {"reason": str(exc)},
            )
            mark_stage_failed(
                db_path,
                run_id,
                stage_name,
                input_uploaded_at=uploaded_at,
                error_code="STAGE_FAILED",
                error_message="Stage execution failed.",
                error_details={"reason": str(exc)},
            )
            raise OrchestratorError(500, "STAGE_FAILED", "Stage execution failed.") from None
        _log_stage_success(stage_name)
        executed.append(stage_name)

    return {
        "run_id": run_id,
        "started_stage": started_stage,
        "executed_stages": executed,
        "stages": list_pipeline_stages(db_path, run_id),
    }
