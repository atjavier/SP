import os
import uuid
import json
import sqlite3
from datetime import datetime, timezone

from storage.db import init_schema, open_connection
from storage.stages import ensure_pipeline_stages_exist


class RunNotFoundError(Exception):
    pass


class RunNotCancelableError(Exception):
    def __init__(self, current_status: str) -> None:
        super().__init__(f"Run is not cancelable from status: {current_status}")
        self.current_status = current_status


class AnotherRunRunningError(Exception):
    def __init__(self, running_run_id: str) -> None:
        super().__init__("Another run is currently running.")
        self.running_run_id = running_run_id


class RunAlreadyRunningError(Exception):
    pass


class RunNotStartableError(Exception):
    def __init__(self, current_status: str) -> None:
        super().__init__(f"Run is not startable from status: {current_status}")
        self.current_status = current_status


_VALID_RUN_STATUSES: frozenset[str] = frozenset({"queued", "running", "failed", "canceled"})
_VALID_ANNOTATION_EVIDENCE_POLICIES: frozenset[str] = frozenset({"stop", "continue"})
_VALID_EVIDENCE_MODES: frozenset[str] = frozenset({"online", "offline", "hybrid"})
_EVIDENCE_SOURCES: tuple[str, ...] = ("dbsnp", "clinvar", "gnomad")


class RunPolicyNotUpdatableError(Exception):
    def __init__(self, current_status: str) -> None:
        super().__init__(f"Run policy cannot be updated from status: {current_status}")
        self.current_status = current_status


def _truthy_env(name: str) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def normalize_annotation_evidence_policy(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text in _VALID_ANNOTATION_EVIDENCE_POLICIES:
        return text
    return None


def normalize_evidence_mode(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text in {"local", "offline_local"}:
        return "offline"
    if text in _VALID_EVIDENCE_MODES:
        return text
    return None


def default_annotation_evidence_policy() -> str:
    explicit = normalize_annotation_evidence_policy(os.environ.get("SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT"))
    if explicit:
        return explicit
    legacy = os.environ.get("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR")
    if legacy is None:
        return "continue"
    return "stop" if _truthy_env("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR") else "continue"


def default_requested_evidence_mode() -> str:
    explicit = normalize_evidence_mode(os.environ.get("SP_EVIDENCE_MODE"))
    return explicit or "online"


def _normalize_offline_sources_configured(value: object) -> dict[str, bool]:
    loaded: object = value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except Exception:
            loaded = {}
    if not isinstance(loaded, dict):
        loaded = {}
    return {
        "dbsnp": bool(loaded.get("dbsnp", False)),
        "clinvar": bool(loaded.get("clinvar", False)),
        "gnomad": bool(loaded.get("gnomad", False)),
    }


def _normalize_online_available(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if int(value) in {0, 1}:
            return bool(int(value))
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _serialize_offline_sources_configured(value: object) -> str:
    return json.dumps(_normalize_offline_sources_configured(value), separators=(",", ":"))


def _row_to_run_record(row: tuple) -> dict[str, object]:
    requested = normalize_evidence_mode(row[5]) or default_requested_evidence_mode()
    effective = normalize_evidence_mode(row[6])
    return {
        "run_id": row[0],
        "status": row[1],
        "created_at": row[2],
        "reference_build": row[3],
        "annotation_evidence_policy": normalize_annotation_evidence_policy(row[4])
        or default_annotation_evidence_policy(),
        "evidence_mode_requested": requested,
        "evidence_mode_effective": effective,
        "evidence_online_available": _normalize_online_available(row[7]),
        "evidence_offline_sources_configured": _normalize_offline_sources_configured(row[8]),
        "evidence_mode_decision_reason": str(row[9]).strip() if row[9] is not None else None,
        "evidence_mode_detected_at": row[10],
    }


def recover_interrupted_runs(db_path: str) -> dict[str, int]:
    """
    Normalize stale `running` runs after process restart/interruption.

    - runs.status=running -> failed when interrupted stage is recovered, otherwise queued
    - run_stages.status=running -> failed (RUN_INTERRUPTED)
    """
    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        running_rows = conn.execute(
            "SELECT run_id FROM runs WHERE status = ?",
            ("running",),
        ).fetchall()
        if not running_rows:
            conn.commit()
            return {"runs_recovered": 0, "stages_recovered": 0}

        run_ids = [row[0] for row in running_rows]
        placeholders = ", ".join("?" for _ in run_ids)
        interrupted_at = datetime.now(timezone.utc).isoformat()
        interrupted_details = json.dumps({"reason": "startup_recovery"})

        stage_cursor = conn.execute(
            f"""
            UPDATE run_stages
            SET status = ?,
                completed_at = ?,
                stats_json = NULL,
                error_code = COALESCE(error_code, ?),
                error_message = COALESCE(error_message, ?),
                error_details_json = COALESCE(error_details_json, ?)
            WHERE run_id IN ({placeholders})
              AND status = ?
            """,
            (
                "failed",
                interrupted_at,
                "RUN_INTERRUPTED",
                "Run was interrupted before completion.",
                interrupted_details,
                *run_ids,
                "running",
            ),
        )
        stages_recovered = max(0, int(stage_cursor.rowcount or 0))

        conn.execute(
            f"""
            UPDATE runs
            SET status = ?
            WHERE run_id IN ({placeholders})
              AND EXISTS (
                SELECT 1
                FROM run_stages rs
                WHERE rs.run_id = runs.run_id
                  AND rs.status = ?
                  AND rs.error_code = ?
              )
            """,
            ("failed", *run_ids, "failed", "RUN_INTERRUPTED"),
        )
        conn.execute(
            f"UPDATE runs SET status = ? WHERE run_id IN ({placeholders}) AND status = ?",
            ("queued", *run_ids, "running"),
        )
        conn.commit()
        return {"runs_recovered": len(run_ids), "stages_recovered": stages_recovered}


def get_running_run_id(db_path: str) -> str | None:
    with open_connection(db_path) as conn:
        init_schema(conn)
        row = conn.execute(
            "SELECT run_id FROM runs WHERE status = ? LIMIT 1",
            ("running",),
        ).fetchone()
    return row[0] if row else None


def claim_run_for_execution(db_path: str, run_id: str) -> None:
    """
    Mark the given run as running while enforcing the demo constraint that only
    one run may be running at a time.

    This is implemented as a small atomic transaction so two starts cannot both
    observe "no running run" and proceed concurrently.
    """
    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            raise RunNotFoundError("Run not found.")

        current_status = row[0]
        if current_status == "canceled":
            conn.rollback()
            raise RunNotStartableError(current_status)

        other = conn.execute(
            "SELECT run_id FROM runs WHERE status = ? AND run_id <> ? LIMIT 1",
            ("running", run_id),
        ).fetchone()
        if other:
            conn.rollback()
            raise AnotherRunRunningError(other[0])

        if current_status == "running":
            conn.rollback()
            raise RunAlreadyRunningError("Run is already running.")

        conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            ("running", run_id),
        )
        conn.commit()


def set_run_status(db_path: str, run_id: str, status: str) -> None:
    if status not in _VALID_RUN_STATUSES:
        raise ValueError(f"Invalid run status: {status}")
    with open_connection(db_path) as conn:
        init_schema(conn)
        cursor = conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            (status, run_id),
        )
        conn.commit()
        if cursor.rowcount and cursor.rowcount > 0:
            return
    raise RunNotFoundError("Run not found.")


def set_run_status_if_not_canceled(db_path: str, run_id: str, status: str) -> None:
    if status not in _VALID_RUN_STATUSES:
        raise ValueError(f"Invalid run status: {status}")
    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            raise RunNotFoundError("Run not found.")
        if row[0] == "canceled":
            conn.rollback()
            return
        conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            (status, run_id),
        )
        conn.commit()


def list_runs(db_path: str) -> list[dict[str, object]]:
    with open_connection(db_path) as conn:
        init_schema(conn)
        rows = conn.execute(
            """
            SELECT
              run_id,
              status,
              created_at,
              reference_build,
              annotation_evidence_policy,
              evidence_mode_requested,
              evidence_mode_effective,
              evidence_online_available,
              evidence_offline_sources_configured_json,
              evidence_mode_decision_reason,
              evidence_mode_detected_at
            FROM runs
            ORDER BY created_at DESC
            """,
        ).fetchall()
    return [_row_to_run_record(row) for row in rows]


def create_run(db_path: str, *, annotation_evidence_policy: str | None = None) -> dict[str, object]:
    run_id = str(uuid.uuid4())
    status = "queued"
    created_at = datetime.now(timezone.utc).isoformat()
    reference_build = "GRCh38"
    policy = normalize_annotation_evidence_policy(annotation_evidence_policy) or default_annotation_evidence_policy()
    requested_mode = default_requested_evidence_mode()
    offline_sources = _serialize_offline_sources_configured({})

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute(
            """
            INSERT INTO runs (
              run_id, status, created_at, reference_build, annotation_evidence_policy,
              evidence_mode_requested, evidence_mode_effective, evidence_online_available,
              evidence_offline_sources_configured_json, evidence_mode_decision_reason, evidence_mode_detected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
            """,
            (
                run_id,
                status,
                created_at,
                reference_build,
                policy,
                requested_mode,
                offline_sources,
                "not_evaluated",
            ),
        )
        ensure_pipeline_stages_exist(db_path, run_id, conn=conn, commit=False)
        conn.commit()

    record = get_run(db_path, run_id)
    if not record:
        raise RunNotFoundError("Run not found.")
    return record


def get_run(db_path: str, run_id: str) -> dict[str, object] | None:
    with open_connection(db_path) as conn:
        init_schema(conn)
        row = conn.execute(
            """
            SELECT
              run_id,
              status,
              created_at,
              reference_build,
              annotation_evidence_policy,
              evidence_mode_requested,
              evidence_mode_effective,
              evidence_online_available,
              evidence_offline_sources_configured_json,
              evidence_mode_decision_reason,
              evidence_mode_detected_at
            FROM runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
    if not row:
        return None
    return _row_to_run_record(row)


def update_run_annotation_evidence_policy(
    db_path: str,
    run_id: str,
    *,
    annotation_evidence_policy: str,
) -> dict[str, object]:
    policy = normalize_annotation_evidence_policy(annotation_evidence_policy)
    if policy is None:
        raise ValueError("Invalid annotation_evidence_policy value.")

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            raise RunNotFoundError("Run not found.")
        current_status = row[0]
        if current_status == "running":
            conn.rollback()
            raise RunPolicyNotUpdatableError(current_status)
        conn.execute(
            "UPDATE runs SET annotation_evidence_policy = ? WHERE run_id = ?",
            (policy, run_id),
        )
        conn.commit()

    record = get_run(db_path, run_id)
    if not record:
        raise RunNotFoundError("Run not found.")
    return record


def update_run_evidence_mode_decision(
    db_path: str,
    run_id: str,
    *,
    requested_mode: str,
    effective_mode: str,
    online_available: bool | None,
    offline_sources_configured: dict[str, bool] | None,
    decision_reason: str | None,
    detected_at: str | None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    requested = normalize_evidence_mode(requested_mode) or default_requested_evidence_mode()
    effective = normalize_evidence_mode(effective_mode) or requested
    online_value = None if online_available is None else (1 if bool(online_available) else 0)
    offline_json = _serialize_offline_sources_configured(offline_sources_configured or {})
    reason_text = str(decision_reason).strip() if decision_reason is not None else None
    detected_text = str(detected_at).strip() if detected_at is not None else None

    if conn is None:
        with open_connection(db_path) as active:
            init_schema(active)
            cursor = active.execute(
                """
                UPDATE runs
                SET evidence_mode_requested = ?,
                    evidence_mode_effective = ?,
                    evidence_online_available = ?,
                    evidence_offline_sources_configured_json = ?,
                    evidence_mode_decision_reason = ?,
                    evidence_mode_detected_at = ?
                WHERE run_id = ?
                """,
                (
                    requested,
                    effective,
                    online_value,
                    offline_json,
                    reason_text,
                    detected_text,
                    run_id,
                ),
            )
            if commit:
                active.commit()
            if not cursor.rowcount:
                raise RunNotFoundError("Run not found.")
        return

    init_schema(conn)
    cursor = conn.execute(
        """
        UPDATE runs
        SET evidence_mode_requested = ?,
            evidence_mode_effective = ?,
            evidence_online_available = ?,
            evidence_offline_sources_configured_json = ?,
            evidence_mode_decision_reason = ?,
            evidence_mode_detected_at = ?
        WHERE run_id = ?
        """,
        (
            requested,
            effective,
            online_value,
            offline_json,
            reason_text,
            detected_text,
            run_id,
        ),
    )
    if commit:
        conn.commit()
    if not cursor.rowcount:
        raise RunNotFoundError("Run not found.")


def reset_run_evidence_mode_decision(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    requested = default_requested_evidence_mode()
    offline_json = _serialize_offline_sources_configured({})

    if conn is None:
        with open_connection(db_path) as active:
            init_schema(active)
            cursor = active.execute(
                """
                UPDATE runs
                SET evidence_mode_requested = ?,
                    evidence_mode_effective = NULL,
                    evidence_online_available = NULL,
                    evidence_offline_sources_configured_json = ?,
                    evidence_mode_decision_reason = ?,
                    evidence_mode_detected_at = NULL
                WHERE run_id = ?
                """,
                (requested, offline_json, "not_evaluated", run_id),
            )
            if commit:
                active.commit()
            if not cursor.rowcount:
                raise RunNotFoundError("Run not found.")
        return

    init_schema(conn)
    cursor = conn.execute(
        """
        UPDATE runs
        SET evidence_mode_requested = ?,
            evidence_mode_effective = NULL,
            evidence_online_available = NULL,
            evidence_offline_sources_configured_json = ?,
            evidence_mode_decision_reason = ?,
            evidence_mode_detected_at = NULL
        WHERE run_id = ?
        """,
        (requested, offline_json, "not_evaluated", run_id),
    )
    if commit:
        conn.commit()
    if not cursor.rowcount:
        raise RunNotFoundError("Run not found.")


def cancel_run(db_path: str, run_id: str) -> dict[str, object]:
    with open_connection(db_path) as conn:
        init_schema(conn)
        cursor = conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ? AND status IN (?, ?)",
            ("canceled", run_id, "queued", "running"),
        )

        if cursor.rowcount and cursor.rowcount > 0:
            ensure_pipeline_stages_exist(db_path, run_id, conn=conn, commit=False)
            canceled_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                UPDATE run_stages
                SET status = ?, completed_at = ?, stats_json = NULL,
                    error_code = NULL, error_message = NULL, error_details_json = NULL
                WHERE run_id = ? AND status IN (?, ?)
                """,
                ("canceled", canceled_at, run_id, "queued", "running"),
            )

            from storage.classifications import clear_classifications_for_run
            from storage.clinvar_evidence import clear_clinvar_evidence_for_run
            from storage.dbsnp_evidence import clear_dbsnp_evidence_for_run
            from storage.gnomad_evidence import clear_gnomad_evidence_for_run
            from storage.pre_annotations import clear_pre_annotations_for_run
            from storage.predictor_outputs import clear_predictor_outputs_for_run

            clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
            clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
            clear_predictor_outputs_for_run(db_path, run_id, conn=conn, commit=False)
            clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            conn.commit()

            row = conn.execute(
                """
                SELECT
                  run_id,
                  status,
                  created_at,
                  reference_build,
                  annotation_evidence_policy,
                  evidence_mode_requested,
                  evidence_mode_effective,
                  evidence_online_available,
                  evidence_offline_sources_configured_json,
                  evidence_mode_decision_reason,
                  evidence_mode_detected_at
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if not row:
                raise RunNotFoundError("Run not found.")
            return _row_to_run_record(row)

        conn.commit()

        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            raise RunNotFoundError("Run not found.")
        raise RunNotCancelableError(row[0])
