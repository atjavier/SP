import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from storage.db import init_schema, open_connection


PIPELINE_STAGE_ORDER: tuple[str, ...] = (
    "parser",
    "pre_annotation",
    "classification",
    "prediction",
    "annotation",
    "reporting",
)

_VALID_STAGE_STATUSES: frozenset[str] = frozenset(
    {"queued", "running", "succeeded", "failed", "canceled"}
)


class StageResetRunCanceledError(Exception):
    pass


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def get_stage(db_path: str, run_id: str, stage_name: str) -> dict | None:
    with _maybe_connection(db_path, None) as conn:
        init_schema(conn)
        row = conn.execute(
            """
            SELECT status, started_at, completed_at, input_uploaded_at, stats_json,
                   error_code, error_message, error_details_json
            FROM run_stages
            WHERE run_id = ? AND stage_name = ?
            """,
            (run_id, stage_name),
        ).fetchone()
    if not row:
        return None
    stats = json.loads(row[4] or "null")
    details = json.loads(row[7] or "null")
    return {
        "run_id": run_id,
        "stage_name": stage_name,
        "status": row[0],
        "started_at": row[1],
        "completed_at": row[2],
        "input_uploaded_at": row[3],
        "stats": stats,
        "error": {
            "code": row[5],
            "message": row[6],
            "details": details,
        }
        if row[5] or row[6] or details
        else None,
    }


def ensure_pipeline_stages_exist(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        for stage_name in PIPELINE_STAGE_ORDER:
            active.execute(
                """
                INSERT OR IGNORE INTO run_stages (
                  run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
                  stats_json, error_code, error_message, error_details_json
                )
                VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                (run_id, stage_name, "queued"),
            )
        if commit:
            active.commit()


def list_pipeline_stages(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    backfill_missing: bool = False,
    commit: bool = True,
) -> list[dict]:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        if backfill_missing:
            for stage_name in PIPELINE_STAGE_ORDER:
                active.execute(
                    """
                    INSERT OR IGNORE INTO run_stages (
                      run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
                      stats_json, error_code, error_message, error_details_json
                    )
                    VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                    """,
                    (run_id, stage_name, "queued"),
                )
            if commit:
                active.commit()

        rows = active.execute(
            """
            SELECT stage_name, status, started_at, completed_at, input_uploaded_at, stats_json,
                   error_code, error_message, error_details_json
            FROM run_stages
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchall()

    by_name: dict[str, dict] = {}
    for row in rows:
        stage_name = row[0]
        if stage_name not in PIPELINE_STAGE_ORDER:
            continue
        stats = json.loads(row[5] or "null")
        details = json.loads(row[8] or "null")

        status = row[1]
        unknown_status = status not in _VALID_STAGE_STATUSES
        if unknown_status:
            status = "failed"
            if not (row[6] or row[7] or details):
                details = {"original_status": row[1]}
                error_code = "UNKNOWN_STAGE_STATUS"
                error_message = "Stage has an unknown persisted status."
            else:
                error_code = row[6]
                error_message = row[7]
        else:
            error_code = row[6]
            error_message = row[7]

        by_name[stage_name] = {
            "stage_name": stage_name,
            "status": status,
            "started_at": row[2],
            "completed_at": row[3],
            "input_uploaded_at": row[4],
            "stats": stats,
            "error": {
                "code": error_code,
                "message": error_message,
                "details": details,
            }
            if error_code or error_message or details
            else None,
        }

    stages: list[dict] = []
    for stage_name in PIPELINE_STAGE_ORDER:
        stages.append(by_name.get(stage_name) or {"stage_name": stage_name, "status": "queued"})
    return stages


def mark_stage_running(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    input_uploaded_at: str | None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute(
            """
            INSERT INTO run_stages (
              run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
              stats_json, error_code, error_message, error_details_json
            )
            VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL)
            ON CONFLICT(run_id, stage_name) DO UPDATE SET
              status = excluded.status,
              started_at = excluded.started_at,
              completed_at = NULL,
              input_uploaded_at = excluded.input_uploaded_at,
              stats_json = NULL,
              error_code = NULL,
              error_message = NULL,
              error_details_json = NULL
            WHERE run_stages.status != 'canceled'
            """,
            (run_id, stage_name, "running", started_at, input_uploaded_at),
        )
        if commit:
            active.commit()


def reset_stage_and_downstream(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> list[str]:
    if stage_name not in PIPELINE_STAGE_ORDER:
        raise ValueError(f"Unknown stage: {stage_name}")

    start_index = PIPELINE_STAGE_ORDER.index(stage_name)
    stages_to_reset = list(PIPELINE_STAGE_ORDER[start_index:])

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)

        began_transaction = False
        if not active.in_transaction:
            active.execute("BEGIN IMMEDIATE")
            began_transaction = True

        try:
            run_row = active.execute(
                "SELECT status FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row and run_row[0] == "canceled":
                raise StageResetRunCanceledError("Run is canceled.")

            ensure_pipeline_stages_exist(db_path, run_id, conn=active, commit=False)

            placeholders = ", ".join("?" for _ in stages_to_reset)
            active.execute(
                f"""
                UPDATE run_stages
                SET status = ?,
                    started_at = NULL,
                    completed_at = NULL,
                    input_uploaded_at = NULL,
                    stats_json = NULL,
                    error_code = NULL,
                    error_message = NULL,
                    error_details_json = NULL
                WHERE run_id = ?
                  AND stage_name IN ({placeholders})
                """,
                ("queued", run_id, *stages_to_reset),
            )

            if stage_name == "parser":
                from storage.variants import clear_variants_for_run

                clear_variants_for_run(db_path, run_id, conn=active, commit=False)

            if "pre_annotation" in stages_to_reset:
                from storage.pre_annotations import clear_pre_annotations_for_run

                clear_pre_annotations_for_run(db_path, run_id, conn=active, commit=False)

            if "classification" in stages_to_reset:
                from storage.classifications import clear_classifications_for_run

                clear_classifications_for_run(db_path, run_id, conn=active, commit=False)

            if "prediction" in stages_to_reset:
                from storage.predictor_outputs import clear_predictor_outputs_for_run

                clear_predictor_outputs_for_run(db_path, run_id, conn=active, commit=False)

            if "annotation" in stages_to_reset:
                from storage.clinvar_evidence import clear_clinvar_evidence_for_run
                from storage.dbsnp_evidence import clear_dbsnp_evidence_for_run
                from storage.gnomad_evidence import clear_gnomad_evidence_for_run

                clear_dbsnp_evidence_for_run(db_path, run_id, conn=active, commit=False)
                clear_clinvar_evidence_for_run(db_path, run_id, conn=active, commit=False)
                clear_gnomad_evidence_for_run(db_path, run_id, conn=active, commit=False)

            if commit and began_transaction:
                active.commit()
        except Exception:
            if began_transaction:
                active.rollback()
            raise

    return stages_to_reset


def mark_stage_succeeded(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    input_uploaded_at: str | None,
    stats: dict,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    completed_at = datetime.now(timezone.utc).isoformat()
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute(
            """
            INSERT INTO run_stages (
              run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
              stats_json, error_code, error_message, error_details_json
            )
            VALUES (?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL)
            ON CONFLICT(run_id, stage_name) DO UPDATE SET
              status = excluded.status,
              completed_at = excluded.completed_at,
              input_uploaded_at = excluded.input_uploaded_at,
              stats_json = excluded.stats_json,
              error_code = NULL,
              error_message = NULL,
              error_details_json = NULL
            WHERE run_stages.status != 'canceled'
            """,
            (run_id, stage_name, "succeeded", completed_at, input_uploaded_at, json.dumps(stats)),
        )
        if commit:
            active.commit()


def mark_stage_failed(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    input_uploaded_at: str | None,
    error_code: str,
    error_message: str,
    error_details: dict | None = None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    completed_at = datetime.now(timezone.utc).isoformat()
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute(
            """
            INSERT INTO run_stages (
              run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
              stats_json, error_code, error_message, error_details_json
            )
            VALUES (?, ?, ?, NULL, ?, ?, NULL, ?, ?, ?)
            ON CONFLICT(run_id, stage_name) DO UPDATE SET
              status = excluded.status,
              completed_at = excluded.completed_at,
              input_uploaded_at = excluded.input_uploaded_at,
              stats_json = NULL,
              error_code = excluded.error_code,
              error_message = excluded.error_message,
              error_details_json = excluded.error_details_json
            WHERE run_stages.status != 'canceled'
            """,
            (
                run_id,
                stage_name,
                "failed",
                completed_at,
                input_uploaded_at,
                error_code,
                error_message,
                json.dumps(error_details or {}),
            ),
        )
        if commit:
            active.commit()


def mark_stage_blocked(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    input_uploaded_at: str | None,
    error_code: str,
    error_message: str,
    error_details: dict | None = None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    mark_stage_failed(
        db_path,
        run_id,
        stage_name,
        input_uploaded_at=input_uploaded_at,
        error_code=error_code,
        error_message=error_message,
        error_details=error_details,
        conn=conn,
        commit=commit,
    )


def mark_stage_canceled(
    db_path: str,
    run_id: str,
    stage_name: str,
    *,
    input_uploaded_at: str | None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    completed_at = datetime.now(timezone.utc).isoformat()
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute(
            """
            INSERT INTO run_stages (
              run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
              stats_json, error_code, error_message, error_details_json
            )
            VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, NULL)
            ON CONFLICT(run_id, stage_name) DO UPDATE SET
              status = excluded.status,
              completed_at = excluded.completed_at,
              input_uploaded_at = excluded.input_uploaded_at,
              stats_json = NULL,
              error_code = NULL,
              error_message = NULL,
              error_details_json = NULL
            """,
            (run_id, stage_name, "canceled", completed_at, input_uploaded_at),
        )
        if commit:
            active.commit()
