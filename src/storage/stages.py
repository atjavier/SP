import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from storage.db import init_schema, open_connection


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
            """,
            (run_id, stage_name, "running", started_at, input_uploaded_at),
        )
        if commit:
            active.commit()


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
            """,
            (
                run_id,
                stage_name,
                "blocked",
                completed_at,
                input_uploaded_at,
                error_code,
                error_message,
                json.dumps(error_details or {}),
            ),
        )
        if commit:
            active.commit()
