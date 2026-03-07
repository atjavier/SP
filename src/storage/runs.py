import uuid
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


_VALID_RUN_STATUSES: frozenset[str] = frozenset({"queued", "running", "canceled"})


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


def list_runs(db_path: str) -> list[dict[str, str]]:
    with open_connection(db_path) as conn:
        init_schema(conn)
        rows = conn.execute(
            "SELECT run_id, status, created_at, reference_build FROM runs ORDER BY created_at DESC",
        ).fetchall()
    return [
        {"run_id": row[0], "status": row[1], "created_at": row[2], "reference_build": row[3]}
        for row in rows
    ]


def create_run(db_path: str) -> dict[str, str]:
    run_id = str(uuid.uuid4())
    status = "queued"
    created_at = datetime.now(timezone.utc).isoformat()
    reference_build = "GRCh38"

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute(
            "INSERT INTO runs (run_id, status, created_at, reference_build) VALUES (?, ?, ?, ?)",
            (run_id, status, created_at, reference_build),
        )
        ensure_pipeline_stages_exist(db_path, run_id, conn=conn, commit=False)
        conn.commit()

    return {
        "run_id": run_id,
        "status": status,
        "created_at": created_at,
        "reference_build": reference_build,
    }


def get_run(db_path: str, run_id: str) -> dict[str, str] | None:
    with open_connection(db_path) as conn:
        init_schema(conn)
        row = conn.execute(
            "SELECT run_id, status, created_at, reference_build FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    if not row:
        return None
    return {"run_id": row[0], "status": row[1], "created_at": row[2], "reference_build": row[3]}


def cancel_run(db_path: str, run_id: str) -> dict[str, str]:
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
            conn.commit()

            row = conn.execute(
                "SELECT run_id, status, created_at, reference_build FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                raise RunNotFoundError("Run not found.")
            return {
                "run_id": row[0],
                "status": row[1],
                "created_at": row[2],
                "reference_build": row[3],
            }

        conn.commit()

        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            raise RunNotFoundError("Run not found.")
        raise RunNotCancelableError(row[0])
