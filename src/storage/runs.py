import uuid
from datetime import datetime, timezone

from storage.db import init_schema, open_connection


class RunNotFoundError(Exception):
    pass


class RunNotCancelableError(Exception):
    def __init__(self, current_status: str) -> None:
        super().__init__(f"Run is not cancelable from status: {current_status}")
        self.current_status = current_status


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
        conn.commit()

        if cursor.rowcount and cursor.rowcount > 0:
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

        row = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if not row:
            raise RunNotFoundError("Run not found.")
        raise RunNotCancelableError(row[0])
