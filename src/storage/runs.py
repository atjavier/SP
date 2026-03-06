import uuid
from datetime import datetime, timezone

from storage.db import init_schema, open_connection


def create_run(db_path: str) -> dict[str, str]:
    run_id = str(uuid.uuid4())
    status = "created"
    created_at = datetime.now(timezone.utc).isoformat()

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.execute(
            "INSERT INTO runs (run_id, status, created_at) VALUES (?, ?, ?)",
            (run_id, status, created_at),
        )
        conn.commit()

    return {"run_id": run_id, "status": status, "created_at": created_at}


def get_run(db_path: str, run_id: str) -> dict[str, str] | None:
    with open_connection(db_path) as conn:
        init_schema(conn)
        row = conn.execute(
            "SELECT run_id, status, created_at FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    if not row:
        return None
    return {"run_id": row[0], "status": row[1], "created_at": row[2]}
