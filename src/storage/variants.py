import sqlite3
import uuid
from datetime import datetime, timezone

from storage.db import init_schema, open_connection


def clear_variants_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    if conn is None:
        with open_connection(db_path) as opened:
            init_schema(opened)
            opened.execute("DELETE FROM run_variants WHERE run_id = ?", (run_id,))
            if commit:
                opened.commit()
        return

    init_schema(conn)
    conn.execute("DELETE FROM run_variants WHERE run_id = ?", (run_id,))
    if commit:
        conn.commit()


def insert_variants_for_run(db_path: str, run_id: str, variants: list[dict]) -> None:
    if not variants:
        return
    created_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for v in variants:
        rows.append(
            (
                str(uuid.uuid4()),
                run_id,
                v["chrom"],
                int(v["pos"]),
                v["ref"],
                v["alt"],
                v.get("source_line"),
                created_at,
            )
        )

    with open_connection(db_path) as conn:
        init_schema(conn)
        conn.executemany(
            """
            INSERT OR IGNORE INTO run_variants (
              variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def list_variants_for_run(db_path: str, run_id: str, *, limit: int = 10) -> list[dict]:
    with open_connection(db_path) as conn:
        init_schema(conn)
        rows = conn.execute(
            """
            SELECT chrom, pos, ref, alt, source_line
            FROM run_variants
            WHERE run_id = ?
            ORDER BY pos ASC, chrom ASC
            LIMIT ?
            """,
            (run_id, limit),
        ).fetchall()
    return [
        {"chrom": r[0], "pos": r[1], "ref": r[2], "alt": r[3], "source_line": r[4]} for r in rows
    ]
