import sqlite3
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_VARIANT_ORDER_BY = "\n" + variant_order_by()


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
            """
            + _VARIANT_ORDER_BY
            + """
            LIMIT ?
            """,
            (run_id, limit),
        ).fetchall()
    return [
        {"chrom": r[0], "pos": r[1], "ref": r[2], "alt": r[3], "source_line": r[4]} for r in rows
    ]


def list_variants_for_run_with_ids(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    if conn is None:
        with open_connection(db_path) as opened:
            init_schema(opened)
            rows = opened.execute(
                """
                SELECT variant_id, chrom, pos, ref, alt, source_line
                FROM run_variants
                WHERE run_id = ?
                """
                + _VARIANT_ORDER_BY
                + """
                """,
                (run_id,),
            ).fetchall()
        return [
            {
                "variant_id": r[0],
                "chrom": r[1],
                "pos": r[2],
                "ref": r[3],
                "alt": r[4],
                "source_line": r[5],
            }
            for r in rows
        ]

    init_schema(conn)
    rows = conn.execute(
        """
        SELECT variant_id, chrom, pos, ref, alt, source_line
        FROM run_variants
        WHERE run_id = ?
        """
        + _VARIANT_ORDER_BY
        + """
        """,
        (run_id,),
    ).fetchall()
    return [
        {
            "variant_id": r[0],
            "chrom": r[1],
            "pos": r[2],
            "ref": r[3],
            "alt": r[4],
            "source_line": r[5],
        }
        for r in rows
    ]


def iter_variants_for_run_with_ids(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection,
) -> Iterator[dict]:
    init_schema(conn)
    cursor = conn.execute(
        """
        SELECT variant_id, chrom, pos, ref, alt, source_line
        FROM run_variants
        WHERE run_id = ?
        """
        + _VARIANT_ORDER_BY
        + """
        """,
        (run_id,),
    )
    for row in cursor:
        yield {
            "variant_id": row[0],
            "chrom": row[1],
            "pos": row[2],
            "ref": row[3],
            "alt": row[4],
            "source_line": row[5],
        }
