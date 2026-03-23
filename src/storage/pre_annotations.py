import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection


_PRE_ANNOTATION_ORDER_BY = """
ORDER BY
  CASE
    WHEN v.chrom GLOB '[0-9]*' THEN 0
    WHEN v.chrom = 'X' THEN 1
    WHEN v.chrom = 'Y' THEN 2
    WHEN v.chrom = 'MT' THEN 3
    ELSE 4
  END,
  CASE
    WHEN v.chrom GLOB '[0-9]*' THEN CAST(v.chrom AS INTEGER)
    WHEN v.chrom = 'X' THEN 23
    WHEN v.chrom = 'Y' THEN 24
    WHEN v.chrom = 'MT' THEN 25
    ELSE 1000
  END,
  v.pos ASC,
  v.ref ASC,
  v.alt ASC
"""


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def clear_pre_annotations_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute("DELETE FROM run_pre_annotations WHERE run_id = ?", (run_id,))
        if commit:
            active.commit()


def upsert_pre_annotations_for_run(
    db_path: str,
    run_id: str,
    pre_annotations: list[dict],
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    if not pre_annotations:
        return

    rows: list[tuple] = []
    for a in pre_annotations:
        rows.append(
            (
                run_id,
                a["variant_id"],
                a["variant_key"],
                a["base_change"],
                a["substitution_class"],
                a["ref_class"],
                a["alt_class"],
                json.dumps(a.get("details") or {}),
                a["created_at"],
            )
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.executemany(
            """
            INSERT INTO run_pre_annotations (
              run_id, variant_id, variant_key, base_change, substitution_class,
              ref_class, alt_class, details_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, variant_id) DO UPDATE SET
              variant_key = excluded.variant_key,
              base_change = excluded.base_change,
              substitution_class = excluded.substitution_class,
              ref_class = excluded.ref_class,
              alt_class = excluded.alt_class,
              details_json = excluded.details_json,
              created_at = excluded.created_at
            """,
            rows,
        )
        if commit:
            active.commit()


def list_pre_annotations_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        rows = active.execute(
            """
            SELECT a.run_id, a.variant_id, a.variant_key, a.base_change, a.substitution_class,
                   a.ref_class, a.alt_class, a.details_json, a.created_at
            FROM run_pre_annotations a
            JOIN run_variants v ON v.variant_id = a.variant_id AND v.run_id = a.run_id
            WHERE a.run_id = ?
            """
            + _PRE_ANNOTATION_ORDER_BY
            + """
            """,
            (run_id,),
        ).fetchall()

    return [
        {
            "run_id": r[0],
            "variant_id": r[1],
            "variant_key": r[2],
            "base_change": r[3],
            "substitution_class": r[4],
            "ref_class": r[5],
            "alt_class": r[6],
            "details": json.loads(r[7] or "{}"),
            "created_at": r[8],
        }
        for r in rows
    ]


def list_pre_annotations_for_run_public(
    db_path: str,
    run_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    variant_id: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    safe_offset = max(0, int(offset or 0))
    requested_variant_id = (variant_id or "").strip() or None
    if requested_variant_id:
        safe_limit = 1
        safe_offset = 0

    params: list[object] = [run_id]
    extra_where = ""
    if requested_variant_id:
        extra_where = " AND a.variant_id = ?"
        params.append(requested_variant_id)

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        rows = active.execute(
            """
            SELECT
              a.run_id,
              a.variant_id,
              a.variant_key,
              a.base_change,
              a.substitution_class,
              a.ref_class,
              a.alt_class,
              a.created_at
            FROM run_pre_annotations a
            JOIN run_variants v ON v.variant_id = a.variant_id AND v.run_id = a.run_id
            WHERE a.run_id = ?
            """
            + extra_where
            + "\n"
            + _PRE_ANNOTATION_ORDER_BY
            + """
            LIMIT ? OFFSET ?
            """,
            (*params, safe_limit, safe_offset),
        ).fetchall()

    return [
        {
            "run_id": r[0],
            "variant_id": r[1],
            "variant_key": r[2],
            "base_change": r[3],
            "substitution_class": r[4],
            "ref_class": r[5],
            "alt_class": r[6],
            "created_at": r[7],
        }
        for r in rows
    ]


def count_pre_annotations_for_run_public(
    db_path: str,
    run_id: str,
    *,
    variant_id: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> int:
    requested_variant_id = (variant_id or "").strip() or None

    params: list[object] = [run_id]
    extra_where = ""
    if requested_variant_id:
        extra_where = " AND a.variant_id = ?"
        params.append(requested_variant_id)

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        row = active.execute(
            """
            SELECT COUNT(*)
            FROM run_pre_annotations a
            JOIN run_variants v ON v.variant_id = a.variant_id AND v.run_id = a.run_id
            WHERE a.run_id = ?
            """
            + extra_where,
            params,
        ).fetchone()
    return int(row[0] if row else 0)
