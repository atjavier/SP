import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_VALID_OUTCOMES: frozenset[str] = frozenset({"computed", "not_applicable", "not_computed", "error"})

_PREDICTOR_OUTPUTS_ORDER_BY = "\n" + variant_order_by("v") + ",\n  o.predictor_key ASC"


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def clear_predictor_outputs_for_run(
    db_path: str,
    run_id: str,
    *,
    predictor_key: str | None = None,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        if predictor_key:
            active.execute(
                "DELETE FROM run_predictor_outputs WHERE run_id = ? AND predictor_key = ?",
                (run_id, predictor_key),
            )
        else:
            active.execute("DELETE FROM run_predictor_outputs WHERE run_id = ?", (run_id,))
        if commit:
            active.commit()


def upsert_predictor_outputs_for_run(
    db_path: str,
    run_id: str,
    outputs: list[dict],
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    if not outputs:
        return

    rows: list[tuple] = []
    for o in outputs:
        predictor_key = o.get("predictor_key")
        if not predictor_key:
            raise ValueError("predictor_key is required.")

        outcome = o.get("outcome")
        if outcome not in _VALID_OUTCOMES:
            raise ValueError(f"Invalid outcome: {outcome!r}")

        rows.append(
            (
                run_id,
                o["variant_id"],
                predictor_key,
                outcome,
                o.get("score"),
                o.get("label"),
                o.get("reason_code"),
                o.get("reason_message"),
                json.dumps(o.get("details") or {}),
                o["created_at"],
            )
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.executemany(
            """
            INSERT INTO run_predictor_outputs (
              run_id,
              variant_id,
              predictor_key,
              outcome,
              score,
              label,
              reason_code,
              reason_message,
              details_json,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, variant_id, predictor_key) DO UPDATE SET
              outcome = excluded.outcome,
              score = excluded.score,
              label = excluded.label,
              reason_code = excluded.reason_code,
              reason_message = excluded.reason_message,
              details_json = excluded.details_json,
              created_at = excluded.created_at
            """,
            rows,
        )
        if commit:
            active.commit()


def list_predictor_outputs_for_run(
    db_path: str,
    run_id: str,
    *,
    predictor_key: str | None = None,
    variant_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    safe_offset = max(0, int(offset or 0))
    if variant_id:
        safe_offset = 0

    where = ["o.run_id = ?"]
    params: list[object] = [run_id]
    if predictor_key:
        where.append("o.predictor_key = ?")
        params.append(predictor_key)
    if variant_id:
        where.append("o.variant_id = ?")
        params.append(variant_id)

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        variant_ids = [
            row[0]
            for row in active.execute(
                """
                SELECT v.variant_id
                FROM run_variants v
                JOIN run_predictor_outputs o
                  ON o.run_id = v.run_id AND o.variant_id = v.variant_id
                WHERE
                """
                + " AND ".join(where)
                + "\n"
                + variant_order_by("v")
                + """
                LIMIT ? OFFSET ?
                """,
                (*params, safe_limit, safe_offset),
            ).fetchall()
        ]

        if not variant_ids:
            return []

        variant_placeholders = ", ".join("?" for _ in variant_ids)
        output_where = ["o.run_id = ?", f"o.variant_id IN ({variant_placeholders})"]
        output_params: list[object] = [run_id, *variant_ids]
        if predictor_key:
            output_where.append("o.predictor_key = ?")
            output_params.append(predictor_key)

        rows = active.execute(
            """
            SELECT
              o.run_id,
              o.variant_id,
              v.chrom,
              v.pos,
              v.ref,
              v.alt,
              v.source_line,
              o.predictor_key,
              o.outcome,
              o.score,
              o.label,
              o.reason_code,
              o.reason_message,
              o.details_json,
              o.created_at
            FROM run_predictor_outputs o
            JOIN run_variants v ON v.variant_id = o.variant_id AND v.run_id = o.run_id
            WHERE
            """
            + " AND ".join(output_where)
            + "\n"
            + _PREDICTOR_OUTPUTS_ORDER_BY
            + """
            """,
            output_params,
        ).fetchall()

    items: list[dict] = []
    for r in rows:
        chrom = r[2]
        pos = r[3]
        ref = r[4]
        alt = r[5]
        items.append(
            {
                "run_id": r[0],
                "variant_id": r[1],
                "variant_key": f"{chrom}:{pos}:{ref}>{alt}",
                "chrom": chrom,
                "pos": pos,
                "ref": ref,
                "alt": alt,
                "source_line": r[6],
                "predictor_key": r[7],
                "outcome": r[8],
                "score": r[9],
                "label": r[10],
                "reason_code": r[11],
                "reason_message": r[12],
                "details": json.loads(r[13] or "{}"),
                "created_at": r[14],
            }
        )

    return items


def count_predictor_outputs_for_run(
    db_path: str,
    run_id: str,
    *,
    predictor_key: str | None = None,
    variant_id: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> int:
    where = ["o.run_id = ?"]
    params: list[object] = [run_id]
    if predictor_key:
        where.append("o.predictor_key = ?")
        params.append(predictor_key)
    if variant_id:
        where.append("o.variant_id = ?")
        params.append(variant_id)

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        row = active.execute(
            """
            SELECT COUNT(DISTINCT v.variant_id)
            FROM run_variants v
            JOIN run_predictor_outputs o
              ON o.run_id = v.run_id AND o.variant_id = v.variant_id
            WHERE
            """
            + " AND ".join(where),
            params,
        ).fetchone()
    return int(row[0] if row else 0)
