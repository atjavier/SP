import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_VALID_OUTCOMES: frozenset[str] = frozenset({"found", "not_found", "error"})
_SOURCE = "gnomad"
_ORDER_BY = "\n" + variant_order_by("v")


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def clear_gnomad_evidence_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute("DELETE FROM run_gnomad_evidence WHERE run_id = ?", (run_id,))
        if commit:
            active.commit()


def upsert_gnomad_evidence_for_run(
    db_path: str,
    run_id: str,
    evidence_rows: list[dict],
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    if not evidence_rows:
        return

    rows: list[tuple] = []
    for row in evidence_rows:
        source = str(row.get("source") or _SOURCE).strip().lower()
        if source != _SOURCE:
            raise ValueError(f"Invalid gnomAD source: {source!r}")

        outcome = row.get("outcome")
        if outcome not in _VALID_OUTCOMES:
            raise ValueError(f"Invalid gnomAD outcome: {outcome!r}")

        gnomad_variant_id = row.get("gnomad_variant_id")
        if outcome == "found" and not gnomad_variant_id:
            raise ValueError("gnomad_variant_id is required for outcome='found'.")
        if outcome != "found" and gnomad_variant_id:
            raise ValueError("gnomad_variant_id must be null for outcome != 'found'.")

        rows.append(
            (
                run_id,
                row["variant_id"],
                source,
                outcome,
                gnomad_variant_id,
                row.get("global_af"),
                row.get("reason_code"),
                row.get("reason_message"),
                json.dumps(row.get("details") or {}),
                row["retrieved_at"],
            )
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.executemany(
            """
            INSERT INTO run_gnomad_evidence (
              run_id,
              variant_id,
              source,
              outcome,
              gnomad_variant_id,
              global_af,
              reason_code,
              reason_message,
              details_json,
              retrieved_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, variant_id, source) DO UPDATE SET
              outcome = excluded.outcome,
              gnomad_variant_id = excluded.gnomad_variant_id,
              global_af = excluded.global_af,
              reason_code = excluded.reason_code,
              reason_message = excluded.reason_message,
              details_json = excluded.details_json,
              retrieved_at = excluded.retrieved_at
            """,
            rows,
        )
        if commit:
            active.commit()


def list_gnomad_evidence_for_run(
    db_path: str,
    run_id: str,
    *,
    variant_id: str | None = None,
    limit: int = 100,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    requested_variant_id = (variant_id or "").strip() or None
    if requested_variant_id:
        safe_limit = 1

    where = ["e.run_id = ?", "e.source = ?"]
    params: list[object] = [run_id, _SOURCE]
    if requested_variant_id:
        where.append("e.variant_id = ?")
        params.append(requested_variant_id)

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        rows = active.execute(
            """
            SELECT
              e.run_id,
              e.variant_id,
              v.chrom,
              v.pos,
              v.ref,
              v.alt,
              v.source_line,
              e.source,
              e.outcome,
              e.gnomad_variant_id,
              e.global_af,
              e.reason_code,
              e.reason_message,
              e.details_json,
              e.retrieved_at
            FROM run_gnomad_evidence e
            JOIN run_variants v ON v.variant_id = e.variant_id AND v.run_id = e.run_id
            WHERE
            """
            + " AND ".join(where)
            + "\n"
            + _ORDER_BY
            + """
            LIMIT ?
            """,
            (*params, safe_limit),
        ).fetchall()

    items: list[dict] = []
    for row in rows:
        chrom = row[2]
        pos = row[3]
        ref = row[4]
        alt = row[5]
        items.append(
            {
                "run_id": row[0],
                "variant_id": row[1],
                "variant_key": f"{chrom}:{pos}:{ref}>{alt}",
                "chrom": chrom,
                "pos": pos,
                "ref": ref,
                "alt": alt,
                "source_line": row[6],
                "source": row[7],
                "outcome": row[8],
                "gnomad_variant_id": row[9],
                "global_af": row[10],
                "reason_code": row[11],
                "reason_message": row[12],
                "details": json.loads(row[13] or "{}"),
                "retrieved_at": row[14],
            }
        )
    return items

