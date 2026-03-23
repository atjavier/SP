import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_VALID_OUTCOMES: frozenset[str] = frozenset({"found", "not_found", "error"})
_VALID_CLASSIFICATIONS: frozenset[str] = frozenset(
    {"unclassified", "synonymous", "missense", "nonsense", "other"}
)
_SOURCE = "dbsnp"
_ORDER_BY = "\n" + variant_order_by("v")


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def clear_dbsnp_evidence_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute("DELETE FROM run_dbsnp_evidence WHERE run_id = ?", (run_id,))
        if commit:
            active.commit()


def upsert_dbsnp_evidence_for_run(
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
            raise ValueError(f"Invalid dbSNP source: {source!r}")

        outcome = row.get("outcome")
        if outcome not in _VALID_OUTCOMES:
            raise ValueError(f"Invalid dbSNP outcome: {outcome!r}")

        rsid = row.get("rsid")
        if outcome == "found" and not rsid:
            raise ValueError("rsid is required for outcome='found'.")
        if outcome != "found" and rsid:
            raise ValueError("rsid must be null for outcome != 'found'.")

        rows.append(
            (
                run_id,
                row["variant_id"],
                source,
                outcome,
                rsid,
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
            INSERT INTO run_dbsnp_evidence (
              run_id,
              variant_id,
              source,
              outcome,
              rsid,
              reason_code,
              reason_message,
              details_json,
              retrieved_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, variant_id, source) DO UPDATE SET
              outcome = excluded.outcome,
              rsid = excluded.rsid,
              reason_code = excluded.reason_code,
              reason_message = excluded.reason_message,
              details_json = excluded.details_json,
              retrieved_at = excluded.retrieved_at
            """,
            rows,
        )
        if commit:
            active.commit()


def list_dbsnp_evidence_for_run(
    db_path: str,
    run_id: str,
    *,
    variant_id: str | None = None,
    classification: str | None = None,
    outcome: str | None = None,
    limit: int = 100,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    requested_variant_id = (variant_id or "").strip() or None
    requested_classification = _normalize_classification_filter(classification)
    requested_outcome = _normalize_outcome_filter(outcome)
    if requested_variant_id:
        safe_limit = 1
        requested_classification = None
        requested_outcome = None

    where = ["e.run_id = ?", "e.source = ?"]
    params: list[object] = [run_id, _SOURCE]
    if requested_variant_id:
        where.append("e.variant_id = ?")
        params.append(requested_variant_id)
    join_classification = ""
    if requested_classification:
        join_classification = "JOIN run_classifications c ON c.run_id = e.run_id AND c.variant_id = e.variant_id"
        where.append("c.consequence_category = ?")
        params.append(requested_classification)
    if requested_outcome:
        where.append("e.outcome = ?")
        params.append(requested_outcome)

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
              e.rsid,
              e.reason_code,
              e.reason_message,
              e.details_json,
              e.retrieved_at
            FROM run_dbsnp_evidence e
            JOIN run_variants v ON v.variant_id = e.variant_id AND v.run_id = e.run_id
            """
            + (f"\n{join_classification}" if join_classification else "")
            + """
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
                "rsid": row[9],
                "reason_code": row[10],
                "reason_message": row[11],
                "details": json.loads(row[12] or "{}"),
                "retrieved_at": row[13],
            }
        )
    return items


def _normalize_classification_filter(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if not normalized or normalized == "all":
        return None
    if normalized in _VALID_CLASSIFICATIONS:
        return normalized
    return None


def _normalize_outcome_filter(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if not normalized or normalized == "all":
        return None
    if normalized in _VALID_OUTCOMES:
        return normalized
    return None
