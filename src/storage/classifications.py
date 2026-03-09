import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_VALID_CONSEQUENCE_CATEGORIES: frozenset[str] = frozenset(
    {"unclassified", "synonymous", "missense", "nonsense", "other"}
)

_CLASSIFICATION_ORDER_BY = "\n" + variant_order_by("v")


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def clear_classifications_for_run(
    db_path: str,
    run_id: str,
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.execute("DELETE FROM run_classifications WHERE run_id = ?", (run_id,))
        if commit:
            active.commit()


def upsert_classifications_for_run(
    db_path: str,
    run_id: str,
    classifications: list[dict],
    *,
    conn: sqlite3.Connection | None = None,
    commit: bool = True,
) -> None:
    if not classifications:
        return

    rows: list[tuple] = []
    for c in classifications:
        category = c.get("consequence_category")
        if category not in _VALID_CONSEQUENCE_CATEGORIES:
            raise ValueError(f"Invalid consequence_category: {category!r}")
        if category == "unclassified" and not c.get("reason_code"):
            raise ValueError("reason_code is required when consequence_category is 'unclassified'.")

        rows.append(
            (
                run_id,
                c["variant_id"],
                category,
                c.get("reason_code"),
                c.get("reason_message"),
                json.dumps(c.get("details") or {}),
                c["created_at"],
            )
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        active.executemany(
            """
            INSERT INTO run_classifications (
              run_id,
              variant_id,
              consequence_category,
              reason_code,
              reason_message,
              details_json,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, variant_id) DO UPDATE SET
              consequence_category = excluded.consequence_category,
              reason_code = excluded.reason_code,
              reason_message = excluded.reason_message,
              details_json = excluded.details_json,
              created_at = excluded.created_at
            """,
            rows,
        )
        if commit:
            active.commit()


def list_classifications_for_run(
    db_path: str,
    run_id: str,
    *,
    limit: int = 100,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        rows = active.execute(
            """
            SELECT
              c.run_id,
              c.variant_id,
              v.chrom,
              v.pos,
              v.ref,
              v.alt,
              v.source_line,
              c.consequence_category,
              c.reason_code,
              c.reason_message,
              c.details_json,
              c.created_at
            FROM run_classifications c
            JOIN run_variants v ON v.variant_id = c.variant_id AND v.run_id = c.run_id
            WHERE c.run_id = ?
            """
            + _CLASSIFICATION_ORDER_BY
            + """
            LIMIT ?
            """,
            (run_id, safe_limit),
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
                "consequence_category": r[7],
                "reason_code": r[8],
                "reason_message": r[9],
                "details": json.loads(r[10] or "{}"),
                "created_at": r[11],
            }
        )

    return items
