import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from storage.db import init_schema, open_connection
from storage.variant_ordering import variant_order_by


_ORDER_BY = "\n" + variant_order_by("v")


@contextmanager
def _maybe_connection(db_path: str, conn: sqlite3.Connection | None) -> Iterator[sqlite3.Connection]:
    if conn is not None:
        yield conn
        return
    with open_connection(db_path) as opened:
        yield opened


def list_variant_summaries_for_run(
    db_path: str,
    run_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    completeness: str | None = None,
    stage_statuses: dict[str, str] | None = None,
    annotation_evidence_completeness: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    safe_offset = max(0, int(offset or 0))

    completeness_filter = (completeness or "").strip().lower() or None
    completeness_case = ""
    completeness_params: list[object] = []
    if completeness_filter:
        completeness_case, completeness_params = _build_completeness_case(
            stage_statuses, annotation_evidence_completeness
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        query = (
            """
            SELECT
              v.variant_id,
              v.chrom,
              v.pos,
              v.ref,
              v.alt,
              v.source_line,
              c.consequence_category,
              CASE WHEN p.variant_id IS NULL THEN 0 ELSE 1 END AS has_prediction,
              CASE WHEN d.variant_id IS NULL THEN 0 ELSE 1 END AS has_dbsnp,
              CASE WHEN cv.variant_id IS NULL THEN 0 ELSE 1 END AS has_clinvar,
              CASE WHEN g.variant_id IS NULL THEN 0 ELSE 1 END AS has_gnomad
            FROM run_variants v
            LEFT JOIN run_classifications c
              ON c.run_id = v.run_id AND c.variant_id = v.variant_id
            LEFT JOIN (
              SELECT run_id, variant_id
              FROM run_predictor_outputs
              GROUP BY run_id, variant_id
            ) p ON p.run_id = v.run_id AND p.variant_id = v.variant_id
            LEFT JOIN (
              SELECT run_id, variant_id
              FROM run_dbsnp_evidence
              GROUP BY run_id, variant_id
            ) d ON d.run_id = v.run_id AND d.variant_id = v.variant_id
            LEFT JOIN (
              SELECT run_id, variant_id
              FROM run_clinvar_evidence
              GROUP BY run_id, variant_id
            ) cv ON cv.run_id = v.run_id AND cv.variant_id = v.variant_id
            LEFT JOIN (
              SELECT run_id, variant_id
              FROM run_gnomad_evidence
              GROUP BY run_id, variant_id
            ) g ON g.run_id = v.run_id AND g.variant_id = v.variant_id
            WHERE v.run_id = ?
            """
        )
        params: list[object] = [run_id]
        if completeness_filter:
            query += f" AND ({completeness_case}) = ?"
            params.extend(completeness_params)
            params.append(completeness_filter)
        query += _ORDER_BY + "\nLIMIT ? OFFSET ?"
        params.extend([safe_limit, safe_offset])
        rows = active.execute(query, params).fetchall()

    items: list[dict] = []
    for row in rows:
        chrom = row[1]
        pos = row[2]
        ref = row[3]
        alt = row[4]
        items.append(
            {
                "variant_id": row[0],
                "variant_key": f"{chrom}:{pos}:{ref}>{alt}",
                "chrom": chrom,
                "pos": pos,
                "ref": ref,
                "alt": alt,
                "source_line": row[5],
                "consequence_category": row[6],
                "has_prediction": bool(row[7]),
                "has_dbsnp": bool(row[8]),
                "has_clinvar": bool(row[9]),
                "has_gnomad": bool(row[10]),
            }
        )

    return items


def count_variant_summaries_for_run(
    db_path: str,
    run_id: str,
    *,
    completeness: str | None = None,
    stage_statuses: dict[str, str] | None = None,
    annotation_evidence_completeness: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> int:
    completeness_filter = (completeness or "").strip().lower() or None
    completeness_case = ""
    completeness_params: list[object] = []
    if completeness_filter:
        completeness_case, completeness_params = _build_completeness_case(
            stage_statuses, annotation_evidence_completeness
        )

    with _maybe_connection(db_path, conn) as active:
        init_schema(active)
        query = """
            SELECT COUNT(*)
            FROM run_variants v
            LEFT JOIN run_classifications c
              ON c.run_id = v.run_id AND c.variant_id = v.variant_id
            WHERE v.run_id = ?
            """
        params: list[object] = [run_id]
        if completeness_filter:
            query += f" AND ({completeness_case}) = ?"
            params.extend(completeness_params)
            params.append(completeness_filter)
        row = active.execute(query, params).fetchone()
    return int(row[0] if row else 0)


def _build_completeness_case(
    stage_statuses: dict[str, str] | None,
    annotation_evidence_completeness: str | None,
) -> tuple[str, list[object]]:
    status_map = stage_statuses or {}
    failed_stage_names = ("parser", "classification", "prediction", "annotation")
    has_failed_stage = any(status_map.get(stage) == "failed" for stage in failed_stage_names)
    parser_status = status_map.get("parser") or ""
    classification_status = status_map.get("classification") or ""
    prediction_status = status_map.get("prediction") or ""
    annotation_status = status_map.get("annotation") or ""
    evidence_completeness = (annotation_evidence_completeness or "").strip().lower()

    completeness_case = """
    CASE
      WHEN ? = 1 THEN 'failed'
      WHEN ? != 'succeeded' THEN 'partial'
      WHEN ? != 'succeeded' THEN 'partial'
      WHEN c.consequence_category IS NULL OR c.consequence_category = '' THEN 'partial'
      WHEN lower(c.consequence_category) = 'missense' AND ? != 'succeeded' THEN 'partial'
      WHEN ? != 'succeeded' THEN 'partial'
      WHEN ? = 'unavailable' THEN 'unavailable'
      WHEN ? = 'partial' THEN 'partial'
      ELSE 'complete'
    END
    """
    params: list[object] = [
        1 if has_failed_stage else 0,
        parser_status,
        classification_status,
        prediction_status,
        annotation_status,
        evidence_completeness,
        evidence_completeness,
    ]
    return completeness_case, params
