from __future__ import annotations

from datetime import datetime, timezone

from pipeline.parser_stage import StageExecutionError
from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.pre_annotations import clear_pre_annotations_for_run, upsert_pre_annotations_for_run
from storage.stages import (
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)
from storage.variants import iter_variants_for_run_with_ids


_PURINES: frozenset[str] = frozenset({"A", "G"})
_PYRIMIDINES: frozenset[str] = frozenset({"C", "T"})
_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {("A", "G"), ("G", "A"), ("C", "T"), ("T", "C")}
)


def _get_run_status(conn, run_id: str) -> str | None:
    row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    return row[0] if row else None


def _get_stage_status(conn, run_id: str, stage_name: str) -> tuple[str | None, str | None]:
    row = conn.execute(
        "SELECT status, input_uploaded_at FROM run_stages WHERE run_id = ? AND stage_name = ?",
        (run_id, stage_name),
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _base_class(base: str) -> str:
    base = (base or "").upper()
    if base in _PURINES:
        return "purine"
    if base in _PYRIMIDINES:
        return "pyrimidine"
    raise ValueError(f"Unknown base: {base!r}")


def _substitution_class(ref: str, alt: str) -> str:
    ref = (ref or "").upper()
    alt = (alt or "").upper()
    if (ref, alt) in _TRANSITIONS:
        return "transition"
    return "transversion"


def _normalize_and_validate_snv(ref: str | None, alt: str | None) -> tuple[str, str]:
    normalized_ref = (ref or "").upper()
    normalized_alt = (alt or "").upper()
    if len(normalized_ref) != 1 or len(normalized_alt) != 1:
        raise ValueError("Alleles must be length-1 for SNVs.")
    if normalized_ref == normalized_alt:
        raise ValueError("REF and ALT must differ for SNVs.")
    if normalized_ref not in _PURINES and normalized_ref not in _PYRIMIDINES:
        raise ValueError(f"Invalid REF base: {normalized_ref!r}")
    if normalized_alt not in _PURINES and normalized_alt not in _PYRIMIDINES:
        raise ValueError(f"Invalid ALT base: {normalized_alt!r}")
    return normalized_ref, normalized_alt


def run_pre_annotation_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    logger,
    force: bool = False,
) -> dict:
    created_at = datetime.now(timezone.utc).isoformat()
    stats: dict = {}
    processed = 0

    def fail_invalid_variant(reason: str, *, variant: dict) -> None:
        details = {
            "reason": reason,
            "variant_id": variant.get("variant_id"),
            "chrom": variant.get("chrom"),
            "pos": variant.get("pos"),
            "ref": variant.get("ref"),
            "alt": variant.get("alt"),
        }

        conn.execute("BEGIN IMMEDIATE")
        clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
        mark_stage_failed(
            db_path,
            run_id,
            "pre_annotation",
            input_uploaded_at=uploaded_at,
            error_code="INVALID_VARIANT",
            error_message="Persisted variant record is not a valid SNV for pre-annotation.",
            error_details=details,
            conn=conn,
            commit=False,
        )
        conn.commit()
        raise StageExecutionError(
            409,
            "INVALID_VARIANT",
            "Persisted variant record is not a valid SNV for pre-annotation.",
            details=details,
        )

    try:
        conn = _connect_db(db_path)
        try:
            _init_schema(conn)

            conn.execute("BEGIN IMMEDIATE")
            run_status = _get_run_status(conn, run_id)
            if run_status is None:
                conn.rollback()
                raise StageExecutionError(404, "RUN_NOT_FOUND", "Run not found.")

            if run_status == "canceled":
                clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "pre_annotation",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run is canceled and cannot be pre-annotated.")

            stage_status, stage_uploaded_at = _get_stage_status(conn, run_id, "pre_annotation")
            if stage_status == "running" and not force:
                conn.rollback()
                raise StageExecutionError(409, "STAGE_RUNNING", "Pre-annotation stage is already running.")

            if stage_status == "succeeded" and stage_uploaded_at == uploaded_at:
                conn.rollback()
                raise StageExecutionError(
                    409, "ALREADY_PRE_ANNOTATED", "This upload was already pre-annotated."
                )

            parser_status, parser_uploaded_at = _get_stage_status(conn, run_id, "parser")
            if parser_status != "succeeded" or parser_uploaded_at != uploaded_at:
                mark_stage_failed(
                    db_path,
                    run_id,
                    "pre_annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="MISSING_PARSER_OUTPUT",
                    error_message="Parser stage must succeed for this upload before pre-annotation can run.",
                    error_details={
                        "parser_status": parser_status,
                        "parser_input_uploaded_at": parser_uploaded_at,
                    },
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    409,
                    "MISSING_PARSER_OUTPUT",
                    "Parser stage must succeed for this upload before pre-annotation can run.",
                    details={
                        "parser_status": parser_status,
                        "parser_input_uploaded_at": parser_uploaded_at,
                    },
                )

            mark_stage_running(
                db_path,
                run_id,
                "pre_annotation",
                input_uploaded_at=uploaded_at,
                conn=conn,
                commit=False,
            )
            clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
            conn.commit()

            current_stage_status, _ = _get_stage_status(conn, run_id, "pre_annotation")
            if current_stage_status == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            batch: list[dict] = []
            for variant in iter_variants_for_run_with_ids(db_path, run_id, conn=conn):
                processed += 1
                try:
                    ref, alt = _normalize_and_validate_snv(variant.get("ref"), variant.get("alt"))
                except ValueError as exc:
                    fail_invalid_variant(str(exc), variant=variant)

                batch.append(
                    {
                        "variant_id": variant["variant_id"],
                        "variant_key": f"{variant['chrom']}:{variant['pos']}:{ref}>{alt}",
                        "base_change": f"{ref}>{alt}",
                        "substitution_class": _substitution_class(ref, alt),
                        "ref_class": _base_class(ref),
                        "alt_class": _base_class(alt),
                        "details": {"source_line": variant.get("source_line")},
                        "created_at": created_at,
                    }
                )

                if len(batch) < 500:
                    continue

                if _get_run_status(conn, run_id) == "canceled":
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_canceled(
                        db_path,
                        run_id,
                        "pre_annotation",
                        input_uploaded_at=uploaded_at,
                        conn=conn,
                        commit=False,
                    )
                    clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                    conn.commit()
                    raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

                conn.execute("BEGIN IMMEDIATE")
                upsert_pre_annotations_for_run(db_path, run_id, batch, conn=conn, commit=False)
                conn.commit()
                batch.clear()

                stage_after_batch, _ = _get_stage_status(conn, run_id, "pre_annotation")
                if stage_after_batch == "canceled":
                    conn.execute("BEGIN IMMEDIATE")
                    clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                    conn.commit()
                    raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            if batch:
                if _get_run_status(conn, run_id) == "canceled":
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_canceled(
                        db_path,
                        run_id,
                        "pre_annotation",
                        input_uploaded_at=uploaded_at,
                        conn=conn,
                        commit=False,
                    )
                    clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                    conn.commit()
                    raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

                conn.execute("BEGIN IMMEDIATE")
                upsert_pre_annotations_for_run(db_path, run_id, batch, conn=conn, commit=False)
                conn.commit()

            stats["variants_processed"] = processed
            stats["pre_annotations_persisted"] = processed

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "pre_annotation",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            mark_stage_succeeded(
                db_path,
                run_id,
                "pre_annotation",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            conn.commit()

            stage_after_success, _ = _get_stage_status(conn, run_id, "pre_annotation")
            if stage_after_success == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")
        finally:
            conn.close()
    except StageExecutionError:
        raise
    except Exception as exc:
        logger.exception("Pre-annotation stage failed")
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                clear_pre_annotations_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "pre_annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="PRE_ANNOTATION_FAILED",
                    error_message="Pre-annotation stage failed.",
                    error_details={"reason": str(exc)},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist pre-annotation failure state")
        raise StageExecutionError(500, "PRE_ANNOTATION_FAILED", "Pre-annotation stage failed.") from None

    return {"pre_annotation": {"status": "succeeded", "stats": stats}}
