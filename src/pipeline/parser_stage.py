import os
import uuid
from datetime import datetime, timezone

from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.run_inputs import delete_run_upload_checked
from storage.runs import get_run
from storage.stages import get_stage, mark_stage_canceled, mark_stage_failed, mark_stage_running, mark_stage_succeeded
from storage.variants import clear_variants_for_run
from vcf_parser import VcfParseError, iter_vcf_snv_records


class StageExecutionError(Exception):
    def __init__(
        self,
        http_status: int,
        code: str,
        message: str,
        *,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.details = details or {}


def run_parser_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    upload_path: str | None,
    max_decompressed_bytes: int,
    logger,
    force: bool = False,
) -> dict:
    run = get_run(db_path, run_id)
    if run and run.get("status") == "canceled":
        mark_stage_canceled(db_path, run_id, "parser", input_uploaded_at=uploaded_at)
        raise StageExecutionError(409, "RUN_CANCELED", "Run is canceled and cannot be parsed.")

    stage = get_stage(db_path, run_id, "parser")
    if stage and stage.get("status") == "running":
        if not force:
            raise StageExecutionError(409, "STAGE_RUNNING", "Parser stage is already running.")

    if stage and stage.get("status") == "succeeded" and stage.get("input_uploaded_at") == uploaded_at:
        raise StageExecutionError(409, "ALREADY_PARSED", "This upload was already parsed.")

    if not upload_path or not os.path.exists(upload_path):
        mark_stage_failed(
            db_path,
            run_id,
            "parser",
            input_uploaded_at=uploaded_at,
            error_code="VCF_ATTACHMENT_MISSING",
            error_message="VCF attachment is missing on disk.",
            error_details={"path": upload_path},
        )
        raise StageExecutionError(
            409,
            "VCF_ATTACHMENT_MISSING",
            "VCF attachment is missing on disk.",
            details={"path": upload_path},
        )

    stats: dict = {}
    sample: list[dict] = []
    created_at = datetime.now(timezone.utc).isoformat()
    rows: list[tuple] = []
    attempted = 0
    inserted = 0

    try:
        conn = _connect_db(db_path)
        try:
            _init_schema(conn)
            conn.execute("BEGIN")

            mark_stage_running(
                db_path, run_id, "parser", input_uploaded_at=uploaded_at, conn=conn, commit=False
            )
            clear_variants_for_run(db_path, run_id, conn=conn, commit=False)

            for record in iter_vcf_snv_records(
                upload_path,
                stats=stats,
                sample=sample,
                sample_limit=10,
                max_decompressed_bytes=max_decompressed_bytes,
            ):
                rows.append(
                    (
                        str(uuid.uuid4()),
                        run_id,
                        record["chrom"],
                        int(record["pos"]),
                        record["ref"],
                        record["alt"],
                        record.get("source_line"),
                        created_at,
                    )
                )

                if len(rows) >= 500:
                    attempted += len(rows)
                    before = conn.total_changes
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO run_variants (
                          variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                    inserted += conn.total_changes - before
                    rows.clear()

            if rows:
                attempted += len(rows)
                before = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                inserted += conn.total_changes - before

            stats["snv_records_persisted"] = inserted
            stats["duplicate_records_ignored"] = max(0, attempted - inserted)

            mark_stage_succeeded(
                db_path,
                run_id,
                "parser",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            conn.commit()
        finally:
            conn.close()
    except VcfParseError as exc:
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN")
                clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "parser",
                    input_uploaded_at=uploaded_at,
                    error_code=exc.code,
                    error_message=exc.message,
                    error_details={"line_number": exc.line_number, **(exc.details or {})},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist parser failure state")

        raise StageExecutionError(
            422,
            "VCF_PARSE_FAILED",
            "Failed to parse VCF.",
            details={
                "error_code": exc.code,
                "line_number": exc.line_number,
                **(exc.details or {}),
            },
        ) from None
    except Exception as exc:
        logger.exception("Unexpected parse failure")
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN")
                clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "parser",
                    input_uploaded_at=uploaded_at,
                    error_code="UNEXPECTED_ERROR",
                    error_message="Unexpected parser failure.",
                    error_details={"reason": str(exc)},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist unexpected parser failure state")
        raise StageExecutionError(500, "VCF_PARSE_FAILED", "Failed to parse VCF.") from None

    cleanup = delete_run_upload_checked(db_path, run_id)
    if not cleanup.get("ok"):
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN")
                clear_variants_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "parser",
                    input_uploaded_at=uploaded_at,
                    error_code="CLEANUP_FAILED",
                    error_message="Failed to delete uploaded VCF after parsing.",
                    error_details={"path": upload_path, **cleanup},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist cleanup failure state")
        raise StageExecutionError(500, "VCF_CLEANUP_FAILED", "Failed to delete uploaded VCF after parsing.")

    return {"parser": {"status": "succeeded", "stats": stats}, "variants_sample": sample}
