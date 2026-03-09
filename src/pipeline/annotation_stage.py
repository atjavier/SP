from __future__ import annotations

import os
import shlex
import subprocess
from datetime import datetime, timezone

from pipeline.parser_stage import StageExecutionError
from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.run_artifacts import ensure_run_artifacts_dir
from storage.stages import (
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)


def _get_run_status(conn, run_id: str) -> str | None:
    row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    return row[0] if row else None


def _get_reference_build(conn, run_id: str) -> str | None:
    row = conn.execute("SELECT reference_build FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    return row[0] if row else None


def _get_stage_status(conn, run_id: str, stage_name: str) -> tuple[str | None, str | None]:
    row = conn.execute(
        "SELECT status, input_uploaded_at FROM run_stages WHERE run_id = ? AND stage_name = ?",
        (run_id, stage_name),
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _truthy_env(name: str) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _positive_int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value < 1:
        return default
    return value


def _apply_container_snpeff_fallback(config: dict) -> dict:
    # Docker profile fallback: when host .env paths leak into container startup,
    # prefer the known in-container runtime paths if they exist.
    if os.name == "nt":
        return config

    fallback_home = "/opt/snpeff/snpEff"
    fallback_jar = f"{fallback_home}/snpEff.jar"

    home = str(config.get("home") or "").strip()
    jar_path = str(config.get("jar_path") or "").strip()

    if (not home or not os.path.isdir(home)) and os.path.isdir(fallback_home):
        config["home"] = fallback_home
        home = fallback_home

    if (not jar_path or not os.path.isfile(jar_path)) and os.path.isfile(fallback_jar):
        config["jar_path"] = fallback_jar

    data_dir = str(config.get("data_dir") or "").strip()
    windows_style_data_dir = ":" in data_dir and "\\" in data_dir
    if (not data_dir or windows_style_data_dir) and home and os.path.isdir(os.path.join(home, "data")):
        config["data_dir"] = "./data"

    return config


def _snpeff_config(reference_build: str | None) -> dict:
    enabled_raw = os.environ.get("SP_SNPEFF_ENABLED")
    enabled = _truthy_env("SP_SNPEFF_ENABLED") if enabled_raw is not None else True
    jar_path = (os.environ.get("SP_SNPEFF_JAR_PATH") or "").strip()
    home = (os.environ.get("SP_SNPEFF_HOME") or "").strip()
    config_path = (os.environ.get("SP_SNPEFF_CONFIG_PATH") or "").strip()
    data_dir = (os.environ.get("SP_SNPEFF_DATA_DIR") or "").strip()
    java_cmd = (os.environ.get("SP_JAVA_CMD") or "").strip() or "java"
    java_xmx = (os.environ.get("SP_SNPEFF_JAVA_XMX") or "").strip() or "2g"
    timeout_seconds = _positive_int_env("SP_SNPEFF_TIMEOUT_SECONDS", 900)

    genome_from_env = (os.environ.get("SP_SNPEFF_GENOME") or "").strip()
    if genome_from_env:
        genome = genome_from_env
    elif (reference_build or "").upper() == "GRCH38":
        genome = "GRCh38.86"
    else:
        genome = "GRCh38.86"

    if not jar_path and home:
        jar_candidate = os.path.join(home, "snpEff.jar")
        if os.path.exists(jar_candidate):
            jar_path = jar_candidate

    args_raw = (os.environ.get("SP_SNPEFF_ARGS") or "").strip()
    extra_args: list[str] = []
    if args_raw:
        try:
            extra_args = shlex.split(args_raw, posix=False)
        except ValueError:
            extra_args = args_raw.split()

    config = {
        "enabled": enabled,
        "jar_path": jar_path or None,
        "home": home or None,
        "config_path": config_path or None,
        "data_dir": data_dir or None,
        "java_cmd": java_cmd,
        "java_xmx": java_xmx,
        "timeout_seconds": timeout_seconds,
        "genome": genome,
        "extra_args": extra_args,
    }
    return _apply_container_snpeff_fallback(config)


def _iter_variants(conn, run_id: str):
    cursor = conn.execute(
        """
        SELECT chrom, pos, ref, alt
        FROM run_variants
        WHERE run_id = ?
        ORDER BY chrom, pos, ref, alt
        """,
        (run_id,),
    )
    for row in cursor:
        yield {"chrom": row[0], "pos": row[1], "ref": row[2], "alt": row[3]}


def _write_minimal_vcf_fast(conn, run_id: str, out_path: str) -> int:
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("##fileformat=VCFv4.2\n")
        f.write("##source=sp-minimal-vcf\n")
        f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for variant in _iter_variants(conn, run_id):
            chrom = variant["chrom"] or "."
            pos = int(variant["pos"] or 0)
            ref = variant["ref"] or "."
            alt = variant["alt"] or "."
            f.write(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\t.\t.\n")
            count += 1
    return count


def _tail(text: str, max_chars: int = 1500) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def run_annotation_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    logger,
    force: bool = False,
) -> dict:
    created_at = datetime.now(timezone.utc).isoformat()
    stats: dict = {"tool": "snpeff", "impl_version": "2026-03-09-r5"}

    try:
        conn = _connect_db(db_path)
        try:
            _init_schema(conn)

            conn.execute("BEGIN IMMEDIATE")
            run_status = _get_run_status(conn, run_id)
            if run_status is None:
                conn.rollback()
                raise StageExecutionError(404, "RUN_NOT_FOUND", "Run not found.")

            reference_build = _get_reference_build(conn, run_id)

            if run_status == "canceled":
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run is canceled and cannot be annotated.")

            stage_status, stage_uploaded_at = _get_stage_status(conn, run_id, "annotation")
            if stage_status == "running" and not force:
                conn.rollback()
                raise StageExecutionError(409, "STAGE_RUNNING", "Annotation stage is already running.")

            if stage_status == "succeeded" and stage_uploaded_at == uploaded_at:
                conn.rollback()
                raise StageExecutionError(409, "ALREADY_ANNOTATED", "This upload was already annotated.")

            prediction_status, prediction_uploaded_at = _get_stage_status(conn, run_id, "prediction")
            if prediction_status != "succeeded" or prediction_uploaded_at != uploaded_at:
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="MISSING_PREDICTION_OUTPUT",
                    error_message="Prediction stage must succeed for this upload before annotation can run.",
                    error_details={
                        "prediction_status": prediction_status,
                        "prediction_input_uploaded_at": prediction_uploaded_at,
                    },
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    409,
                    "MISSING_PREDICTION_OUTPUT",
                    "Prediction stage must succeed for this upload before annotation can run.",
                    details={
                        "prediction_status": prediction_status,
                        "prediction_input_uploaded_at": prediction_uploaded_at,
                    },
                )

            mark_stage_running(
                db_path,
                run_id,
                "annotation",
                input_uploaded_at=uploaded_at,
                conn=conn,
                commit=False,
            )
            conn.commit()

            config = _snpeff_config(reference_build)
            stats["enabled"] = bool(config.get("enabled"))
            stats["reference_build"] = reference_build
            stats["genome"] = config.get("genome")

            if not config.get("enabled"):
                stats["note"] = "SnpEff is disabled (SP_SNPEFF_ENABLED=0)."
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_succeeded(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    stats=stats,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                return {"annotation": {"status": "succeeded", "stats": stats}}

            jar_path = config.get("jar_path")
            if not jar_path or not os.path.exists(jar_path):
                details = {
                    "jar_path": jar_path,
                    "sp_snpeff_jar_path": os.environ.get("SP_SNPEFF_JAR_PATH"),
                    "sp_snpeff_home": os.environ.get("SP_SNPEFF_HOME"),
                    "hint": (
                        "Set SP_SNPEFF_JAR_PATH to snpEff.jar or set SP_SNPEFF_HOME "
                        "to a directory containing snpEff.jar."
                    ),
                }
                logger.warning(
                    "SnpEff enabled but snpEff.jar is not configured for run_id=%s (SP_SNPEFF_JAR_PATH=%s SP_SNPEFF_HOME=%s)",
                    run_id,
                    os.environ.get("SP_SNPEFF_JAR_PATH"),
                    os.environ.get("SP_SNPEFF_HOME"),
                )
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="SNPEFF_NOT_CONFIGURED",
                    error_message="SnpEff is enabled but snpEff.jar is not configured or missing.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    500,
                    "SNPEFF_NOT_CONFIGURED",
                    "SnpEff is enabled but snpEff.jar is not configured or missing.",
                    details=details,
                )

            workdir = config.get("home") or os.path.dirname(jar_path)

            genome = config.get("genome") or "GRCh38.86"
            data_dir_raw = (config.get("data_dir") or "").strip()
            if not data_dir_raw:
                data_dir_arg = "./data"
            else:
                data_dir_arg = data_dir_raw

            if os.name == "nt" and os.path.isabs(data_dir_arg):
                try:
                    rel = os.path.relpath(data_dir_arg, workdir)
                except ValueError:
                    rel = None
                if not rel or rel.startswith(".."):
                    details = {
                        "workdir": workdir,
                        "data_dir": data_dir_arg,
                        "hint": "On Windows, use a data dir under SP_SNPEFF_HOME (e.g. set SP_SNPEFF_DATA_DIR=./data).",
                    }
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "annotation",
                        input_uploaded_at=uploaded_at,
                        error_code="SNPEFF_DATADIR_INVALID",
                        error_message="SnpEff dataDir must be under SP_SNPEFF_HOME on Windows.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "SNPEFF_DATADIR_INVALID",
                        "SnpEff dataDir must be under SP_SNPEFF_HOME on Windows.",
                        details=details,
                    )
                data_dir_arg = rel

            data_dir_fs = (
                data_dir_arg
                if os.path.isabs(data_dir_arg)
                else os.path.normpath(os.path.join(workdir, data_dir_arg))
            )

            expected_genome_dir = os.path.join(data_dir_fs, genome) if data_dir_fs else ""
            expected_db_file = (
                os.path.join(expected_genome_dir, "snpEffectPredictor.bin")
                if expected_genome_dir
                else ""
            )
            if expected_db_file and not os.path.isfile(expected_db_file):
                details = {
                    "genome": genome,
                    "data_dir": data_dir_fs,
                    "expected_genome_dir": expected_genome_dir,
                    "expected_db_file": expected_db_file,
                    "hint": (
                        f'Download the database first, e.g. '
                        f'java -jar "{jar_path}" download -v -dataDir "{data_dir_fs}" {genome}'
                    ),
                }
                logger.warning(
                    "SnpEff enabled but genome database missing for run_id=%s genome=%s expected_db_file=%s",
                    run_id,
                    genome,
                    expected_db_file,
                )
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="SNPEFF_DB_MISSING",
                    error_message="SnpEff genome database is missing. Download it before running annotation.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    500,
                    "SNPEFF_DB_MISSING",
                    "SnpEff genome database is missing. Download it before running annotation.",
                    details=details,
                )

            artifacts_dir = ensure_run_artifacts_dir(db_path, run_id)
            input_vcf_path = os.path.join(artifacts_dir, "snpeff.input.vcf")
            output_vcf_path = os.path.join(artifacts_dir, "snpeff.annotated.vcf")

            variants_written = _write_minimal_vcf_fast(conn, run_id, input_vcf_path)
            timeout_seconds = int(config.get("timeout_seconds") or 900)
            stats["input_vcf_path"] = input_vcf_path
            stats["output_vcf_path"] = output_vcf_path
            stats["variants_written"] = variants_written
            stats["configured"] = True
            stats["workdir"] = workdir
            stats["data_dir"] = data_dir_fs
            stats["data_dir_arg"] = data_dir_arg
            stats["timeout_seconds"] = timeout_seconds

            # In Docker, keep writable CWD under /app/instance while still
            # pointing dataDir to the installed SnpEff database path.
            if os.name == "nt":
                cmd_data_dir = data_dir_arg
                cmd_cwd = workdir
            else:
                cmd_data_dir = data_dir_fs
                cmd_cwd = artifacts_dir
            stats["command_workdir"] = cmd_cwd
            stats["command_data_dir"] = cmd_data_dir

            java_cmd = config.get("java_cmd") or "java"
            java_xmx = config.get("java_xmx") or "2g"
            extra_args = list(config.get("extra_args") or [])

            cmd: list[str] = [
                java_cmd,
                f"-Xmx{java_xmx}",
                "-jar",
                jar_path,
            ]
            if config.get("config_path"):
                cmd.extend(["-c", config["config_path"]])
            if cmd_data_dir:
                cmd.extend(["-dataDir", cmd_data_dir])
            cmd.extend(extra_args)
            cmd.extend([genome, input_vcf_path])

            cwd = cmd_cwd
            os.makedirs(os.path.dirname(output_vcf_path), exist_ok=True)
            with open(output_vcf_path, "wb") as out_f:
                try:
                    completed = subprocess.run(
                        cmd,
                        cwd=cwd or None,
                        stdout=out_f,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=timeout_seconds,
                    )
                except subprocess.TimeoutExpired as exc:
                    stderr_raw = exc.stderr
                    if isinstance(stderr_raw, bytes):
                        stderr_text = stderr_raw.decode("utf-8", errors="replace")
                    else:
                        stderr_text = stderr_raw or ""
                    details = {
                        "timeout_seconds": timeout_seconds,
                        "stderr_tail": _tail(stderr_text),
                        "cmd": cmd,
                        "workdir": cwd,
                        "resolved_data_dir_arg": data_dir_arg,
                        "resolved_data_dir_fs": data_dir_fs,
                        "impl_version": "2026-03-09-r5",
                    }
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "annotation",
                        input_uploaded_at=uploaded_at,
                        error_code="SNPEFF_TIMEOUT",
                        error_message=f"SnpEff timed out after {timeout_seconds} seconds.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "SNPEFF_TIMEOUT",
                        f"SnpEff timed out after {timeout_seconds} seconds.",
                        details={"timeout_seconds": timeout_seconds},
                    )

            stats["snpeff_exit_code"] = int(completed.returncode)
            stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace")
            if stderr_text:
                stats["snpeff_stderr_tail"] = _tail(stderr_text)

            if completed.returncode != 0:
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="SNPEFF_FAILED",
                    error_message="SnpEff execution failed.",
                    error_details={
                        "exit_code": completed.returncode,
                        "stderr_tail": _tail(stderr_text),
                        "cmd": cmd,
                        "workdir": cwd,
                        "resolved_data_dir_arg": data_dir_arg,
                        "resolved_data_dir_fs": data_dir_fs,
                        "impl_version": "2026-03-09-r5",
                    },
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(500, "SNPEFF_FAILED", "SnpEff execution failed.")

            conn.execute("BEGIN IMMEDIATE")
            mark_stage_succeeded(
                db_path,
                run_id,
                "annotation",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            conn.commit()
        finally:
            conn.close()
    except StageExecutionError:
        raise
    except Exception as exc:
        logger.exception("Annotation stage failed")
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="ANNOTATION_FAILED",
                    error_message="Annotation stage failed.",
                    error_details={"reason": str(exc)},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist annotation failure state")
        raise StageExecutionError(500, "ANNOTATION_FAILED", "Annotation stage failed.") from None

    return {"annotation": {"status": "succeeded", "stats": stats}}
