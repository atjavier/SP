from __future__ import annotations

import json
import os
import shutil
import shlex
import subprocess
from datetime import datetime, timezone

from pipeline.parser_stage import StageExecutionError
from storage.classifications import clear_classifications_for_run, upsert_classifications_for_run
from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.run_artifacts import ensure_run_artifacts_dir
from storage.stages import (
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)
from storage.variants import iter_variants_for_run_with_ids


_MISSENSE_TERMS: frozenset[str] = frozenset({"missense_variant"})
_SYNONYMOUS_TERMS: frozenset[str] = frozenset({"synonymous_variant", "stop_retained_variant"})
_NONSENSE_TERMS: frozenset[str] = frozenset({"stop_gained", "stop_lost"})


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


def _tail(text: str, max_chars: int = 1500) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _apply_container_vep_fallback(config: dict) -> dict:
    if os.name == "nt":
        return config

    fallback_script = "/opt/vep/ensembl-vep/vep"
    fallback_cache_dir = "/opt/vep/.vep"
    fallback_plugin_dir = "/opt/vep/.vep/Plugins"

    script_path = str(config.get("script_path") or "").strip()
    cache_dir = str(config.get("cache_dir") or "").strip()
    plugin_dir = str(config.get("plugin_dir") or "").strip()
    cmd = str(config.get("cmd") or "").strip()

    if (not script_path or not os.path.isfile(script_path)) and os.path.isfile(fallback_script):
        config["script_path"] = fallback_script
        script_path = fallback_script
    if (not cache_dir or not os.path.isdir(cache_dir)) and os.path.isdir(fallback_cache_dir):
        config["cache_dir"] = fallback_cache_dir
    if (not plugin_dir or not os.path.isdir(plugin_dir)) and os.path.isdir(fallback_plugin_dir):
        config["plugin_dir"] = fallback_plugin_dir

    if (
        (not cmd or ("\\" in cmd) or (":" in cmd))
        and not shutil.which(cmd)
        and shutil.which("perl")
        and script_path
        and os.path.isfile(script_path)
    ):
        config["cmd"] = "perl"

    return config


def _vep_config(overrides: dict | None = None) -> dict:
    args_raw = (os.environ.get("SP_VEP_EXTRA_ARGS") or "").strip()
    extra_args: list[str] = []
    if args_raw:
        try:
            extra_args = shlex.split(args_raw, posix=False)
        except ValueError:
            extra_args = args_raw.split()

    config = {
        "cmd": (os.environ.get("SP_VEP_CMD") or "").strip() or "vep",
        "script_path": (os.environ.get("SP_VEP_SCRIPT_PATH") or "").strip() or None,
        "cache_dir": (os.environ.get("SP_VEP_CACHE_DIR") or "").strip() or None,
        "plugin_dir": (os.environ.get("SP_VEP_PLUGIN_DIR") or "").strip() or None,
        "fasta_path": (os.environ.get("SP_VEP_FASTA_PATH") or "").strip() or None,
        "assembly": (os.environ.get("SP_VEP_ASSEMBLY") or "").strip() or "GRCh38",
        "timeout_seconds": _positive_int_env("SP_VEP_TIMEOUT_SECONDS", 1200),
        "extra_args": extra_args,
    }

    if overrides:
        for key in (
            "cmd",
            "script_path",
            "cache_dir",
            "plugin_dir",
            "fasta_path",
            "assembly",
            "timeout_seconds",
            "extra_args",
        ):
            value = overrides.get(key)
            if value is None:
                continue
            if key == "timeout_seconds":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    continue
                if value < 1:
                    continue
            if key == "extra_args":
                value = list(value) if isinstance(value, (list, tuple)) else []
            config[key] = value

    return _apply_container_vep_fallback(config)


def _validate_vep_config(config: dict) -> dict | None:
    cache_dir = config.get("cache_dir")
    script_path = config.get("script_path")
    plugin_dir = config.get("plugin_dir")
    fasta_path = config.get("fasta_path")

    if not cache_dir:
        return {
            "missing": "SP_VEP_CACHE_DIR",
            "hint": "Set SP_VEP_CACHE_DIR to your local VEP cache directory.",
        }
    if not os.path.isdir(cache_dir):
        return {
            "missing": "SP_VEP_CACHE_DIR",
            "cache_dir": cache_dir,
            "hint": "SP_VEP_CACHE_DIR must point to an existing directory.",
        }
    if script_path and not os.path.isfile(script_path):
        return {
            "missing": "SP_VEP_SCRIPT_PATH",
            "script_path": script_path,
            "hint": "SP_VEP_SCRIPT_PATH must point to an existing executable script.",
        }
    if plugin_dir and not os.path.isdir(plugin_dir):
        return {
            "missing": "SP_VEP_PLUGIN_DIR",
            "plugin_dir": plugin_dir,
            "hint": "SP_VEP_PLUGIN_DIR must point to an existing directory when set.",
        }
    if fasta_path and not os.path.isfile(fasta_path):
        return {
            "missing": "SP_VEP_FASTA_PATH",
            "fasta_path": fasta_path,
            "hint": "SP_VEP_FASTA_PATH must point to an existing FASTA file when set.",
        }
    return None


def _write_minimal_vcf_fast(variants: list[dict], out_path: str) -> int:
    count = 0
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("##fileformat=VCFv4.2\n")
        f.write("##source=sp-classification\n")
        f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for variant in variants:
            chrom = variant.get("chrom") or "."
            pos = int(variant.get("pos") or 0)
            ref = variant.get("ref") or "."
            alt = variant.get("alt") or "."
            f.write(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\t.\t.\n")
            count += 1
    return count


def _normalized_chrom(value: str | None) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    return text


def _extract_variant_key(record: dict) -> tuple[str, int, str, str] | None:
    chrom = _normalized_chrom(record.get("seq_region_name"))
    pos = record.get("start")
    allele_string = str(record.get("allele_string") or "")
    ref = ""
    alt = ""
    if "/" in allele_string:
        ref, alt = allele_string.split("/", 1)
        alt = alt.split(",")[0]

    if not chrom or not pos or not ref or not alt:
        input_value = str(record.get("input") or "")
        if input_value:
            fields = input_value.split("\t")
            if len(fields) >= 5:
                chrom = _normalized_chrom(fields[0])
                try:
                    pos = int(fields[1])
                except ValueError:
                    return None
                ref = fields[3]
                alt = fields[4].split(",")[0]

    if not chrom or not pos or not ref or not alt:
        return None
    try:
        pos_int = int(pos)
    except (TypeError, ValueError):
        return None
    return (chrom, pos_int, str(ref), str(alt))


def _extract_consequence_terms(record: dict) -> set[str]:
    terms: set[str] = set()

    most_severe = record.get("most_severe_consequence")
    if most_severe:
        terms.add(str(most_severe).strip().lower())

    transcripts = record.get("transcript_consequences") or []
    if not isinstance(transcripts, list):
        transcripts = []

    for tc in transcripts:
        if not isinstance(tc, dict):
            continue
        tc_terms = tc.get("consequence_terms")
        if isinstance(tc_terms, list):
            for term in tc_terms:
                normalized = str(term or "").strip().lower()
                if normalized:
                    terms.add(normalized)
        elif isinstance(tc_terms, str):
            normalized = tc_terms.strip().lower()
            if normalized:
                terms.add(normalized)

    return terms


def _category_from_terms(terms: set[str]) -> str | None:
    if terms & _MISSENSE_TERMS:
        return "missense"
    if terms & _SYNONYMOUS_TERMS:
        return "synonymous"
    if terms & _NONSENSE_TERMS:
        return "nonsense"
    if terms:
        return "other"
    return None


def _load_vep_outputs(output_path: str) -> list[dict]:
    rows: list[dict] = []
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            rows.append(json.loads(stripped))
    return rows


def run_classification_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    logger,
    force: bool = False,
    vep_config_overrides: dict | None = None,
) -> dict:
    created_at = datetime.now(timezone.utc).isoformat()
    stats: dict = {"tool": "vep"}

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
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run is canceled and cannot be classified.")

            stage_status, stage_uploaded_at = _get_stage_status(conn, run_id, "classification")
            if stage_status == "running" and not force:
                conn.rollback()
                raise StageExecutionError(409, "STAGE_RUNNING", "Classification stage is already running.")

            if stage_status == "succeeded" and stage_uploaded_at == uploaded_at:
                conn.rollback()
                raise StageExecutionError(409, "ALREADY_CLASSIFIED", "This upload was already classified.")

            parser_status, parser_uploaded_at = _get_stage_status(conn, run_id, "parser")
            if parser_status != "succeeded" or parser_uploaded_at != uploaded_at:
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="MISSING_PARSER_OUTPUT",
                    error_message="Parser stage must succeed for this upload before classification can run.",
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
                    "Parser stage must succeed for this upload before classification can run.",
                    details={
                        "parser_status": parser_status,
                        "parser_input_uploaded_at": parser_uploaded_at,
                    },
                )

            pre_status, pre_uploaded_at = _get_stage_status(conn, run_id, "pre_annotation")
            if pre_status != "succeeded" or pre_uploaded_at != uploaded_at:
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="MISSING_PRE_ANNOTATION_OUTPUT",
                    error_message="Pre-annotation stage must succeed for this upload before classification can run.",
                    error_details={
                        "pre_annotation_status": pre_status,
                        "pre_annotation_input_uploaded_at": pre_uploaded_at,
                    },
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    409,
                    "MISSING_PRE_ANNOTATION_OUTPUT",
                    "Pre-annotation stage must succeed for this upload before classification can run.",
                    details={
                        "pre_annotation_status": pre_status,
                        "pre_annotation_input_uploaded_at": pre_uploaded_at,
                    },
                )

            mark_stage_running(
                db_path,
                run_id,
                "classification",
                input_uploaded_at=uploaded_at,
                conn=conn,
                commit=False,
            )
            clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
            conn.commit()

            config = _vep_config(vep_config_overrides)
            config_error = _validate_vep_config(config)
            if config_error:
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="VEP_NOT_CONFIGURED",
                    error_message="Classification stage requires VEP runtime configuration.",
                    error_details=config_error,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    500,
                    "VEP_NOT_CONFIGURED",
                    "Classification stage requires VEP runtime configuration.",
                    details=config_error,
                )

            variants = list(iter_variants_for_run_with_ids(db_path, run_id, conn=conn))
            artifacts_dir = ensure_run_artifacts_dir(db_path, run_id)
            input_vcf_path = os.path.join(artifacts_dir, "classification.input.vcf")
            output_json_path = os.path.join(artifacts_dir, "classification.vep.jsonl")
            variants_written = _write_minimal_vcf_fast(variants, input_vcf_path)

            cmd: list[str] = [str(config["cmd"])]
            if config.get("script_path"):
                cmd.append(str(config["script_path"]))
            cmd.extend(
                [
                    "--input_file",
                    input_vcf_path,
                    "--output_file",
                    output_json_path,
                    "--format",
                    "vcf",
                    "--json",
                    "--force_overwrite",
                    "--offline",
                    "--cache",
                    "--dir_cache",
                    str(config["cache_dir"]),
                    "--assembly",
                    str(config["assembly"]),
                ]
            )
            if config.get("plugin_dir"):
                cmd.extend(["--dir_plugins", str(config["plugin_dir"])])
            if config.get("fasta_path"):
                cmd.extend(["--fasta", str(config["fasta_path"])])
            cmd.extend(list(config.get("extra_args") or []))

            stats["input_vcf_path"] = input_vcf_path
            stats["output_json_path"] = output_json_path
            stats["variants_processed"] = len(variants)
            stats["variants_written"] = variants_written
            stats["vep_cmd"] = cmd[:2] if len(cmd) >= 2 else cmd
            stats["vep_timeout_seconds"] = int(config["timeout_seconds"])

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            try:
                completed = subprocess.run(
                    cmd,
                    cwd=None,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    check=False,
                    timeout=int(config["timeout_seconds"]),
                )
            except subprocess.TimeoutExpired as exc:
                stderr_raw = exc.stderr
                if isinstance(stderr_raw, bytes):
                    stderr_text = stderr_raw.decode("utf-8", errors="replace")
                else:
                    stderr_text = stderr_raw or ""
                details = {
                    "timeout_seconds": int(config["timeout_seconds"]),
                    "stderr_tail": _tail(stderr_text),
                    "cmd": cmd,
                }
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="VEP_TIMEOUT",
                    error_message=f"VEP timed out after {config['timeout_seconds']} seconds.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    500,
                    "VEP_TIMEOUT",
                    f"VEP timed out after {config['timeout_seconds']} seconds.",
                    details={"timeout_seconds": int(config["timeout_seconds"])},
                )

            stats["vep_exit_code"] = int(completed.returncode)
            stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace")
            if stderr_text:
                stats["vep_stderr_tail"] = _tail(stderr_text)

            if completed.returncode != 0:
                details = {
                    "exit_code": completed.returncode,
                    "stderr_tail": _tail(stderr_text),
                    "cmd": cmd,
                    "hint": "Check VEP cache/plugin paths and command-line options.",
                }
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="VEP_FAILED",
                    error_message="VEP execution failed.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(500, "VEP_FAILED", "VEP execution failed.", details=details)

            try:
                records = _load_vep_outputs(output_json_path)
            except Exception as exc:
                details = {"reason": str(exc), "output_path": output_json_path}
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="VEP_PARSE_FAILED",
                    error_message="Failed to parse VEP output.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(500, "VEP_PARSE_FAILED", "Failed to parse VEP output.", details=details)

            variant_by_key: dict[tuple[str, int, str, str], dict] = {}
            for variant in variants:
                key = (
                    _normalized_chrom(variant.get("chrom")),
                    int(variant.get("pos")),
                    str(variant.get("ref")),
                    str(variant.get("alt")),
                )
                variant_by_key[key] = variant

            terms_by_variant_id: dict[str, set[str]] = {}
            matched_variant_ids: set[str] = set()
            for record in records:
                if not isinstance(record, dict):
                    continue
                key = _extract_variant_key(record)
                if not key:
                    continue
                variant = variant_by_key.get(key)
                if not variant:
                    continue
                variant_id = str(variant["variant_id"])
                matched_variant_ids.add(variant_id)
                terms = _extract_consequence_terms(record)
                if not terms:
                    continue
                existing = terms_by_variant_id.get(variant_id) or set()
                if len(terms) > len(existing):
                    terms_by_variant_id[variant_id] = terms

            category_counts = {
                "missense": 0,
                "synonymous": 0,
                "nonsense": 0,
                "other": 0,
                "unclassified": 0,
            }
            rows: list[dict] = []
            for variant in variants:
                variant_id = str(variant["variant_id"])
                terms = terms_by_variant_id.get(variant_id) or set()
                category = _category_from_terms(terms)
                reason_code = None
                reason_message = None
                if not category:
                    category = "unclassified"
                    if variant_id in matched_variant_ids:
                        reason_code = "MISSING_VEP_CONSEQUENCE"
                        reason_message = "VEP did not return consequence terms for this variant."
                    else:
                        reason_code = "TOOL_OUTPUT_MISSING"
                        reason_message = "Variant was not present in VEP output."

                category_counts[category] = category_counts.get(category, 0) + 1
                rows.append(
                    {
                        "variant_id": variant_id,
                        "consequence_category": category,
                        "reason_code": reason_code,
                        "reason_message": reason_message,
                        "details": {
                            "source_line": variant.get("source_line"),
                            "source_tool": "vep",
                            "vep_consequence_terms": sorted(terms),
                        },
                        "created_at": created_at,
                    }
                )

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            conn.execute("BEGIN IMMEDIATE")
            upsert_classifications_for_run(db_path, run_id, rows, conn=conn, commit=False)
            conn.commit()

            stats["vep_records_parsed"] = len(records)
            stats["variants_with_vep_match"] = len(matched_variant_ids)
            stats["category_counts"] = category_counts
            stats["unclassified_count"] = category_counts.get("unclassified", 0)

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            mark_stage_succeeded(
                db_path,
                run_id,
                "classification",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            conn.commit()

            stage_after_success, _ = _get_stage_status(conn, run_id, "classification")
            if stage_after_success == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")
        finally:
            conn.close()
    except StageExecutionError:
        raise
    except Exception as exc:
        logger.exception("Classification stage failed")
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                clear_classifications_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "classification",
                    input_uploaded_at=uploaded_at,
                    error_code="CLASSIFICATION_FAILED",
                    error_message="Classification stage failed.",
                    error_details={"reason": str(exc)},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist classification failure state")
        raise StageExecutionError(500, "CLASSIFICATION_FAILED", "Classification stage failed.") from None

    return {"classification": {"status": "succeeded", "stats": stats}}
