from __future__ import annotations

import json
import os
import shutil
import shlex
import subprocess
from datetime import datetime, timezone

from pipeline.parser_stage import StageExecutionError
from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.predictor_outputs import clear_predictor_outputs_for_run, upsert_predictor_outputs_for_run
from storage.run_artifacts import ensure_run_artifacts_dir
from storage.stages import (
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)
from storage.variants import iter_variants_for_run_with_ids


_SIFT_PREDICTOR_KEY = "sift"
_POLYPHEN2_PREDICTOR_KEY = "polyphen2"
_ALPHAMISSENSE_PREDICTOR_KEY = "alphamissense"

_PREDICTOR_SPECS: tuple[tuple[str, str], ...] = (
    (_SIFT_PREDICTOR_KEY, "SIFT"),
    (_POLYPHEN2_PREDICTOR_KEY, "PolyPhen-2"),
    (_ALPHAMISSENSE_PREDICTOR_KEY, "AlphaMissense"),
)
_PREDICTORS_EXECUTED: tuple[str, ...] = tuple(key for key, _label in _PREDICTOR_SPECS)


def _clear_outputs_for_predictors(conn, db_path: str, run_id: str, predictor_keys: tuple[str, ...]) -> None:
    for predictor_key in predictor_keys:
        clear_predictor_outputs_for_run(
            db_path, run_id, predictor_key=predictor_key, conn=conn, commit=False
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
    # Docker profile fallback: if Windows host paths are loaded in-container,
    # switch to mounted runtime paths when available.
    if os.name == "nt":
        return config

    fallback_script = "/opt/vep/ensembl-vep/vep"
    fallback_cache_dir = "/opt/vep/.vep"
    fallback_plugin_dir = "/opt/vep/.vep/Plugins"
    fallback_alpha = "/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz"

    script_path = str(config.get("script_path") or "").strip()
    cache_dir = str(config.get("cache_dir") or "").strip()
    plugin_dir = str(config.get("plugin_dir") or "").strip()
    alpha_file = str(config.get("alphamissense_file") or "").strip()
    cmd = str(config.get("cmd") or "").strip()

    if (not script_path or not os.path.isfile(script_path)) and os.path.isfile(fallback_script):
        config["script_path"] = fallback_script
        script_path = fallback_script
    if (not cache_dir or not os.path.isdir(cache_dir)) and os.path.isdir(fallback_cache_dir):
        config["cache_dir"] = fallback_cache_dir
    if (not plugin_dir or not os.path.isdir(plugin_dir)) and os.path.isdir(fallback_plugin_dir):
        config["plugin_dir"] = fallback_plugin_dir
    if (not alpha_file or not os.path.isfile(alpha_file)) and os.path.isfile(fallback_alpha):
        config["alphamissense_file"] = fallback_alpha

    if (
        (not cmd or ("\\" in cmd) or (":" in cmd))
        and not shutil.which(cmd)
        and shutil.which("perl")
        and script_path
        and os.path.isfile(script_path)
    ):
        config["cmd"] = "perl"

    return config


def _vep_config() -> dict:
    args_raw = (os.environ.get("SP_VEP_EXTRA_ARGS") or "").strip()
    extra_args: list[str] = []
    if args_raw:
        try:
            extra_args = shlex.split(args_raw, posix=False)
        except ValueError:
            extra_args = args_raw.split()

    return {
        "cmd": (os.environ.get("SP_VEP_CMD") or "").strip() or "vep",
        "script_path": (os.environ.get("SP_VEP_SCRIPT_PATH") or "").strip() or None,
        "cache_dir": (os.environ.get("SP_VEP_CACHE_DIR") or "").strip() or None,
        "plugin_dir": (os.environ.get("SP_VEP_PLUGIN_DIR") or "").strip() or None,
        "alphamissense_file": (os.environ.get("SP_VEP_ALPHAMISSENSE_FILE") or "").strip() or None,
        "fasta_path": (os.environ.get("SP_VEP_FASTA_PATH") or "").strip() or None,
        "assembly": (os.environ.get("SP_VEP_ASSEMBLY") or "").strip() or "GRCh38",
        "timeout_seconds": _positive_int_env("SP_VEP_TIMEOUT_SECONDS", 1200),
        "extra_args": extra_args,
    }


def _validate_vep_config(config: dict) -> dict | None:
    cache_dir = config.get("cache_dir")
    alpha_file = config.get("alphamissense_file")
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
    if not alpha_file:
        return {
            "missing": "SP_VEP_ALPHAMISSENSE_FILE",
            "hint": "Set SP_VEP_ALPHAMISSENSE_FILE to the AlphaMissense plugin data file.",
        }
    if not os.path.isfile(alpha_file):
        return {
            "missing": "SP_VEP_ALPHAMISSENSE_FILE",
            "alphamissense_file": alpha_file,
            "hint": "SP_VEP_ALPHAMISSENSE_FILE must point to an existing file.",
        }
    alpha_file_text = str(alpha_file)
    if alpha_file_text.endswith(".gz"):
        alpha_tbi = f"{alpha_file_text}.tbi"
        if not os.path.isfile(alpha_tbi):
            return {
                "missing": "SP_VEP_ALPHAMISSENSE_FILE_TBI",
                "alphamissense_file": alpha_file_text,
                "alphamissense_tbi_file": alpha_tbi,
                "hint": "AlphaMissense .tbi index is missing; run sp-vep-init to rebuild plugin data/index.",
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
        f.write("##source=sp-prediction\n")
        f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for variant in variants:
            f.write(
                f"{variant['chrom']}\t{int(variant['pos'])}\t.\t{variant['ref']}\t{variant['alt']}\t.\t.\t.\n"
            )
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


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_predictor_values(record: dict) -> dict:
    values: dict[str, dict] = {}
    transcripts = record.get("transcript_consequences") or []
    if not isinstance(transcripts, list):
        transcripts = []

    for tc in transcripts:
        if not isinstance(tc, dict):
            continue

        if _SIFT_PREDICTOR_KEY not in values:
            score = _safe_float(tc.get("sift_score"))
            label = tc.get("sift_prediction")
            if score is not None:
                values[_SIFT_PREDICTOR_KEY] = {"score": score, "label": str(label) if label else None}

        if _POLYPHEN2_PREDICTOR_KEY not in values:
            score = _safe_float(tc.get("polyphen_score"))
            label = tc.get("polyphen_prediction")
            if score is not None:
                values[_POLYPHEN2_PREDICTOR_KEY] = {
                    "score": score,
                    "label": str(label) if label else None,
                }

        if _ALPHAMISSENSE_PREDICTOR_KEY not in values:
            score = _safe_float(tc.get("am_pathogenicity"))
            label = tc.get("am_class")
            if score is None:
                score = _safe_float(tc.get("alphamissense_score"))
                label = label or tc.get("alphamissense_class")

            # Some VEP AlphaMissense plugin versions emit a nested payload:
            # transcript_consequences[].alphamissense = {am_pathogenicity, am_class}
            if score is None:
                alpha_payload = tc.get("alphamissense")
                if isinstance(alpha_payload, dict):
                    score = _safe_float(alpha_payload.get("am_pathogenicity"))
                    label = label or alpha_payload.get("am_class")
                    if score is None:
                        score = _safe_float(alpha_payload.get("alphamissense_score"))
                        label = label or alpha_payload.get("alphamissense_class")
            if score is not None:
                values[_ALPHAMISSENSE_PREDICTOR_KEY] = {
                    "score": score,
                    "label": str(label) if label else None,
                }

        if len(values) == 3:
            break

    return values


def _load_vep_outputs(output_path: str) -> list[dict]:
    rows: list[dict] = []
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            rows.append(json.loads(stripped))
    return rows


def _make_predictor_output(
    *,
    predictor_key: str,
    predictor_label: str,
    variant: dict,
    consequence_category: str | None,
    predictor_values: dict | None,
    created_at: str,
) -> dict:
    details = {
        "source_line": variant.get("source_line"),
        "consequence_category": consequence_category,
        "source_tool": "vep",
    }

    if consequence_category == "missense":
        if predictor_values and predictor_values.get("score") is not None:
            score = float(predictor_values["score"])
            label = predictor_values.get("label")
            details["integration"] = "vep_json"
            return {
                "variant_id": variant["variant_id"],
                "predictor_key": predictor_key,
                "outcome": "computed",
                "score": score,
                "label": label,
                "reason_code": None,
                "reason_message": None,
                "details": details,
                "created_at": created_at,
            }
        return {
            "variant_id": variant["variant_id"],
            "predictor_key": predictor_key,
            "outcome": "not_computed",
            "score": None,
            "label": None,
            "reason_code": "TOOL_OUTPUT_MISSING",
            "reason_message": f"{predictor_label} output was not present in VEP response.",
            "details": details,
            "created_at": created_at,
        }

    if consequence_category in {"synonymous", "nonsense", "other"}:
        return {
            "variant_id": variant["variant_id"],
            "predictor_key": predictor_key,
            "outcome": "not_applicable",
            "score": None,
            "label": None,
            "reason_code": "NOT_MISSENSE",
            "reason_message": f"{predictor_label} is only applicable to missense variants.",
            "details": details,
            "created_at": created_at,
        }

    if consequence_category is None:
        reason_code = "MISSING_CLASSIFICATION"
        reason_message = "Classification output is missing for this variant."
    else:
        reason_code = "NO_PROTEIN_CONTEXT"
        reason_message = "No transcript/protein context available for prediction."
    return {
        "variant_id": variant["variant_id"],
        "predictor_key": predictor_key,
        "outcome": "not_computed",
        "score": None,
        "label": None,
        "reason_code": reason_code,
        "reason_message": reason_message,
        "details": details,
        "created_at": created_at,
    }


def run_prediction_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    logger,
    force: bool = False,
    vep_config_overrides: dict | None = None,
) -> dict:
    created_at = datetime.now(timezone.utc).isoformat()
    stats: dict = {"predictors_executed": [], "tool": "vep"}
    alpha_file_path: str | None = None
    alpha_plugin_dir: str | None = None

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
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run is canceled and cannot be predicted.")

            stage_status, stage_uploaded_at = _get_stage_status(conn, run_id, "prediction")
            if stage_status == "running" and not force:
                conn.rollback()
                raise StageExecutionError(409, "STAGE_RUNNING", "Prediction stage is already running.")

            if stage_status == "succeeded" and stage_uploaded_at == uploaded_at:
                conn.rollback()
                raise StageExecutionError(409, "ALREADY_PREDICTED", "This upload was already predicted.")

            def require_upstream(stage_name: str, error_code: str, error_message: str) -> None:
                status, stage_input = _get_stage_status(conn, run_id, stage_name)
                if status == "succeeded" and stage_input == uploaded_at:
                    return

                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    error_code=error_code,
                    error_message=error_message,
                    error_details={
                        f"{stage_name}_status": status,
                        f"{stage_name}_input_uploaded_at": stage_input,
                    },
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    409,
                    error_code,
                    error_message,
                    details={
                        f"{stage_name}_status": status,
                        f"{stage_name}_input_uploaded_at": stage_input,
                    },
                )

            require_upstream(
                "parser",
                "MISSING_PARSER_OUTPUT",
                "Parser stage must succeed for this upload before prediction can run.",
            )
            require_upstream(
                "pre_annotation",
                "MISSING_PRE_ANNOTATION_OUTPUT",
                "Pre-annotation stage must succeed for this upload before prediction can run.",
            )
            require_upstream(
                "classification",
                "MISSING_CLASSIFICATION_OUTPUT",
                "Classification stage must succeed for this upload before prediction can run.",
            )

            mark_stage_running(
                db_path,
                run_id,
                "prediction",
                input_uploaded_at=uploaded_at,
                conn=conn,
                commit=False,
            )
            _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
            conn.commit()

            variants = list(iter_variants_for_run_with_ids(db_path, run_id, conn=conn))
            classification_rows = conn.execute(
                "SELECT variant_id, consequence_category FROM run_classifications WHERE run_id = ?",
                (run_id,),
            ).fetchall()
            consequence_by_variant_id = {r[0]: r[1] for r in classification_rows}

            missense_variants = [
                variant
                for variant in variants
                if consequence_by_variant_id.get(variant["variant_id"]) == "missense"
            ]
            other_variants_skipped = sum(
                1
                for variant in variants
                if consequence_by_variant_id.get(variant["variant_id"]) == "other"
            )
            stats["variants_processed"] = len(variants)
            stats["variants_sent_to_vep"] = len(missense_variants)
            stats["variants_skipped_other"] = other_variants_skipped

            records: list[dict] = []
            if missense_variants:
                predictors_executed = list(_PREDICTORS_EXECUTED)
                stats["predictors_executed"] = predictors_executed

                config = _vep_config()
                if vep_config_overrides:
                    config = {**config, **vep_config_overrides}
                config = _apply_container_vep_fallback(config)
                alpha_file_path = str(config.get("alphamissense_file") or "") or None
                alpha_plugin_dir = str(config.get("plugin_dir") or "") or None
                stats["timeout_seconds"] = config["timeout_seconds"]
                stats["assembly"] = config["assembly"]

                config_error = _validate_vep_config(config)
                if config_error:
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_NOT_CONFIGURED",
                        error_message="Prediction stage requires VEP runtime configuration.",
                        error_details=config_error,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "VEP_NOT_CONFIGURED",
                        "Prediction stage requires VEP runtime configuration.",
                        details=config_error,
                    )

                artifacts_dir = ensure_run_artifacts_dir(db_path, run_id)
                input_vcf_path = os.path.join(artifacts_dir, "prediction.input.vcf")
                output_json_path = os.path.join(artifacts_dir, "prediction.vep.jsonl")
                variants_written = _write_minimal_vcf_fast(missense_variants, input_vcf_path)

                cmd: list[str] = [config["cmd"]]
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
                        "--sift",
                        "b",
                        "--polyphen",
                        "b",
                        "--plugin",
                        f"AlphaMissense,file={config['alphamissense_file']}",
                    ]
                )
                if config.get("plugin_dir"):
                    cmd.extend(["--dir_plugins", str(config["plugin_dir"])])
                if config.get("fasta_path"):
                    cmd.extend(["--fasta", str(config["fasta_path"])])
                cmd.extend(list(config.get("extra_args") or []))

                stats["vep_output_path"] = output_json_path
                stats["vep_input_path"] = input_vcf_path
                stats["vep_cmd"] = os.path.basename(str(config["cmd"]))
                stats["variants_written"] = variants_written

                try:
                    completed = subprocess.run(
                        cmd,
                        cwd=None,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                        timeout=int(config["timeout_seconds"]),
                    )
                except FileNotFoundError:
                    details = {
                        "cmd": cmd,
                        "hint": "Set SP_VEP_CMD (and optional SP_VEP_SCRIPT_PATH) to a valid executable.",
                    }
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_NOT_CONFIGURED",
                        error_message="VEP executable is not available.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "VEP_NOT_CONFIGURED",
                        "VEP executable is not available.",
                        details=details,
                    )
                except subprocess.TimeoutExpired as exc:
                    stderr_raw = exc.stderr
                    if isinstance(stderr_raw, bytes):
                        stderr_text = stderr_raw.decode("utf-8", errors="replace")
                    else:
                        stderr_text = str(stderr_raw or "")
                    details = {
                        "timeout_seconds": config["timeout_seconds"],
                        "stderr_tail": _tail(stderr_text),
                        "cmd": cmd,
                    }
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_TIMEOUT",
                        error_message=f"VEP timed out after {config['timeout_seconds']} seconds.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "VEP_TIMEOUT",
                        f"VEP timed out after {config['timeout_seconds']} seconds.",
                        details={"timeout_seconds": config["timeout_seconds"]},
                    )
                except TimeoutError:
                    details = {"timeout_seconds": config["timeout_seconds"], "cmd": cmd}
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_TIMEOUT",
                        error_message=f"VEP timed out after {config['timeout_seconds']} seconds.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(
                        500,
                        "VEP_TIMEOUT",
                        f"VEP timed out after {config['timeout_seconds']} seconds.",
                        details={"timeout_seconds": config["timeout_seconds"]},
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
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_FAILED",
                        error_message="VEP execution failed.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(500, "VEP_FAILED", "VEP execution failed.", details=details)

                try:
                    records = _load_vep_outputs(output_json_path)
                except Exception as exc:
                    details = {"reason": str(exc), "output_path": output_json_path}
                    conn.execute("BEGIN IMMEDIATE")
                    mark_stage_failed(
                        db_path,
                        run_id,
                        "prediction",
                        input_uploaded_at=uploaded_at,
                        error_code="VEP_PARSE_FAILED",
                        error_message="Failed to parse VEP output.",
                        error_details=details,
                        conn=conn,
                        commit=False,
                    )
                    _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                    conn.commit()
                    raise StageExecutionError(
                        500, "VEP_PARSE_FAILED", "Failed to parse VEP output.", details=details
                    )
            else:
                stats["predictors_executed"] = []
                stats["note"] = "No missense variants found; predictor execution skipped."

            variant_id_by_key: dict[tuple[str, int, str, str], str] = {}
            for variant in variants:
                key = (
                    _normalized_chrom(variant.get("chrom")),
                    int(variant.get("pos")),
                    str(variant.get("ref")),
                    str(variant.get("alt")),
                )
                variant_id_by_key[key] = variant["variant_id"]

            predictor_by_variant_id: dict[str, dict] = {}
            for record in records:
                key = _extract_variant_key(record if isinstance(record, dict) else {})
                if not key:
                    continue
                variant_id = variant_id_by_key.get(key)
                if not variant_id:
                    continue
                predictor_by_variant_id[variant_id] = _extract_predictor_values(record)

            missense_variant_ids = {variant["variant_id"] for variant in missense_variants}
            missense_with_sift_or_polyphen = 0
            missense_with_alphamissense = 0
            for variant_id in missense_variant_ids:
                values = predictor_by_variant_id.get(variant_id) or {}
                if _SIFT_PREDICTOR_KEY in values or _POLYPHEN2_PREDICTOR_KEY in values:
                    missense_with_sift_or_polyphen += 1
                if _ALPHAMISSENSE_PREDICTOR_KEY in values:
                    missense_with_alphamissense += 1

            stats["missense_variants"] = len(missense_variant_ids)
            stats["missense_with_sift_or_polyphen"] = missense_with_sift_or_polyphen
            stats["missense_with_alphamissense"] = missense_with_alphamissense

            if (
                missense_variant_ids
                and missense_with_sift_or_polyphen > 0
                and missense_with_alphamissense == 0
            ):
                details = {
                    "alpha_file": alpha_file_path,
                    "alpha_plugin_dir": alpha_plugin_dir,
                    "missense_variants": len(missense_variant_ids),
                    "missense_with_sift_or_polyphen": missense_with_sift_or_polyphen,
                    "hint": "AlphaMissense plugin appears unavailable. Verify SP_VEP_ALPHAMISSENSE_FILE, tabix index (.tbi), and plugin runtime setup.",
                }
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_failed(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    error_code="ALPHAMISSENSE_NOT_AVAILABLE",
                    error_message="AlphaMissense output was missing for all missense variants.",
                    error_details=details,
                    conn=conn,
                    commit=False,
                )
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                conn.commit()
                raise StageExecutionError(
                    500,
                    "ALPHAMISSENSE_NOT_AVAILABLE",
                    "AlphaMissense output was missing for all missense variants.",
                    details=details,
                )

            outcome_counts_by_predictor: dict[str, dict[str, int]] = {
                key: {} for key in _PREDICTORS_EXECUTED
            }

            outputs: list[dict] = []
            for variant in variants:
                category = consequence_by_variant_id.get(variant["variant_id"])
                if category == "other":
                    continue
                predictor_values = predictor_by_variant_id.get(variant["variant_id"], {})

                for predictor_key, predictor_label in _PREDICTOR_SPECS:
                    row = _make_predictor_output(
                        predictor_key=predictor_key,
                        predictor_label=predictor_label,
                        variant=variant,
                        consequence_category=category,
                        predictor_values=predictor_values.get(predictor_key),
                        created_at=created_at,
                    )
                    outputs.append(row)
                    by_outcome = outcome_counts_by_predictor[predictor_key]
                    outcome = row["outcome"]
                    by_outcome[outcome] = by_outcome.get(outcome, 0) + 1

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            conn.execute("BEGIN IMMEDIATE")
            upsert_predictor_outputs_for_run(db_path, run_id, outputs, conn=conn, commit=False)
            conn.commit()

            if _get_run_status(conn, run_id) == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                mark_stage_canceled(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    conn=conn,
                    commit=False,
                )
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")

            stats["outputs_persisted_by_predictor"] = {
                key: outcome_counts_by_predictor[key].get("computed", 0)
                + outcome_counts_by_predictor[key].get("not_computed", 0)
                + outcome_counts_by_predictor[key].get("not_applicable", 0)
                + outcome_counts_by_predictor[key].get("error", 0)
                for key in _PREDICTORS_EXECUTED
            }
            stats["outcome_counts_by_predictor"] = outcome_counts_by_predictor

            mark_stage_succeeded(
                db_path,
                run_id,
                "prediction",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            conn.commit()

            stage_after_success, _ = _get_stage_status(conn, run_id, "prediction")
            if stage_after_success == "canceled":
                conn.execute("BEGIN IMMEDIATE")
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                conn.commit()
                raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")
        finally:
            conn.close()
    except StageExecutionError:
        raise
    except Exception as exc:
        logger.exception("Prediction stage failed")
        try:
            conn = _connect_db(db_path)
            try:
                _init_schema(conn)
                conn.execute("BEGIN IMMEDIATE")
                _clear_outputs_for_predictors(conn, db_path, run_id, _PREDICTORS_EXECUTED)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "prediction",
                    input_uploaded_at=uploaded_at,
                    error_code="PREDICTION_FAILED",
                    error_message="Prediction stage failed.",
                    error_details={"reason": str(exc)},
                    conn=conn,
                    commit=False,
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist prediction failure state")
        raise StageExecutionError(500, "PREDICTION_FAILED", "Prediction stage failed.") from None

    return {"prediction": {"status": "succeeded", "stats": stats}}
