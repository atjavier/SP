from __future__ import annotations

import os
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pipeline.clinvar_client import ClinvarConfig, fetch_clinvar_evidence_for_variant
from pipeline.dbsnp_client import DbsnpConfig, fetch_dbsnp_evidence_for_variant
from pipeline.gnomad_client import GnomadConfig, fetch_gnomad_evidence_for_variant
from pipeline.local_evidence import (
    fetch_clinvar_evidence_from_local_vcf,
    fetch_dbsnp_evidence_from_local_vcf,
    fetch_gnomad_evidence_from_local_vcf,
)
from pipeline.cancel_signals import clear_run_cancel_request, is_run_cancel_requested
from pipeline.parser_stage import StageExecutionError
from storage.db import connect as _connect_db
from storage.db import init_schema as _init_schema
from storage.clinvar_evidence import clear_clinvar_evidence_for_run, upsert_clinvar_evidence_for_run
from storage.dbsnp_evidence import clear_dbsnp_evidence_for_run, upsert_dbsnp_evidence_for_run
from storage.gnomad_evidence import clear_gnomad_evidence_for_run, upsert_gnomad_evidence_for_run
from storage.runs import update_run_evidence_mode_decision
from storage.run_artifacts import ensure_run_artifacts_dir
from storage.stages import (
    mark_stage_canceled,
    mark_stage_failed,
    mark_stage_running,
    mark_stage_succeeded,
)
from storage.variants import iter_variants_for_run_with_ids

_VALID_EVIDENCE_POLICIES: frozenset[str] = frozenset({"stop", "continue"})
_VALID_EVIDENCE_PROFILES: frozenset[str] = frozenset({"full", "minimum_exome", "predictor_only"})
_ENFORCED_EVIDENCE_PROFILE = "predictor_only"
_VALID_EVIDENCE_MODES: frozenset[str] = frozenset({"online", "offline", "hybrid"})
_EVIDENCE_SOURCES: tuple[str, ...] = ("dbsnp", "clinvar", "gnomad")
_NO_VALID_SOURCE_REASONS: frozenset[str] = frozenset(
    {
        "requested_online_no_valid_source",
        "requested_offline_no_valid_source",
        "requested_hybrid_no_valid_source",
    }
)
_EXOME_PROFILE_ALLOWED_CATEGORIES: frozenset[str] = frozenset(
    {"synonymous", "missense", "nonsense"}
)
_PREDICTOR_PROFILE_ALLOWED_CATEGORIES: frozenset[str] = frozenset({"missense"})
_VALID_VCF_SUFFIXES: tuple[str, ...] = (".vcf", ".vcf.gz", ".vcf.bgz")


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


def _positive_float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value < 0:
        return default
    return value


def _max_workers_env(name: str, default: int = 1) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return max(1, int(default))
    try:
        value = int(raw)
    except ValueError:
        return max(1, int(default))
    return max(1, value)


def _collect_evidence_results(
    *,
    db_path: str,
    conn,
    run_id: str,
    uploaded_at: str,
    variants: list[dict],
    evidence_profile: str,
    categories_by_variant: dict,
    max_workers: int,
    fetch_fn,
) -> tuple[list[tuple[dict, dict]], int]:
    eligible: list[dict] = []
    skipped = 0
    for variant in variants:
        _ensure_annotation_not_canceled(
            db_path,
            conn,
            run_id,
            uploaded_at=uploaded_at,
        )
        if not _is_variant_in_evidence_scope(
            evidence_profile=evidence_profile,
            variant_id=variant.get("variant_id"),
            categories_by_variant=categories_by_variant,
        ):
            skipped += 1
            continue
        eligible.append(variant)

    results: list[tuple[dict, dict]] = []
    if max_workers <= 1 or len(eligible) <= 1:
        for variant in eligible:
            _ensure_annotation_not_canceled(
                db_path,
                conn,
                run_id,
                uploaded_at=uploaded_at,
            )
            results.append((variant, fetch_fn(variant)))
        return results, skipped

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_variant = {executor.submit(fetch_fn, variant): variant for variant in eligible}
        for future in as_completed(future_to_variant):
            _ensure_annotation_not_canceled(
                db_path,
                conn,
                run_id,
                uploaded_at=uploaded_at,
            )
            variant = future_to_variant[future]
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                result = {
                    "outcome": "error",
                    "reason_code": "REQUEST_ERROR",
                    "reason_message": f"Evidence request failed unexpectedly: {exc}",
                    "details": {"error": str(exc)},
                }
            results.append((variant, result))

    return results, skipped


def _dbsnp_config(reference_build: str | None) -> DbsnpConfig:
    enabled_raw = os.environ.get("SP_DBSNP_ENABLED")
    enabled = _truthy_env("SP_DBSNP_ENABLED") if enabled_raw is not None else True
    base_url = (os.environ.get("SP_DBSNP_API_BASE_URL") or "").strip() or "https://api.ncbi.nlm.nih.gov/variation/v0"
    timeout_seconds = _positive_int_env("SP_DBSNP_TIMEOUT_SECONDS", 10)
    retry_max_attempts = _positive_int_env("SP_DBSNP_RETRY_MAX_ATTEMPTS", 3)
    backoff_base = _positive_float_env("SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS", 0.5)
    backoff_max = _positive_float_env("SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS", 8.0)
    if backoff_max < backoff_base:
        backoff_max = backoff_base
    api_key = (os.environ.get("SP_DBSNP_API_KEY") or "").strip() or None
    assembly = (os.environ.get("SP_DBSNP_ASSEMBLY") or "").strip() or (reference_build or "GRCh38")
    return DbsnpConfig(
        enabled=enabled,
        api_base_url=base_url,
        timeout_seconds=timeout_seconds,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_base_seconds=backoff_base,
        retry_backoff_max_seconds=backoff_max,
        api_key=api_key,
        assembly=assembly,
    )


def _clinvar_config() -> ClinvarConfig:
    enabled_raw = os.environ.get("SP_CLINVAR_ENABLED")
    enabled = _truthy_env("SP_CLINVAR_ENABLED") if enabled_raw is not None else True
    base_url = (
        (os.environ.get("SP_CLINVAR_API_BASE_URL") or "").strip()
        or "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    )
    timeout_seconds = _positive_int_env("SP_CLINVAR_TIMEOUT_SECONDS", 10)
    retry_max_attempts = _positive_int_env("SP_CLINVAR_RETRY_MAX_ATTEMPTS", 3)
    backoff_base = _positive_float_env("SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS", 0.5)
    backoff_max = _positive_float_env("SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS", 8.0)
    if backoff_max < backoff_base:
        backoff_max = backoff_base
    api_key = (os.environ.get("SP_CLINVAR_API_KEY") or "").strip() or None
    return ClinvarConfig(
        enabled=enabled,
        api_base_url=base_url,
        timeout_seconds=timeout_seconds,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_base_seconds=backoff_base,
        retry_backoff_max_seconds=backoff_max,
        api_key=api_key,
    )


def _gnomad_config() -> GnomadConfig:
    enabled_raw = os.environ.get("SP_GNOMAD_ENABLED")
    enabled = _truthy_env("SP_GNOMAD_ENABLED") if enabled_raw is not None else True
    base_url = (os.environ.get("SP_GNOMAD_API_BASE_URL") or "").strip() or "https://gnomad.broadinstitute.org/api"
    dataset_id = (os.environ.get("SP_GNOMAD_DATASET_ID") or "").strip() or "gnomad_r4"
    reference_genome = (os.environ.get("SP_GNOMAD_REFERENCE_GENOME") or "").strip() or "GRCh38"
    timeout_seconds = _positive_int_env("SP_GNOMAD_TIMEOUT_SECONDS", 10)
    retry_max_attempts = _positive_int_env("SP_GNOMAD_RETRY_MAX_ATTEMPTS", 3)
    backoff_base = _positive_float_env("SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS", 0.5)
    backoff_max = _positive_float_env("SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS", 8.0)
    min_request_interval_seconds = _positive_float_env("SP_GNOMAD_MIN_REQUEST_INTERVAL_SECONDS", 1.0)
    if backoff_max < backoff_base:
        backoff_max = backoff_base
    return GnomadConfig(
        enabled=enabled,
        api_base_url=base_url,
        dataset_id=dataset_id,
        reference_genome=reference_genome,
        timeout_seconds=timeout_seconds,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_base_seconds=backoff_base,
        retry_backoff_max_seconds=backoff_max,
        min_request_interval_seconds=min_request_interval_seconds,
    )


def _normalize_evidence_policy(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text in _VALID_EVIDENCE_POLICIES:
        return text
    return None


def _default_annotation_evidence_policy() -> str:
    explicit = _normalize_evidence_policy(os.environ.get("SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT"))
    if explicit:
        return explicit
    raw = os.environ.get("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR")
    if raw is None:
        return "continue"
    return "stop" if _truthy_env("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR") else "continue"


def _resolve_annotation_evidence_policy(evidence_failure_policy: str | None = None) -> str:
    normalized = _normalize_evidence_policy(evidence_failure_policy)
    if normalized:
        return normalized
    return _default_annotation_evidence_policy()


def _annotation_fail_on_evidence_error(evidence_failure_policy: str | None = None) -> bool:
    return _resolve_annotation_evidence_policy(evidence_failure_policy) == "stop"


def _safe_non_negative_int(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    if parsed < 0:
        return 0
    return parsed


def _source_completeness_from_stats(stats: dict, source_key: str) -> tuple[str, str]:
    enabled = stats.get(f"{source_key}_enabled")
    if enabled is False:
        return ("unavailable", "disabled")

    eligible = _safe_non_negative_int(stats.get(f"{source_key}_variants_eligible"))
    skipped = _safe_non_negative_int(stats.get(f"{source_key}_skipped_out_of_scope"))
    found = _safe_non_negative_int(stats.get(f"{source_key}_found"))
    not_found = _safe_non_negative_int(stats.get(f"{source_key}_not_found"))
    errors = _safe_non_negative_int(stats.get(f"{source_key}_errors"))
    resolved = found + not_found

    if errors > 0 and resolved > 0:
        return ("partial", "errors_present")
    if errors > 0:
        return ("unavailable", "errors_only")
    if eligible <= 0:
        return ("unavailable", "no_eligible_variants")
    return ("complete", "evidence_available")


def _compute_evidence_completeness_from_stats(stats: dict) -> tuple[dict[str, str], dict[str, str], str]:
    source_completeness: dict[str, str] = {}
    source_reasons: dict[str, str] = {}
    for source in _EVIDENCE_SOURCES:
        completeness, reason = _source_completeness_from_stats(stats, source)
        source_completeness[source] = completeness
        source_reasons[source] = reason

    values = list(source_completeness.values())
    if any(value == "partial" for value in values):
        aggregate = "partial"
    elif all(value == "complete" for value in values):
        aggregate = "complete"
    elif any(value == "complete" for value in values):
        aggregate = "partial"
    else:
        aggregate = "unavailable"

    return source_completeness, source_reasons, aggregate


def _augment_stats_with_evidence_completeness(stats: dict) -> None:
    source_completeness, source_reasons, aggregate = _compute_evidence_completeness_from_stats(stats)
    stats["evidence_source_completeness"] = source_completeness
    stats["evidence_source_completeness_reason"] = source_reasons
    stats["annotation_evidence_completeness"] = aggregate
    stats["evidence_complete_sources"] = [s for s in _EVIDENCE_SOURCES if source_completeness.get(s) == "complete"]
    stats["evidence_partial_sources"] = [s for s in _EVIDENCE_SOURCES if source_completeness.get(s) == "partial"]
    stats["evidence_unavailable_sources"] = [
        s for s in _EVIDENCE_SOURCES if source_completeness.get(s) == "unavailable"
    ]


def _evidence_failure_details(
    details: dict,
    *,
    failed_source: str,
    policy: str,
    processed_source_states: dict[str, tuple[str, str]] | None = None,
) -> dict:
    merged = dict(details or {})
    merged["failed_source"] = failed_source
    processed_states = dict(processed_source_states or {})

    if failed_source in _EVIDENCE_SOURCES:
        failed_index = _EVIDENCE_SOURCES.index(failed_source)
        downstream_sources = list(_EVIDENCE_SOURCES[failed_index + 1 :])
    else:
        downstream_sources = []

    missing_outputs = [failed_source] + [s for s in downstream_sources if s != failed_source]
    source_completeness: dict[str, str] = {}
    source_reasons: dict[str, str] = {}
    for source in _EVIDENCE_SOURCES:
        if source == failed_source:
            source_completeness[source] = "unavailable"
            source_reasons[source] = "failed_source_error"
        elif source in downstream_sources:
            source_completeness[source] = "unavailable"
            source_reasons[source] = "not_executed_due_failure"
        else:
            source_completeness[source] = "unavailable"
            source_reasons[source] = "unavailable_before_failure"

    for source, source_state in processed_states.items():
        if source not in _EVIDENCE_SOURCES:
            continue
        if source == failed_source or source in downstream_sources:
            continue
        completeness = str((source_state or ("unavailable", ""))[0] or "unavailable").strip().lower()
        if completeness not in {"complete", "partial", "unavailable"}:
            completeness = "unavailable"
        reason = str((source_state or ("", ""))[1] or "").strip() or "completed_before_failure"
        source_completeness[source] = completeness
        source_reasons[source] = reason

    has_any_available = any(
        source_completeness.get(source) in {"complete", "partial"} for source in _EVIDENCE_SOURCES
    )
    merged["missing_outputs"] = missing_outputs
    merged["annotation_evidence_policy"] = policy
    merged["evidence_source_completeness"] = source_completeness
    merged["evidence_source_completeness_reason"] = source_reasons
    merged["annotation_evidence_completeness"] = "partial" if has_any_available else "unavailable"
    return merged


def _resolve_evidence_profile() -> str:
    # Evidence annotation is enforced as missense-only; ignore env overrides.
    return _ENFORCED_EVIDENCE_PROFILE


def _resolve_evidence_mode() -> str:
    raw = (os.environ.get("SP_EVIDENCE_MODE") or "").strip().lower()
    if raw in {"local", "offline_local"}:
        return "offline"
    if raw in _VALID_EVIDENCE_MODES:
        return raw
    return "online"


def _probe_connectivity_enabled() -> bool:
    raw = os.environ.get("SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED")
    if raw is None:
        return True
    return _truthy_env("SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED")


def _probe_timeout_seconds() -> float:
    timeout = _positive_float_env("SP_EVIDENCE_CONNECTIVITY_PROBE_TIMEOUT_SECONDS", 1.5)
    return max(0.2, timeout)


def _probe_max_attempts() -> int:
    return max(1, _positive_int_env("SP_EVIDENCE_CONNECTIVITY_PROBE_MAX_ATTEMPTS", 1))


def _probe_http_base_url(url: str, *, timeout_seconds: float, max_attempts: int) -> bool:
    if not url:
        return False

    for attempt in range(max_attempts):
        del attempt
        for method in ("HEAD", "GET"):
            req = Request(url, method=method, headers={"User-Agent": "SP/annotation-probe"})
            try:
                with urlopen(req, timeout=timeout_seconds) as resp:
                    status = int(getattr(resp, "status", 200) or 200)
                    if status < 500:
                        return True
            except HTTPError as exc:
                if 400 <= int(exc.code) < 500:
                    return True
                continue
            except (TimeoutError, URLError, OSError):
                continue
            except Exception:
                continue
    return False


def _local_source_scan_max_files() -> int:
    return max(100, _positive_int_env("SP_EVIDENCE_LOCAL_SCAN_MAX_FILES", 2000))


def _local_source_scan_max_depth() -> int:
    return max(0, _positive_int_env("SP_EVIDENCE_LOCAL_SCAN_MAX_DEPTH", 4))


def _is_indexed_vcf_candidate(path: str) -> bool:
    lowered = path.lower()
    if lowered.endswith(".tbi") or lowered.endswith(".csi"):
        return False
    if not lowered.endswith(_VALID_VCF_SUFFIXES):
        return False
    # Local evidence readers depend on compressed VCF + tabix/csi index.
    if lowered.endswith(".vcf"):
        return False
    return True


def _has_vcf_index(path: str) -> bool:
    return os.path.isfile(f"{path}.tbi") or os.path.isfile(f"{path}.csi")


def _local_vcf_source_state(local_vcf_path: str | None) -> dict[str, object]:
    raw_path = str(local_vcf_path or "").strip()
    if not raw_path:
        return {"configured": False, "ready": False, "reason": "not_configured"}

    if os.path.isfile(raw_path):
        if not _is_indexed_vcf_candidate(raw_path):
            return {"configured": True, "ready": False, "reason": "unsupported_path"}
        if not _has_vcf_index(raw_path):
            return {"configured": True, "ready": False, "reason": "index_missing"}
        return {"configured": True, "ready": True, "reason": "ready"}

    if not os.path.isdir(raw_path):
        return {"configured": True, "ready": False, "reason": "path_missing"}

    scan_root = os.path.abspath(raw_path)
    max_files = _local_source_scan_max_files()
    max_depth = _local_source_scan_max_depth()
    scanned_files = 0

    for root, dirs, files in os.walk(scan_root, topdown=True):
        rel = os.path.relpath(root, scan_root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        if depth >= max_depth:
            dirs[:] = []
        for name in files:
            scanned_files += 1
            if scanned_files > max_files:
                return {"configured": True, "ready": False, "reason": "scan_limit_reached"}
            candidate = os.path.join(root, name)
            if not _is_indexed_vcf_candidate(candidate):
                continue
            if _has_vcf_index(candidate):
                return {"configured": True, "ready": True, "reason": "ready"}
    return {"configured": True, "ready": False, "reason": "no_indexed_vcf_found"}


def _is_local_vcf_source_ready(local_vcf_path: str | None) -> bool:
    state = _local_vcf_source_state(local_vcf_path)
    return bool(state.get("ready"))


def _resolve_evidence_mode_decision(
    *,
    requested_mode: str,
    online_available: bool,
    offline_sources_configured: dict[str, bool],
    offline_sources_available: dict[str, bool] | None = None,
    offline_sources_unavailable_reason: dict[str, str] | None = None,
) -> dict[str, object]:
    requested = (requested_mode or "").strip().lower()
    if requested in {"local", "offline_local"}:
        requested = "offline"
    if requested not in _VALID_EVIDENCE_MODES:
        requested = "online"

    normalized_sources = {
        "dbsnp": bool((offline_sources_configured or {}).get("dbsnp", False)),
        "clinvar": bool((offline_sources_configured or {}).get("clinvar", False)),
        "gnomad": bool((offline_sources_configured or {}).get("gnomad", False)),
    }
    normalized_available = {
        "dbsnp": bool((offline_sources_available or normalized_sources).get("dbsnp", False)),
        "clinvar": bool((offline_sources_available or normalized_sources).get("clinvar", False)),
        "gnomad": bool((offline_sources_available or normalized_sources).get("gnomad", False)),
    }
    normalized_unavailable_reason = {
        "dbsnp": str((offline_sources_unavailable_reason or {}).get("dbsnp", "") or ""),
        "clinvar": str((offline_sources_unavailable_reason or {}).get("clinvar", "") or ""),
        "gnomad": str((offline_sources_unavailable_reason or {}).get("gnomad", "") or ""),
    }
    offline_available = any(normalized_available.values())
    online_ok = bool(online_available)

    if requested == "online":
        if online_ok:
            effective = "online"
            reason = "requested_online_online_available"
        elif offline_available:
            effective = "offline"
            reason = "requested_online_fallback_offline"
        else:
            effective = "online"
            reason = "requested_online_no_valid_source"
    elif requested == "offline":
        if offline_available:
            effective = "offline"
            reason = "requested_offline_offline_available"
        elif online_ok:
            effective = "online"
            reason = "requested_offline_fallback_online"
        else:
            effective = "offline"
            reason = "requested_offline_no_valid_source"
    else:  # hybrid
        if offline_available and online_ok:
            effective = "hybrid"
            reason = "requested_hybrid_both_available"
        elif offline_available:
            effective = "offline"
            reason = "requested_hybrid_online_unavailable"
        elif online_ok:
            effective = "online"
            reason = "requested_hybrid_offline_unavailable"
        else:
            effective = "hybrid"
            reason = "requested_hybrid_no_valid_source"

    return {
        "requested_mode": requested,
        "effective_mode": effective,
        "online_available": online_ok,
        "offline_sources_configured": normalized_sources,
        "offline_sources_available": normalized_available,
        "offline_sources_unavailable_reason": normalized_unavailable_reason,
        "decision_reason": reason,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def _detect_evidence_mode_decision(
    *,
    requested_mode: str,
    dbsnp_local_vcf_path: str | None,
    clinvar_local_vcf_path: str | None,
    gnomad_local_vcf_path: str | None,
    dbsnp_enabled: bool,
    clinvar_enabled: bool,
    gnomad_enabled: bool,
) -> dict[str, object]:
    requested_normalized = (requested_mode or "").strip().lower()
    if requested_normalized in {"local", "offline_local"}:
        requested_normalized = "offline"
    if requested_normalized not in _VALID_EVIDENCE_MODES:
        requested_normalized = "online"

    dbsnp_local_state = _local_vcf_source_state(dbsnp_local_vcf_path)
    clinvar_local_state = _local_vcf_source_state(clinvar_local_vcf_path)
    gnomad_local_state = _local_vcf_source_state(gnomad_local_vcf_path)
    offline_sources_configured = {
        "dbsnp": bool(dbsnp_local_state.get("configured")),
        "clinvar": bool(clinvar_local_state.get("configured")),
        "gnomad": bool(gnomad_local_state.get("configured")),
    }
    offline_sources_available = {
        "dbsnp": bool(dbsnp_enabled) and bool(dbsnp_local_state.get("ready")),
        "clinvar": bool(clinvar_enabled) and bool(clinvar_local_state.get("ready")),
        "gnomad": bool(gnomad_enabled) and bool(gnomad_local_state.get("ready")),
    }
    offline_sources_unavailable_reason = {
        "dbsnp": (
            str(dbsnp_local_state.get("reason") or "")
            if offline_sources_configured["dbsnp"] and not bool(dbsnp_local_state.get("ready"))
            else ""
        ),
        "clinvar": (
            str(clinvar_local_state.get("reason") or "")
            if offline_sources_configured["clinvar"] and not bool(clinvar_local_state.get("ready"))
            else ""
        ),
        "gnomad": (
            str(gnomad_local_state.get("reason") or "")
            if offline_sources_configured["gnomad"] and not bool(gnomad_local_state.get("ready"))
            else ""
        ),
    }

    online_probe_sources = []
    if dbsnp_enabled:
        online_probe_sources.append(
            (os.environ.get("SP_DBSNP_API_BASE_URL") or "").strip() or "https://api.ncbi.nlm.nih.gov/variation/v0"
        )
    if clinvar_enabled:
        online_probe_sources.append(
            (os.environ.get("SP_CLINVAR_API_BASE_URL") or "").strip()
            or "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        )
    if gnomad_enabled:
        online_probe_sources.append(
            (os.environ.get("SP_GNOMAD_API_BASE_URL") or "").strip()
            or "https://gnomad.broadinstitute.org/api"
        )

    online_available = False
    should_probe_online = bool(online_probe_sources)
    if requested_normalized == "offline" and any(offline_sources_available.values()):
        # Offline request with ready local sources should not be blocked by
        # extra online probing latency.
        should_probe_online = False

    if should_probe_online:
        if not _probe_connectivity_enabled():
            online_available = True
        else:
            timeout_seconds = _probe_timeout_seconds()
            max_attempts = _probe_max_attempts()
            for base_url in online_probe_sources:
                if _probe_http_base_url(base_url, timeout_seconds=timeout_seconds, max_attempts=max_attempts):
                    online_available = True
                    break

    return _resolve_evidence_mode_decision(
        requested_mode=requested_normalized,
        online_available=online_available,
        offline_sources_configured=offline_sources_configured,
        offline_sources_available=offline_sources_available,
        offline_sources_unavailable_reason=offline_sources_unavailable_reason,
    )


def _enabled_evidence_sources(
    *,
    dbsnp_enabled: bool,
    clinvar_enabled: bool,
    gnomad_enabled: bool,
) -> dict[str, bool]:
    return {
        "dbsnp": bool(dbsnp_enabled),
        "clinvar": bool(clinvar_enabled),
        "gnomad": bool(gnomad_enabled),
    }


def _local_vcf_path(env_name: str) -> str | None:
    text = (os.environ.get(env_name) or "").strip()
    return text or None


def _get_variant_consequence_categories(conn, run_id: str) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT variant_id, consequence_category
        FROM run_classifications
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    result: dict[str, str] = {}
    for row in rows:
        variant_id = str(row[0] or "").strip()
        category = str(row[1] or "").strip().lower()
        if not variant_id:
            continue
        result[variant_id] = category
    return result


def _is_variant_in_evidence_scope(
    *,
    evidence_profile: str,
    variant_id: str | None,
    categories_by_variant: dict[str, str],
) -> bool:
    if evidence_profile != _ENFORCED_EVIDENCE_PROFILE:
        evidence_profile = _ENFORCED_EVIDENCE_PROFILE
    key = str(variant_id or "").strip()
    if not key:
        return False
    category = categories_by_variant.get(key)
    if category is None:
        # Evidence is missense-only; require classification to be present.
        return False
    if evidence_profile == "predictor_only":
        return category in _PREDICTOR_PROFILE_ALLOWED_CATEGORIES
    return False


def _fetch_dbsnp_evidence(
    config: DbsnpConfig,
    *,
    evidence_mode: str,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    if evidence_mode == "online":
        return fetch_dbsnp_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)

    if local_vcf_path:
        local_result = fetch_dbsnp_evidence_from_local_vcf(
            local_vcf_path=local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=config.timeout_seconds,
        )
        if evidence_mode == "offline" or local_result.get("outcome") in {"found", "not_found"}:
            return local_result
        local_details = dict(local_result.get("details") or {})
        fallback = fetch_dbsnp_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)
        merged_details = dict(fallback.get("details") or {})
        merged_details["local_attempt"] = {
            "reason_code": local_result.get("reason_code"),
            "reason_message": local_result.get("reason_message"),
            "details": local_details,
        }
        fallback["details"] = merged_details
        return fallback

    if evidence_mode == "offline":
        return {
            "outcome": "error",
            "rsid": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local dbSNP VCF path is not configured.",
            "details": {"env_var": "SP_DBSNP_LOCAL_VCF_PATH"},
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "retry_attempts": 0,
        }
    return fetch_dbsnp_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)


def _fetch_clinvar_evidence(
    config: ClinvarConfig,
    *,
    evidence_mode: str,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    if evidence_mode == "online":
        return fetch_clinvar_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)

    if local_vcf_path:
        local_result = fetch_clinvar_evidence_from_local_vcf(
            local_vcf_path=local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=config.timeout_seconds,
        )
        if evidence_mode == "offline" or local_result.get("outcome") in {"found", "not_found"}:
            return local_result
        local_details = dict(local_result.get("details") or {})
        fallback = fetch_clinvar_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)
        merged_details = dict(fallback.get("details") or {})
        merged_details["local_attempt"] = {
            "reason_code": local_result.get("reason_code"),
            "reason_message": local_result.get("reason_message"),
            "details": local_details,
        }
        fallback["details"] = merged_details
        return fallback

    if evidence_mode == "offline":
        return {
            "outcome": "error",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local ClinVar VCF path is not configured.",
            "details": {"env_var": "SP_CLINVAR_LOCAL_VCF_PATH"},
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "retry_attempts": 0,
        }
    return fetch_clinvar_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)


def _fetch_gnomad_evidence(
    config: GnomadConfig,
    *,
    evidence_mode: str,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    if evidence_mode == "online":
        return fetch_gnomad_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)

    if local_vcf_path:
        local_result = fetch_gnomad_evidence_from_local_vcf(
            local_vcf_path=local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=config.timeout_seconds,
        )
        if evidence_mode == "offline" or local_result.get("outcome") in {"found", "not_found"}:
            return local_result
        local_details = dict(local_result.get("details") or {})
        fallback = fetch_gnomad_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)
        merged_details = dict(fallback.get("details") or {})
        merged_details["local_attempt"] = {
            "reason_code": local_result.get("reason_code"),
            "reason_message": local_result.get("reason_message"),
            "details": local_details,
        }
        fallback["details"] = merged_details
        return fallback

    if evidence_mode == "offline":
        return {
            "outcome": "error",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local gnomAD VCF path is not configured.",
            "details": {"env_var": "SP_GNOMAD_LOCAL_VCF_PATH"},
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "retry_attempts": 0,
        }
    return fetch_gnomad_evidence_for_variant(config, chrom=chrom, pos=pos, ref=ref, alt=alt)


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


def _variant_key(variant: dict) -> str:
    return f"{variant.get('chrom')}:{variant.get('pos')}:{variant.get('ref')}>{variant.get('alt')}"


def _cancel_annotation_and_raise(
    db_path: str,
    conn,
    run_id: str,
    *,
    uploaded_at: str,
) -> None:
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE runs SET status = ? WHERE run_id = ? AND status IN (?, ?)",
        ("canceled", run_id, "queued", "running"),
    )
    clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
    clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
    clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
    mark_stage_canceled(
        db_path,
        run_id,
        "annotation",
        input_uploaded_at=uploaded_at,
        conn=conn,
        commit=False,
    )
    conn.commit()
    clear_run_cancel_request(run_id)
    raise StageExecutionError(409, "RUN_CANCELED", "Run was canceled.")


def _ensure_annotation_not_canceled(
    db_path: str,
    conn,
    run_id: str,
    *,
    uploaded_at: str,
) -> None:
    if is_run_cancel_requested(run_id):
        _cancel_annotation_and_raise(
            db_path,
            conn,
            run_id,
            uploaded_at=uploaded_at,
        )
    if _get_run_status(conn, run_id) == "canceled":
        _cancel_annotation_and_raise(
            db_path,
            conn,
            run_id,
            uploaded_at=uploaded_at,
        )


def run_annotation_stage(
    db_path: str,
    run_id: str,
    *,
    uploaded_at: str,
    logger,
    force: bool = False,
    evidence_failure_policy: str | None = None,
) -> dict:
    created_at = datetime.now(timezone.utc).isoformat()
    stats: dict = {"tool": "snpeff", "impl_version": "2026-03-10-r9"}
    annotation_evidence_policy = _resolve_annotation_evidence_policy(evidence_failure_policy)
    fail_on_evidence_error = _annotation_fail_on_evidence_error(annotation_evidence_policy)
    stats["annotation_evidence_policy"] = annotation_evidence_policy
    stats["fail_on_evidence_error"] = bool(fail_on_evidence_error)
    failed_sources: list[str] = []

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
                clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
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
                clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
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

            conn.execute("BEGIN IMMEDIATE")
            clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
            conn.commit()

            config = _snpeff_config(reference_build)
            stats["snpeff_enabled"] = bool(config.get("enabled"))
            stats["reference_build"] = reference_build
            stats["genome"] = config.get("genome")

            if config.get("enabled"):
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
                data_dir_arg = data_dir_raw or "./data"

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
                stats["snpeff_configured"] = True
                stats["workdir"] = workdir
                stats["data_dir"] = data_dir_fs
                stats["data_dir_arg"] = data_dir_arg
                stats["timeout_seconds"] = timeout_seconds

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
                            "impl_version": "2026-03-09-r6",
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
                            "impl_version": "2026-03-09-r6",
                        },
                        conn=conn,
                        commit=False,
                    )
                    conn.commit()
                    raise StageExecutionError(500, "SNPEFF_FAILED", "SnpEff execution failed.")
            else:
                stats["snpeff_note"] = "SnpEff is disabled (SP_SNPEFF_ENABLED=0)."

            dbsnp_config = _dbsnp_config(reference_build)
            clinvar_config = _clinvar_config()
            gnomad_config = _gnomad_config()
            stats["dbsnp_enabled"] = bool(dbsnp_config.enabled)
            stats["dbsnp_timeout_seconds"] = int(dbsnp_config.timeout_seconds)
            stats["dbsnp_retry_max_attempts"] = int(dbsnp_config.retry_max_attempts)
            stats["dbsnp_retry_backoff_base_seconds"] = float(dbsnp_config.retry_backoff_base_seconds)
            stats["dbsnp_retry_backoff_max_seconds"] = float(dbsnp_config.retry_backoff_max_seconds)
            stats["dbsnp_assembly"] = str(dbsnp_config.assembly)
            stats["clinvar_enabled"] = bool(clinvar_config.enabled)
            stats["clinvar_timeout_seconds"] = int(clinvar_config.timeout_seconds)
            stats["clinvar_retry_max_attempts"] = int(clinvar_config.retry_max_attempts)
            stats["clinvar_retry_backoff_base_seconds"] = float(clinvar_config.retry_backoff_base_seconds)
            stats["clinvar_retry_backoff_max_seconds"] = float(clinvar_config.retry_backoff_max_seconds)
            stats["gnomad_enabled"] = bool(gnomad_config.enabled)
            stats["gnomad_timeout_seconds"] = int(gnomad_config.timeout_seconds)
            stats["gnomad_retry_max_attempts"] = int(gnomad_config.retry_max_attempts)
            stats["gnomad_retry_backoff_base_seconds"] = float(gnomad_config.retry_backoff_base_seconds)
            stats["gnomad_retry_backoff_max_seconds"] = float(gnomad_config.retry_backoff_max_seconds)
            stats["gnomad_min_request_interval_seconds"] = float(gnomad_config.min_request_interval_seconds)
            stats["gnomad_dataset_id"] = gnomad_config.dataset_id
            stats["gnomad_reference_genome"] = gnomad_config.reference_genome

            variants = list(iter_variants_for_run_with_ids(db_path, run_id, conn=conn))
            evidence_profile = _resolve_evidence_profile()
            evidence_mode_requested = _resolve_evidence_mode()
            dbsnp_local_vcf_path = _local_vcf_path("SP_DBSNP_LOCAL_VCF_PATH")
            clinvar_local_vcf_path = _local_vcf_path("SP_CLINVAR_LOCAL_VCF_PATH")
            gnomad_local_vcf_path = _local_vcf_path("SP_GNOMAD_LOCAL_VCF_PATH")
            mode_decision = _detect_evidence_mode_decision(
                requested_mode=evidence_mode_requested,
                dbsnp_local_vcf_path=dbsnp_local_vcf_path,
                clinvar_local_vcf_path=clinvar_local_vcf_path,
                gnomad_local_vcf_path=gnomad_local_vcf_path,
                dbsnp_enabled=bool(dbsnp_config.enabled),
                clinvar_enabled=bool(clinvar_config.enabled),
                gnomad_enabled=bool(gnomad_config.enabled),
            )
            evidence_mode = str(mode_decision.get("effective_mode") or evidence_mode_requested)
            categories_by_variant = _get_variant_consequence_categories(conn, run_id)
            stats["evidence_profile"] = evidence_profile
            stats["evidence_mode"] = evidence_mode
            stats["evidence_mode_requested"] = mode_decision.get("requested_mode")
            stats["evidence_mode_effective"] = mode_decision.get("effective_mode")
            stats["evidence_online_available"] = bool(mode_decision.get("online_available"))
            stats["evidence_offline_sources_configured"] = mode_decision.get("offline_sources_configured") or {}
            stats["evidence_offline_sources_available"] = mode_decision.get("offline_sources_available") or {}
            stats["evidence_offline_sources_unavailable_reason"] = (
                mode_decision.get("offline_sources_unavailable_reason") or {}
            )
            stats["evidence_mode_decision_reason"] = mode_decision.get("decision_reason")
            stats["evidence_mode_detected_at"] = mode_decision.get("detected_at")
            offline_sources = mode_decision.get("offline_sources_configured") or {}
            offline_available_sources = mode_decision.get("offline_sources_available") or {}
            stats["dbsnp_local_vcf_configured"] = bool(offline_sources.get("dbsnp"))
            stats["clinvar_local_vcf_configured"] = bool(offline_sources.get("clinvar"))
            stats["gnomad_local_vcf_configured"] = bool(offline_sources.get("gnomad"))
            stats["dbsnp_local_vcf_available"] = bool(offline_available_sources.get("dbsnp"))
            stats["clinvar_local_vcf_available"] = bool(offline_available_sources.get("clinvar"))
            stats["gnomad_local_vcf_available"] = bool(offline_available_sources.get("gnomad"))
            conn.execute("BEGIN IMMEDIATE")
            update_run_evidence_mode_decision(
                db_path,
                run_id,
                requested_mode=str(mode_decision.get("requested_mode") or evidence_mode_requested),
                effective_mode=str(mode_decision.get("effective_mode") or evidence_mode),
                online_available=bool(mode_decision.get("online_available")),
                offline_sources_configured=mode_decision.get("offline_sources_configured") or {},
                decision_reason=str(mode_decision.get("decision_reason") or ""),
                detected_at=mode_decision.get("detected_at"),
                conn=conn,
                commit=False,
            )
            conn.commit()
            enabled_sources = _enabled_evidence_sources(
                dbsnp_enabled=bool(dbsnp_config.enabled),
                clinvar_enabled=bool(clinvar_config.enabled),
                gnomad_enabled=bool(gnomad_config.enabled),
            )
            required_sources = [source for source in _EVIDENCE_SOURCES if enabled_sources.get(source)]
            stats["evidence_sources_enabled"] = enabled_sources
            stats["evidence_required_sources"] = required_sources

            decision_reason = str(mode_decision.get("decision_reason") or "")
            online_available = bool(mode_decision.get("online_available"))
            no_offline_available_for_required = bool(required_sources) and all(
                not bool(offline_available_sources.get(source)) for source in required_sources
            )
            no_valid_sources_for_required = (
                bool(required_sources)
                and (
                    decision_reason in _NO_VALID_SOURCE_REASONS
                    or (not online_available and no_offline_available_for_required)
                )
            )
            if no_valid_sources_for_required:
                missing_sources = [
                    source
                    for source in required_sources
                    if (not online_available) and (not bool(offline_available_sources.get(source)))
                ]
                source_completeness = {}
                source_completeness_reason = {}
                for source in _EVIDENCE_SOURCES:
                    source_completeness[source] = "unavailable"
                    source_completeness_reason[source] = (
                        "disabled" if not enabled_sources.get(source) else "no_valid_source"
                    )

                env_var_by_source = {
                    "dbsnp": "SP_DBSNP_LOCAL_VCF_PATH",
                    "clinvar": "SP_CLINVAR_LOCAL_VCF_PATH",
                    "gnomad": "SP_GNOMAD_LOCAL_VCF_PATH",
                }
                local_path_env_vars = [env_var_by_source[source] for source in missing_sources]
                details = {
                    "requested_mode": mode_decision.get("requested_mode"),
                    "effective_mode": mode_decision.get("effective_mode"),
                    "decision_reason": decision_reason or "no_valid_source",
                    "online_available": online_available,
                    "offline_sources_configured": stats.get("evidence_offline_sources_configured") or {},
                    "offline_sources_available": stats.get("evidence_offline_sources_available") or {},
                    "offline_sources_unavailable_reason": (
                        stats.get("evidence_offline_sources_unavailable_reason") or {}
                    ),
                    "enabled_sources": enabled_sources,
                    "required_sources": required_sources,
                    "missing_sources": missing_sources,
                    "missing_source_env_vars": local_path_env_vars,
                    "missing_outputs": [*missing_sources, "reporting"],
                    "blocked_outputs": ["annotation", "reporting"],
                    "failed_source": "all_evidence_sources_unavailable",
                    "annotation_evidence_policy": annotation_evidence_policy,
                    "annotation_evidence_completeness": "unavailable",
                    "evidence_source_completeness": source_completeness,
                    "evidence_source_completeness_reason": source_completeness_reason,
                    "hint": (
                        "Neither online evidence APIs nor offline local evidence sources are available. "
                        "Restore connectivity or configure indexed local VCF paths "
                        f"({', '.join(local_path_env_vars) if local_path_env_vars else 'for enabled sources'}) and retry."
                    ),
                }
                stats["evidence_failed_sources"] = missing_sources
                stats["strict_block_reason"] = "no_valid_evidence_sources"
                stats["strict_missing_sources"] = missing_sources
                stats["strict_missing_source_env_vars"] = local_path_env_vars
                stats["strict_blocked_outputs"] = details["blocked_outputs"]
                stats["annotation_evidence_completeness"] = "unavailable"
                stats["evidence_source_completeness"] = source_completeness
                stats["evidence_source_completeness_reason"] = source_completeness_reason

                conn.execute("BEGIN IMMEDIATE")
                clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                mark_stage_failed(
                    db_path,
                    run_id,
                    "annotation",
                    input_uploaded_at=uploaded_at,
                    error_code="EVIDENCE_SOURCES_UNAVAILABLE",
                    error_message=(
                        "Neither online nor offline evidence sources are available for annotation."
                    ),
                    error_details=details,
                    stats=stats,
                    conn=conn,
                    commit=False,
                )
                conn.commit()
                raise StageExecutionError(
                    503,
                    "EVIDENCE_SOURCES_UNAVAILABLE",
                    "Neither online nor offline evidence sources are available for annotation.",
                    details=details,
                )
            if evidence_profile == "minimum_exome":
                stats["evidence_profile_scope"] = "coding_only"
                stats["evidence_profile_allowed_categories"] = sorted(
                    _EXOME_PROFILE_ALLOWED_CATEGORIES
                )
            elif evidence_profile == "predictor_only":
                stats["evidence_profile_scope"] = "predictor_routed_only"
                stats["evidence_profile_allowed_categories"] = sorted(
                    _PREDICTOR_PROFILE_ALLOWED_CATEGORIES
                )
            stats["classified_variants_available"] = len(categories_by_variant)
            stats["dbsnp_variants_processed"] = len(variants)
            dbsnp_rows: list[dict] = []
            dbsnp_errors: list[dict] = []
            dbsnp_error_reason_counts: dict[str, int] = {}
            dbsnp_error_http_status_counts: dict[str, int] = {}
            dbsnp_retry_attempts_total = 0
            dbsnp_found_count = 0
            dbsnp_not_found_count = 0
            dbsnp_skipped_out_of_scope = 0

            if dbsnp_config.enabled:
                dbsnp_max_workers = _max_workers_env("SP_DBSNP_MAX_WORKERS", 1)

                def _fetch_dbsnp_variant(variant: dict) -> dict:
                    return _fetch_dbsnp_evidence(
                        dbsnp_config,
                        evidence_mode=evidence_mode,
                        local_vcf_path=dbsnp_local_vcf_path,
                        chrom=str(variant.get("chrom") or ""),
                        pos=int(variant.get("pos") or 0),
                        ref=str(variant.get("ref") or ""),
                        alt=str(variant.get("alt") or ""),
                    )

                dbsnp_results, dbsnp_skipped_out_of_scope = _collect_evidence_results(
                    db_path=db_path,
                    conn=conn,
                    run_id=run_id,
                    uploaded_at=uploaded_at,
                    variants=variants,
                    evidence_profile=evidence_profile,
                    categories_by_variant=categories_by_variant,
                    max_workers=dbsnp_max_workers,
                    fetch_fn=_fetch_dbsnp_variant,
                )

                stats["dbsnp_max_workers"] = dbsnp_max_workers

                for variant, result in dbsnp_results:
                    retries_used = int(result.get("retry_attempts") or 0)
                    dbsnp_retry_attempts_total += retries_used
                    outcome = str(result.get("outcome") or "error")
                    if outcome == "found":
                        dbsnp_found_count += 1
                    elif outcome == "not_found":
                        dbsnp_not_found_count += 1
                    else:
                        reason_code = str(result.get("reason_code") or "UNKNOWN_ERROR")
                        dbsnp_error_reason_counts[reason_code] = dbsnp_error_reason_counts.get(reason_code, 0) + 1
                        result_details = result.get("details") or {}
                        status_code = result_details.get("status_code")
                        if status_code is not None:
                            status_key = str(status_code)
                            dbsnp_error_http_status_counts[status_key] = (
                                dbsnp_error_http_status_counts.get(status_key, 0) + 1
                            )
                        dbsnp_errors.append(
                            {
                                "variant_id": variant.get("variant_id"),
                                "variant_key": _variant_key(variant),
                                "reason_code": reason_code,
                                "reason_message": result.get("reason_message"),
                                "details": result_details,
                            }
                        )

                    dbsnp_rows.append(
                        {
                            "variant_id": variant["variant_id"],
                            "source": "dbsnp",
                            "outcome": outcome,
                            "rsid": result.get("rsid"),
                            "reason_code": result.get("reason_code"),
                            "reason_message": result.get("reason_message"),
                            "details": result.get("details") or {},
                            "retrieved_at": result.get("retrieved_at") or created_at,
                        }
                    )

                stats["dbsnp_retry_attempts"] = dbsnp_retry_attempts_total
                stats["dbsnp_found"] = dbsnp_found_count
                stats["dbsnp_not_found"] = dbsnp_not_found_count
                stats["dbsnp_errors"] = len(dbsnp_errors)
                stats["dbsnp_error_reason_counts"] = dbsnp_error_reason_counts
                stats["dbsnp_error_http_status_counts"] = dbsnp_error_http_status_counts
                stats["dbsnp_variants_eligible"] = len(variants) - dbsnp_skipped_out_of_scope
                stats["dbsnp_skipped_out_of_scope"] = dbsnp_skipped_out_of_scope

                if dbsnp_errors:
                    failed_sources.append("dbsnp")
                    hint = "Check dbSNP network connectivity/API availability and retry settings."
                    if evidence_mode == "offline":
                        hint = "Check local dbSNP VCF path/index configuration and tabix availability."
                    elif evidence_mode == "hybrid":
                        hint = (
                            "Check local dbSNP VCF path/index configuration and tabix availability; "
                            "also verify network/API connectivity for hybrid fallback."
                        )
                    details = {
                        "errors": dbsnp_errors[:10],
                        "error_count": len(dbsnp_errors),
                        "timeout_seconds": int(dbsnp_config.timeout_seconds),
                        "retry_max_attempts": int(dbsnp_config.retry_max_attempts),
                        "retry_attempts_total": dbsnp_retry_attempts_total,
                        "hint": hint,
                    }
                    stats["dbsnp_error_details"] = details
                    stats["dbsnp_warning"] = "dbSNP retrieval had one or more errors."
                    if fail_on_evidence_error:
                        details = _evidence_failure_details(
                            details,
                            failed_source="dbsnp",
                            policy=annotation_evidence_policy,
                            processed_source_states={},
                        )
                        conn.execute("BEGIN IMMEDIATE")
                        clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        mark_stage_failed(
                            db_path,
                            run_id,
                            "annotation",
                            input_uploaded_at=uploaded_at,
                            error_code="DBSNP_RETRIEVAL_FAILED",
                            error_message="dbSNP retrieval failed for one or more variants.",
                            error_details=details,
                            conn=conn,
                            commit=False,
                        )
                        conn.commit()
                        raise StageExecutionError(
                            500,
                            "DBSNP_RETRIEVAL_FAILED",
                            "dbSNP retrieval failed for one or more variants.",
                            details=details,
                        )

                conn.execute("BEGIN IMMEDIATE")
                _ensure_annotation_not_canceled(
                    db_path,
                    conn,
                    run_id,
                    uploaded_at=uploaded_at,
                )
                upsert_dbsnp_evidence_for_run(db_path, run_id, dbsnp_rows, conn=conn, commit=False)
                conn.commit()
            else:
                stats["dbsnp_note"] = "dbSNP retrieval is disabled (SP_DBSNP_ENABLED=0)."
                stats["dbsnp_retry_attempts"] = 0
                stats["dbsnp_found"] = 0
                stats["dbsnp_not_found"] = 0
                stats["dbsnp_errors"] = 0
                stats["dbsnp_error_reason_counts"] = {}
                stats["dbsnp_error_http_status_counts"] = {}
                stats["dbsnp_variants_eligible"] = 0
                stats["dbsnp_skipped_out_of_scope"] = 0

            stats["clinvar_variants_processed"] = len(variants)

            clinvar_rows: list[dict] = []
            clinvar_errors: list[dict] = []
            clinvar_error_reason_counts: dict[str, int] = {}
            clinvar_error_http_status_counts: dict[str, int] = {}
            clinvar_retry_attempts_total = 0
            clinvar_found_count = 0
            clinvar_not_found_count = 0
            clinvar_skipped_out_of_scope = 0

            if clinvar_config.enabled:
                clinvar_max_workers = _max_workers_env("SP_CLINVAR_MAX_WORKERS", 1)

                def _fetch_clinvar_variant(variant: dict) -> dict:
                    return _fetch_clinvar_evidence(
                        clinvar_config,
                        evidence_mode=evidence_mode,
                        local_vcf_path=clinvar_local_vcf_path,
                        chrom=str(variant.get("chrom") or ""),
                        pos=int(variant.get("pos") or 0),
                        ref=str(variant.get("ref") or ""),
                        alt=str(variant.get("alt") or ""),
                    )

                clinvar_results, clinvar_skipped_out_of_scope = _collect_evidence_results(
                    db_path=db_path,
                    conn=conn,
                    run_id=run_id,
                    uploaded_at=uploaded_at,
                    variants=variants,
                    evidence_profile=evidence_profile,
                    categories_by_variant=categories_by_variant,
                    max_workers=clinvar_max_workers,
                    fetch_fn=_fetch_clinvar_variant,
                )

                stats["clinvar_max_workers"] = clinvar_max_workers

                for variant, result in clinvar_results:
                    retries_used = int(result.get("retry_attempts") or 0)
                    clinvar_retry_attempts_total += retries_used
                    outcome = str(result.get("outcome") or "error")
                    if outcome == "found":
                        clinvar_found_count += 1
                    elif outcome == "not_found":
                        clinvar_not_found_count += 1
                    else:
                        reason_code = str(result.get("reason_code") or "UNKNOWN_ERROR")
                        clinvar_error_reason_counts[reason_code] = clinvar_error_reason_counts.get(reason_code, 0) + 1
                        result_details = result.get("details") or {}
                        status_code = result_details.get("status_code")
                        if status_code is not None:
                            status_key = str(status_code)
                            clinvar_error_http_status_counts[status_key] = (
                                clinvar_error_http_status_counts.get(status_key, 0) + 1
                            )
                        clinvar_errors.append(
                            {
                                "variant_id": variant.get("variant_id"),
                                "variant_key": _variant_key(variant),
                                "reason_code": reason_code,
                                "reason_message": result.get("reason_message"),
                                "details": result_details,
                            }
                        )

                    clinvar_rows.append(
                        {
                            "variant_id": variant["variant_id"],
                            "source": "clinvar",
                            "outcome": outcome,
                            "clinvar_id": result.get("clinvar_id"),
                            "clinical_significance": result.get("clinical_significance"),
                            "reason_code": result.get("reason_code"),
                            "reason_message": result.get("reason_message"),
                            "details": result.get("details") or {},
                            "retrieved_at": result.get("retrieved_at") or created_at,
                        }
                    )

                stats["clinvar_retry_attempts"] = clinvar_retry_attempts_total
                stats["clinvar_found"] = clinvar_found_count
                stats["clinvar_not_found"] = clinvar_not_found_count
                stats["clinvar_errors"] = len(clinvar_errors)
                stats["clinvar_error_reason_counts"] = clinvar_error_reason_counts
                stats["clinvar_error_http_status_counts"] = clinvar_error_http_status_counts
                stats["clinvar_variants_eligible"] = len(variants) - clinvar_skipped_out_of_scope
                stats["clinvar_skipped_out_of_scope"] = clinvar_skipped_out_of_scope

                if clinvar_errors:
                    failed_sources.append("clinvar")
                    hint = "Check ClinVar network connectivity/API availability and retry settings."
                    if evidence_mode == "offline":
                        hint = "Check local ClinVar VCF path/index configuration and tabix availability."
                    elif evidence_mode == "hybrid":
                        hint = (
                            "Check local ClinVar VCF path/index configuration and tabix availability; "
                            "also verify network/API connectivity for hybrid fallback."
                        )
                    details = {
                        "errors": clinvar_errors[:10],
                        "error_count": len(clinvar_errors),
                        "timeout_seconds": int(clinvar_config.timeout_seconds),
                        "retry_max_attempts": int(clinvar_config.retry_max_attempts),
                        "retry_attempts_total": clinvar_retry_attempts_total,
                        "hint": hint,
                    }
                    stats["clinvar_error_details"] = details
                    stats["clinvar_warning"] = "ClinVar retrieval had one or more errors."
                    if fail_on_evidence_error:
                        processed_source_states = {
                            "dbsnp": _source_completeness_from_stats(stats, "dbsnp"),
                        }
                        details = _evidence_failure_details(
                            details,
                            failed_source="clinvar",
                            policy=annotation_evidence_policy,
                            processed_source_states=processed_source_states,
                        )
                        conn.execute("BEGIN IMMEDIATE")
                        clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        mark_stage_failed(
                            db_path,
                            run_id,
                            "annotation",
                            input_uploaded_at=uploaded_at,
                            error_code="CLINVAR_RETRIEVAL_FAILED",
                            error_message="ClinVar retrieval failed for one or more variants.",
                            error_details=details,
                            conn=conn,
                            commit=False,
                        )
                        conn.commit()
                        raise StageExecutionError(
                            500,
                            "CLINVAR_RETRIEVAL_FAILED",
                            "ClinVar retrieval failed for one or more variants.",
                            details=details,
                        )

                conn.execute("BEGIN IMMEDIATE")
                _ensure_annotation_not_canceled(
                    db_path,
                    conn,
                    run_id,
                    uploaded_at=uploaded_at,
                )
                upsert_clinvar_evidence_for_run(db_path, run_id, clinvar_rows, conn=conn, commit=False)
                conn.commit()
            else:
                stats["clinvar_note"] = "ClinVar retrieval is disabled (SP_CLINVAR_ENABLED=0)."
                stats["clinvar_retry_attempts"] = 0
                stats["clinvar_found"] = 0
                stats["clinvar_not_found"] = 0
                stats["clinvar_errors"] = 0
                stats["clinvar_error_reason_counts"] = {}
                stats["clinvar_error_http_status_counts"] = {}
                stats["clinvar_variants_eligible"] = 0
                stats["clinvar_skipped_out_of_scope"] = 0

            stats["gnomad_variants_processed"] = len(variants)

            gnomad_errors: list[dict] = []
            gnomad_rows: list[dict] = []
            gnomad_error_reason_counts: dict[str, int] = {}
            gnomad_error_http_status_counts: dict[str, int] = {}
            gnomad_retry_attempts_total = 0
            gnomad_found_count = 0
            gnomad_not_found_count = 0
            gnomad_skipped_out_of_scope = 0

            if gnomad_config.enabled:
                gnomad_max_workers = _max_workers_env("SP_GNOMAD_MAX_WORKERS", 1)

                def _fetch_gnomad_variant(variant: dict) -> dict:
                    return _fetch_gnomad_evidence(
                        gnomad_config,
                        evidence_mode=evidence_mode,
                        local_vcf_path=gnomad_local_vcf_path,
                        chrom=str(variant.get("chrom") or ""),
                        pos=int(variant.get("pos") or 0),
                        ref=str(variant.get("ref") or ""),
                        alt=str(variant.get("alt") or ""),
                    )

                gnomad_results, gnomad_skipped_out_of_scope = _collect_evidence_results(
                    db_path=db_path,
                    conn=conn,
                    run_id=run_id,
                    uploaded_at=uploaded_at,
                    variants=variants,
                    evidence_profile=evidence_profile,
                    categories_by_variant=categories_by_variant,
                    max_workers=gnomad_max_workers,
                    fetch_fn=_fetch_gnomad_variant,
                )

                stats["gnomad_max_workers"] = gnomad_max_workers

                for variant, result in gnomad_results:
                    retries_used = int(result.get("retry_attempts") or 0)
                    gnomad_retry_attempts_total += retries_used
                    outcome = str(result.get("outcome") or "error")
                    if outcome == "found":
                        gnomad_found_count += 1
                    elif outcome == "not_found":
                        gnomad_not_found_count += 1
                    else:
                        reason_code = str(result.get("reason_code") or "UNKNOWN_ERROR")
                        gnomad_error_reason_counts[reason_code] = gnomad_error_reason_counts.get(reason_code, 0) + 1
                        result_details = result.get("details") or {}
                        status_code = result_details.get("status_code")
                        if status_code is not None:
                            status_key = str(status_code)
                            gnomad_error_http_status_counts[status_key] = (
                                gnomad_error_http_status_counts.get(status_key, 0) + 1
                            )
                        gnomad_errors.append(
                            {
                                "variant_id": variant.get("variant_id"),
                                "variant_key": _variant_key(variant),
                                "reason_code": reason_code,
                                "reason_message": result.get("reason_message"),
                                "details": result_details,
                            }
                        )

                    gnomad_rows.append(
                        {
                            "variant_id": variant["variant_id"],
                            "source": "gnomad",
                            "outcome": outcome,
                            "gnomad_variant_id": result.get("gnomad_variant_id"),
                            "global_af": result.get("global_af"),
                            "reason_code": result.get("reason_code"),
                            "reason_message": result.get("reason_message"),
                            "details": result.get("details") or {},
                            "retrieved_at": result.get("retrieved_at") or created_at,
                        }
                    )

                stats["gnomad_retry_attempts"] = gnomad_retry_attempts_total
                stats["gnomad_found"] = gnomad_found_count
                stats["gnomad_not_found"] = gnomad_not_found_count
                stats["gnomad_errors"] = len(gnomad_errors)
                stats["gnomad_error_reason_counts"] = gnomad_error_reason_counts
                stats["gnomad_error_http_status_counts"] = gnomad_error_http_status_counts
                stats["gnomad_variants_eligible"] = len(variants) - gnomad_skipped_out_of_scope
                stats["gnomad_skipped_out_of_scope"] = gnomad_skipped_out_of_scope

                if gnomad_errors:
                    failed_sources.append("gnomad")
                    gnomad_reason_codes = set(gnomad_error_reason_counts.keys())
                    hint = "Check gnomAD network connectivity/API availability and retry settings."
                    if evidence_mode == "offline":
                        hint = "Check local gnomAD VCF path/index configuration and tabix availability."
                    elif evidence_mode == "hybrid":
                        if gnomad_local_vcf_path:
                            hint = (
                                "Check local gnomAD VCF path/index configuration and tabix availability; "
                                "also verify network/API connectivity for hybrid fallback."
                            )
                        else:
                            hint = (
                                "gnomAD local source is not configured in hybrid mode; "
                                "current lookup is online. Verify gnomAD API connectivity/retry settings."
                            )
                    elif gnomad_reason_codes and gnomad_reason_codes.issubset(
                        {"GRAPHQL_ERROR", "GRAPHQL_SCHEMA_ERROR"}
                    ):
                        hint = (
                            "Check gnomAD GraphQL query compatibility (dataset/schema), "
                            "then verify API connectivity and retry settings."
                        )
                    details = {
                        "errors": gnomad_errors[:10],
                        "error_count": len(gnomad_errors),
                        "timeout_seconds": int(gnomad_config.timeout_seconds),
                        "retry_max_attempts": int(gnomad_config.retry_max_attempts),
                        "retry_attempts_total": gnomad_retry_attempts_total,
                        "hint": hint,
                    }
                    stats["gnomad_error_details"] = details
                    stats["gnomad_warning"] = "gnomAD retrieval had one or more errors."
                    if fail_on_evidence_error:
                        processed_source_states = {
                            "dbsnp": _source_completeness_from_stats(stats, "dbsnp"),
                            "clinvar": _source_completeness_from_stats(stats, "clinvar"),
                        }
                        details = _evidence_failure_details(
                            details,
                            failed_source="gnomad",
                            policy=annotation_evidence_policy,
                            processed_source_states=processed_source_states,
                        )
                        conn.execute("BEGIN IMMEDIATE")
                        clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                        mark_stage_failed(
                            db_path,
                            run_id,
                            "annotation",
                            input_uploaded_at=uploaded_at,
                            error_code="GNOMAD_RETRIEVAL_FAILED",
                            error_message="gnomAD retrieval failed for one or more variants.",
                            error_details=details,
                            conn=conn,
                            commit=False,
                        )
                        conn.commit()
                        raise StageExecutionError(
                            500,
                            "GNOMAD_RETRIEVAL_FAILED",
                            "gnomAD retrieval failed for one or more variants.",
                            details=details,
                        )

                conn.execute("BEGIN IMMEDIATE")
                _ensure_annotation_not_canceled(
                    db_path,
                    conn,
                    run_id,
                    uploaded_at=uploaded_at,
                )
                upsert_gnomad_evidence_for_run(db_path, run_id, gnomad_rows, conn=conn, commit=False)
                conn.commit()
            else:
                stats["gnomad_note"] = "gnomAD retrieval is disabled (SP_GNOMAD_ENABLED=0)."
                stats["gnomad_retry_attempts"] = 0
                stats["gnomad_found"] = 0
                stats["gnomad_not_found"] = 0
                stats["gnomad_errors"] = 0
                stats["gnomad_error_reason_counts"] = {}
                stats["gnomad_error_http_status_counts"] = {}
                stats["gnomad_variants_eligible"] = 0
                stats["gnomad_skipped_out_of_scope"] = 0

            stats["evidence_failed_sources"] = sorted(set(failed_sources))
            _augment_stats_with_evidence_completeness(stats)
            conn.execute("BEGIN IMMEDIATE")
            _ensure_annotation_not_canceled(
                db_path,
                conn,
                run_id,
                uploaded_at=uploaded_at,
            )
            mark_stage_succeeded(
                db_path,
                run_id,
                "annotation",
                input_uploaded_at=uploaded_at,
                stats=stats,
                conn=conn,
                commit=False,
            )
            if is_run_cancel_requested(run_id):
                conn.rollback()
                _cancel_annotation_and_raise(
                    db_path,
                    conn,
                    run_id,
                    uploaded_at=uploaded_at,
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
                clear_dbsnp_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_clinvar_evidence_for_run(db_path, run_id, conn=conn, commit=False)
                clear_gnomad_evidence_for_run(db_path, run_id, conn=conn, commit=False)
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
                clear_run_cancel_request(run_id)
            finally:
                conn.close()
        except Exception:
            logger.exception("Failed to persist annotation failure state")
        raise StageExecutionError(500, "ANNOTATION_FAILED", "Annotation stage failed.") from None

    return {"annotation": {"status": "succeeded", "stats": stats}}
