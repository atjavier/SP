from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timezone
from functools import lru_cache
from glob import glob


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalized_chrom(chrom: str) -> str:
    text = (chrom or "").strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    return text


_REFSEQ_CHROM_CANDIDATES: dict[str, tuple[str, ...]] = {
    "1": ("NC_000001.11", "NC_000001.10"),
    "2": ("NC_000002.12", "NC_000002.11"),
    "3": ("NC_000003.12", "NC_000003.11"),
    "4": ("NC_000004.12", "NC_000004.11"),
    "5": ("NC_000005.10", "NC_000005.9"),
    "6": ("NC_000006.12", "NC_000006.11"),
    "7": ("NC_000007.14", "NC_000007.13"),
    "8": ("NC_000008.11", "NC_000008.10"),
    "9": ("NC_000009.12", "NC_000009.11"),
    "10": ("NC_000010.11", "NC_000010.10"),
    "11": ("NC_000011.10", "NC_000011.9"),
    "12": ("NC_000012.12", "NC_000012.11"),
    "13": ("NC_000013.11", "NC_000013.10"),
    "14": ("NC_000014.9", "NC_000014.8"),
    "15": ("NC_000015.10", "NC_000015.9"),
    "16": ("NC_000016.10", "NC_000016.9"),
    "17": ("NC_000017.11", "NC_000017.10"),
    "18": ("NC_000018.10", "NC_000018.9"),
    "19": ("NC_000019.10", "NC_000019.9"),
    "20": ("NC_000020.11", "NC_000020.10"),
    "21": ("NC_000021.9", "NC_000021.8"),
    "22": ("NC_000022.11", "NC_000022.10"),
    "X": ("NC_000023.11", "NC_000023.10"),
    "Y": ("NC_000024.10", "NC_000024.9"),
    "MT": ("NC_012920.1",),
}


def _refseq_candidates_for_chrom(chrom: str) -> list[str]:
    normalized = _normalized_chrom(chrom).upper()
    if normalized == "M":
        normalized = "MT"
    return list(_REFSEQ_CHROM_CANDIDATES.get(normalized, ()))


def _variant_key(chrom: str, pos: int, ref: str, alt: str) -> str:
    return f"{chrom}-{int(pos)}-{str(ref).upper()}-{str(alt).upper()}"


def _chrom_candidates(chrom: str, *, include_refseq: bool = False) -> list[str]:
    normalized = _normalized_chrom(chrom)
    candidates: list[str] = []
    if normalized:
        candidates.append(normalized)
        if not normalized.lower().startswith("chr"):
            candidates.append(f"chr{normalized}")
    if normalized.upper() == "MT":
        candidates.extend(["M", "chrM"])
    if normalized.upper() == "M":
        candidates.extend(["MT", "chrM"])
    if not candidates:
        text = str(chrom).strip()
        if text:
            candidates.append(text)

    if include_refseq:
        candidates.extend(_refseq_candidates_for_chrom(normalized or str(chrom)))

    deduped: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _parse_info(info_text: str) -> dict[str, str]:
    info_map: dict[str, str] = {}
    for part in str(info_text or "").split(";"):
        token = part.strip()
        if not token:
            continue
        if "=" in token:
            key, value = token.split("=", 1)
            info_map[key] = value
        else:
            info_map[token] = "1"
    return info_map


def _float_or_none(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _is_supported_vcf_file(path: str) -> bool:
    lowered = str(path or "").strip().lower()
    if not lowered:
        return False
    if lowered.endswith(".tbi") or lowered.endswith(".csi"):
        return False
    return lowered.endswith(".vcf") or lowered.endswith(".vcf.gz") or lowered.endswith(".vcf.bgz")


@lru_cache(maxsize=16)
def _list_directory_vcf_files(directory: str) -> tuple[str, ...]:
    matches = []
    for path in glob(os.path.join(directory, "**", "*"), recursive=True):
        if not os.path.isfile(path):
            continue
        if not _is_supported_vcf_file(path):
            continue
        matches.append(path)
    return tuple(sorted(matches))


def _chrom_file_match_score(basename_lower: str, chrom_token: str) -> int:
    token = str(chrom_token or "").strip().lower()
    if not token:
        return 0
    pattern = re.compile(rf"(?:^|[._-])(?:chr)?{re.escape(token)}(?:[._-]|$)")
    if pattern.search(basename_lower):
        return 10
    if f"chr{token}" in basename_lower:
        return 3
    if token in basename_lower:
        return 1
    return 0


@lru_cache(maxsize=256)
def _resolve_local_vcf_file_for_chrom(local_vcf_path: str, chrom: str) -> str | None:
    path = str(local_vcf_path or "").strip()
    if not path:
        raise FileNotFoundError("Local VCF path is not configured.")
    if os.path.isfile(path):
        if not _is_supported_vcf_file(path):
            raise FileNotFoundError(f"Local VCF path is not a supported VCF/VCF.GZ/VCF.BGZ file: {path}")
        return path
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Local VCF file or directory not found: {path}")

    files = _list_directory_vcf_files(path)
    if not files:
        raise FileNotFoundError(f"No local VCF files found under directory: {path}")
    if len(files) == 1:
        return files[0]

    chrom_tokens = _chrom_candidates(chrom)
    if not chrom_tokens:
        return files[0]

    best_match = None
    best_score = -1
    for file_path in files:
        base = os.path.basename(file_path).lower()
        score = 0
        for chrom_token in chrom_tokens:
            score = max(score, _chrom_file_match_score(base, chrom_token))
        if score > best_score:
            best_match = file_path
            best_score = score

    if best_score <= 0:
        # Directory mode may contain per-chrom files; treat missing chrom file as not-found for this query.
        return None
    return best_match


def _extract_matching_vcf_record(
    vcf_path: str,
    *,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    timeout_seconds: int = 10,
    include_refseq: bool = False,
) -> dict | None:
    resolved_vcf_path = _resolve_local_vcf_file_for_chrom(vcf_path, chrom)
    if not resolved_vcf_path:
        return None

    expected_ref = str(ref or "").upper()
    expected_alt = str(alt or "").upper()
    expected_pos = int(pos)
    stderr_tail = ""

    for chrom_candidate in _chrom_candidates(chrom, include_refseq=include_refseq):
        region = f"{chrom_candidate}:{expected_pos}-{expected_pos}"
        completed = subprocess.run(
            ["tabix", resolved_vcf_path, region],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
        stderr_tail = (completed.stderr or "").strip()
        if completed.returncode != 0:
            lowered = stderr_tail.lower()
            if "sequence not found" in lowered or "the index file exists but" in lowered:
                continue
            raise RuntimeError(
                f"tabix query failed for region={region} exit_code={completed.returncode} stderr={stderr_tail}"
            )

        for line in (completed.stdout or "").splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 8:
                continue
            row_chrom = str(parts[0]).strip()
            try:
                row_pos = int(parts[1])
            except ValueError:
                continue
            row_id = str(parts[2]).strip()
            row_ref = str(parts[3]).strip().upper()
            row_alts = [value.strip().upper() for value in str(parts[4]).split(",")]
            row_info = str(parts[7]).strip()
            if row_pos != expected_pos or row_ref != expected_ref:
                continue
            for idx, row_alt in enumerate(row_alts):
                if row_alt != expected_alt:
                    continue
                return {
                    "chrom": row_chrom,
                    "pos": row_pos,
                    "id": row_id,
                    "ref": row_ref,
                    "alt": row_alt,
                    "alt_index": idx,
                    "info_text": row_info,
                    "info": _parse_info(row_info),
                    "region": region,
                    "vcf_path": resolved_vcf_path,
                }

    return None


def fetch_dbsnp_evidence_from_local_vcf(
    *,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    timeout_seconds: int = 10,
) -> dict:
    if not local_vcf_path:
        return {
            "outcome": "error",
            "rsid": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local dbSNP VCF path is not configured.",
            "details": {},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    try:
        record = _extract_matching_vcf_record(
            local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=timeout_seconds,
            include_refseq=True,
        )
    except FileNotFoundError as exc:
        return {
            "outcome": "error",
            "rsid": None,
            "reason_code": "LOCAL_DB_MISSING",
            "reason_message": str(exc),
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except subprocess.TimeoutExpired:
        return {
            "outcome": "error",
            "rsid": None,
            "reason_code": "TIMEOUT",
            "reason_message": f"Local dbSNP tabix query timed out after {timeout_seconds} seconds.",
            "details": {"local_vcf_path": local_vcf_path, "timeout_seconds": timeout_seconds},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "outcome": "error",
            "rsid": None,
            "reason_code": "LOCAL_QUERY_FAILED",
            "reason_message": f"Local dbSNP lookup failed: {exc}",
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    if not record:
        return {
            "outcome": "not_found",
            "rsid": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No dbSNP rsID found in local database for this variant.",
            "details": {"local_vcf_path": local_vcf_path, "variant_key": _variant_key(chrom, pos, ref, alt)},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    raw_id = str(record.get("id") or "").strip()
    rsid = None
    if raw_id and raw_id != ".":
        first = raw_id.split(";", 1)[0].split(",", 1)[0].strip()
        if first:
            rsid = first if first.lower().startswith("rs") else f"rs{first}"

    if not rsid:
        return {
            "outcome": "not_found",
            "rsid": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No dbSNP rsID found in local database for this variant.",
            "details": {
                "local_vcf_path": local_vcf_path,
                "variant_key": _variant_key(chrom, pos, ref, alt),
                "record_id": raw_id,
            },
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    return {
        "outcome": "found",
        "rsid": rsid,
        "reason_code": None,
        "reason_message": None,
        "details": {
            "local_vcf_path": local_vcf_path,
            "variant_key": _variant_key(chrom, pos, ref, alt),
            "record_region": record.get("region"),
            "source_mode": "offline_local",
        },
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": 0,
    }


def fetch_clinvar_evidence_from_local_vcf(
    *,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    timeout_seconds: int = 10,
) -> dict:
    if not local_vcf_path:
        return {
            "outcome": "error",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local ClinVar VCF path is not configured.",
            "details": {},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    try:
        record = _extract_matching_vcf_record(
            local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=timeout_seconds,
        )
    except FileNotFoundError as exc:
        return {
            "outcome": "error",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "LOCAL_DB_MISSING",
            "reason_message": str(exc),
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except subprocess.TimeoutExpired:
        return {
            "outcome": "error",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "TIMEOUT",
            "reason_message": f"Local ClinVar tabix query timed out after {timeout_seconds} seconds.",
            "details": {"local_vcf_path": local_vcf_path, "timeout_seconds": timeout_seconds},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "outcome": "error",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "LOCAL_QUERY_FAILED",
            "reason_message": f"Local ClinVar lookup failed: {exc}",
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    if not record:
        return {
            "outcome": "not_found",
            "clinvar_id": None,
            "clinical_significance": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No ClinVar record found in local database for this variant.",
            "details": {"local_vcf_path": local_vcf_path, "variant_key": _variant_key(chrom, pos, ref, alt)},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    info = record.get("info") or {}
    raw_id = str(record.get("id") or "").strip()
    clinvar_id = (
        (str(info.get("CLNVID") or "").strip() or None)
        or (str(info.get("ALLELEID") or "").strip() or None)
        or (raw_id if raw_id and raw_id != "." else None)
    )
    clnsig = str(info.get("CLNSIG") or "").strip()
    clinical_significance = clnsig.replace("|", ", ") if clnsig else None

    return {
        "outcome": "found",
        "clinvar_id": clinvar_id,
        "clinical_significance": clinical_significance,
        "reason_code": None,
        "reason_message": None,
        "details": {
            "local_vcf_path": local_vcf_path,
            "variant_key": _variant_key(chrom, pos, ref, alt),
            "record_region": record.get("region"),
            "source_mode": "offline_local",
        },
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": 0,
    }


def fetch_gnomad_evidence_from_local_vcf(
    *,
    local_vcf_path: str | None,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
    timeout_seconds: int = 10,
) -> dict:
    if not local_vcf_path:
        return {
            "outcome": "error",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "LOCAL_DB_NOT_CONFIGURED",
            "reason_message": "Local gnomAD VCF path is not configured.",
            "details": {},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    try:
        record = _extract_matching_vcf_record(
            local_vcf_path,
            chrom=chrom,
            pos=pos,
            ref=ref,
            alt=alt,
            timeout_seconds=timeout_seconds,
        )
    except FileNotFoundError as exc:
        return {
            "outcome": "error",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "LOCAL_DB_MISSING",
            "reason_message": str(exc),
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except subprocess.TimeoutExpired:
        return {
            "outcome": "error",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "TIMEOUT",
            "reason_message": f"Local gnomAD tabix query timed out after {timeout_seconds} seconds.",
            "details": {"local_vcf_path": local_vcf_path, "timeout_seconds": timeout_seconds},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "outcome": "error",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "LOCAL_QUERY_FAILED",
            "reason_message": f"Local gnomAD lookup failed: {exc}",
            "details": {"local_vcf_path": local_vcf_path},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    if not record:
        return {
            "outcome": "not_found",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No gnomAD record found in local database for this variant.",
            "details": {"local_vcf_path": local_vcf_path, "variant_key": _variant_key(chrom, pos, ref, alt)},
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": 0,
        }

    info = record.get("info") or {}
    alt_index = int(record.get("alt_index") or 0)
    af = None
    af_source = None
    af_text = str(info.get("AF") or "").strip()
    if af_text:
        parts = [item.strip() for item in af_text.split(",")]
        if alt_index < len(parts):
            af = _float_or_none(parts[alt_index])
        if af is None and parts:
            af = _float_or_none(parts[0])
        af_source = "AF"
    if af is None:
        af = _float_or_none(info.get("AF_POPMAX"))
        if af is not None:
            af_source = "AF_POPMAX"

    return {
        "outcome": "found",
        "gnomad_variant_id": _variant_key(record.get("chrom") or chrom, pos, ref, alt),
        "global_af": af,
        "reason_code": None,
        "reason_message": None,
        "details": {
            "local_vcf_path": local_vcf_path,
            "variant_key": _variant_key(chrom, pos, ref, alt),
            "record_region": record.get("region"),
            "af_source": af_source,
            "source_mode": "offline_local",
        },
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": 0,
    }
