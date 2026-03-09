from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_DEFAULT_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


@dataclass(frozen=True)
class ClinvarConfig:
    enabled: bool
    api_base_url: str
    timeout_seconds: int
    retry_max_attempts: int
    retry_backoff_base_seconds: float
    retry_backoff_max_seconds: float
    api_key: str | None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _retryable_http(code: int) -> bool:
    return code in {408, 429, 500, 502, 503, 504}


def _backoff_seconds(config: ClinvarConfig, attempt: int) -> float:
    delay = config.retry_backoff_base_seconds * (2 ** max(0, attempt - 1))
    return min(delay, config.retry_backoff_max_seconds)


def _build_params(config: ClinvarConfig, extra: dict[str, str]) -> dict[str, str]:
    params = dict(extra)
    if config.api_key:
        params["api_key"] = config.api_key
    return params


def _normalized_chrom(chrom: str) -> str:
    text = (chrom or "").strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    return text


def _normalized_allele(base: str) -> str:
    return (base or "").strip().upper()


def _build_esearch_url(config: ClinvarConfig, *, chrom: str, pos: int, ref: str, alt: str) -> str:
    base = config.api_base_url.rstrip("/")
    normalized_chrom = _normalized_chrom(chrom)
    normalized_ref = _normalized_allele(ref)
    normalized_alt = _normalized_allele(alt)
    term = (
        f"{normalized_chrom}[Chromosome] AND {int(pos)}[Base Position for Assembly GRCh38] "
        f"AND {normalized_ref}>{normalized_alt}[Canonical SPDI]"
    )
    params = _build_params(
        config,
        {
            "db": "clinvar",
            "retmode": "json",
            "retmax": "1",
            "term": term,
        },
    )
    return f"{base}/esearch.fcgi?{urlencode(params)}"


def _build_esummary_url(config: ClinvarConfig, *, clinvar_uid: str) -> str:
    base = config.api_base_url.rstrip("/")
    params = _build_params(
        config,
        {
            "db": "clinvar",
            "retmode": "json",
            "id": clinvar_uid,
        },
    )
    return f"{base}/esummary.fcgi?{urlencode(params)}"


def _extract_uid(search_payload: object) -> str | None:
    if not isinstance(search_payload, dict):
        return None
    esearchresult = search_payload.get("esearchresult")
    if not isinstance(esearchresult, dict):
        return None
    ids = esearchresult.get("idlist")
    if not isinstance(ids, list) or not ids:
        return None
    uid = str(ids[0]).strip()
    return uid or None


def _extract_clinvar_fields(summary_payload: object, *, uid: str) -> tuple[str | None, str | None]:
    if not isinstance(summary_payload, dict):
        return None, None
    result = summary_payload.get("result")
    if not isinstance(result, dict):
        return None, None
    row = result.get(str(uid))
    if not isinstance(row, dict):
        return None, None

    clinvar_id: str | None = None
    accession = row.get("accession")
    if accession:
        accession_text = str(accession).strip()
        if accession_text:
            clinvar_id = accession_text.split(".", 1)[0]
    if not clinvar_id:
        uid_text = str(row.get("uid") or uid).strip()
        clinvar_id = uid_text or None

    clinical_significance: str | None = None
    cs = row.get("clinical_significance")
    if isinstance(cs, dict):
        desc = cs.get("description")
        if desc:
            clinical_significance = str(desc).strip() or None
    elif cs:
        clinical_significance = str(cs).strip() or None

    if not clinical_significance:
        gc = row.get("germline_classification")
        if isinstance(gc, dict):
            desc = gc.get("description")
            if desc:
                clinical_significance = str(desc).strip() or None
        elif gc:
            clinical_significance = str(gc).strip() or None

    return clinvar_id, clinical_significance


def _request_json(url: str, timeout_seconds: int) -> tuple[int, object]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(request, timeout=timeout_seconds) as response:
        status_code = int(getattr(response, "status", 200) or 200)
        body = response.read().decode("utf-8", errors="replace")
    payload = json.loads(body) if body else {}
    return status_code, payload


def fetch_clinvar_evidence_for_variant(
    config: ClinvarConfig,
    *,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    search_url = _build_esearch_url(config, chrom=chrom, pos=pos, ref=ref, alt=alt)
    attempts = max(1, int(config.retry_max_attempts))
    last_error: dict | None = None
    last_attempt = 0

    for attempt in range(1, attempts + 1):
        last_attempt = attempt
        summary_url = None
        try:
            search_status, search_payload = _request_json(search_url, config.timeout_seconds)
            uid = _extract_uid(search_payload)
            if not uid:
                return {
                    "outcome": "not_found",
                    "clinvar_id": None,
                    "clinical_significance": None,
                    "reason_code": "NOT_FOUND",
                    "reason_message": "No ClinVar record found for this variant.",
                    "details": {
                        "search_url": search_url,
                        "search_status_code": search_status,
                    },
                    "retrieved_at": _utc_now_iso(),
                    "retry_attempts": attempt - 1,
                }

            summary_url = _build_esummary_url(config, clinvar_uid=uid)
            summary_status, summary_payload = _request_json(summary_url, config.timeout_seconds)
            clinvar_id, clinical_significance = _extract_clinvar_fields(summary_payload, uid=uid)
            if clinvar_id:
                return {
                    "outcome": "found",
                    "clinvar_id": clinvar_id,
                    "clinical_significance": clinical_significance,
                    "reason_code": None,
                    "reason_message": None,
                    "details": {
                        "search_url": search_url,
                        "summary_url": summary_url,
                        "search_status_code": search_status,
                        "summary_status_code": summary_status,
                    },
                    "retrieved_at": _utc_now_iso(),
                    "retry_attempts": attempt - 1,
                }

            return {
                "outcome": "error",
                "clinvar_id": None,
                "clinical_significance": None,
                "reason_code": "MALFORMED_RESPONSE",
                "reason_message": "ClinVar summary response did not contain a stable identifier.",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                    "uid": uid,
                    "search_status_code": search_status,
                    "summary_status_code": summary_status,
                },
                "retrieved_at": _utc_now_iso(),
                "retry_attempts": attempt - 1,
            }
        except HTTPError as exc:
            status_code = int(exc.code or 0)
            if status_code == 404:
                return {
                    "outcome": "not_found",
                    "clinvar_id": None,
                    "clinical_significance": None,
                    "reason_code": "NOT_FOUND",
                    "reason_message": "ClinVar returned not found for this variant.",
                    "details": {
                        "search_url": search_url,
                        "status_code": status_code,
                    },
                    "retrieved_at": _utc_now_iso(),
                    "retry_attempts": attempt - 1,
                }
            last_error = {
                "reason_code": "HTTP_ERROR",
                "reason_message": f"ClinVar request failed with HTTP {status_code}.",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                    "status_code": status_code,
                },
            }
            if _retryable_http(status_code) and attempt < attempts:
                time.sleep(_backoff_seconds(config, attempt))
                continue
            break
        except json.JSONDecodeError as exc:
            last_error = {
                "reason_code": "JSON_PARSE_ERROR",
                "reason_message": "Failed to parse ClinVar JSON response.",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                    "error": str(exc),
                },
            }
            break
        except TimeoutError:
            last_error = {
                "reason_code": "TIMEOUT",
                "reason_message": f"ClinVar request timed out after {config.timeout_seconds} seconds.",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                    "timeout_seconds": config.timeout_seconds,
                },
            }
            if attempt < attempts:
                time.sleep(_backoff_seconds(config, attempt))
                continue
            break
        except URLError as exc:
            reason_text = str(getattr(exc, "reason", exc))
            timeout_like = "timed out" in reason_text.lower()
            reason_code = "TIMEOUT" if timeout_like else "NETWORK_ERROR"
            last_error = {
                "reason_code": reason_code,
                "reason_message": f"ClinVar request failed: {reason_text}",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                },
            }
            if attempt < attempts:
                time.sleep(_backoff_seconds(config, attempt))
                continue
            break
        except Exception as exc:  # noqa: BLE001
            last_error = {
                "reason_code": "REQUEST_ERROR",
                "reason_message": f"ClinVar request failed unexpectedly: {exc}",
                "details": {
                    "search_url": search_url,
                    "summary_url": summary_url,
                },
            }
            break

    if last_error is None:
        last_error = {
            "reason_code": "REQUEST_ERROR",
            "reason_message": "ClinVar request failed.",
            "details": {"search_url": search_url},
        }

    return {
        "outcome": "error",
        "clinvar_id": None,
        "clinical_significance": None,
        "reason_code": last_error["reason_code"],
        "reason_message": last_error["reason_message"],
        "details": last_error.get("details") or {"search_url": search_url},
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": max(0, last_attempt - 1),
    }
