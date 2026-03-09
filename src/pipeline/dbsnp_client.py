from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


_DEFAULT_BASE_URL = "https://api.ncbi.nlm.nih.gov/variation/v0"
_DEFAULT_USER_AGENT = "SP-Variant-Pipeline/1.0"
_DEFAULT_ASSEMBLY = "GRCh38"


@dataclass(frozen=True)
class DbsnpConfig:
    enabled: bool
    api_base_url: str
    timeout_seconds: int
    retry_max_attempts: int
    retry_backoff_base_seconds: float
    retry_backoff_max_seconds: float
    api_key: str | None
    assembly: str = _DEFAULT_ASSEMBLY


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _retryable_http(code: int) -> bool:
    return code in {408, 429, 500, 502, 503, 504}


def _extract_rsid(payload: object) -> str | None:
    candidate = None
    if isinstance(payload, dict):
        rsids = payload.get("rsids")
        if isinstance(rsids, list) and rsids:
            candidate = rsids[0]
        elif isinstance(payload.get("refsnp_ids"), list) and payload["refsnp_ids"]:
            candidate = payload["refsnp_ids"][0]
        elif payload.get("refsnp_id") is not None:
            candidate = payload.get("refsnp_id")
    elif isinstance(payload, list) and payload:
        candidate = payload[0]

    if candidate is None:
        return None
    text = str(candidate).strip()
    if not text:
        return None
    if not text.lower().startswith("rs"):
        return f"rs{text}"
    return text


def _backoff_seconds(config: DbsnpConfig, attempt: int) -> float:
    # attempt is 1-indexed.
    delay = config.retry_backoff_base_seconds * (2 ** max(0, attempt - 1))
    return min(delay, config.retry_backoff_max_seconds)


def _normalized_chrom(chrom: str) -> str:
    text = (chrom or "").strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    return text


def _normalized_allele(base: str) -> str:
    return (base or "").strip().upper()


def _build_contextual_urls(config: DbsnpConfig, *, chrom: str, pos: int, ref: str, alt: str) -> list[str]:
    base = config.api_base_url.rstrip("/")
    normalized_chrom = _normalized_chrom(chrom)
    normalized_ref = _normalized_allele(ref)
    normalized_alt = _normalized_allele(alt)
    chrom_candidates: list[str] = []

    if normalized_chrom:
        chrom_candidates.append(normalized_chrom)
        if not normalized_chrom.lower().startswith("chr"):
            chrom_candidates.append(f"chr{normalized_chrom}")

    if normalized_chrom.upper() == "MT":
        chrom_candidates.extend(["M", "chrM"])
    if normalized_chrom.upper() == "M":
        chrom_candidates.extend(["MT", "chrM"])
    if not chrom_candidates:
        chrom_candidates = [str(chrom).strip()]

    urls: list[str] = []
    seen: set[str] = set()
    for chrom_candidate in chrom_candidates:
        if not chrom_candidate:
            continue
        query_params = {"assembly": (config.assembly or _DEFAULT_ASSEMBLY).strip() or _DEFAULT_ASSEMBLY}
        if config.api_key:
            query_params["api_key"] = config.api_key
        query = urlencode(query_params)
        url = f"{base}/vcf/{chrom_candidate}/{int(pos)}/{normalized_ref}/{normalized_alt}/contextuals?{query}"
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _build_spdi_rsids_url(config: DbsnpConfig, spdi: str) -> str:
    base = config.api_base_url.rstrip("/")
    encoded_spdi = quote(spdi, safe="")
    url = f"{base}/spdi/{encoded_spdi}/rsids"
    if config.api_key:
        url = f"{url}?{urlencode({'api_key': config.api_key})}"
    return url


def _extract_spdis(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    spdis = data.get("spdis")
    if not isinstance(spdis, list):
        return []

    collected: list[str] = []
    for entry in spdis:
        if not isinstance(entry, dict):
            continue
        seq_id = str(entry.get("seq_id") or "").strip()
        position = entry.get("position")
        deleted = str(entry.get("deleted_sequence") or "").strip()
        inserted = str(entry.get("inserted_sequence") or "").strip()
        if not seq_id or position is None or deleted == "" or inserted == "":
            continue
        try:
            pos_int = int(position)
        except (TypeError, ValueError):
            continue
        collected.append(f"{seq_id}:{pos_int}:{deleted}:{inserted}")
    return collected


def fetch_dbsnp_evidence_for_variant(
    config: DbsnpConfig,
    *,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    urls = _build_contextual_urls(config, chrom=chrom, pos=pos, ref=ref, alt=alt)
    attempts = max(1, int(config.retry_max_attempts))
    last_error: dict | None = None
    total_retries = 0
    tried_status_codes: list[int] = []
    attempted_spdis: list[str] = []

    for contextual_url in urls:
        contextual_payload: object | None = None
        contextual_status_code: int | None = None

        for contextual_attempt in range(1, attempts + 1):
            request = Request(
                contextual_url,
                headers={"Accept": "application/json", "User-Agent": _DEFAULT_USER_AGENT},
                method="GET",
            )
            try:
                with urlopen(request, timeout=config.timeout_seconds) as response:
                    contextual_status_code = int(getattr(response, "status", 200) or 200)
                    body = response.read().decode("utf-8", errors="replace")
                contextual_payload = json.loads(body) if body else {}
                break
            except HTTPError as exc:
                status_code = int(exc.code or 0)
                tried_status_codes.append(status_code)
                if status_code in {404, 422}:
                    # Try alternative chromosome forms before concluding not found.
                    break
                last_error = {
                    "reason_code": "HTTP_ERROR",
                    "reason_message": f"dbSNP request failed with HTTP {status_code}.",
                    "details": {
                        "status_code": status_code,
                        "url": contextual_url,
                        "tried_urls": urls,
                        "assembly": config.assembly,
                    },
                }
                if _retryable_http(status_code) and contextual_attempt < attempts:
                    total_retries += 1
                    time.sleep(_backoff_seconds(config, contextual_attempt))
                    continue
                break
            except json.JSONDecodeError as exc:
                last_error = {
                    "reason_code": "JSON_PARSE_ERROR",
                    "reason_message": "Failed to parse dbSNP JSON response.",
                    "details": {"url": contextual_url, "error": str(exc), "tried_urls": urls},
                }
                break
            except TimeoutError:
                last_error = {
                    "reason_code": "TIMEOUT",
                    "reason_message": f"dbSNP request timed out after {config.timeout_seconds} seconds.",
                    "details": {
                        "url": contextual_url,
                        "timeout_seconds": config.timeout_seconds,
                        "tried_urls": urls,
                    },
                }
                if contextual_attempt < attempts:
                    total_retries += 1
                    time.sleep(_backoff_seconds(config, contextual_attempt))
                    continue
                break
            except URLError as exc:
                reason_text = str(getattr(exc, "reason", exc))
                timeout_like = "timed out" in reason_text.lower()
                reason_code = "TIMEOUT" if timeout_like else "NETWORK_ERROR"
                last_error = {
                    "reason_code": reason_code,
                    "reason_message": f"dbSNP request failed: {reason_text}",
                    "details": {"url": contextual_url, "tried_urls": urls},
                }
                if contextual_attempt < attempts:
                    total_retries += 1
                    time.sleep(_backoff_seconds(config, contextual_attempt))
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_error = {
                    "reason_code": "REQUEST_ERROR",
                    "reason_message": f"dbSNP request failed unexpectedly: {exc}",
                    "details": {"url": contextual_url, "tried_urls": urls},
                }
                break

        if contextual_payload is None:
            continue

        spdis = _extract_spdis(contextual_payload)
        if not spdis:
            return {
                "outcome": "not_found",
                "rsid": None,
                "reason_code": "NOT_FOUND",
                "reason_message": "No dbSNP context found for this variant.",
                "details": {
                    "status_code": contextual_status_code,
                    "url": contextual_url,
                    "tried_urls": urls,
                    "assembly": config.assembly,
                },
                "retrieved_at": _utc_now_iso(),
                "retry_attempts": total_retries,
            }

        for spdi in spdis:
            attempted_spdis.append(spdi)
            rsid_url = _build_spdi_rsids_url(config, spdi)
            for rsid_attempt in range(1, attempts + 1):
                request = Request(
                    rsid_url,
                    headers={"Accept": "application/json", "User-Agent": _DEFAULT_USER_AGENT},
                    method="GET",
                )
                try:
                    with urlopen(request, timeout=config.timeout_seconds) as response:
                        status_code = int(getattr(response, "status", 200) or 200)
                        body = response.read().decode("utf-8", errors="replace")
                    payload = json.loads(body) if body else {}
                    rsid = _extract_rsid(payload) or _extract_rsid(payload.get("data") if isinstance(payload, dict) else None)
                    if rsid:
                        return {
                            "outcome": "found",
                            "rsid": rsid,
                            "reason_code": None,
                            "reason_message": None,
                            "details": {
                                "status_code": status_code,
                                "url": rsid_url,
                                "contextual_url": contextual_url,
                                "spdi": spdi,
                                "tried_urls": urls,
                                "assembly": config.assembly,
                            },
                            "retrieved_at": _utc_now_iso(),
                            "retry_attempts": total_retries,
                        }
                    break
                except HTTPError as exc:
                    status_code = int(exc.code or 0)
                    tried_status_codes.append(status_code)
                    if status_code == 404:
                        # Valid SPDI but no mapped RSID.
                        break
                    last_error = {
                        "reason_code": "HTTP_ERROR",
                        "reason_message": f"dbSNP request failed with HTTP {status_code}.",
                        "details": {
                            "status_code": status_code,
                            "url": rsid_url,
                            "contextual_url": contextual_url,
                            "spdi": spdi,
                            "tried_urls": urls,
                            "assembly": config.assembly,
                        },
                    }
                    if _retryable_http(status_code) and rsid_attempt < attempts:
                        total_retries += 1
                        time.sleep(_backoff_seconds(config, rsid_attempt))
                        continue
                    break
                except json.JSONDecodeError as exc:
                    last_error = {
                        "reason_code": "JSON_PARSE_ERROR",
                        "reason_message": "Failed to parse dbSNP JSON response.",
                        "details": {"url": rsid_url, "error": str(exc), "spdi": spdi, "tried_urls": urls},
                    }
                    break
                except TimeoutError:
                    last_error = {
                        "reason_code": "TIMEOUT",
                        "reason_message": f"dbSNP request timed out after {config.timeout_seconds} seconds.",
                        "details": {"url": rsid_url, "timeout_seconds": config.timeout_seconds, "spdi": spdi},
                    }
                    if rsid_attempt < attempts:
                        total_retries += 1
                        time.sleep(_backoff_seconds(config, rsid_attempt))
                        continue
                    break
                except URLError as exc:
                    reason_text = str(getattr(exc, "reason", exc))
                    timeout_like = "timed out" in reason_text.lower()
                    reason_code = "TIMEOUT" if timeout_like else "NETWORK_ERROR"
                    last_error = {
                        "reason_code": reason_code,
                        "reason_message": f"dbSNP request failed: {reason_text}",
                        "details": {"url": rsid_url, "spdi": spdi},
                    }
                    if rsid_attempt < attempts:
                        total_retries += 1
                        time.sleep(_backoff_seconds(config, rsid_attempt))
                        continue
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = {
                        "reason_code": "REQUEST_ERROR",
                        "reason_message": f"dbSNP request failed unexpectedly: {exc}",
                        "details": {"url": rsid_url, "spdi": spdi},
                    }
                    break

        # Context was resolved but no SPDI matched an RSID.
        return {
            "outcome": "not_found",
            "rsid": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No dbSNP rsID found for this variant.",
            "details": {
                "contextual_url": contextual_url,
                "spdis": spdis,
                "tried_urls": urls,
                "assembly": config.assembly,
            },
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": total_retries,
        }

    # If deterministic variant-format statuses were returned for all tried URLs,
    # classify as not_found/unsupported instead of hard error.
    if last_error is None and tried_status_codes and all(code in {404, 422} for code in tried_status_codes):
        return {
            "outcome": "not_found",
            "rsid": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "dbSNP has no rsID for this variant (or variant format is unsupported).",
            "details": {
                "status_codes": tried_status_codes,
                "tried_urls": urls,
                "attempted_spdis": attempted_spdis,
                "assembly": config.assembly,
            },
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": total_retries,
        }

    if last_error is None:
        last_error = {
            "reason_code": "NOT_FOUND" if attempted_spdis else "REQUEST_ERROR",
            "reason_message": "No dbSNP rsID found for this variant." if attempted_spdis else "dbSNP request failed.",
            "details": {"tried_urls": urls, "attempted_spdis": attempted_spdis, "assembly": config.assembly},
        }

    return {
        "outcome": "not_found" if last_error["reason_code"] == "NOT_FOUND" else "error",
        "rsid": None,
        "reason_code": last_error["reason_code"],
        "reason_message": last_error["reason_message"],
        "details": last_error.get("details") or {"tried_urls": urls},
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": total_retries,
    }
