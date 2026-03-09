from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


_DEFAULT_BASE_URL = "https://gnomad.broadinstitute.org/api"
_DEFAULT_USER_AGENT = "SP-Variant-Pipeline/1.0"
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_AT = 0.0
_RATE_LIMIT_UNTIL = 0.0
_QUERY = """
query Variant($variantId: String!, $dataset: DatasetId!) {
  variant(variantId: $variantId, dataset: $dataset) {
    variantId
    rsid
    joint {
      ac
      an
    }
    genome {
      af
      ac
      an
    }
    exome {
      af
      ac
      an
    }
  }
}
""".strip()


@dataclass(frozen=True)
class GnomadConfig:
    enabled: bool
    api_base_url: str
    dataset_id: str
    reference_genome: str
    timeout_seconds: int
    retry_max_attempts: int
    retry_backoff_base_seconds: float
    retry_backoff_max_seconds: float
    min_request_interval_seconds: float


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _retryable_http(code: int) -> bool:
    return code in {408, 429, 500, 502, 503, 504}


def _backoff_seconds(config: GnomadConfig, attempt: int) -> float:
    delay = config.retry_backoff_base_seconds * (2 ** max(0, attempt - 1))
    return min(delay, config.retry_backoff_max_seconds)


def _normalized_chrom(chrom: str) -> str:
    text = (chrom or "").strip()
    if text.lower().startswith("chr"):
        text = text[3:]
    return text


def _variant_id(chrom: str, pos: int, ref: str, alt: str) -> str:
    return f"{_normalized_chrom(chrom)}-{int(pos)}-{str(ref).upper()}-{str(alt).upper()}"


def _variant_id_candidates(chrom: str, pos: int, ref: str, alt: str) -> list[str]:
    normalized = _normalized_chrom(chrom)
    ref_norm = str(ref).upper()
    alt_norm = str(alt).upper()

    chrom_candidates: list[str] = [normalized]
    if normalized and not normalized.lower().startswith("chr"):
        chrom_candidates.append(f"chr{normalized}")
    if normalized.upper() == "MT":
        chrom_candidates.extend(["M", "chrM"])
    if normalized.upper() == "M":
        chrom_candidates.extend(["MT", "chrM"])

    seen: set[str] = set()
    ids: list[str] = []
    for chrom_candidate in chrom_candidates:
        if not chrom_candidate:
            continue
        vid = f"{chrom_candidate}-{int(pos)}-{ref_norm}-{alt_norm}"
        if vid in seen:
            continue
        seen.add(vid)
        ids.append(vid)

    return ids or [_variant_id(chrom, pos, ref, alt)]


def _respect_rate_limit(min_interval_seconds: float) -> None:
    global _LAST_REQUEST_AT
    interval = max(0.0, float(min_interval_seconds))
    with _REQUEST_LOCK:
        now = time.monotonic()
        required_wait = 0.0
        if interval > 0:
            elapsed = now - _LAST_REQUEST_AT
            if elapsed < interval:
                required_wait = max(required_wait, interval - elapsed)
        if _RATE_LIMIT_UNTIL > now:
            required_wait = max(required_wait, _RATE_LIMIT_UNTIL - now)
        if required_wait > 0:
            time.sleep(required_wait)
            now = time.monotonic()
        _LAST_REQUEST_AT = now


def _set_rate_limit_cooldown(wait_seconds: float) -> None:
    global _RATE_LIMIT_UNTIL
    if wait_seconds <= 0:
        return
    with _REQUEST_LOCK:
        _RATE_LIMIT_UNTIL = max(_RATE_LIMIT_UNTIL, time.monotonic() + wait_seconds)


def _retry_after_seconds(exc: HTTPError) -> float | None:
    headers = getattr(exc, "headers", None)
    if headers is None:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _graphql_messages(errors: object) -> list[str]:
    if not isinstance(errors, list):
        return []
    messages: list[str] = []
    for err in errors:
        if not isinstance(err, dict):
            continue
        text = str(err.get("message") or "").strip()
        if text:
            messages.append(text)
    return messages


def _is_retryable_graphql_error(errors: object) -> bool:
    messages = " | ".join(_graphql_messages(errors)).lower()
    if not messages:
        return False
    retryable_markers = (
        "rate limit",
        "too many requests",
        "throttl",
        "timed out",
        "timeout",
        "temporar",
        "service unavailable",
        "internal server error",
        "try again",
    )
    return any(marker in messages for marker in retryable_markers)


def _is_schema_graphql_error(errors: object) -> bool:
    messages = " | ".join(_graphql_messages(errors)).lower()
    if not messages:
        return False
    schema_markers = (
        "unknown argument",
        "cannot query field",
        "unknown type",
        "validation error",
    )
    return any(marker in messages for marker in schema_markers)


def _extract_global_af(variant: object) -> tuple[float | None, str | None]:
    if not isinstance(variant, dict):
        return None, None

    direct = _safe_float(variant.get("af"))
    if direct is not None:
        return direct, "af"

    for source in ("genome", "exome", "joint"):
        block = variant.get(source)
        if not isinstance(block, dict):
            continue
        af = _safe_float(block.get("af"))
        if af is not None:
            return af, f"{source}.af"

    joint = variant.get("joint")
    if isinstance(joint, dict):
        ac = _safe_float(joint.get("ac"))
        an = _safe_float(joint.get("an"))
        if ac is not None and an is not None and an > 0:
            return ac / an, "joint.ac_an"

    return None, None


def fetch_gnomad_evidence_for_variant(
    config: GnomadConfig,
    *,
    chrom: str,
    pos: int,
    ref: str,
    alt: str,
) -> dict:
    variant_ids = _variant_id_candidates(chrom, pos, ref, alt)
    url = (config.api_base_url or _DEFAULT_BASE_URL).strip() or _DEFAULT_BASE_URL

    attempts = max(1, int(config.retry_max_attempts))
    last_error: dict | None = None
    total_retries = 0
    deterministic_statuses: list[int] = []

    for variant_id in variant_ids:
        payload = {
            "query": _QUERY,
            "variables": {
                "variantId": variant_id,
                "dataset": config.dataset_id,
            },
        }

        for attempt in range(1, attempts + 1):
            _respect_rate_limit(config.min_request_interval_seconds)
            request = Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": _DEFAULT_USER_AGENT,
                },
                method="POST",
            )
            try:
                with urlopen(request, timeout=config.timeout_seconds) as response:
                    status_code = int(getattr(response, "status", 200) or 200)
                    body = response.read().decode("utf-8", errors="replace")

                decoded = json.loads(body) if body else {}
                data = decoded.get("data") if isinstance(decoded, dict) else None
                variant = data.get("variant") if isinstance(data, dict) else None
                errors = decoded.get("errors") if isinstance(decoded, dict) else None

                # Accept partial GraphQL responses when variant payload exists.
                if not variant:
                    if errors:
                        reason_code = "GRAPHQL_SCHEMA_ERROR" if _is_schema_graphql_error(errors) else "GRAPHQL_ERROR"
                        reason_message = (
                            "gnomAD GraphQL schema mismatch."
                            if reason_code == "GRAPHQL_SCHEMA_ERROR"
                            else "gnomAD GraphQL returned errors."
                        )
                        details = {
                            "url": url,
                            "variant_id": variant_id,
                            "errors": errors,
                            "status_code": status_code,
                            "variant_id_candidates": variant_ids,
                        }
                        if reason_code == "GRAPHQL_SCHEMA_ERROR":
                            details["hint"] = (
                                "Query/field mismatch against current gnomAD schema. "
                                "Validate dataset/query fields and rebuild app image."
                            )
                        last_error = {
                            "reason_code": reason_code,
                            "reason_message": reason_message,
                            "details": details,
                        }
                        if _is_retryable_graphql_error(errors) and attempt < attempts:
                            total_retries += 1
                            wait = _backoff_seconds(config, attempt)
                            _set_rate_limit_cooldown(wait)
                            time.sleep(wait)
                            continue
                    # try alternate IDs before declaring not found
                    break

                gnomad_variant_id = str(variant.get("variantId") or variant_id)
                global_af, af_source = _extract_global_af(variant)
                return {
                    "outcome": "found",
                    "gnomad_variant_id": gnomad_variant_id,
                    "global_af": global_af,
                    "reason_code": None,
                    "reason_message": None,
                    "details": {
                        "url": url,
                        "variant_id": variant_id,
                        "dataset_id": config.dataset_id,
                        "reference_genome": config.reference_genome,
                        "status_code": status_code,
                        "af_source": af_source,
                        "variant_id_candidates": variant_ids,
                    },
                    "retrieved_at": _utc_now_iso(),
                    "retry_attempts": total_retries,
                }
            except HTTPError as exc:
                status_code = int(exc.code or 0)
                if status_code in {400, 404}:
                    deterministic_statuses.append(status_code)
                    break
                last_error = {
                    "reason_code": "HTTP_ERROR",
                    "reason_message": f"gnomAD request failed with HTTP {status_code}.",
                    "details": {
                        "url": url,
                        "variant_id": variant_id,
                        "status_code": status_code,
                        "variant_id_candidates": variant_ids,
                    },
                }
                if _retryable_http(status_code) and attempt < attempts:
                    total_retries += 1
                    retry_after = _retry_after_seconds(exc)
                    wait = _backoff_seconds(config, attempt)
                    if retry_after is not None:
                        wait = max(wait, retry_after)
                    if status_code == 429:
                        _set_rate_limit_cooldown(wait)
                    time.sleep(wait)
                    continue
                break
            except json.JSONDecodeError as exc:
                last_error = {
                    "reason_code": "JSON_PARSE_ERROR",
                    "reason_message": "Failed to parse gnomAD JSON response.",
                    "details": {
                        "url": url,
                        "variant_id": variant_id,
                        "error": str(exc),
                        "variant_id_candidates": variant_ids,
                    },
                }
                break
            except TimeoutError:
                last_error = {
                    "reason_code": "TIMEOUT",
                    "reason_message": f"gnomAD request timed out after {config.timeout_seconds} seconds.",
                    "details": {
                        "url": url,
                        "variant_id": variant_id,
                        "timeout_seconds": config.timeout_seconds,
                        "variant_id_candidates": variant_ids,
                    },
                }
                if attempt < attempts:
                    total_retries += 1
                    time.sleep(_backoff_seconds(config, attempt))
                    continue
                break
            except URLError as exc:
                reason_text = str(getattr(exc, "reason", exc))
                timeout_like = "timed out" in reason_text.lower()
                reason_code = "TIMEOUT" if timeout_like else "NETWORK_ERROR"
                last_error = {
                    "reason_code": reason_code,
                    "reason_message": f"gnomAD request failed: {reason_text}",
                    "details": {"url": url, "variant_id": variant_id, "variant_id_candidates": variant_ids},
                }
                if attempt < attempts:
                    total_retries += 1
                    time.sleep(_backoff_seconds(config, attempt))
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_error = {
                    "reason_code": "REQUEST_ERROR",
                    "reason_message": f"gnomAD request failed unexpectedly: {exc}",
                    "details": {"url": url, "variant_id": variant_id, "variant_id_candidates": variant_ids},
                }
                break

    if last_error is None and deterministic_statuses:
        return {
            "outcome": "not_found",
            "gnomad_variant_id": None,
            "global_af": None,
            "reason_code": "NOT_FOUND",
            "reason_message": "No gnomAD record found for this variant.",
            "details": {
                "url": url,
                "variant_id": variant_ids[0],
                "dataset_id": config.dataset_id,
                "reference_genome": config.reference_genome,
                "status_codes": deterministic_statuses,
                "variant_id_candidates": variant_ids,
            },
            "retrieved_at": _utc_now_iso(),
            "retry_attempts": total_retries,
        }

    if last_error is None:
        last_error = {
            "reason_code": "REQUEST_ERROR",
            "reason_message": "gnomAD request failed.",
            "details": {"url": url, "variant_id": variant_ids[0], "variant_id_candidates": variant_ids},
        }

    return {
        "outcome": "error",
        "gnomad_variant_id": None,
        "global_af": None,
        "reason_code": last_error["reason_code"],
        "reason_message": last_error["reason_message"],
        "details": last_error.get("details") or {"url": url, "variant_id": variant_ids[0]},
        "retrieved_at": _utc_now_iso(),
        "retry_attempts": total_retries,
    }
