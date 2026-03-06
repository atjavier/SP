import gzip
import os
from collections.abc import Iterator
from contextlib import contextmanager


def _error(code: str, message: str, details: dict | None = None) -> dict:
    payload = {"code": code, "message": message}
    if details:
        payload["details"] = details
    return payload


def _warning(code: str, message: str, details: dict | None = None) -> dict:
    payload = {"code": code, "message": message}
    if details:
        payload["details"] = details
    return payload


@contextmanager
def _open_text(path: str) -> Iterator[object]:
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
            yield handle
    else:
        with open(path, "rt", encoding="utf-8", errors="replace") as handle:
            yield handle


def validate_vcf_path(path: str, *, max_scan_lines: int = 5000) -> dict:
    errors: list[dict] = []
    warnings: list[dict] = []

    if not os.path.exists(path):
        return {
            "ok": False,
            "errors": [_error("FILE_NOT_FOUND", "Uploaded file was not found on disk.")],
            "warnings": [],
        }

    if os.path.getsize(path) == 0:
        return {
            "ok": False,
            "errors": [_error("EMPTY_FILE", "Uploaded file is empty.")],
            "warnings": [],
        }

    try:
        with _open_text(path) as handle:
            header_columns: list[str] | None = None
            saw_chrom_header = False
            lines_scanned = 0
            multi_alt_count = 0

            for line in handle:
                if lines_scanned >= max_scan_lines:
                    break
                lines_scanned += 1

                stripped = line.rstrip("\r\n")
                if not stripped:
                    continue

                if stripped.startswith("##"):
                    continue

                if stripped.startswith("#CHROM"):
                    saw_chrom_header = True
                    if "\t" not in stripped:
                        errors.append(
                            _error(
                                "NOT_TAB_DELIMITED",
                                "VCF header must be tab-delimited.",
                            )
                        )
                        break

                    header_columns = stripped.split("\t")
                    required = ["#CHROM", "POS", "REF", "ALT"]
                    missing = [name for name in required if name not in header_columns]
                    if missing:
                        errors.append(
                            _error(
                                "MISSING_REQUIRED_COLUMNS",
                                "VCF header is missing required columns.",
                                {"missing": missing},
                            )
                        )
                    continue

                if stripped.startswith("#"):
                    continue

                if header_columns is None:
                    continue

                parts = stripped.split("\t")
                try:
                    alt_idx = header_columns.index("ALT")
                except ValueError:
                    continue

                if len(parts) <= alt_idx:
                    errors.append(
                        _error(
                            "MALFORMED_ROW",
                            "A data row does not match the header column structure.",
                        )
                    )
                    continue

                if "," in parts[alt_idx]:
                    multi_alt_count += 1

            if not saw_chrom_header:
                errors.append(
                    _error(
                        "MISSING_CHROM_HEADER",
                        "VCF is missing the #CHROM header line.",
                    )
                )

            if multi_alt_count:
                warnings.append(
                    _warning(
                        "MULTI_ALT_PRESENT",
                        "VCF contains multi-ALT entries; Parser will split ALTs into per-allele records (SNVs only).",
                        {"count": multi_alt_count},
                    )
                )

    except gzip.BadGzipFile:
        errors.append(_error("INVALID_GZIP", "Uploaded .gz file is not a valid gzip archive."))
    except OSError as exc:
        errors.append(_error("READ_FAILED", "Failed to read uploaded file.", {"reason": str(exc)}))

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
