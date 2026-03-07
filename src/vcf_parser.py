import gzip
import os
from collections.abc import Iterator
from contextlib import contextmanager


BASES = {"A", "C", "G", "T"}
DEFAULT_MAX_DECOMPRESSED_BYTES = 250 * 1024 * 1024


class VcfParseError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        line_number: int | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.line_number = line_number
        self.details = details or {}


@contextmanager
def _open_binary(path: str) -> Iterator[object]:
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rb") as handle:
            yield handle
    else:
        with open(path, "rb") as handle:
            yield handle


def _normalize_chrom(raw: str) -> str:
    value = (raw or "").strip()
    lower = value.lower()
    if lower.startswith("chr"):
        value = value[3:]

    lower = value.lower()
    if lower in {"m", "mt"}:
        return "MT"

    upper = value.upper()
    if upper in {"X", "Y"}:
        return upper

    if value.isdigit():
        try:
            return str(int(value))
        except ValueError:
            return value

    return value


def _is_snv_allele(ref: str, alt: str) -> bool:
    if len(ref) != 1 or len(alt) != 1:
        return False
    if ref not in BASES or alt not in BASES:
        return False
    if ref == alt:
        return False
    return True


def iter_vcf_snv_records(
    path: str,
    *,
    stats: dict,
    sample: list[dict] | None = None,
    sample_limit: int = 0,
    max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES,
) -> Iterator[dict]:
    if not os.path.exists(path):
        raise VcfParseError("FILE_NOT_FOUND", "VCF attachment was not found on disk.")

    stats.setdefault("lines_read", 0)
    stats.setdefault("records_seen", 0)
    stats.setdefault("multi_alt_rows_seen", 0)
    stats.setdefault("snv_records_created", 0)
    stats.setdefault("non_snv_alleles_skipped", 0)

    header_columns: list[str] | None = None
    chrom_idx = pos_idx = ref_idx = alt_idx = -1
    saw_chrom_header = False

    try:
        bytes_read = 0
        with _open_binary(path) as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                bytes_read += len(raw_line)
                if max_decompressed_bytes and bytes_read > max_decompressed_bytes:
                    raise VcfParseError(
                        "DECOMPRESSED_TOO_LARGE",
                        "VCF is too large to parse safely after decompression.",
                        line_number=line_number,
                        details={"max_decompressed_bytes": max_decompressed_bytes},
                    )

                try:
                    line = raw_line.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise VcfParseError(
                        "INVALID_ENCODING",
                        "VCF must be valid UTF-8 text.",
                        line_number=line_number,
                        details={"reason": str(exc)},
                    ) from exc

                stats["lines_read"] += 1

                stripped = line.rstrip("\r\n")
                if not stripped:
                    continue

                if stripped.startswith("##"):
                    continue

                if stripped.startswith("#CHROM"):
                    saw_chrom_header = True
                    if "\t" not in stripped:
                        raise VcfParseError(
                            "NOT_TAB_DELIMITED",
                            "VCF header must be tab-delimited.",
                            line_number=line_number,
                        )
                    header_columns = stripped.split("\t")
                    required = ["#CHROM", "POS", "REF", "ALT"]
                    missing = [name for name in required if name not in header_columns]
                    if missing:
                        raise VcfParseError(
                            "MISSING_REQUIRED_COLUMNS",
                            "VCF header is missing required columns.",
                            line_number=line_number,
                            details={"missing": missing},
                        )
                    chrom_idx = header_columns.index("#CHROM")
                    pos_idx = header_columns.index("POS")
                    ref_idx = header_columns.index("REF")
                    alt_idx = header_columns.index("ALT")
                    continue

                if stripped.startswith("#"):
                    continue

                if header_columns is None:
                    raise VcfParseError(
                        "MISSING_CHROM_HEADER",
                        "VCF data encountered before the #CHROM header line.",
                        line_number=line_number,
                    )

                parts = stripped.split("\t")
                if len(parts) <= max(chrom_idx, pos_idx, ref_idx, alt_idx):
                    raise VcfParseError(
                        "MALFORMED_ROW",
                        "A data row does not match the header column structure.",
                        line_number=line_number,
                    )

                stats["records_seen"] += 1

                chrom = _normalize_chrom(parts[chrom_idx])
                pos_raw = parts[pos_idx]
                ref = (parts[ref_idx] or "").upper()
                alt_field = parts[alt_idx] or ""

                try:
                    pos = int(pos_raw)
                except ValueError:
                    raise VcfParseError(
                        "INVALID_POS",
                        "VCF POS value is not an integer.",
                        line_number=line_number,
                        details={"pos": pos_raw},
                    ) from None

                alts = alt_field.split(",") if alt_field else [""]
                if len(alts) > 1:
                    stats["multi_alt_rows_seen"] += 1

                for alt_raw in alts:
                    alt = (alt_raw or "").upper()
                    if not alt or alt == "." or alt == "*" or alt.startswith("<"):
                        stats["non_snv_alleles_skipped"] += 1
                        continue

                    if not _is_snv_allele(ref, alt):
                        stats["non_snv_alleles_skipped"] += 1
                        continue

                    stats["snv_records_created"] += 1
                    record = {
                        "chrom": chrom,
                        "pos": pos,
                        "ref": ref,
                        "alt": alt,
                        "source_line": line_number,
                    }
                    if sample is not None and sample_limit and len(sample) < sample_limit:
                        sample.append(record)
                    yield record
    except gzip.BadGzipFile as exc:
        raise VcfParseError("INVALID_GZIP", "Uploaded .gz file is not a valid gzip archive.") from exc
    except OSError as exc:
        raise VcfParseError("READ_FAILED", "Failed to read VCF attachment.", details={"reason": str(exc)}) from exc

    if not saw_chrom_header:
        raise VcfParseError("MISSING_CHROM_HEADER", "VCF is missing the #CHROM header line.")

    return


def parse_vcf_to_snvs(path: str, *, sample_limit: int = 10) -> tuple[list[dict], dict]:
    sample: list[dict] = []
    stats: dict = {}
    for _ in iter_vcf_snv_records(path, stats=stats, sample=sample, sample_limit=sample_limit):
        pass
    return sample, stats
