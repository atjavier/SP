from __future__ import annotations

import argparse
import json
import os
import sys


def _chrom_without_chr(chrom: str) -> str:
    value = (chrom or "").strip()
    if value.lower().startswith("chr"):
        return value[3:]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--input_file", required=True)
    parser.add_argument("--output_file")
    args, _unknown = parser.parse_known_args()

    forced_exit_code = (os.environ.get("SP_MOCK_VEP_EXIT_CODE") or "").strip()
    if forced_exit_code:
        try:
            return int(forced_exit_code)
        except ValueError:
            return 2

    lines: list[str] = []
    with open(args.input_file, "r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            fields = stripped.split("\t")
            if len(fields) < 5:
                continue
            chrom, pos_raw, _id, ref, alt_raw = fields[:5]
            alt = alt_raw.split(",")[0]
            try:
                pos = int(pos_raw)
            except ValueError:
                continue
            payload = {
                "seq_region_name": _chrom_without_chr(chrom),
                "start": pos,
                "allele_string": f"{ref}/{alt}",
                "most_severe_consequence": "missense_variant",
                "transcript_consequences": [
                    {
                        "consequence_terms": ["missense_variant"],
                        "sift_prediction": "deleterious",
                        "sift_score": 0.05,
                        "polyphen_prediction": "possibly_damaging",
                        "polyphen_score": 0.65,
                        "am_pathogenicity": 0.42,
                        "am_class": "ambiguous",
                    }
                ],
            }
            lines.append(json.dumps(payload))

    if os.environ.get("SP_MOCK_VEP_WRITE_INVALID_JSON") == "1":
        lines.append("{invalid_json")

    rendered = "\n".join(lines)
    if rendered:
        rendered += "\n"

    if args.output_file:
        with open(args.output_file, "w", encoding="utf-8", newline="\n") as out_f:
            out_f.write(rendered)
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
