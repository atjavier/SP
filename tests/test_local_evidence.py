import os
import sys
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class LocalEvidenceTestCase(unittest.TestCase):
    def test_chrom_candidates_optionally_include_refseq(self):
        from pipeline.local_evidence import _chrom_candidates  # noqa: E402

        basic = _chrom_candidates("1")
        with_refseq = _chrom_candidates("1", include_refseq=True)

        self.assertIn("1", basic)
        self.assertIn("chr1", basic)
        self.assertNotIn("NC_000001.11", basic)
        self.assertIn("NC_000001.11", with_refseq)

    def test_local_dbsnp_lookup_tries_refseq_contig_names(self):
        from pipeline.local_evidence import fetch_dbsnp_evidence_from_local_vcf  # noqa: E402

        class Completed:
            def __init__(self, returncode: int, stdout: str, stderr: str):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        queried_regions: list[str] = []

        def fake_run(cmd, **kwargs):  # noqa: ANN001
            del kwargs
            region = cmd[2]
            queried_regions.append(region)
            if region == "NC_000001.11:100-100":
                return Completed(
                    0,
                    "NC_000001.11\t100\trs555\tA\tG\t.\t.\tRS=555\n",
                    "",
                )
            return Completed(1, "", "sequence not found")

        with (
            patch(
                "pipeline.local_evidence._resolve_local_vcf_file_for_chrom",
                return_value="/tmp/dbsnp.vcf.gz",
            ),
            patch("pipeline.local_evidence.subprocess.run", side_effect=fake_run),
        ):
            result = fetch_dbsnp_evidence_from_local_vcf(
                local_vcf_path="/tmp/dbsnp.vcf.gz",
                chrom="1",
                pos=100,
                ref="A",
                alt="G",
                timeout_seconds=5,
            )

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("rsid"), "rs555")
        self.assertIn("NC_000001.11:100-100", queried_regions)


if __name__ == "__main__":
    unittest.main()
