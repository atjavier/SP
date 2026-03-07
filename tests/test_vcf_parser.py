import gzip
import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from vcf_parser import VcfParseError, parse_vcf_to_snvs  # noqa: E402


class VcfParserTestCase(unittest.TestCase):
    def test_parser_splits_multi_alt_and_filters_non_snv_and_normalizes(self):
        contents = (
            b"##fileformat=VCFv4.2\n"
            b"#CHROM\tPOS\tREF\tALT\n"
            b"chr01\t10\ta\tT,G,<DEL>,.\n"
            b"1\t11\tAT\tA\n"
            b"chrM\t12\tC\tt\n"
            b"X\t13\tN\tA\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sample.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            snvs, stats = parse_vcf_to_snvs(path, sample_limit=100)

            self.assertEqual(
                snvs,
                [
                    {"chrom": "1", "pos": 10, "ref": "A", "alt": "T", "source_line": 3},
                    {"chrom": "1", "pos": 10, "ref": "A", "alt": "G", "source_line": 3},
                    {"chrom": "MT", "pos": 12, "ref": "C", "alt": "T", "source_line": 5},
                ],
            )
            self.assertEqual(stats["multi_alt_rows_seen"], 1)
            self.assertEqual(stats["snv_records_created"], 3)
            self.assertEqual(stats["non_snv_alleles_skipped"], 4)

    def test_parser_error_includes_line_number(self):
        contents = b"#CHROM\tPOS\tREF\tALT\n1\t10\tA\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "bad.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            with self.assertRaises(VcfParseError) as ctx:
                parse_vcf_to_snvs(path)

            exc = ctx.exception
            self.assertIsInstance(exc.code, str)
            self.assertEqual(exc.line_number, 2)

    def test_parser_handles_gz(self):
        contents = b"#CHROM\tPOS\tREF\tALT\n1\t10\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sample.vcf.gz")
            with gzip.open(path, "wb") as handle:
                handle.write(contents)

            snvs, stats = parse_vcf_to_snvs(path, sample_limit=10)
            self.assertEqual(snvs, [{"chrom": "1", "pos": 10, "ref": "A", "alt": "T", "source_line": 2}])
            self.assertEqual(stats["snv_records_created"], 1)

    def test_parser_fails_when_data_before_header(self):
        contents = b"1\t10\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "no_header.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            with self.assertRaises(VcfParseError) as ctx:
                parse_vcf_to_snvs(path)
            self.assertEqual(ctx.exception.code, "MISSING_CHROM_HEADER")
            self.assertEqual(ctx.exception.line_number, 1)

    def test_parser_invalid_gzip(self):
        contents = b"not really gzip"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "bad.vcf.gz")
            with open(path, "wb") as handle:
                handle.write(contents)

            with self.assertRaises(VcfParseError) as ctx:
                parse_vcf_to_snvs(path)
            self.assertEqual(ctx.exception.code, "INVALID_GZIP")


if __name__ == "__main__":
    unittest.main()
