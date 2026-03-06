import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from vcf_validation import validate_vcf_path  # noqa: E402


class VcfValidationTestCase(unittest.TestCase):
    def test_empty_file_is_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.vcf")
            with open(path, "wb") as handle:
                handle.write(b"")

            result = validate_vcf_path(path)
            self.assertIs(result["ok"], False)
            codes = [err["code"] for err in result["errors"]]
            self.assertIn("EMPTY_FILE", codes)

    def test_missing_required_columns_is_error(self):
        contents = b"#CHROM\tPOS\tREF\n1\t1\tA\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "missing_cols.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            result = validate_vcf_path(path)
            self.assertIs(result["ok"], False)
            codes = [err["code"] for err in result["errors"]]
            self.assertIn("MISSING_REQUIRED_COLUMNS", codes)

    def test_not_tab_delimited_is_error(self):
        contents = b"#CHROM POS REF ALT\n1 1 A T\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "not_tab.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            result = validate_vcf_path(path)
            self.assertIs(result["ok"], False)
            codes = [err["code"] for err in result["errors"]]
            self.assertIn("NOT_TAB_DELIMITED", codes)

    def test_multi_alt_emits_warning(self):
        contents = (
            b"##fileformat=VCFv4.2\n"
            b"#CHROM\tPOS\tREF\tALT\n"
            b"1\t1\tA\tT,G\n"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "multi_alt.vcf")
            with open(path, "wb") as handle:
                handle.write(contents)

            result = validate_vcf_path(path)
            self.assertIs(result["ok"], True)
            codes = [warn["code"] for warn in result["warnings"]]
            self.assertIn("MULTI_ALT_PRESENT", codes)


if __name__ == "__main__":
    unittest.main()

