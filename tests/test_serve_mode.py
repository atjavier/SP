import os
import sys
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class ServeModeTestCase(unittest.TestCase):
    def test_serve_module_importable_without_waitress(self):
        import serve as sp_serve

        self.assertTrue(callable(getattr(sp_serve, "main", None)))

    def test_serve_import_does_not_import_waitress(self):
        import serve  # noqa: F401

        self.assertNotIn("waitress", sys.modules)


if __name__ == "__main__":
    unittest.main()
