import os
import sys
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_DIR)


class AppTestCase(unittest.TestCase):
    def test_create_app_exists(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        self.assertIsNotNone(flask_app)

    def test_index_route_returns_200(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)

    def test_index_includes_app_css_link(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)
        self.assertIn("/static/app.css", html)

    def test_index_contains_required_sections_and_upload_accessibility(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)

        self.assertIn("Upload", html)
        self.assertIn("Run Status", html)
        self.assertIn("Results", html)

        self.assertIn("id=\"upload-section\"", html)
        self.assertIn("id=\"run-status-section\"", html)
        self.assertIn("id=\"results-section\"", html)

        self.assertIn("id=\"vcf-file\"", html)
        self.assertIn("for=\"vcf-file\"", html)
        self.assertIn("id=\"vcf-file-help\"", html)
        self.assertIn("aria-describedby=\"vcf-file-help\"", html)

        self.assertIn("New run", html)
        self.assertIn("Cancel run", html)
        self.assertIn("id=\"new-run-btn\"", html)
        self.assertIn("id=\"cancel-run-btn\"", html)
        self.assertIn("id=\"current-run-status\"", html)


if __name__ == "__main__":
    unittest.main()
