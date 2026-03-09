import os
import re
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

        self.assertIn("Start", html)
        self.assertIn("Workspace", html)
        self.assertIn("Progress", html)
        self.assertIn("Results", html)

        self.assertIn("id=\"upload-section\"", html)
        self.assertIn("id=\"workspace-section\"", html)

        self.assertIn("id=\"vcf-file\"", html)
        self.assertIn("for=\"vcf-file\"", html)
        self.assertIn("id=\"vcf-file-help\"", html)
        self.assertIn("aria-describedby=\"vcf-file-help\"", html)

        self.assertIn("id=\"start-btn\"", html)
        self.assertIn("id=\"new-run-btn\"", html)
        self.assertIn("id=\"upload-validation-message\"", html)
        self.assertIn("id=\"upload-validation-results\"", html)

        self.assertIn("Cancel run", html)
        self.assertIn("id=\"cancel-run-btn\"", html)
        self.assertIn("id=\"current-run-status\"", html)
        self.assertIn("id=\"current-run-reference-build\"", html)
        self.assertIn("id=\"current-run-stages\"", html)
        self.assertIn("id=\"current-run-stages-message\"", html)
        self.assertIn("id=\"annotation-vcf-message\"", html)
        self.assertIn("id=\"annotation-vcf-table\"", html)
        self.assertIn("id=\"annotation-vcf-head-row\"", html)
        self.assertIn("id=\"annotation-vcf-body\"", html)

        self.assertNotIn("id=\"upload-validate-btn\"", html)
        self.assertNotIn("id=\"start-run-btn\"", html)

    def test_results_controls_has_drawer_failure_resume_guard(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/results_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertRegex(
            script,
            r"function resumeAfterDrawerFailure\(\)\s*{[\s\S]*?pausePolling = false;[\s\S]*?scheduleNextRefresh\(0\);",
        )
        self.assertRegex(
            script,
            r"Offcanvas\.getOrCreateInstance\(offcanvasEl\)\.show\(\);\s*}\s*catch\s*{\s*resumeAfterDrawerFailure\(\);\s*return;\s*}",
        )
        self.assertRegex(
            script,
            r'hidden\.bs\.offcanvas[\s\S]*toFocus\.setAttribute\("aria-expanded", "false"\);[\s\S]*catch\s*{\s*// keep cleanup/resume path running even if focus management fails\s*}',
        )
        self.assertRegex(
            script,
            r'hidden\.bs\.offcanvas[\s\S]*toFocus\.focus\(\);[\s\S]*catch\s*{\s*// keep cleanup/resume path running even if focus management fails\s*}',
        )


if __name__ == "__main__":
    unittest.main()
