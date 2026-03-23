import os
import re
import sys
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_DIR)


class KeyboardAccessibilityTestCase(unittest.TestCase):
    def _get_index_html(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        self.assertEqual(resp.status_code, 200)
        html = resp.get_data(as_text=True)
        resp.close()
        return html

    def _get_script(self, path):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get(path)
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()
        return script

    def test_results_scroll_boxes_are_focusable_and_labeled(self):
        html = self._get_index_html()
        boxes = re.findall(r"<div[^>]*class=\"[^\"]*results-scroll-box[^\"]*\"[^>]*>", html)
        self.assertGreater(len(boxes), 0, "Expected results-scroll-box containers to exist")
        for tag in boxes:
            self.assertIn("tabindex=\"0\"", tag, f"Missing tabindex on: {tag}")
            self.assertRegex(
                tag,
                r"aria-label=\"[^\"]+\"|aria-labelledby=\"[^\"]+\"",
                f"Missing accessible label on: {tag}",
            )

    def test_run_logs_panel_is_focusable_and_labeled(self):
        html = self._get_index_html()
        self.assertRegex(html, r"id=\"run-logs-panel\"[^>]*tabindex=\"0\"")
        self.assertRegex(html, r"id=\"run-logs-panel\"[^>]*aria-label=\"Run logs\"")

    def test_html_artifact_iframe_is_labeled_and_focusable(self):
        script = self._get_script("/static/results_controls.js")

        self.assertRegex(script, r"renderHtmlArtifactCard[\s\S]*iframe\.setAttribute\(\"title\"")
        self.assertRegex(script, r"renderHtmlArtifactCard[\s\S]*iframe\.setAttribute\(\"tabindex\",\s*\"0\"\)")

    def test_workspace_tabs_have_keyboard_handler(self):
        script = self._get_script("/static/run_controls.js")

        self.assertIn("workspace-tabs", script)
        self.assertRegex(script, r"addEventListener\(\"keydown\",\s*handleWorkspaceTabKeydown\)")
        self.assertRegex(script, r"function handleWorkspaceTabKeydown[\s\S]*case \"Home\"")
        self.assertRegex(script, r"function handleWorkspaceTabKeydown[\s\S]*case \"End\"")
        self.assertRegex(script, r"moveWorkspaceTabFocus\([\s\S]*,\s*false\)")

    def test_variant_row_keyboard_activation_and_focus_return(self):
        script = self._get_script("/static/results_controls.js")
        self.assertRegex(script, r"tr\.addEventListener\(\"keydown\"[\s\S]*event\.key === \"Enter\"")
        self.assertRegex(script, r"tr\.addEventListener\(\"keydown\"[\s\S]*event\.key === \" \"")
        self.assertRegex(script, r"hidden\.bs\.offcanvas[\s\S]*lastTriggerEl")
        self.assertRegex(script, r"hidden\.bs\.offcanvas[\s\S]*\.focus\(")

    def test_live_updates_do_not_steal_focus(self):
        script = self._get_script("/static/run_controls.js")
        match = re.search(r"function setLiveUpdates[\s\S]*?^  }", script, re.MULTILINE)
        self.assertIsNotNone(match, "Expected setLiveUpdates function to exist")
        self.assertNotIn(".focus(", match.group(0))


if __name__ == "__main__":
    unittest.main()
