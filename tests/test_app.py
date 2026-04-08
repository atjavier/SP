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
        self.assertIn("Run summary", html)

        self.assertIn("id=\"upload-section\"", html)
        self.assertIn("id=\"workspace-section\"", html)

        self.assertIn("id=\"vcf-file\"", html)
        self.assertIn("for=\"vcf-file\"", html)
        self.assertIn("id=\"vcf-file-help\"", html)
        self.assertIn("aria-describedby=\"vcf-file-help\"", html)
        self.assertIn("id=\"annotation-evidence-policy-stop\"", html)
        self.assertIn("id=\"annotation-evidence-policy-continue\"", html)
        self.assertIn("id=\"annotation-evidence-policy-help\"", html)

        self.assertIn("id=\"start-btn\"", html)
        self.assertIn("id=\"new-run-btn\"", html)
        self.assertIn("id=\"upload-validation-message\"", html)
        self.assertIn("id=\"upload-validation-results\"", html)
        self.assertRegex(html, r'id="upload-validation-results"[^>]*role="status"')
        self.assertRegex(html, r'id="upload-validation-results"[^>]*aria-live="polite"')
        self.assertRegex(html, r'id="upload-validation-results"[^>]*aria-label="Validation details"')

        self.assertIn("Cancel run", html)
        self.assertIn("id=\"cancel-run-btn\"", html)
        self.assertIn("id=\"current-run-status\"", html)
        self.assertIn("id=\"current-run-reference-build\"", html)
        self.assertIn("id=\"current-run-stages\"", html)
        self.assertIn("id=\"current-run-stages-message\"", html)
        self.assertIn("id=\"run-logs-panel\"", html)
        self.assertIn("id=\"run-logs-console\"", html)
        self.assertIn("id=\"run-logs-message\"", html)
        self.assertIn("id=\"annotation-vcf-message\"", html)
        self.assertIn("id=\"annotation-vcf-table\"", html)
        self.assertIn("id=\"annotation-vcf-head-row\"", html)
        self.assertIn("id=\"annotation-vcf-body\"", html)
        self.assertIn("id=\"variant-evidence-message\"", html)
        self.assertIn("id=\"variant-ev-dbsnp-completeness\"", html)
        self.assertIn("id=\"variant-ev-clinvar-completeness\"", html)
        self.assertIn("id=\"variant-ev-gnomad-completeness\"", html)
        self.assertIn("Pipeline-level outcome only.", html)
        self.assertIn("Stage-level technical output:", html)
        self.assertIn("Reports Summary", html)
        self.assertIn("reporting-significant-results", html)
        self.assertIn("reporting-evidence-diagnostics", html)
        self.assertIn("reporting-summary-note", html)
        self.assertIn("variant-summary-completeness-filter", html)
        self.assertIn("annotation-evidence-classification-filter", html)

        self.assertNotIn("id=\"upload-validate-btn\"", html)
        self.assertNotIn("id=\"start-run-btn\"", html)

    def test_index_includes_accessible_table_captions_and_controls(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)

        for caption in [
            "Variant summary table",
            "Pre-annotation results table",
            "Classification results table",
            "Classification input VCF preview table",
            "Prediction results table",
            "Prediction input VCF preview table",
            "Annotation diagnostics table",
            "dbSNP evidence results table",
            "ClinVar evidence results table",
            "gnomAD evidence results table",
            "Annotated VCF preview table",
        ]:
            self.assertIn(caption, html)

        self.assertIn('aria-label="Workspace tabs"', html)
        self.assertIn('aria-label="Results stage tabs"', html)
        self.assertIn('aria-label="Previous variant summary page"', html)
        self.assertIn('aria-label="Next variant summary page"', html)
        self.assertIn('aria-label="Search classification input VCF by position"', html)
        self.assertIn('aria-label="Search prediction input VCF by position"', html)
        self.assertIn('aria-label="Search annotated VCF by position"', html)
        self.assertIn('aria-labelledby="html-artifacts-heading"', html)

    def test_index_includes_tooltip_glossary_markers(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)

        self.assertIn("/static/tooltip_controls.js", html)
        self.assertIn('data-tooltip-key="section.run_summary"', html)
        self.assertIn('data-tooltip-key="stage.parser"', html)
        self.assertIn('data-tooltip-key="term.completeness"', html)
        self.assertIn('data-tooltip-key="provenance.retrieved"', html)
        self.assertIn('data-tooltip-key="evidence_policy.stop"', html)
        self.assertIn('data-tooltip-key="evidence_policy.continue"', html)

    def test_tooltip_controls_script_exposes_glossary(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/tooltip_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn("TOOLTIP_GLOSSARY", script)
        self.assertIn("section.run_summary", script)
        self.assertIn("stage.parser", script)
        self.assertIn("status.succeeded", script)

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
        self.assertIn("/dbsnp_evidence?variant_id=", script)
        self.assertIn("/clinvar_evidence?variant_id=", script)
        self.assertIn("/gnomad_evidence?variant_id=", script)
        self.assertIn("variant-ev-dbsnp-completeness", script)
        self.assertIn("variant-ev-clinvar-completeness", script)
        self.assertIn("variant-ev-gnomad-completeness", script)
        self.assertIn("function evidenceSourceLabel", script)
        self.assertIn("source_mode", script)
        self.assertIn("variantDetailsRequestSeq", script)
        self.assertIn("requestToken === variantDetailsRequestSeq", script)
        self.assertIn("Variant ID unavailable.", script)
        self.assertIn("See Annotation for per-source diagnostics and error breakdowns.", script)
        self.assertIn("EVIDENCE_SOURCES_UNAVAILABLE", script)
        self.assertIn("Missing sources", script)
        self.assertIn("Blocked outputs", script)
        self.assertNotRegex(script, r"classification\\s*&&\\s*classification\\s*!==\\s*\"all\"")
        self.assertIn("renderEvidenceDiagnosticsTable", script)

    def test_results_controls_rows_are_actionable(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/results_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertRegex(
            script,
            r"function wireVariantRowSelection\([^)]*\)\s*{[\s\S]*?tabIndex\s*=\s*0;[\s\S]*?addEventListener\(\"click\"",
        )
        self.assertRegex(
            script,
            r"function wireVariantRowSelection\([^)]*\)\s*{[\s\S]*?setAttribute\(\"role\", \"button\"\)",
        )
        self.assertRegex(
            script,
            r"function wireVariantRowSelection\([^)]*\)\s*{[\s\S]*?setAttribute\(\"aria-label\",",
        )
        self.assertRegex(
            script,
            r"function wireVariantRowSelection\([^)]*\)\s*{[\s\S]*?addEventListener\(\"keydown\"",
        )
        self.assertRegex(
            script,
            r"function renderVariantSummaryRows\([^)]*\)\s*{[\s\S]*?wireVariantRowSelection\(row,\s*tr\)",
        )
        self.assertRegex(
            script,
            r"function renderPredictionRows\([^)]*\)\s*{[\s\S]*?wireVariantRowSelection\(row,\s*tr\)",
        )

    def test_run_controls_dispatches_run_changed_and_ignores_self_events(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/run_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertRegex(
            script,
            r"function dispatchRunChanged\(run\)\s*{[\s\S]*?new CustomEvent\(\"sp:run-changed\",[\s\S]*?source:\s*\"run-controls\"",
        )
        self.assertRegex(
            script,
            r"function setRun\(run\)\s*{[\s\S]*?dispatchRunChanged\(run\);",
        )
        self.assertRegex(
            script,
            r"window\.addEventListener\(\"sp:run-changed\",[\s\S]*?detail\?\.source === \"run-controls\"[\s\S]*?return;",
        )
        self.assertIn("refreshFromServer(runId);", script)
        self.assertIn("EVIDENCE_SOURCES_UNAVAILABLE", script)
        self.assertIn("Missing evidence sources", script)

    def test_run_controls_dispatches_variant_result_event(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/run_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn('addEventListener("variant_result"', script)
        self.assertIn('new CustomEvent("sp:variant-result"', script)

    def test_results_controls_listens_for_variant_result(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/results_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn('addEventListener("sp:variant-result"', script)

    def test_status_indicator_css_utility_exists(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/app.css")
        self.assertEqual(resp.status_code, 200)
        css = resp.get_data(as_text=True)
        resp.close()

        self.assertIn(".status-indicator", css)
        self.assertIn(".status-icon", css)
        self.assertIn(".workspace-block", css)
        self.assertIn(".workspace-block-title", css)

    def test_run_controls_uses_status_indicator_helper(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/run_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn("status-indicator", script)
        self.assertRegex(script, r"function buildStatusIndicator\([^)]*\)")
        self.assertRegex(script, r"function setStatusIndicator\([^)]*\)")
        self.assertRegex(script, r"setStatusIndicator\(statusEl,")
        self.assertRegex(script, r"setStatusIndicator\(badgeEl,")
        self.assertIn('labelSpan.className = "status-label"', script)
        self.assertIn("STATUS_LABEL_OVERRIDES", script)
        self.assertIn("Queued", script)
        self.assertIn("Running", script)
        self.assertIn("Succeeded", script)
        self.assertIn("Failed", script)
        self.assertIn("Canceled", script)

    def test_results_controls_uses_status_indicator_helper(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/results_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn("status-indicator", script)
        self.assertRegex(script, r"function buildStatusIndicator\([^)]*\)")
        self.assertRegex(script, r"function setStatusIndicator\([^)]*\)")
        self.assertRegex(
            script,
            r"function renderVariantSummaryRows\([^)]*\)\s*{[\s\S]*?setStatusIndicator",
        )
        self.assertRegex(
            script,
            r"function renderEvidenceDiagnosticsTable\([^)]*\)\s*{[\s\S]*?buildStatusIndicator",
        )
        self.assertIn('labelSpan.className = "status-label"', script)
        self.assertIn("STATUS_LABEL_OVERRIDES", script)
        self.assertIn("Partial", script)
        self.assertIn("Unavailable", script)
        self.assertIn("Not applicable", script)
        self.assertIn("Not available", script)

    def test_docs_route_returns_200(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/docs")
        self.assertEqual(resp.status_code, 200)

    def test_nav_includes_docs_link(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)
        self.assertIn('href="/docs"', html)
        self.assertIn('aria-label="Docs"', html)

    def test_branding_and_sidebar_toggle(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/")
        html = resp.get_data(as_text=True)

        self.assertIn("BioEvidence", html)
        self.assertIn("Teach and trace SNV outcomes.", html)
        self.assertIn("<title>BioEvidence", html)
        self.assertIn('id="sidebar-toggle"', html)
        self.assertIn('aria-controls="app-sidebar"', html)

        resp = client.get("/docs")
        docs_html = resp.get_data(as_text=True)
        self.assertIn("BioEvidence Documentation", docs_html)
        self.assertIn("Teach and trace SNV outcomes.", docs_html)

    def test_sidebar_controls_script_served(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/static/sidebar_controls.js")
        self.assertEqual(resp.status_code, 200)
        script = resp.get_data(as_text=True)
        resp.close()

        self.assertIn("sp_sidebar_collapsed", script)

    def test_docs_contains_required_sections_and_anchors(self):
        import app as sp_app

        flask_app = sp_app.create_app()
        client = flask_app.test_client()
        resp = client.get("/docs")
        html = resp.get_data(as_text=True)

        self.assertIn("Getting Started", html)
        self.assertIn("Pipeline Stages", html)
        self.assertIn("Tools and Evidence Sources", html)
        self.assertIn("Evidence Modes and Policy", html)
        self.assertIn("Results Interpretation and Provenance", html)
        self.assertIn("Troubleshooting and FAQ", html)

        for anchor in [
            "docs-getting-started",
            "docs-pipeline-stages",
            "docs-tools-evidence",
            "docs-evidence-modes",
            "docs-results-provenance",
            "docs-troubleshooting",
        ]:
            self.assertIn(f'id=\"{anchor}\"', html)
            self.assertIn(f'href=\"#{anchor}\"', html)


if __name__ == "__main__":
    unittest.main()
