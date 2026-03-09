import io
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class OrchestratorApiTestCase(unittest.TestCase):
    def _create_client(self, db_path: str):
        import app as sp_app  # noqa: E402

        flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
        return flask_app.test_client()

    def _create_run(self, client) -> str:
        created = json.loads(client.post("/api/v1/runs").get_data(as_text=True))
        return created["data"]["run_id"]

    def _upload(self, client, run_id: str, vcf_bytes: bytes, filename: str = "sample.vcf"):
        return client.post(
            f"/api/v1/runs/{run_id}/vcf",
            data={"vcf_file": (io.BytesIO(vcf_bytes), filename)},
            content_type="multipart/form-data",
        )

    def _wait_for_all_stages_succeeded(self, client, run_id: str, timeout_s: float = 5.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            stages_payload = json.loads(
                client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
            )
            stages = stages_payload["data"]["stages"]
            if stages and all(stage["status"] == "succeeded" for stage in stages):
                return stages
            time.sleep(0.01)
        self.fail("Timed out waiting for stages to succeed.")

    def _wait_for_run_not_running(self, client, run_id: str, timeout_s: float = 5.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
            status = run_payload["data"]["status"]
            if status != "running":
                return status
            time.sleep(0.01)
        self.fail("Timed out waiting for run to stop running.")

    def test_start_runs_all_stages_and_resets_run_to_queued(self):
        vcf_bytes = b"##fileformat=VCFv4.2\n#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["started_stage"], "parser")

            stages = self._wait_for_all_stages_succeeded(client, run_id)
            status = self._wait_for_run_not_running(client, run_id)
            self.assertEqual(status, "queued")

            by_name = {stage["stage_name"]: stage for stage in stages}
            prediction_stats = by_name["prediction"]["stats"]
            self.assertEqual(prediction_stats["predictors_executed"], ["sift", "polyphen2", "alphamissense"])
            self.assertGreaterEqual(prediction_stats["variants_processed"], 1)
            self.assertEqual(prediction_stats["outputs_persisted_by_predictor"]["sift"], 1)
            self.assertEqual(prediction_stats["outputs_persisted_by_predictor"]["polyphen2"], 1)
            self.assertEqual(prediction_stats["outputs_persisted_by_predictor"]["alphamissense"], 1)

            conn = sqlite3.connect(db_path)
            try:
                sift_count = conn.execute(
                    "SELECT COUNT(*) FROM run_predictor_outputs WHERE run_id = ? AND predictor_key = ?",
                    (run_id, "sift"),
                ).fetchone()[0]
                polyphen2_count = conn.execute(
                    "SELECT COUNT(*) FROM run_predictor_outputs WHERE run_id = ? AND predictor_key = ?",
                    (run_id, "polyphen2"),
                ).fetchone()[0]
                alphamissense_count = conn.execute(
                    "SELECT COUNT(*) FROM run_predictor_outputs WHERE run_id = ? AND predictor_key = ?",
                    (run_id, "alphamissense"),
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(sift_count, 1)
            self.assertEqual(polyphen2_count, 1)
            self.assertEqual(alphamissense_count, 1)

            for stage in stages:
                self.assertEqual(stage["status"], "succeeded")
                self.assertIsInstance(stage.get("started_at"), str)
                self.assertIsInstance(stage.get("completed_at"), str)

                started = datetime.fromisoformat(stage["started_at"])
                completed = datetime.fromisoformat(stage["completed_at"])
                self.assertIsNotNone(started.tzinfo)
                self.assertIsNotNone(completed.tzinfo)
                self.assertGreaterEqual(completed, started)

            # run already awaited above

    def test_start_persists_pre_annotations_for_run_variants(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)

            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            conn = sqlite3.connect(db_path)
            try:
                variants = conn.execute(
                    "SELECT COUNT(*) FROM run_variants WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                anns = conn.execute(
                    "SELECT COUNT(*) FROM run_pre_annotations WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                self.assertEqual(variants, 1)
                self.assertEqual(anns, variants)

                row = conn.execute(
                    """
                    SELECT substitution_class, ref_class, alt_class
                    FROM run_pre_annotations
                    WHERE run_id = ? AND variant_key = ?
                    """,
                    (run_id, "1:1:A>G"),
                ).fetchone()
                self.assertEqual(row, ("transition", "purine", "purine"))
            finally:
                conn.close()

    def test_start_persists_classifications_for_run_variants(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)

            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            conn = sqlite3.connect(db_path)
            try:
                variants = conn.execute(
                    "SELECT COUNT(*) FROM run_variants WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                classified = conn.execute(
                    "SELECT COUNT(*) FROM run_classifications WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                self.assertEqual(variants, 1)
                self.assertEqual(classified, variants)

                row = conn.execute(
                    """
                    SELECT consequence_category, reason_code
                    FROM run_classifications
                    WHERE run_id = ?
                    """,
                    (run_id,),
                ).fetchone()
                self.assertEqual(row, ("missense", None))
            finally:
                conn.close()

            listing = client.get(f"/api/v1/runs/{run_id}/classifications?limit=10")
            self.assertEqual(listing.status_code, 200)
            payload = json.loads(listing.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            items = payload["data"]["classifications"]
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["variant_key"], "1:1:A>G")
            self.assertEqual(items[0]["consequence_category"], "missense")
            self.assertIsNone(items[0]["reason_code"])

    def test_predictor_outputs_endpoint_is_stage_gated_to_latest_upload(self):
        vcf_bytes_1 = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_bytes_2 = b"#CHROM\tPOS\tREF\tALT\n1\t2\tC\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes_1).status_code, 200)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            outputs_payload = json.loads(
                client.get(f"/api/v1/runs/{run_id}/predictor_outputs").get_data(as_text=True)
            )
            self.assertTrue(outputs_payload["ok"])
            self.assertEqual(outputs_payload["data"]["run_id"], run_id)
            self.assertEqual(outputs_payload["data"]["stage"]["status"], "succeeded")
            predictor_outputs = outputs_payload["data"]["predictor_outputs"]
            self.assertEqual(len(predictor_outputs), 3)
            self.assertEqual(
                {o["predictor_key"] for o in predictor_outputs}, {"sift", "polyphen2", "alphamissense"}
            )
            variant_id = predictor_outputs[0]["variant_id"]
            self.assertTrue(all(o["variant_id"] == variant_id for o in predictor_outputs))

            by_variant_payload = json.loads(
                client.get(
                    f"/api/v1/runs/{run_id}/predictor_outputs?variant_id={variant_id}"
                ).get_data(as_text=True)
            )
            self.assertTrue(by_variant_payload["ok"])
            self.assertEqual(by_variant_payload["data"]["run_id"], run_id)
            self.assertEqual(len(by_variant_payload["data"]["predictor_outputs"]), 3)
            self.assertEqual(
                {o["predictor_key"] for o in by_variant_payload["data"]["predictor_outputs"]},
                {"sift", "polyphen2", "alphamissense"},
            )
            self.assertTrue(
                all(o["variant_id"] == variant_id for o in by_variant_payload["data"]["predictor_outputs"])
            )

            polyphen2_payload = json.loads(
                client.get(
                    f"/api/v1/runs/{run_id}/predictor_outputs?predictor_key=polyphen2"
                ).get_data(as_text=True)
            )
            self.assertTrue(polyphen2_payload["ok"])
            self.assertEqual(len(polyphen2_payload["data"]["predictor_outputs"]), 1)
            self.assertEqual(
                polyphen2_payload["data"]["predictor_outputs"][0]["predictor_key"], "polyphen2"
            )

            alphamissense_payload = json.loads(
                client.get(
                    f"/api/v1/runs/{run_id}/predictor_outputs?predictor_key=alphamissense"
                ).get_data(as_text=True)
            )
            self.assertTrue(alphamissense_payload["ok"])
            self.assertEqual(len(alphamissense_payload["data"]["predictor_outputs"]), 1)
            self.assertEqual(
                alphamissense_payload["data"]["predictor_outputs"][0]["predictor_key"],
                "alphamissense",
            )

            polyphen2_by_variant_payload = json.loads(
                client.get(
                    f"/api/v1/runs/{run_id}/predictor_outputs?variant_id={variant_id}&predictor_key=polyphen2"
                ).get_data(as_text=True)
            )
            self.assertTrue(polyphen2_by_variant_payload["ok"])
            self.assertEqual(len(polyphen2_by_variant_payload["data"]["predictor_outputs"]), 1)
            self.assertEqual(
                polyphen2_by_variant_payload["data"]["predictor_outputs"][0]["predictor_key"], "polyphen2"
            )
            self.assertEqual(
                polyphen2_by_variant_payload["data"]["predictor_outputs"][0]["variant_id"], variant_id
            )

            self.assertEqual(self._upload(client, run_id, vcf_bytes_2, filename="sample2.vcf").status_code, 200)
            stale_payload = json.loads(
                client.get(f"/api/v1/runs/{run_id}/predictor_outputs").get_data(as_text=True)
            )
            self.assertTrue(stale_payload["ok"])
            self.assertEqual(stale_payload["data"]["run_id"], run_id)
            self.assertEqual(stale_payload["data"]["predictor_outputs"], [])

            stale_by_variant_payload = json.loads(
                client.get(
                    f"/api/v1/runs/{run_id}/predictor_outputs?variant_id={variant_id}"
                ).get_data(as_text=True)
            )
            self.assertTrue(stale_by_variant_payload["ok"])
            self.assertEqual(stale_by_variant_payload["data"]["run_id"], run_id)
            self.assertEqual(stale_by_variant_payload["data"]["predictor_outputs"], [])

    def test_index_variant_details_includes_predictions_section(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            resp = client.get("/")
            self.assertEqual(resp.status_code, 200)
            html = resp.get_data(as_text=True)

            self.assertIn('id="variant-details-offcanvas"', html)
            self.assertIn('id="new-run-btn"', html)
            self.assertNotIn('id="start-run-btn"', html)
            self.assertIn('id="start-btn"', html)
            self.assertIn('id="variant-predictions-message"', html)
            self.assertIn('id="variant-pred-sift-outcome"', html)
            self.assertIn('id="variant-pred-polyphen2-outcome"', html)
            self.assertIn('id="variant-pred-alphamissense-outcome"', html)
            self.assertIn('id="prediction-show-not-applicable"', html)

    def test_pre_annotations_endpoint_returns_rows_and_gates_to_latest_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n"
        vcf_bytes_new = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            uploaded_at = upload_payload["data"]["uploaded_at"]

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            cls_listing = client.get(f"/api/v1/runs/{run_id}/classifications?limit=10")
            self.assertEqual(cls_listing.status_code, 200)
            cls_payload = json.loads(cls_listing.get_data(as_text=True))
            self.assertIs(cls_payload.get("ok"), True)
            self.assertEqual(len(cls_payload["data"]["classifications"]), 1)
            variant_id = cls_payload["data"]["classifications"][0]["variant_id"]
            variant_key = cls_payload["data"]["classifications"][0]["variant_key"]

            pre_listing = client.get(f"/api/v1/runs/{run_id}/pre_annotations?variant_id={variant_id}")
            self.assertEqual(pre_listing.status_code, 200)
            pre_payload = json.loads(pre_listing.get_data(as_text=True))
            self.assertIs(pre_payload.get("ok"), True)
            pre_rows = pre_payload["data"]["pre_annotations"]
            self.assertEqual(len(pre_rows), 1)
            self.assertEqual(pre_rows[0]["variant_id"], variant_id)
            self.assertEqual(pre_rows[0]["variant_key"], variant_key)

            upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
            uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            if uploaded_at_2 == uploaded_at:
                upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
                uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at, uploaded_at_2)

            stale_listing = client.get(f"/api/v1/runs/{run_id}/pre_annotations?variant_id={variant_id}")
            self.assertEqual(stale_listing.status_code, 200)
            stale_payload = json.loads(stale_listing.get_data(as_text=True))
            self.assertIs(stale_payload.get("ok"), True)
            self.assertEqual(stale_payload["data"]["pre_annotations"], [])

    def test_annotation_output_endpoint_returns_preview_and_gates_to_latest_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n"
        vcf_bytes_new = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            uploaded_at = upload_payload["data"]["uploaded_at"]

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            listing = client.get(f"/api/v1/runs/{run_id}/annotation_output?limit=50")
            self.assertEqual(listing.status_code, 200)
            payload = json.loads(listing.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["stage"]["status"], "succeeded")
            self.assertIn("preview_lines", payload["data"])
            self.assertIn("preview_line_count", payload["data"])
            self.assertIn("truncated", payload["data"])

            upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
            uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            if uploaded_at_2 == uploaded_at:
                upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
                uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at, uploaded_at_2)

            stale_listing = client.get(f"/api/v1/runs/{run_id}/annotation_output?limit=50")
            self.assertEqual(stale_listing.status_code, 200)
            stale_payload = json.loads(stale_listing.get_data(as_text=True))
            self.assertIs(stale_payload.get("ok"), True)
            self.assertEqual(stale_payload["data"]["preview_lines"], [])

    def test_dbsnp_evidence_endpoint_is_stage_gated_to_latest_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n"
        vcf_bytes_new = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            uploaded_at = upload_payload["data"]["uploaded_at"]

            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            listing = client.get(f"/api/v1/runs/{run_id}/dbsnp_evidence?limit=50")
            self.assertEqual(listing.status_code, 200)
            payload = json.loads(listing.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["stage"]["status"], "succeeded")
            self.assertIn("dbsnp_evidence", payload["data"])
            self.assertEqual(payload["data"]["dbsnp_evidence"], [])

            upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
            uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            if uploaded_at_2 == uploaded_at:
                upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
                uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at, uploaded_at_2)

            stale_listing = client.get(f"/api/v1/runs/{run_id}/dbsnp_evidence?limit=50")
            self.assertEqual(stale_listing.status_code, 200)
            stale_payload = json.loads(stale_listing.get_data(as_text=True))
            self.assertIs(stale_payload.get("ok"), True)
            self.assertEqual(stale_payload["data"]["dbsnp_evidence"], [])

    def test_clinvar_evidence_endpoint_is_stage_gated_to_latest_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n1\t2\tC\tT\n"
        vcf_bytes_new = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            uploaded_at = upload_payload["data"]["uploaded_at"]

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(req, timeout=10):
                del timeout
                if "esearch.fcgi" in req.full_url:
                    return Response('{"esearchresult":{"idlist":["123"]}}')
                return Response(
                    '{"result":{"uids":["123"],"123":{"uid":"123","accession":"VCV000123.2","clinical_significance":{"description":"Pathogenic"}}}}'
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "3",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                    self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            listing = client.get(f"/api/v1/runs/{run_id}/clinvar_evidence?limit=50")
            self.assertEqual(listing.status_code, 200)
            payload = json.loads(listing.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["stage"]["status"], "succeeded")
            self.assertIn("clinvar_evidence", payload["data"])
            rows = payload["data"]["clinvar_evidence"]
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["source"] == "clinvar" for row in rows))
            self.assertTrue(all(row["outcome"] == "found" for row in rows))
            self.assertTrue(all(row["clinvar_id"] == "VCV000123" for row in rows))

            variant_id = rows[0]["variant_id"]
            by_variant_listing = client.get(f"/api/v1/runs/{run_id}/clinvar_evidence?variant_id={variant_id}&limit=50")
            self.assertEqual(by_variant_listing.status_code, 200)
            by_variant_payload = json.loads(by_variant_listing.get_data(as_text=True))
            self.assertIs(by_variant_payload.get("ok"), True)
            self.assertEqual(len(by_variant_payload["data"]["clinvar_evidence"]), 1)
            self.assertEqual(by_variant_payload["data"]["clinvar_evidence"][0]["variant_id"], variant_id)

            limited_listing = client.get(f"/api/v1/runs/{run_id}/clinvar_evidence?limit=1")
            self.assertEqual(limited_listing.status_code, 200)
            limited_payload = json.loads(limited_listing.get_data(as_text=True))
            self.assertIs(limited_payload.get("ok"), True)
            self.assertEqual(len(limited_payload["data"]["clinvar_evidence"]), 1)

            upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
            uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            if uploaded_at_2 == uploaded_at:
                upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
                uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at, uploaded_at_2)

            stale_listing = client.get(f"/api/v1/runs/{run_id}/clinvar_evidence?limit=50")
            self.assertEqual(stale_listing.status_code, 200)
            stale_payload = json.loads(stale_listing.get_data(as_text=True))
            self.assertIs(stale_payload.get("ok"), True)
            self.assertEqual(stale_payload["data"]["clinvar_evidence"], [])

    def test_gnomad_evidence_endpoint_is_stage_gated_to_latest_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tG\n1\t2\tC\tT\n"
        vcf_bytes_new = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            uploaded_at = upload_payload["data"]["uploaded_at"]

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(req, timeout=10):
                del timeout
                payload = json.loads((req.data or b"{}").decode("utf-8"))
                variant_id = (((payload.get("variables") or {}).get("variantId")) or "1-1-A-G")
                return Response(
                    json.dumps(
                        {
                            "data": {
                                "variant": {
                                    "variantId": variant_id,
                                    "genome": {"af": 0.01},
                                }
                            }
                        }
                    )
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_GNOMAD_TIMEOUT_SECONDS": "5",
                    "SP_GNOMAD_RETRY_MAX_ATTEMPTS": "3",
                    "SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS": "0",
                    "SP_GNOMAD_MIN_REQUEST_INTERVAL_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.gnomad_client.time.sleep", return_value=None),
                ):
                    self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                    self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            listing = client.get(f"/api/v1/runs/{run_id}/gnomad_evidence?limit=50")
            self.assertEqual(listing.status_code, 200)
            payload = json.loads(listing.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["stage"]["status"], "succeeded")
            self.assertIn("gnomad_evidence", payload["data"])
            rows = payload["data"]["gnomad_evidence"]
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["source"] == "gnomad" for row in rows))
            self.assertTrue(all(row["outcome"] == "found" for row in rows))

            variant_id = rows[0]["variant_id"]
            by_variant_listing = client.get(f"/api/v1/runs/{run_id}/gnomad_evidence?variant_id={variant_id}&limit=50")
            self.assertEqual(by_variant_listing.status_code, 200)
            by_variant_payload = json.loads(by_variant_listing.get_data(as_text=True))
            self.assertIs(by_variant_payload.get("ok"), True)
            self.assertEqual(len(by_variant_payload["data"]["gnomad_evidence"]), 1)
            self.assertEqual(by_variant_payload["data"]["gnomad_evidence"][0]["variant_id"], variant_id)

            limited_listing = client.get(f"/api/v1/runs/{run_id}/gnomad_evidence?limit=1")
            self.assertEqual(limited_listing.status_code, 200)
            limited_payload = json.loads(limited_listing.get_data(as_text=True))
            self.assertIs(limited_payload.get("ok"), True)
            self.assertEqual(len(limited_payload["data"]["gnomad_evidence"]), 1)

            upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
            uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            if uploaded_at_2 == uploaded_at:
                upload_payload_2 = json.loads(self._upload(client, run_id, vcf_bytes_new).get_data(as_text=True))
                uploaded_at_2 = upload_payload_2["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at, uploaded_at_2)

            stale_listing = client.get(f"/api/v1/runs/{run_id}/gnomad_evidence?limit=50")
            self.assertEqual(stale_listing.status_code, 200)
            stale_payload = json.loads(stale_listing.get_data(as_text=True))
            self.assertIs(stale_payload.get("ok"), True)
            self.assertEqual(stale_payload["data"]["gnomad_evidence"], [])

    def test_start_begins_at_pre_annotation_when_parser_already_succeeded(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/parse").status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["data"]["started_stage"], "pre_annotation")

            stages = self._wait_for_all_stages_succeeded(client, run_id)
            by_name = {stage["stage_name"]: stage for stage in stages}
            self.assertEqual(by_name["parser"]["status"], "succeeded")
            self.assertEqual(by_name["reporting"]["status"], "succeeded")

            status = self._wait_for_run_not_running(client, run_id)
            self.assertEqual(status, "queued")

    def test_start_refuses_without_upload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "VCF_NOT_UPLOADED")

            run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
            self.assertEqual(run_payload["data"]["status"], "queued")

    def test_start_refuses_when_validation_not_ok(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\n1\t1\tA\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            upload_payload = json.loads(self._upload(client, run_id, vcf_bytes).get_data(as_text=True))
            self.assertIs(upload_payload["data"]["validation"]["ok"], False)

            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "VCF_NOT_VALIDATED")

    def test_start_refuses_when_another_run_running(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_a = self._create_run(client)
            run_b = self._create_run(client)

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("UPDATE runs SET status = 'running' WHERE run_id = ?", (run_a,))
                conn.commit()
            finally:
                conn.close()

            self.assertEqual(self._upload(client, run_b, vcf_bytes).status_code, 200)
            resp = client.post(f"/api/v1/runs/{run_b}/start")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "ANOTHER_RUN_RUNNING")
            self.assertEqual(payload["error"]["details"]["running_run_id"], run_a)

    def test_start_reparses_when_upload_changes(self):
        vcf_a = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_b = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_a, "a.vcf").status_code, 200)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/parse").status_code, 200)

            self.assertEqual(self._upload(client, run_id, vcf_b, "b.vcf").status_code, 200)
            resp = client.post(f"/api/v1/runs/{run_id}/start")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["data"]["started_stage"], "parser")

            stages = self._wait_for_all_stages_succeeded(client, run_id)
            by_name = {stage["stage_name"]: stage for stage in stages}

            status = self._wait_for_run_not_running(client, run_id)
            self.assertEqual(status, "queued")

            input_payload = json.loads(client.get(f"/api/v1/runs/{run_id}/vcf").get_data(as_text=True))
            uploaded_at = input_payload["data"]["uploaded_at"]
            self.assertEqual(by_name["parser"]["input_uploaded_at"], uploaded_at)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT chrom, pos, ref, alt FROM run_variants WHERE run_id = ? ORDER BY pos ASC",
                    (run_id,),
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [("1", 2, "A", "G")])

    def test_cancel_during_execution_keeps_run_canceled(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            entered = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            import pipeline.parser_stage as parser_stage  # noqa: E402
            from storage import runs as runs_storage  # noqa: E402

            original_iter = parser_stage.iter_vcf_snv_records
            original_set_run_status_if_not_canceled = runs_storage.set_run_status_if_not_canceled

            def gated_iter(*args, **kwargs):
                entered.set()
                if not release.wait(timeout=5.0):
                    raise RuntimeError("Timed out waiting for test to release parser stage.")
                yield from original_iter(*args, **kwargs)

            def wrapped_set_run_status_if_not_canceled(*args, **kwargs):
                try:
                    return original_set_run_status_if_not_canceled(*args, **kwargs)
                finally:
                    finished.set()

            def wait_for_all_stages_canceled(timeout_s: float = 5.0):
                deadline = time.time() + timeout_s
                while time.time() < deadline:
                    stages_payload = json.loads(
                        client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                    )
                    stages = stages_payload["data"]["stages"]
                    if stages and all(stage["status"] == "canceled" for stage in stages):
                        return stages
                    time.sleep(0.01)
                self.fail("Timed out waiting for stages to become canceled.")

            with (
                patch("pipeline.parser_stage.iter_vcf_snv_records", side_effect=gated_iter),
                patch(
                    "storage.runs.set_run_status_if_not_canceled",
                    side_effect=wrapped_set_run_status_if_not_canceled,
                ),
            ):
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                self.assertTrue(entered.wait(timeout=5.0))

                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)
                run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload["data"]["status"], "canceled")

                release.set()

                stages = wait_for_all_stages_canceled()
                self.assertTrue(stages)
                self.assertTrue(finished.wait(timeout=5.0))

    def test_cancel_prevents_inflight_stage_from_flipping_to_succeeded(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            import pipeline.orchestrator as orch  # noqa: E402
            import pipeline.pre_annotation_stage as pre  # noqa: E402

            entered = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            from storage import runs as runs_storage  # noqa: E402

            original_set_run_status_if_not_canceled = runs_storage.set_run_status_if_not_canceled
            original_mark_succeeded = pre.mark_stage_succeeded

            def gated_mark_succeeded(*args, **kwargs):
                stage_name = args[2] if len(args) > 2 else kwargs.get("stage_name")
                if stage_name == "pre_annotation":
                    entered.set()
                    if not release.wait(timeout=20.0):
                        raise RuntimeError("Timed out waiting for test to release pre-annotation gate.")
                return original_mark_succeeded(*args, **kwargs)

            def wrapped_set_run_status_if_not_canceled(*args, **kwargs):
                try:
                    return original_set_run_status_if_not_canceled(*args, **kwargs)
                finally:
                    finished.set()

            with (
                patch("pipeline.pre_annotation_stage.mark_stage_succeeded", side_effect=gated_mark_succeeded),
                patch(
                    "storage.runs.set_run_status_if_not_canceled",
                    side_effect=wrapped_set_run_status_if_not_canceled,
                ),
            ):
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                self.assertTrue(entered.wait(timeout=5.0))

                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)
                run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload["data"]["status"], "canceled")

                release.set()

                deadline = time.time() + 5.0
                while time.time() < deadline:
                    stages_payload = json.loads(
                        client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                    )
                    by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
                    if by_name["pre_annotation"]["status"] == "canceled":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("Timed out waiting for pre_annotation to remain canceled.")

                stages_payload = json.loads(
                    client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                )
                by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
                self.assertEqual(by_name["pre_annotation"]["status"], "canceled")
                self.assertEqual(by_name["classification"]["status"], "canceled")
                self.assertNotEqual(by_name["classification"]["status"], "succeeded")

                conn = sqlite3.connect(db_path)
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM run_pre_annotations WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                finally:
                    conn.close()
                self.assertEqual(count, 0)
                self.assertTrue(finished.wait(timeout=5.0))

    def test_cancel_prevents_inflight_classification_from_flipping_to_succeeded(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            import pipeline.classification_stage as cl  # noqa: E402
            from storage import runs as runs_storage  # noqa: E402

            entered = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            original_mark_succeeded = cl.mark_stage_succeeded
            original_set_run_status_if_not_canceled = runs_storage.set_run_status_if_not_canceled

            def gated_mark_succeeded(*args, **kwargs):
                stage_name = args[2] if len(args) > 2 else kwargs.get("stage_name")
                if stage_name == "classification":
                    entered.set()
                    if not release.wait(timeout=20.0):
                        raise RuntimeError("Timed out waiting for test to release classification gate.")
                return original_mark_succeeded(*args, **kwargs)

            def wrapped_set_run_status_if_not_canceled(*args, **kwargs):
                try:
                    return original_set_run_status_if_not_canceled(*args, **kwargs)
                finally:
                    finished.set()

            with (
                patch("pipeline.classification_stage.mark_stage_succeeded", side_effect=gated_mark_succeeded),
                patch(
                    "storage.runs.set_run_status_if_not_canceled",
                    side_effect=wrapped_set_run_status_if_not_canceled,
                ),
            ):
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                self.assertTrue(entered.wait(timeout=5.0))

                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)
                run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload["data"]["status"], "canceled")

                release.set()

                deadline = time.time() + 5.0
                while time.time() < deadline:
                    stages_payload = json.loads(
                        client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                    )
                    by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
                    if by_name["classification"]["status"] == "canceled":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("Timed out waiting for classification to remain canceled.")

                conn = sqlite3.connect(db_path)
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM run_classifications WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                finally:
                    conn.close()
                self.assertEqual(count, 0)
                self.assertTrue(finished.wait(timeout=5.0))

    def test_cancel_prevents_inflight_prediction_from_flipping_to_succeeded(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            import pipeline.prediction_stage as pred  # noqa: E402
            from storage import runs as runs_storage  # noqa: E402

            entered = threading.Event()
            release = threading.Event()
            finished = threading.Event()

            original_mark_succeeded = pred.mark_stage_succeeded
            original_set_run_status_if_not_canceled = runs_storage.set_run_status_if_not_canceled

            def gated_mark_succeeded(*args, **kwargs):
                stage_name = args[2] if len(args) > 2 else kwargs.get("stage_name")
                if stage_name == "prediction":
                    entered.set()
                    if not release.wait(timeout=20.0):
                        raise RuntimeError("Timed out waiting for test to release prediction gate.")
                return original_mark_succeeded(*args, **kwargs)

            def wrapped_set_run_status_if_not_canceled(*args, **kwargs):
                try:
                    return original_set_run_status_if_not_canceled(*args, **kwargs)
                finally:
                    finished.set()

            with (
                patch("pipeline.prediction_stage.mark_stage_succeeded", side_effect=gated_mark_succeeded),
                patch(
                    "storage.runs.set_run_status_if_not_canceled",
                    side_effect=wrapped_set_run_status_if_not_canceled,
                ),
            ):
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                self.assertTrue(entered.wait(timeout=5.0))

                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)
                run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload["data"]["status"], "canceled")

                release.set()

                deadline = time.time() + 5.0
                while time.time() < deadline:
                    stages_payload = json.loads(
                        client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                    )
                    by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
                    if by_name["prediction"]["status"] == "canceled":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("Timed out waiting for prediction to remain canceled.")

                conn = sqlite3.connect(db_path)
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM run_predictor_outputs WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                finally:
                    conn.close()
                self.assertEqual(count, 0)
                self.assertTrue(finished.wait(timeout=5.0))

    def test_run_pipeline_propagates_run_canceled_in_post_start_window(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            import logging
            import pipeline.orchestrator as orch  # noqa: E402
            import pipeline.pre_annotation_stage as pre  # noqa: E402
            from storage.runs import cancel_run as cancel_run_record

            test_tools_dir = os.path.join(tmpdir, ".test-tools")
            os.makedirs(test_tools_dir, exist_ok=True)
            vep_cache_dir = os.path.join(test_tools_dir, "vep-cache")
            os.makedirs(vep_cache_dir, exist_ok=True)
            alpha_file_path = os.path.join(test_tools_dir, "alphamissense.tsv")
            with open(alpha_file_path, "w", encoding="utf-8", newline="\n") as f:
                f.write("# deterministic test fixture\n")
            prediction_config = {
                "cmd": sys.executable,
                "script_path": os.path.join(PROJECT_ROOT, "scripts", "mock_vep.py"),
                "cache_dir": vep_cache_dir,
                "alphamissense_file": alpha_file_path,
                "timeout_seconds": 30,
                "plugin_dir": None,
                "fasta_path": None,
                "extra_args": [],
            }

            entered = threading.Event()

            original_mark_running = pre.mark_stage_running

            def cancel_after_mark_running(*args, **kwargs):
                stage_name = args[2] if len(args) > 2 else kwargs.get("stage_name")
                original_mark_running(*args, **kwargs)
                if stage_name == "pre_annotation":
                    entered.set()

            raised: list[Exception] = []

            def run_pipeline_in_thread():
                try:
                    orch.run_pipeline(
                        db_path,
                        run_id,
                        max_decompressed_bytes=10_000_000,
                        logger=logging.getLogger("test"),
                        prediction_config=prediction_config,
                    )
                except Exception as exc:  # noqa: BLE001
                    raised.append(exc)

            with patch("pipeline.pre_annotation_stage.mark_stage_running", side_effect=cancel_after_mark_running):
                thread = threading.Thread(target=run_pipeline_in_thread, daemon=True)
                thread.start()

                self.assertTrue(entered.wait(timeout=5.0))
                cancel_run_record(db_path, run_id)

                thread.join(timeout=5.0)

            self.assertTrue(raised, "Expected pipeline execution to raise after cancellation.")
            self.assertIsInstance(raised[0], orch.OrchestratorError)
            exc = raised[0]
            self.assertEqual(exc.code, "RUN_CANCELED")
            self.assertEqual(exc.http_status, 409)


if __name__ == "__main__":
    unittest.main()
