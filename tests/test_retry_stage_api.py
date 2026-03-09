import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class RetryStageApiTestCase(unittest.TestCase):
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

    def _wait_for_run_not_running(self, client, run_id: str, timeout_s: float = 5.0):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
            status = run_payload["data"]["status"]
            if status != "running":
                return status
            time.sleep(0.01)
        self.fail("Timed out waiting for run to stop running.")

    def test_retry_refuses_when_stage_not_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)

            resp = client.post(f"/api/v1/runs/{run_id}/stages/classification/retry")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "STAGE_NOT_FAILED")
            self.assertEqual(payload["error"]["details"]["stage_name"], "classification")
            self.assertEqual(payload["error"]["details"]["current_status"], "queued")

    def test_retry_rejects_unknown_stage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)

            resp = client.post(f"/api/v1/runs/{run_id}/stages/nope/retry")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "STAGE_NOT_FOUND")

    def test_retry_resets_stage_and_downstream_and_auto_starts_at_stage(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
            self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

            vcf_payload = json.loads(client.get(f"/api/v1/runs/{run_id}/vcf").get_data(as_text=True))
            uploaded_at = vcf_payload["data"]["uploaded_at"]

            from storage.stages import mark_stage_failed  # noqa: E402

            mark_stage_failed(
                db_path,
                run_id,
                "classification",
                input_uploaded_at=uploaded_at,
                error_code="STAGE_FAILED",
                error_message="boom",
            )

            import pipeline.orchestrator as orch  # noqa: E402

            original_run_pipeline = orch.run_pipeline

            open_event = threading.Event()

            def gated_run_pipeline(*args, **kwargs):
                open_event.wait(timeout=5.0)
                return original_run_pipeline(*args, **kwargs)

            with patch("pipeline.orchestrator.run_pipeline", side_effect=gated_run_pipeline):
                resp = client.post(f"/api/v1/runs/{run_id}/stages/classification/retry")
                self.assertEqual(resp.status_code, 200)
                payload = json.loads(resp.get_data(as_text=True))
                self.assertIs(payload.get("ok"), True)
                self.assertEqual(payload["data"]["run_id"], run_id)
                self.assertEqual(payload["data"]["stage_name"], "classification")
                self.assertEqual(payload["data"]["started_stage"], "classification")
                self.assertIn("parser", payload["data"]["preserved_stages"])
                self.assertIn("classification", payload["data"]["reset_stages"])
                self.assertIn("reporting", payload["data"]["reset_stages"])

                stages_payload = json.loads(
                    client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                )
                by_name = {s["stage_name"]: s for s in stages_payload["data"]["stages"]}

                self.assertEqual(by_name["parser"]["status"], "succeeded")
                self.assertEqual(by_name["parser"]["input_uploaded_at"], uploaded_at)
                self.assertEqual(by_name["pre_annotation"]["status"], "succeeded")

                for stage_name in ("classification", "prediction", "annotation", "reporting"):
                    stage = by_name[stage_name]
                    self.assertEqual(stage["status"], "queued")
                    self.assertIsNone(stage.get("started_at"))
                    self.assertIsNone(stage.get("completed_at"))
                    self.assertIsNone(stage.get("stats"))
                    self.assertIsNone(stage.get("error"))
                    self.assertIsNone(stage.get("input_uploaded_at"))

                open_event.set()
                self.assertEqual(self._wait_for_run_not_running(client, run_id), "queued")

    def test_retry_refuses_when_failed_stage_is_for_an_old_upload(self):
        vcf_a = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_b = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_a, "a.vcf").status_code, 200)

            vcf_payload_a = json.loads(
                client.get(f"/api/v1/runs/{run_id}/vcf").get_data(as_text=True)
            )
            uploaded_at_a = vcf_payload_a["data"]["uploaded_at"]

            from storage.stages import mark_stage_failed  # noqa: E402

            mark_stage_failed(
                db_path,
                run_id,
                "classification",
                input_uploaded_at=uploaded_at_a,
                error_code="STAGE_FAILED",
                error_message="boom",
            )

            self.assertEqual(self._upload(client, run_id, vcf_b, "b.vcf").status_code, 200)
            vcf_payload_b = json.loads(
                client.get(f"/api/v1/runs/{run_id}/vcf").get_data(as_text=True)
            )
            uploaded_at_b = vcf_payload_b["data"]["uploaded_at"]
            self.assertNotEqual(uploaded_at_a, uploaded_at_b)

            resp = client.post(f"/api/v1/runs/{run_id}/stages/classification/retry")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            # Uploading a new file now resets all stage statuses to queued,
            # so retrying the old failed stage is rejected as not failed.
            self.assertEqual(payload["error"]["code"], "STAGE_NOT_FAILED")
            self.assertEqual(payload["error"]["details"]["stage_name"], "classification")
            self.assertEqual(payload["error"]["details"]["current_status"], "queued")


if __name__ == "__main__":
    unittest.main()
