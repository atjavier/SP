import io
import json
import os
import sqlite3
import sys
import tempfile
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
            by_name = {stage["stage_name"]: stage for stage in stages}
            self.assertEqual(by_name["prediction"]["stats"]["stub"], True)

            for stage in stages:
                self.assertEqual(stage["status"], "succeeded")
                self.assertIsInstance(stage.get("started_at"), str)
                self.assertIsInstance(stage.get("completed_at"), str)

                started = datetime.fromisoformat(stage["started_at"])
                completed = datetime.fromisoformat(stage["completed_at"])
                self.assertIsNotNone(started.tzinfo)
                self.assertIsNotNone(completed.tzinfo)
                self.assertGreaterEqual(completed, started)

            status = self._wait_for_run_not_running(client, run_id)
            self.assertEqual(status, "queued")

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

            import pipeline.orchestrator as orch  # noqa: E402

            original = orch.run_parser_stage

            def slow_parser(*args, **kwargs):
                time.sleep(0.25)
                return original(*args, **kwargs)

            with patch("pipeline.orchestrator.run_parser_stage", side_effect=slow_parser):
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/start").status_code, 200)
                self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)

                run_payload = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload["data"]["status"], "canceled")

                time.sleep(0.4)
                run_payload2 = json.loads(client.get(f"/api/v1/runs/{run_id}").get_data(as_text=True))
                self.assertEqual(run_payload2["data"]["status"], "canceled")

                stages_payload = json.loads(
                    client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
                )
                by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
                self.assertEqual(by_name["parser"]["status"], "canceled")


if __name__ == "__main__":
    unittest.main()
