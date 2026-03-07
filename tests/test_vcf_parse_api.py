import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import gzip


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class VcfParseApiTestCase(unittest.TestCase):
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

    def test_parse_returns_409_when_no_upload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "VCF_NOT_UPLOADED")

    def test_parse_rejects_when_validation_not_ok(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\n1\t1\tA\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            upload_resp = self._upload(client, run_id, vcf_bytes)
            self.assertEqual(upload_resp.status_code, 200)
            upload_payload = json.loads(upload_resp.get_data(as_text=True))
            self.assertIs(upload_payload["data"]["validation"]["ok"], False)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "VCF_NOT_VALIDATED")

    def test_parse_persists_variants_and_deletes_upload(self):
        vcf_bytes = (
            b"##fileformat=VCFv4.2\n"
            b"#CHROM\tPOS\tREF\tALT\n"
            b"chr1\t1\ta\tT,G\n"
            b"1\t2\tAT\tA\n"
            b"chrM\t3\tC\tt\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self._upload(client, run_id, vcf_bytes)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)

            stats = payload["data"]["parser"]["stats"]
            self.assertEqual(stats["snv_records_created"], 3)
            self.assertGreaterEqual(len(payload["data"]["variants_sample"]), 1)

            stored_path = os.path.join(tmpdir, "uploads", run_id, "input.vcf")
            self.assertFalse(os.path.exists(stored_path))

            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute(
                    "SELECT COUNT(*) FROM run_variants WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                stage = conn.execute(
                    "SELECT status FROM run_stages WHERE run_id = ? AND stage_name = ?",
                    (run_id, "parser"),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(count, 3)
            self.assertIsNotNone(stage)
            self.assertEqual(stage[0], "succeeded")

    def test_parse_handles_vcfgz_and_deletes_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        gz_bytes = gzip.compress(vcf_bytes)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            upload_resp = self._upload(client, run_id, gz_bytes, filename="sample.vcf.gz")
            self.assertEqual(upload_resp.status_code, 200)
            upload_payload = json.loads(upload_resp.get_data(as_text=True))
            self.assertIs(upload_payload["data"]["validation"]["ok"], True)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 200)

            stored_path = os.path.join(tmpdir, "uploads", run_id, "input.vcf.gz")
            self.assertFalse(os.path.exists(stored_path))

    def test_parse_removes_entire_upload_dir_contents(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self._upload(client, run_id, vcf_bytes)

            upload_dir = os.path.join(tmpdir, "uploads", run_id)
            os.makedirs(upload_dir, exist_ok=True)
            with open(os.path.join(upload_dir, "junk.txt"), "wb") as handle:
                handle.write(b"junk")

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 200)

            self.assertFalse(os.path.exists(os.path.join(upload_dir, "input.vcf")))
            self.assertFalse(os.path.exists(os.path.join(upload_dir, "junk.txt")))
            self.assertFalse(os.path.isdir(upload_dir))

    def test_parse_returns_422_on_parser_failure_and_keeps_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\tabc\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            upload_resp = self._upload(client, run_id, vcf_bytes)
            self.assertEqual(upload_resp.status_code, 200)
            self.assertIs(json.loads(upload_resp.get_data(as_text=True))["data"]["validation"]["ok"], True)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 422)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "VCF_PARSE_FAILED")
            self.assertEqual(payload["error"]["details"]["error_code"], "INVALID_POS")
            self.assertEqual(payload["error"]["details"]["line_number"], 2)

            stored_path = os.path.join(tmpdir, "uploads", run_id, "input.vcf")
            self.assertTrue(os.path.exists(stored_path))

            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute(
                    "SELECT COUNT(*) FROM run_variants WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                stage = conn.execute(
                    "SELECT status, error_code FROM run_stages WHERE run_id = ? AND stage_name = ?",
                    (run_id, "parser"),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(count, 0)
            self.assertIsNotNone(stage)
            self.assertEqual(stage[0], "failed")
            self.assertEqual(stage[1], "INVALID_POS")

    def test_parse_returns_409_when_stage_running_unless_forced(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)
            self._upload(client, run_id, vcf_bytes)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS run_stages (
                      run_id TEXT NOT NULL,
                      stage_name TEXT NOT NULL,
                      status TEXT NOT NULL,
                      started_at TEXT,
                      completed_at TEXT,
                      input_uploaded_at TEXT,
                      stats_json TEXT,
                      error_code TEXT,
                      error_message TEXT,
                      error_details_json TEXT,
                      PRIMARY KEY (run_id, stage_name)
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO run_stages (
                      run_id, stage_name, status, started_at, completed_at, input_uploaded_at,
                      stats_json, error_code, error_message, error_details_json
                    )
                    VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL)
                    """,
                    (run_id, "parser", "running", "2026-03-07T00:00:00+00:00"),
                )
                conn.commit()
            finally:
                conn.close()

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertEqual(payload["error"]["code"], "STAGE_RUNNING")

            resp_forced = client.post(f"/api/v1/runs/{run_id}/parse?force=1")
            self.assertEqual(resp_forced.status_code, 200)

    def test_parse_rejects_when_already_parsed_same_upload(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self._upload(client, run_id, vcf_bytes)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/parse").status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "ALREADY_PARSED")

    def test_parse_allows_reparse_after_upload_change(self):
        vcf_a = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_b = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self._upload(client, run_id, vcf_a, "a.vcf")
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/parse").status_code, 200)

            self._upload(client, run_id, vcf_b, "b.vcf")
            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 200)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT chrom, pos, ref, alt FROM run_variants WHERE run_id = ? ORDER BY pos ASC",
                    (run_id,),
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(rows, [("1", 2, "A", "G")])

    def test_parse_rejects_when_run_canceled(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)
            self._upload(client, run_id, vcf_bytes)

            resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_CANCELED")

            stages_resp = client.get(f"/api/v1/runs/{run_id}/stages")
            stages_payload = json.loads(stages_resp.get_data(as_text=True))
            by_name = {stage["stage_name"]: stage for stage in stages_payload["data"]["stages"]}
            self.assertEqual(by_name["parser"]["status"], "canceled")

    def test_parse_stage_status_visible_via_stages_endpoint(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            self._upload(client, run_id, vcf_bytes)
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/parse").status_code, 200)

            resp = client.get(f"/api/v1/runs/{run_id}/stages")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            stages = payload["data"]["stages"]
            by_name = {stage["stage_name"]: stage for stage in stages}
            self.assertEqual(by_name["parser"]["status"], "succeeded")
            self.assertEqual(by_name["prediction"]["status"], "queued")


if __name__ == "__main__":
    unittest.main()
