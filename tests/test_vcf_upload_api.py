import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class VcfUploadApiTestCase(unittest.TestCase):
    def test_get_uploaded_vcf_returns_null_when_no_upload_yet(self):
        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created = json.loads(client.post("/api/v1/runs").get_data(as_text=True))
            run_id = created["data"]["run_id"]

            resp = client.get(f"/api/v1/runs/{run_id}/vcf")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertIs(payload.get("data"), None)

    def test_upload_vcf_validates_and_persists_attachment(self):
        import app as sp_app  # noqa: E402

        vcf_bytes = (
            b"##fileformat=VCFv4.2\n"
            b"#CHROM\tPOS\tREF\tALT\n"
            b"1\t1\tA\tT\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created = json.loads(client.post("/api/v1/runs").get_data(as_text=True))
            run_id = created["data"]["run_id"]

            resp = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_bytes), "sample.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)

            validation = payload["data"]["validation"]
            self.assertIs(validation["ok"], True)
            self.assertEqual(validation["errors"], [])

            stored_path = os.path.join(tmpdir, "uploads", run_id, "input.vcf")
            self.assertTrue(os.path.exists(stored_path))
            with open(stored_path, "rb") as handle:
                self.assertEqual(handle.read(), vcf_bytes)

            fetched = json.loads(
                client.get(f"/api/v1/runs/{run_id}/vcf").get_data(as_text=True)
            )
            self.assertIs(fetched.get("ok"), True)
            self.assertIsNotNone(fetched.get("data"))
            self.assertIs(fetched["data"]["validation"]["ok"], True)

    def test_upload_vcf_replace_overwrites_previous(self):
        import app as sp_app  # noqa: E402

        vcf_a = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_b = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            run_id = json.loads(client.post("/api/v1/runs").get_data(as_text=True))["data"][
                "run_id"
            ]

            client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_a), "a.vcf")},
                content_type="multipart/form-data",
            )
            client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_b), "b.vcf")},
                content_type="multipart/form-data",
            )

            stored_path = os.path.join(tmpdir, "uploads", run_id, "input.vcf")
            with open(stored_path, "rb") as handle:
                self.assertEqual(handle.read(), vcf_b)

    def test_upload_vcf_resets_stage_statuses_when_file_changes(self):
        import app as sp_app  # noqa: E402

        vcf_a = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        vcf_b = b"#CHROM\tPOS\tREF\tALT\n1\t2\tA\tG\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            run_id = json.loads(client.post("/api/v1/runs").get_data(as_text=True))["data"]["run_id"]

            first_upload = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_a), "a.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(first_upload.status_code, 200)

            parse_resp = client.post(f"/api/v1/runs/{run_id}/parse")
            self.assertEqual(parse_resp.status_code, 200)

            stages_after_parse = json.loads(
                client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
            )["data"]["stages"]
            parser_stage = [s for s in stages_after_parse if s["stage_name"] == "parser"][0]
            self.assertEqual(parser_stage["status"], "succeeded")

            second_upload = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_b), "b.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(second_upload.status_code, 200)

            stages_after_reupload = json.loads(
                client.get(f"/api/v1/runs/{run_id}/stages").get_data(as_text=True)
            )["data"]["stages"]
            self.assertEqual(
                [stage["status"] for stage in stages_after_reupload],
                ["queued", "queued", "queued", "queued", "queued", "queued"],
            )

            conn = sqlite3.connect(db_path)
            try:
                variant_count = conn.execute(
                    "SELECT COUNT(*) FROM run_variants WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(variant_count, 0)

    def test_upload_vcf_rejects_while_run_is_running(self):
        import app as sp_app  # noqa: E402

        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            run_id = json.loads(client.post("/api/v1/runs").get_data(as_text=True))["data"]["run_id"]

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("UPDATE runs SET status = 'running' WHERE run_id = ?", (run_id,))
                conn.commit()
            finally:
                conn.close()

            resp = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_bytes), "sample.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_RUNNING")

    def test_upload_vcf_rejects_when_run_is_canceled(self):
        import app as sp_app  # noqa: E402

        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            run_id = json.loads(client.post("/api/v1/runs").get_data(as_text=True))["data"]["run_id"]
            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)

            resp = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_bytes), "sample.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_CANCELED")

    def test_upload_vcf_rejects_unsupported_file_type(self):
        import app as sp_app  # noqa: E402

        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created = json.loads(client.post("/api/v1/runs").get_data(as_text=True))
            run_id = created["data"]["run_id"]

            resp = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(vcf_bytes), "sample.txt")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 400)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "UNSUPPORTED_FILE_TYPE")

    def test_upload_vcf_enforces_size_limit(self):
        import app as sp_app  # noqa: E402

        large_bytes = b"#CHROM\tPOS\tREF\tALT\n" + (b"1\t1\tA\tT\n" * 2000)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app(
                {"TESTING": True, "SP_DB_PATH": db_path, "SP_MAX_UPLOAD_BYTES": 1024}
            )
            client = flask_app.test_client()

            run_id = json.loads(client.post("/api/v1/runs").get_data(as_text=True))["data"][
                "run_id"
            ]

            resp = client.post(
                f"/api/v1/runs/{run_id}/vcf",
                data={"vcf_file": (io.BytesIO(large_bytes), "sample.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 413)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "UPLOAD_TOO_LARGE")

    def test_upload_vcf_unknown_run_returns_404(self):
        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post(
                "/api/v1/runs/not-a-real-run-id/vcf",
                data={"vcf_file": (io.BytesIO(b"#CHROM\tPOS\tREF\tALT\n"), "x.vcf")},
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
