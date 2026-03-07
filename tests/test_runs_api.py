import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")


class RunsApiTestCase(unittest.TestCase):
    def test_post_runs_creates_run_and_persists_row(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post("/api/v1/runs")
            self.assertEqual(resp.status_code, 200)

            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertIn("data", payload)
            data = payload["data"]

            self.assertIsInstance(data.get("run_id"), str)
            self.assertTrue(data["run_id"])
            self.assertEqual(data.get("status"), "queued")
            self.assertEqual(data.get("reference_build"), "GRCh38")

            created_at = data.get("created_at")
            self.assertIsInstance(created_at, str)
            self.assertTrue(created_at)
            parsed = datetime.fromisoformat(created_at)
            self.assertIsNotNone(parsed.tzinfo)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT run_id, status, created_at, reference_build FROM runs WHERE run_id = ?",
                    (data["run_id"],),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(row)
            self.assertEqual(row[3], "GRCh38")

    def test_cancel_run_transitions_queued_to_canceled_and_persists(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.runs import cancel_run, create_run, get_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")

            created = create_run(db_path)
            self.assertEqual(created["status"], "queued")
            self.assertEqual(created["reference_build"], "GRCh38")

            canceled = cancel_run(db_path, created["run_id"])
            self.assertEqual(canceled["run_id"], created["run_id"])
            self.assertEqual(canceled["status"], "canceled")
            self.assertEqual(canceled["reference_build"], "GRCh38")

            fetched = get_run(db_path, created["run_id"])
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched["status"], "canceled")
            self.assertEqual(fetched["reference_build"], "GRCh38")

    def test_cancel_run_unknown_run_raises(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.runs import RunNotFoundError, cancel_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            with self.assertRaises(RunNotFoundError):
                cancel_run(db_path, "not-a-real-run-id")

    def test_cancel_run_non_cancelable_raises(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.runs import RunNotCancelableError, cancel_run, create_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            created = create_run(db_path)

            cancel_run(db_path, created["run_id"])
            with self.assertRaises(RunNotCancelableError):
                cancel_run(db_path, created["run_id"])

    def test_post_cancel_transitions_run_to_canceled(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created_resp = client.post("/api/v1/runs")
            self.assertEqual(created_resp.status_code, 200)
            created_payload = json.loads(created_resp.get_data(as_text=True))
            run_id = created_payload["data"]["run_id"]
            self.assertEqual(created_payload["data"]["reference_build"], "GRCh38")

            cancel_resp = client.post(f"/api/v1/runs/{run_id}/cancel")
            self.assertEqual(cancel_resp.status_code, 200)
            cancel_payload = json.loads(cancel_resp.get_data(as_text=True))
            self.assertIs(cancel_payload.get("ok"), True)
            self.assertEqual(cancel_payload["data"]["run_id"], run_id)
            self.assertEqual(cancel_payload["data"]["status"], "canceled")
            self.assertEqual(cancel_payload["data"]["reference_build"], "GRCh38")

    def test_post_cancel_unknown_run_returns_404(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post("/api/v1/runs/not-a-real-run-id/cancel")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")

    def test_post_cancel_non_cancelable_returns_409(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created_resp = client.post("/api/v1/runs")
            run_id = json.loads(created_resp.get_data(as_text=True))["data"]["run_id"]

            self.assertEqual(client.post(f"/api/v1/runs/{run_id}/cancel").status_code, 200)

            resp = client.post(f"/api/v1/runs/{run_id}/cancel")
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_CANCELABLE")
            self.assertEqual(payload["error"]["details"]["current_status"], "canceled")

    def test_get_run_returns_reference_build(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created_resp = client.post("/api/v1/runs")
            run_id = json.loads(created_resp.get_data(as_text=True))["data"]["run_id"]

            resp = client.get(f"/api/v1/runs/{run_id}")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["reference_build"], "GRCh38")

    def test_get_run_unknown_returns_404(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.get("/api/v1/runs/not-a-real-run-id")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")

    def test_schema_migration_adds_reference_build_for_existing_runs_table(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.runs import get_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE runs (
                      run_id TEXT PRIMARY KEY,
                      status TEXT NOT NULL,
                      created_at TEXT NOT NULL
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO runs (run_id, status, created_at) VALUES (?, ?, ?)",
                    ("legacy-run", "queued", "2026-03-07T00:00:00+00:00"),
                )
                conn.commit()
            finally:
                conn.close()

            record = get_run(db_path, "legacy-run")
            self.assertIsNotNone(record)
            self.assertEqual(record["reference_build"], "GRCh38")


if __name__ == "__main__":
    unittest.main()
