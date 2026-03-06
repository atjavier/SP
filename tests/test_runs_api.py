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

            created_at = data.get("created_at")
            self.assertIsInstance(created_at, str)
            self.assertTrue(created_at)
            parsed = datetime.fromisoformat(created_at)
            self.assertIsNotNone(parsed.tzinfo)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT run_id, status, created_at FROM runs WHERE run_id = ?",
                    (data["run_id"],),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(row)

    def test_cancel_run_transitions_queued_to_canceled_and_persists(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.runs import cancel_run, create_run, get_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")

            created = create_run(db_path)
            self.assertEqual(created["status"], "queued")

            canceled = cancel_run(db_path, created["run_id"])
            self.assertEqual(canceled["run_id"], created["run_id"])
            self.assertEqual(canceled["status"], "canceled")

            fetched = get_run(db_path, created["run_id"])
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched["status"], "canceled")

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

            cancel_resp = client.post(f"/api/v1/runs/{run_id}/cancel")
            self.assertEqual(cancel_resp.status_code, 200)
            cancel_payload = json.loads(cancel_resp.get_data(as_text=True))
            self.assertIs(cancel_payload.get("ok"), True)
            self.assertEqual(cancel_payload["data"]["run_id"], run_id)
            self.assertEqual(cancel_payload["data"]["status"], "canceled")

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


if __name__ == "__main__":
    unittest.main()
