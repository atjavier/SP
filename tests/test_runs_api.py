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
            self.assertEqual(data.get("status"), "created")

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


if __name__ == "__main__":
    unittest.main()
