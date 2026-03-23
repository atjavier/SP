import json
import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class RunLogsApiTestCase(unittest.TestCase):
    def _create_client(self, db_path: str):
        import app as sp_app  # noqa: E402

        flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
        return flask_app.test_client()

    def _create_run(self, client) -> str:
        created = json.loads(client.post("/api/v1/runs").get_data(as_text=True))
        return created["data"]["run_id"]

    def test_logs_endpoint_returns_empty_when_no_log_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            resp = client.get(f"/api/v1/runs/{run_id}/logs")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)
            self.assertEqual(payload["data"]["logs"], [])

    def test_logs_endpoint_returns_404_for_unknown_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            resp = client.get("/api/v1/runs/not-a-real-run/logs")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")

    def test_logs_endpoint_respects_limit_and_order(self):
        from run_logging import build_run_logger, log_run_event

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)
            run_id = self._create_run(client)

            logger = build_run_logger(run_id, instance_dir=tmpdir)
            log_run_event(logger, "event_one", "First")
            log_run_event(logger, "event_two", "Second")
            log_run_event(logger, "event_three", "Third")

            resp = client.get(f"/api/v1/runs/{run_id}/logs?limit=2")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            logs = payload["data"]["logs"]
            self.assertEqual(len(logs), 2)
            self.assertEqual([log["event"] for log in logs], ["event_two", "event_three"])
            self.assertTrue(all(log["run_id"] == run_id for log in logs))


if __name__ == "__main__":
    unittest.main()
