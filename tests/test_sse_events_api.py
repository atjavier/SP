import io
import json
import os
import sys
import tempfile
import time
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class SseEventsApiTestCase(unittest.TestCase):
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

    def _read_sse_events(self, response, *, max_events: int = 25, timeout_s: float = 2.5):
        deadline = time.time() + timeout_s
        buffer = ""
        events: list[tuple[str | None, str]] = []

        for chunk in response.response:
            if time.time() > deadline:
                break
            if not chunk:
                continue

            buffer += chunk.decode("utf-8", errors="replace")
            while "\n\n" in buffer:
                raw, buffer = buffer.split("\n\n", 1)
                if raw.startswith(":"):
                    continue

                event_name: str | None = None
                data_lines: list[str] = []
                for line in raw.splitlines():
                    if line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
                    elif line.startswith("retry:"):
                        continue

                if data_lines:
                    events.append((event_name, "\n".join(data_lines)))
                    if len(events) >= max_events:
                        return events

        return events

    def test_events_returns_json_404_for_unknown_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            resp = client.get("/api/v1/runs/not-a-real-run-id/events")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")

    def test_events_stream_includes_snapshot_and_stage_update(self):
        vcf_bytes = b"#CHROM\tPOS\tREF\tALT\n1\t1\tA\tT\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            client = self._create_client(db_path)

            run_id = self._create_run(client)
            self.assertEqual(self._upload(client, run_id, vcf_bytes).status_code, 200)

            resp = client.get(
                f"/api/v1/runs/{run_id}/events",
                buffered=False,
                headers={"Accept": "text/event-stream"},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertIn("text/event-stream", resp.headers.get("Content-Type", ""))

            client.post(f"/api/v1/runs/{run_id}/start")

            events = self._read_sse_events(resp, max_events=30, timeout_s=4.0)
            self.assertGreaterEqual(len(events), 1)

            any_run_status = False
            any_stage_status = False
            any_parser_progress = False
            for name, data in events:
                payload = json.loads(data)
                self.assertEqual(payload["run_id"], run_id)
                self.assertIn("event_at", payload)
                self.assertIsInstance(payload["event_at"], str)
                self.assertIn("data", payload)
                if name == "run_status":
                    any_run_status = True
                if name == "stage_status":
                    any_stage_status = True
                    stage = payload.get("data") or {}
                    if stage.get("stage_name") == "parser" and stage.get("status") not in {None, "queued"}:
                        any_parser_progress = True

            self.assertTrue(any_run_status)
            self.assertTrue(any_stage_status)
            self.assertTrue(any_parser_progress)


if __name__ == "__main__":
    unittest.main()
