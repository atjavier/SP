import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch


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
            self.assertEqual(data.get("annotation_evidence_policy"), "continue")
            self.assertEqual(data.get("evidence_mode_requested"), "online")
            self.assertIsNone(data.get("evidence_mode_effective"))
            self.assertIsNone(data.get("evidence_online_available"))
            self.assertEqual(data.get("evidence_offline_sources_configured"), {"dbsnp": False, "clinvar": False, "gnomad": False})
            self.assertEqual(data.get("evidence_mode_decision_reason"), "not_evaluated")
            self.assertIsNone(data.get("evidence_mode_detected_at"))

            created_at = data.get("created_at")
            self.assertIsInstance(created_at, str)
            self.assertTrue(created_at)
            parsed = datetime.fromisoformat(created_at)
            self.assertIsNotNone(parsed.tzinfo)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT
                      run_id,
                      status,
                      created_at,
                      reference_build,
                      annotation_evidence_policy,
                      evidence_mode_requested,
                      evidence_mode_effective,
                      evidence_online_available,
                      evidence_offline_sources_configured_json,
                      evidence_mode_decision_reason,
                      evidence_mode_detected_at
                    FROM runs
                    WHERE run_id = ?
                    """,
                    (data["run_id"],),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(row)
            self.assertEqual(row[3], "GRCh38")
            self.assertEqual(row[4], "continue")
            self.assertEqual(row[5], "online")
            self.assertIsNone(row[6])
            self.assertIsNone(row[7])
            self.assertIn('"dbsnp":false', row[8])
            self.assertEqual(row[9], "not_evaluated")
            self.assertIsNone(row[10])

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
            self.assertEqual(created["annotation_evidence_policy"], "continue")
            self.assertEqual(created["evidence_mode_requested"], "online")
            self.assertIsNone(created["evidence_mode_effective"])
            self.assertEqual(created["evidence_offline_sources_configured"], {"dbsnp": False, "clinvar": False, "gnomad": False})

            canceled = cancel_run(db_path, created["run_id"])
            self.assertEqual(canceled["run_id"], created["run_id"])
            self.assertEqual(canceled["status"], "canceled")
            self.assertEqual(canceled["reference_build"], "GRCh38")
            self.assertEqual(canceled["annotation_evidence_policy"], "continue")
            self.assertEqual(canceled["evidence_mode_requested"], "online")

            fetched = get_run(db_path, created["run_id"])
            self.assertIsNotNone(fetched)
            self.assertEqual(fetched["status"], "canceled")
            self.assertEqual(fetched["reference_build"], "GRCh38")
            self.assertEqual(fetched["annotation_evidence_policy"], "continue")
            self.assertEqual(fetched["evidence_mode_requested"], "online")

    def test_cancel_run_clears_gnomad_evidence_rows(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.gnomad_evidence import (  # noqa: E402
            list_gnomad_evidence_for_run,
            upsert_gnomad_evidence_for_run,
        )
        from storage.runs import cancel_run, create_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            created = create_run(db_path)
            run_id = created["run_id"]
            uploaded_at = "2026-03-10T00:00:00+00:00"

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("v1", run_id, "1", 1, "A", "G", 1, uploaded_at),
                )
                conn.commit()

            upsert_gnomad_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "source": "gnomad",
                        "outcome": "found",
                        "gnomad_variant_id": "1-1-A-G",
                        "global_af": 0.2,
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": uploaded_at,
                    }
                ],
            )
            self.assertEqual(len(list_gnomad_evidence_for_run(db_path, run_id, limit=10)), 1)

            canceled = cancel_run(db_path, run_id)
            self.assertEqual(canceled["status"], "canceled")
            self.assertEqual(list_gnomad_evidence_for_run(db_path, run_id, limit=10), [])

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
            self.assertEqual(created_payload["data"]["annotation_evidence_policy"], "continue")
            self.assertEqual(created_payload["data"]["evidence_mode_requested"], "online")

            cancel_resp = client.post(f"/api/v1/runs/{run_id}/cancel")
            self.assertEqual(cancel_resp.status_code, 200)
            cancel_payload = json.loads(cancel_resp.get_data(as_text=True))
            self.assertIs(cancel_payload.get("ok"), True)
            self.assertEqual(cancel_payload["data"]["run_id"], run_id)
            self.assertEqual(cancel_payload["data"]["status"], "canceled")
            self.assertEqual(cancel_payload["data"]["reference_build"], "GRCh38")
            self.assertEqual(cancel_payload["data"]["annotation_evidence_policy"], "continue")
            self.assertEqual(cancel_payload["data"]["evidence_mode_requested"], "online")

            stages_resp = client.get(f"/api/v1/runs/{run_id}/stages")
            self.assertEqual(stages_resp.status_code, 200)
            stages_payload = json.loads(stages_resp.get_data(as_text=True))
            stages = stages_payload["data"]["stages"]
            self.assertTrue(all(stage["status"] == "canceled" for stage in stages))

    def test_post_cancel_clears_gnomad_evidence_rows(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.gnomad_evidence import (  # noqa: E402
            list_gnomad_evidence_for_run,
            upsert_gnomad_evidence_for_run,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            created_resp = client.post("/api/v1/runs")
            self.assertEqual(created_resp.status_code, 200)
            run_id = json.loads(created_resp.get_data(as_text=True))["data"]["run_id"]
            uploaded_at = "2026-03-10T00:00:00+00:00"

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("v1", run_id, "1", 1, "A", "G", 1, uploaded_at),
                )
                conn.commit()

            upsert_gnomad_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "source": "gnomad",
                        "outcome": "found",
                        "gnomad_variant_id": "1-1-A-G",
                        "global_af": 0.2,
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": uploaded_at,
                    }
                ],
            )
            self.assertEqual(len(list_gnomad_evidence_for_run(db_path, run_id, limit=10)), 1)

            cancel_resp = client.post(f"/api/v1/runs/{run_id}/cancel")
            self.assertEqual(cancel_resp.status_code, 200)
            self.assertEqual(list_gnomad_evidence_for_run(db_path, run_id, limit=10), [])

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
            self.assertEqual(payload["data"]["annotation_evidence_policy"], "continue")
            self.assertEqual(payload["data"]["evidence_mode_requested"], "online")
            self.assertIsNone(payload["data"]["evidence_mode_effective"])

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
            self.assertEqual(record["annotation_evidence_policy"], "continue")
            self.assertEqual(record["evidence_mode_requested"], "online")
            self.assertIsNone(record["evidence_mode_effective"])

    def test_schema_migration_uses_legacy_strict_env_for_annotation_policy(self):
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
                      created_at TEXT NOT NULL,
                      reference_build TEXT NOT NULL DEFAULT 'GRCh38'
                    );
                    """
                )
                conn.execute(
                    "INSERT INTO runs (run_id, status, created_at, reference_build) VALUES (?, ?, ?, ?)",
                    ("legacy-run", "queued", "2026-03-07T00:00:00+00:00", "GRCh38"),
                )
                conn.commit()
            finally:
                conn.close()

            with patch.dict(
                os.environ,
                {
                    "SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT": "",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                },
                clear=False,
            ):
                record = get_run(db_path, "legacy-run")

            self.assertIsNotNone(record)
            self.assertEqual(record["annotation_evidence_policy"], "stop")

            conn = sqlite3.connect(db_path)
            try:
                persisted_policy = conn.execute(
                    "SELECT annotation_evidence_policy FROM runs WHERE run_id = ?",
                    ("legacy-run",),
                ).fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(persisted_policy, "stop")

    def test_post_runs_accepts_annotation_evidence_policy(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post(
                "/api/v1/runs",
                data=json.dumps({"annotation_evidence_policy": "stop"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["annotation_evidence_policy"], "stop")

    def test_post_runs_rejects_invalid_annotation_evidence_policy(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post(
                "/api/v1/runs",
                data=json.dumps({"annotation_evidence_policy": "invalid"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"]["code"], "RUN_CREATE_INVALID")

    def test_post_runs_rejects_malformed_json_without_creating_run(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.post(
                "/api/v1/runs",
                data='{"annotation_evidence_policy":"stop"',
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"]["code"], "RUN_CREATE_INVALID")

            created_resp = client.post("/api/v1/runs")
            self.assertEqual(created_resp.status_code, 200)

            conn = sqlite3.connect(db_path)
            try:
                row_count = conn.execute("SELECT COUNT(*) FROM runs;").fetchone()[0]
            finally:
                conn.close()
            self.assertEqual(row_count, 1)

    def test_post_run_settings_updates_annotation_evidence_policy(self):
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

            resp = client.post(
                f"/api/v1/runs/{run_id}/settings",
                data=json.dumps({"annotation_evidence_policy": "stop"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["annotation_evidence_policy"], "stop")

    def test_post_run_settings_rejects_running_run(self):
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

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("UPDATE runs SET status = 'running' WHERE run_id = ?", (run_id,))
                conn.commit()
            finally:
                conn.close()

            resp = client.post(
                f"/api/v1/runs/{run_id}/settings",
                data=json.dumps({"annotation_evidence_policy": "stop"}),
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 409)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"]["code"], "RUN_SETTINGS_NOT_UPDATABLE")

    def test_post_run_settings_rejects_malformed_json(self):
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

            resp = client.post(
                f"/api/v1/runs/{run_id}/settings",
                data='{"annotation_evidence_policy":"continue"',
                content_type="application/json",
            )
            self.assertEqual(resp.status_code, 400)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"]["code"], "RUN_SETTINGS_INVALID")

    def test_get_run_stages_returns_six_queued_stages_in_order(self):
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

            resp = client.get(f"/api/v1/runs/{run_id}/stages")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), True)
            self.assertEqual(payload["data"]["run_id"], run_id)

            stages = payload["data"]["stages"]
            self.assertEqual(
                [s["stage_name"] for s in stages],
                ["parser", "pre_annotation", "classification", "prediction", "annotation", "reporting"],
            )
            self.assertTrue(all(s["status"] == "queued" for s in stages))

    def test_create_app_recovers_interrupted_running_run_on_startup(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402
        from storage.runs import create_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("UPDATE runs SET status = 'running' WHERE run_id = ?", (run_id,))
                conn.execute(
                    """
                    UPDATE run_stages
                    SET status = 'running', started_at = ?, completed_at = NULL
                    WHERE run_id = ? AND stage_name = 'parser'
                    """,
                    ("2026-03-09T00:00:00+00:00", run_id),
                )
                conn.commit()
            finally:
                conn.close()

            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            run_resp = client.get(f"/api/v1/runs/{run_id}")
            self.assertEqual(run_resp.status_code, 200)
            run_payload = json.loads(run_resp.get_data(as_text=True))
            self.assertEqual(run_payload["data"]["status"], "failed")

            stages_resp = client.get(f"/api/v1/runs/{run_id}/stages")
            self.assertEqual(stages_resp.status_code, 200)
            stages_payload = json.loads(stages_resp.get_data(as_text=True))
            by_name = {s["stage_name"]: s for s in stages_payload["data"]["stages"]}
            self.assertEqual(by_name["parser"]["status"], "failed")
            self.assertEqual(by_name["parser"]["error"]["code"], "RUN_INTERRUPTED")

    def test_get_run_stages_unknown_returns_404(self):
        import sys

        if SRC_DIR not in sys.path:
            sys.path.insert(0, SRC_DIR)

        import app as sp_app  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            flask_app = sp_app.create_app({"TESTING": True, "SP_DB_PATH": db_path})
            client = flask_app.test_client()

            resp = client.get("/api/v1/runs/not-a-real-run-id/stages")
            self.assertEqual(resp.status_code, 404)
            payload = json.loads(resp.get_data(as_text=True))
            self.assertIs(payload.get("ok"), False)
            self.assertEqual(payload["error"]["code"], "RUN_NOT_FOUND")

    def test_get_run_stages_normalizes_legacy_blocked_status_to_failed(self):
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

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "UPDATE run_stages SET status = 'blocked' WHERE run_id = ? AND stage_name = ?",
                    (run_id, "parser"),
                )
                conn.commit()
            finally:
                conn.close()

            resp = client.get(f"/api/v1/runs/{run_id}/stages")
            self.assertEqual(resp.status_code, 200)
            payload = json.loads(resp.get_data(as_text=True))
            parser_stage = [s for s in payload["data"]["stages"] if s["stage_name"] == "parser"][0]
            self.assertEqual(parser_stage["status"], "failed")


if __name__ == "__main__":
    unittest.main()
