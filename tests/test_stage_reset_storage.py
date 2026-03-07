import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class StageResetStorageTestCase(unittest.TestCase):
    def test_reset_stage_and_downstream_resets_only_selected_and_downstream(self):
        from storage.runs import create_run  # noqa: E402
        from storage.stages import (  # noqa: E402
            list_pipeline_stages,
            mark_stage_failed,
            mark_stage_succeeded,
            reset_stage_and_downstream,
        )

        uploaded_at = "2026-03-08T00:00:00+00:00"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            mark_stage_succeeded(
                db_path, run_id, "parser", input_uploaded_at=uploaded_at, stats={"ok": True}
            )
            mark_stage_succeeded(
                db_path,
                run_id,
                "pre_annotation",
                input_uploaded_at=uploaded_at,
                stats={"ok": True},
            )
            mark_stage_failed(
                db_path,
                run_id,
                "classification",
                input_uploaded_at=uploaded_at,
                error_code="STAGE_FAILED",
                error_message="boom",
                error_details={"why": "unit test"},
            )
            mark_stage_succeeded(
                db_path,
                run_id,
                "prediction",
                input_uploaded_at=uploaded_at,
                stats={"ok": True},
            )

            reset_stage_and_downstream(db_path, run_id, "classification")

            by_name = {
                stage["stage_name"]: stage for stage in list_pipeline_stages(db_path, run_id)
            }

            self.assertEqual(by_name["parser"]["status"], "succeeded")
            self.assertIsNotNone(by_name["parser"].get("completed_at"))
            self.assertEqual(by_name["parser"]["input_uploaded_at"], uploaded_at)
            self.assertEqual(by_name["parser"]["stats"], {"ok": True})
            self.assertIsNone(by_name["parser"]["error"])

            self.assertEqual(by_name["pre_annotation"]["status"], "succeeded")
            self.assertIsNotNone(by_name["pre_annotation"].get("completed_at"))
            self.assertEqual(by_name["pre_annotation"]["input_uploaded_at"], uploaded_at)

            for stage_name in ("classification", "prediction", "annotation", "reporting"):
                stage = by_name[stage_name]
                self.assertEqual(stage["status"], "queued")
                self.assertIsNone(stage.get("started_at"))
                self.assertIsNone(stage.get("completed_at"))
                self.assertIsNone(stage.get("input_uploaded_at"))
                self.assertIsNone(stage.get("stats"))
                self.assertIsNone(stage.get("error"))

    def test_reset_stage_and_downstream_clears_variants_when_resetting_parser(self):
        from storage.runs import create_run  # noqa: E402
        from storage.stages import reset_stage_and_downstream  # noqa: E402
        from storage.variants import insert_variants_for_run, list_variants_for_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            insert_variants_for_run(
                db_path,
                run_id,
                [
                    {
                        "chrom": "1",
                        "pos": 1,
                        "ref": "A",
                        "alt": "T",
                        "source_line": 1,
                    }
                ],
            )
            self.assertEqual(len(list_variants_for_run(db_path, run_id, limit=10)), 1)

            reset_stage_and_downstream(db_path, run_id, "parser")

            self.assertEqual(len(list_variants_for_run(db_path, run_id, limit=10)), 0)

    def test_reset_stage_and_downstream_rejects_unknown_stage(self):
        from storage.runs import create_run  # noqa: E402
        from storage.stages import reset_stage_and_downstream  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            with self.assertRaises(ValueError):
                reset_stage_and_downstream(db_path, run_id, "nope")

    def test_reset_stage_and_downstream_refuses_when_run_canceled(self):
        from storage.runs import create_run, set_run_status  # noqa: E402
        from storage.stages import StageResetRunCanceledError, reset_stage_and_downstream  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]
            set_run_status(db_path, run_id, "canceled")

            with self.assertRaises(StageResetRunCanceledError):
                reset_stage_and_downstream(db_path, run_id, "parser")


if __name__ == "__main__":
    unittest.main()
