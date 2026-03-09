import json
import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class PredictionStageTestCase(unittest.TestCase):
    def _seed_ready_run(self, db_path: str, uploaded_at: str, *, category: str = "missense") -> str:
        from storage.classifications import upsert_classifications_for_run  # noqa: E402
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.stages import mark_stage_succeeded  # noqa: E402

        run_id = create_run(db_path)["run_id"]
        mark_stage_succeeded(db_path, run_id, "parser", input_uploaded_at=uploaded_at, stats={"ok": True})
        mark_stage_succeeded(
            db_path, run_id, "pre_annotation", input_uploaded_at=uploaded_at, stats={"ok": True}
        )
        mark_stage_succeeded(
            db_path, run_id, "classification", input_uploaded_at=uploaded_at, stats={"ok": True}
        )

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

        upsert_classifications_for_run(
            db_path,
            run_id,
            [
                {
                    "variant_id": "v1",
                    "consequence_category": category,
                    "reason_code": None if category != "unclassified" else "NO_CODING_CONTEXT",
                    "reason_message": None if category != "unclassified" else "No coding context.",
                    "details": {"source_line": 1},
                    "created_at": uploaded_at,
                }
            ],
        )
        return run_id

    def test_prediction_maps_tool_output_to_computed_rows_for_missense(self):
        from pipeline.prediction_stage import run_prediction_stage  # noqa: E402
        from storage.predictor_outputs import list_predictor_outputs_for_run  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, category="missense")

            cache_dir = os.path.join(tmpdir, "vep-cache")
            os.makedirs(cache_dir, exist_ok=True)
            alpha_file = os.path.join(tmpdir, "alphamissense.tsv")
            with open(alpha_file, "w", encoding="utf-8") as f:
                f.write("placeholder\n")

            def fake_run(cmd, cwd, stdout, stderr, check, timeout):
                del cwd, stdout, stderr, check, timeout
                payload = {
                    "seq_region_name": "1",
                    "start": 1,
                    "allele_string": "A/G",
                    "transcript_consequences": [
                        {
                            "sift_prediction": "deleterious",
                            "sift_score": 0.01,
                            "polyphen_prediction": "probably_damaging",
                            "polyphen_score": 0.99,
                            "am_pathogenicity": 0.87,
                            "am_class": "likely_pathogenic",
                        }
                    ],
                }
                output_path = cmd[cmd.index("--output_file") + 1]
                with open(output_path, "w", encoding="utf-8", newline="\n") as out_f:
                    out_f.write(json.dumps(payload) + "\n")

                class Completed:
                    returncode = 0
                    stderr = b""
                    stdout = b""

                return Completed()

            with patch.dict(
                os.environ,
                {
                    "SP_VEP_CMD": "vep",
                    "SP_VEP_CACHE_DIR": cache_dir,
                    "SP_VEP_ALPHAMISSENSE_FILE": alpha_file,
                    "SP_VEP_TIMEOUT_SECONDS": "30",
                },
                clear=False,
            ):
                with patch("pipeline.prediction_stage.subprocess.run", side_effect=fake_run):
                    result = run_prediction_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["prediction"]["status"], "succeeded")
            rows = list_predictor_outputs_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 3)
            by_key = {row["predictor_key"]: row for row in rows}
            self.assertEqual(by_key["sift"]["outcome"], "computed")
            self.assertEqual(by_key["polyphen2"]["outcome"], "computed")
            self.assertEqual(by_key["alphamissense"]["outcome"], "computed")
            self.assertAlmostEqual(by_key["sift"]["score"], 0.01, places=6)
            self.assertAlmostEqual(by_key["polyphen2"]["score"], 0.99, places=6)
            self.assertAlmostEqual(by_key["alphamissense"]["score"], 0.87, places=6)

    def test_prediction_marks_non_missense_as_not_applicable(self):
        from pipeline.prediction_stage import run_prediction_stage  # noqa: E402
        from storage.predictor_outputs import list_predictor_outputs_for_run  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, category="synonymous")

            cache_dir = os.path.join(tmpdir, "vep-cache")
            os.makedirs(cache_dir, exist_ok=True)
            alpha_file = os.path.join(tmpdir, "alphamissense.tsv")
            with open(alpha_file, "w", encoding="utf-8") as f:
                f.write("placeholder\n")

            def fake_run(cmd, cwd, stdout, stderr, check, timeout):
                del cwd, stdout, stderr, check, timeout
                payload = {
                    "seq_region_name": "1",
                    "start": 1,
                    "allele_string": "A/G",
                    "transcript_consequences": [
                        {
                            "sift_prediction": "deleterious",
                            "sift_score": 0.01,
                            "polyphen_prediction": "probably_damaging",
                            "polyphen_score": 0.99,
                            "am_pathogenicity": 0.87,
                            "am_class": "likely_pathogenic",
                        }
                    ],
                }
                output_path = cmd[cmd.index("--output_file") + 1]
                with open(output_path, "w", encoding="utf-8", newline="\n") as out_f:
                    out_f.write(json.dumps(payload) + "\n")

                class Completed:
                    returncode = 0
                    stderr = b""
                    stdout = b""

                return Completed()

            with patch.dict(
                os.environ,
                {
                    "SP_VEP_CMD": "vep",
                    "SP_VEP_CACHE_DIR": cache_dir,
                    "SP_VEP_ALPHAMISSENSE_FILE": alpha_file,
                    "SP_VEP_TIMEOUT_SECONDS": "30",
                },
                clear=False,
            ):
                with patch("pipeline.prediction_stage.subprocess.run", side_effect=fake_run):
                    result = run_prediction_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["prediction"]["status"], "succeeded")
            rows = list_predictor_outputs_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 3)
            for row in rows:
                self.assertEqual(row["outcome"], "not_applicable")
                self.assertEqual(row["reason_code"], "NOT_MISSENSE")
                self.assertIsNone(row["score"])
                self.assertIsNone(row["label"])

    def test_prediction_module_does_not_expose_mvp_placeholder_generator(self):
        import pipeline.prediction_stage as pred  # noqa: E402

        self.assertFalse(hasattr(pred, "_make_mvp_output"))

    def test_prediction_fails_with_actionable_error_when_config_missing(self):
        from pipeline.parser_stage import StageExecutionError  # noqa: E402
        from pipeline.prediction_stage import run_prediction_stage  # noqa: E402
        from storage.predictor_outputs import list_predictor_outputs_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, category="missense")

            with patch.dict(
                os.environ,
                {
                    "SP_VEP_CMD": "vep",
                    "SP_VEP_CACHE_DIR": "",
                    "SP_VEP_ALPHAMISSENSE_FILE": "",
                },
                clear=False,
            ):
                with self.assertRaises(StageExecutionError) as raised:
                    run_prediction_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(raised.exception.code, "VEP_NOT_CONFIGURED")
            stage = get_stage(db_path, run_id, "prediction") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "VEP_NOT_CONFIGURED")
            self.assertEqual(list_predictor_outputs_for_run(db_path, run_id, limit=10), [])

    def test_prediction_timeout_marks_stage_failed_and_clears_outputs(self):
        from pipeline.parser_stage import StageExecutionError  # noqa: E402
        from pipeline.prediction_stage import run_prediction_stage  # noqa: E402
        from storage.predictor_outputs import (  # noqa: E402
            list_predictor_outputs_for_run,
            upsert_predictor_outputs_for_run,
        )
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, category="missense")

            upsert_predictor_outputs_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "predictor_key": "sift",
                        "outcome": "not_computed",
                        "score": None,
                        "label": None,
                        "reason_code": "STALE",
                        "reason_message": "stale",
                        "details": {},
                        "created_at": uploaded_at,
                    }
                ],
            )
            self.assertEqual(len(list_predictor_outputs_for_run(db_path, run_id, limit=10)), 1)

            cache_dir = os.path.join(tmpdir, "vep-cache")
            os.makedirs(cache_dir, exist_ok=True)
            alpha_file = os.path.join(tmpdir, "alphamissense.tsv")
            with open(alpha_file, "w", encoding="utf-8") as f:
                f.write("placeholder\n")

            with patch.dict(
                os.environ,
                {
                    "SP_VEP_CMD": "vep",
                    "SP_VEP_CACHE_DIR": cache_dir,
                    "SP_VEP_ALPHAMISSENSE_FILE": alpha_file,
                    "SP_VEP_TIMEOUT_SECONDS": "3",
                },
                clear=False,
            ):
                with patch(
                    "pipeline.prediction_stage.subprocess.run",
                    side_effect=TimeoutError("timed out"),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_prediction_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "VEP_TIMEOUT")
            stage = get_stage(db_path, run_id, "prediction") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "VEP_TIMEOUT")
            self.assertEqual(list_predictor_outputs_for_run(db_path, run_id, limit=10), [])

    def test_prediction_cancel_wins_and_outputs_are_cleared(self):
        from pipeline.parser_stage import StageExecutionError  # noqa: E402
        import pipeline.prediction_stage as pred  # noqa: E402
        from storage.predictor_outputs import list_predictor_outputs_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, category="missense")

            cache_dir = os.path.join(tmpdir, "vep-cache")
            os.makedirs(cache_dir, exist_ok=True)
            alpha_file = os.path.join(tmpdir, "alphamissense.tsv")
            with open(alpha_file, "w", encoding="utf-8") as f:
                f.write("placeholder\n")

            def fake_run(cmd, cwd, stdout, stderr, check, timeout):
                del cwd, stdout, stderr, check, timeout
                payload = {
                    "seq_region_name": "1",
                    "start": 1,
                    "allele_string": "A/G",
                    "transcript_consequences": [
                        {"sift_prediction": "deleterious", "sift_score": 0.01}
                    ],
                }
                output_path = cmd[cmd.index("--output_file") + 1]
                with open(output_path, "w", encoding="utf-8", newline="\n") as out_f:
                    out_f.write(json.dumps(payload) + "\n")

                class Completed:
                    returncode = 0
                    stderr = b""
                    stdout = b""

                return Completed()

            original_upsert = pred.upsert_predictor_outputs_for_run
            original_get_run_status = pred._get_run_status
            canceled_after_persist = {"value": False}

            def cancel_then_upsert(db_path_arg, run_id_arg, outputs, **kwargs):
                result = original_upsert(db_path_arg, run_id_arg, outputs, **kwargs)
                canceled_after_persist["value"] = True
                return result

            def get_run_status_with_cancel(conn, run_id_arg):
                if run_id_arg == run_id and canceled_after_persist["value"]:
                    return "canceled"
                return original_get_run_status(conn, run_id_arg)

            with patch.dict(
                os.environ,
                {
                    "SP_VEP_CMD": "vep",
                    "SP_VEP_CACHE_DIR": cache_dir,
                    "SP_VEP_ALPHAMISSENSE_FILE": alpha_file,
                    "SP_VEP_TIMEOUT_SECONDS": "30",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.prediction_stage.subprocess.run", side_effect=fake_run),
                    patch(
                        "pipeline.prediction_stage.upsert_predictor_outputs_for_run",
                        side_effect=cancel_then_upsert,
                    ),
                    patch(
                        "pipeline.prediction_stage._get_run_status",
                        side_effect=get_run_status_with_cancel,
                    ),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        pred.run_prediction_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "RUN_CANCELED")
            stage = get_stage(db_path, run_id, "prediction") or {}
            self.assertEqual(stage.get("status"), "canceled")
            self.assertEqual(list_predictor_outputs_for_run(db_path, run_id, limit=10), [])


if __name__ == "__main__":
    unittest.main()
