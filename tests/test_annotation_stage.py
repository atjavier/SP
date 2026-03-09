import logging
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class AnnotationStageTestCase(unittest.TestCase):
    def _seed_ready_run(self, db_path: str, uploaded_at: str) -> str:
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.stages import mark_stage_succeeded  # noqa: E402

        run_id = create_run(db_path)["run_id"]
        mark_stage_succeeded(
            db_path,
            run_id,
            "prediction",
            input_uploaded_at=uploaded_at,
            stats={"ok": True},
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
        return run_id

    def test_annotation_fails_when_snpeff_jar_missing(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "1",
                    "SP_SNPEFF_JAR_PATH": "",
                    "SP_SNPEFF_HOME": "",
                },
                clear=False,
            ):
                with self.assertRaises(StageExecutionError) as raised:
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(raised.exception.code, "SNPEFF_NOT_CONFIGURED")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "SNPEFF_NOT_CONFIGURED")

    def test_annotation_fails_for_windows_absolute_data_dir_outside_home(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            snpeff_home = os.path.join(tmpdir, "snpeff-home")
            os.makedirs(snpeff_home, exist_ok=True)
            jar_path = os.path.join(snpeff_home, "snpEff.jar")
            with open(jar_path, "wb") as f:
                f.write(b"jar-placeholder")

            outside_data_dir = os.path.join(tmpdir, "outside-data")
            os.makedirs(outside_data_dir, exist_ok=True)

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "1",
                    "SP_SNPEFF_HOME": snpeff_home,
                    "SP_SNPEFF_JAR_PATH": jar_path,
                    "SP_SNPEFF_DATA_DIR": outside_data_dir,
                },
                clear=False,
            ):
                with patch("pipeline.annotation_stage.os.name", "nt"):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "SNPEFF_DATADIR_INVALID")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "SNPEFF_DATADIR_INVALID")

    def test_annotation_fails_when_snpeff_times_out(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            snpeff_home = os.path.join(tmpdir, "snpeff-home")
            os.makedirs(snpeff_home, exist_ok=True)
            jar_path = os.path.join(snpeff_home, "snpEff.jar")
            with open(jar_path, "wb") as f:
                f.write(b"jar-placeholder")

            data_dir = os.path.join(snpeff_home, "data", "GRCh38.86")
            os.makedirs(data_dir, exist_ok=True)
            with open(os.path.join(data_dir, "snpEffectPredictor.bin"), "wb") as f:
                f.write(b"db-placeholder")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "1",
                    "SP_SNPEFF_HOME": snpeff_home,
                    "SP_SNPEFF_JAR_PATH": jar_path,
                    "SP_SNPEFF_DATA_DIR": "./data",
                    "SP_SNPEFF_TIMEOUT_SECONDS": "3",
                },
                clear=False,
            ):
                with patch(
                    "pipeline.annotation_stage.subprocess.run",
                    side_effect=subprocess.TimeoutExpired(
                        cmd=["java", "-jar", "snpEff.jar"],
                        timeout=3,
                        stderr=b"timed out",
                    ),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "SNPEFF_TIMEOUT")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "SNPEFF_TIMEOUT")


if __name__ == "__main__":
    unittest.main()
