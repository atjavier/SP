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


class PreAnnotationStageTestCase(unittest.TestCase):
    def test_pre_annotation_derives_and_persists_basic_fields(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.pre_annotations import list_pre_annotations_for_run  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.stages import mark_stage_succeeded  # noqa: E402

        from pipeline.pre_annotation_stage import run_pre_annotation_stage  # noqa: E402

        uploaded_at = "2026-03-08T00:00:00+00:00"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            mark_stage_succeeded(db_path, run_id, "parser", input_uploaded_at=uploaded_at, stats={"ok": True})

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("v1", run_id, "1", 1, "A", "G", 7, uploaded_at),
                )
                conn.commit()

            result = run_pre_annotation_stage(
                db_path,
                run_id,
                uploaded_at=uploaded_at,
                logger=logging.getLogger("test"),
                force=False,
            )
            self.assertEqual(result["pre_annotation"]["status"], "succeeded")
            self.assertEqual(result["pre_annotation"]["stats"]["variants_processed"], 1)

            rows = list_pre_annotations_for_run(db_path, run_id)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["variant_id"], "v1")
            self.assertEqual(row["variant_key"], "1:1:A>G")
            self.assertEqual(row["base_change"], "A>G")
            self.assertEqual(row["substitution_class"], "transition")
            self.assertEqual(row["ref_class"], "purine")
            self.assertEqual(row["alt_class"], "purine")
            self.assertEqual(row["details"]["source_line"], 7)

    def test_pre_annotation_failure_clears_outputs(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.pre_annotations import list_pre_annotations_for_run  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.stages import get_stage, mark_stage_succeeded  # noqa: E402

        import pipeline.pre_annotation_stage as pre  # noqa: E402

        uploaded_at = "2026-03-08T00:00:00+00:00"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            mark_stage_succeeded(db_path, run_id, "parser", input_uploaded_at=uploaded_at, stats={"ok": True})

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("v1", run_id, "1", 1, "A", "G", 7, uploaded_at),
                )
                conn.commit()

            original_upsert = pre.upsert_pre_annotations_for_run

            def upsert_then_fail(*args, **kwargs):
                original_upsert(*args, **kwargs)
                raise RuntimeError("boom")

            silent_logger = logging.getLogger("test-silent")
            silent_logger.disabled = True

            with patch("pipeline.pre_annotation_stage.upsert_pre_annotations_for_run", side_effect=upsert_then_fail):
                with self.assertRaises(pre.StageExecutionError) as raised:
                    pre.run_pre_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=silent_logger,
                        force=False,
                    )

            self.assertEqual(raised.exception.code, "PRE_ANNOTATION_FAILED")

            stage = get_stage(db_path, run_id, "pre_annotation") or {}
            self.assertEqual(stage.get("status"), "failed")

            self.assertEqual(list_pre_annotations_for_run(db_path, run_id), [])


if __name__ == "__main__":
    unittest.main()
