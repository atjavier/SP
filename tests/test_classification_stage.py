import logging
import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class ClassificationStageTestCase(unittest.TestCase):
    def test_classification_precondition_failure_clears_existing_outputs(self):
        from storage.classifications import list_classifications_for_run, upsert_classifications_for_run  # noqa: E402
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        from pipeline.classification_stage import run_classification_stage  # noqa: E402
        from pipeline.parser_stage import StageExecutionError  # noqa: E402

        uploaded_at = "2026-03-08T00:00:00+00:00"

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

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

            upsert_classifications_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "consequence_category": "unclassified",
                        "reason_code": "NO_CODING_CONTEXT",
                        "reason_message": "stale",
                        "details": {"source_line": 7},
                        "created_at": uploaded_at,
                    }
                ],
            )
            self.assertEqual(len(list_classifications_for_run(db_path, run_id, limit=10)), 1)

            with self.assertRaises(StageExecutionError) as raised:
                run_classification_stage(
                    db_path,
                    run_id,
                    uploaded_at=uploaded_at,
                    logger=logging.getLogger("test"),
                    force=False,
                )

            self.assertEqual(raised.exception.code, "MISSING_PARSER_OUTPUT")
            self.assertEqual(list_classifications_for_run(db_path, run_id, limit=10), [])

            stage = get_stage(db_path, run_id, "classification") or {}
            self.assertEqual(stage.get("status"), "failed")


if __name__ == "__main__":
    unittest.main()

