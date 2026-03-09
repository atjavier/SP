import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class ClassificationsStorageTestCase(unittest.TestCase):
    def test_upsert_list_and_clear_classifications_for_run(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.classifications import (  # noqa: E402
            clear_classifications_for_run,
            list_classifications_for_run,
            upsert_classifications_for_run,
        )

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
                    ("v1", run_id, "1", 1, "A", "G", 12, "2026-03-08T00:00:00+00:00"),
                )
                conn.commit()

            created_at = "2026-03-08T00:00:01+00:00"
            upsert_classifications_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "consequence_category": "unclassified",
                        "reason_code": "NO_CODING_CONTEXT",
                        "reason_message": "No coding context.",
                        "details": {"source_line": 12},
                        "created_at": created_at,
                    }
                ],
            )

            rows = list_classifications_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], run_id)
            self.assertEqual(rows[0]["variant_id"], "v1")
            self.assertEqual(rows[0]["variant_key"], "1:1:A>G")
            self.assertEqual(rows[0]["consequence_category"], "unclassified")
            self.assertEqual(rows[0]["reason_code"], "NO_CODING_CONTEXT")
            self.assertEqual(rows[0]["reason_message"], "No coding context.")
            self.assertEqual(rows[0]["details"]["source_line"], 12)
            self.assertEqual(rows[0]["created_at"], created_at)

            upsert_classifications_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "consequence_category": "unclassified",
                        "reason_code": "NO_CODING_CONTEXT",
                        "reason_message": "Updated.",
                        "details": {"source_line": 12, "note": "updated"},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    }
                ],
            )
            updated = list_classifications_for_run(db_path, run_id, limit=10)
            self.assertEqual(updated[0]["reason_message"], "Updated.")
            self.assertEqual(updated[0]["details"]["note"], "updated")

            clear_classifications_for_run(db_path, run_id)
            self.assertEqual(list_classifications_for_run(db_path, run_id, limit=10), [])


if __name__ == "__main__":
    unittest.main()

