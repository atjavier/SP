import os
import sqlite3
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class PreAnnotationsStorageTestCase(unittest.TestCase):
    def test_upsert_list_and_clear_pre_annotations_for_run(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.pre_annotations import (  # noqa: E402
            clear_pre_annotations_for_run,
            list_pre_annotations_for_run,
            list_pre_annotations_for_run_public,
            upsert_pre_annotations_for_run,
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
            upsert_pre_annotations_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "variant_key": "1:1:A>G",
                        "base_change": "A>G",
                        "substitution_class": "transition",
                        "ref_class": "purine",
                        "alt_class": "purine",
                        "details": {"source_line": 12},
                        "created_at": created_at,
                    }
                ],
            )

            rows = list_pre_annotations_for_run(db_path, run_id)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], run_id)
            self.assertEqual(rows[0]["variant_id"], "v1")
            self.assertEqual(rows[0]["variant_key"], "1:1:A>G")
            self.assertEqual(rows[0]["substitution_class"], "transition")
            self.assertEqual(rows[0]["details"]["source_line"], 12)
            self.assertEqual(rows[0]["created_at"], created_at)

            upsert_pre_annotations_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "variant_key": "1:1:A>G",
                        "base_change": "A>G",
                        "substitution_class": "transversion",
                        "ref_class": "purine",
                        "alt_class": "purine",
                        "details": {"source_line": 12, "note": "updated"},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    }
                ],
            )
            updated = list_pre_annotations_for_run(db_path, run_id)
            self.assertEqual(updated[0]["substitution_class"], "transversion")
            self.assertEqual(updated[0]["details"]["note"], "updated")

            public_rows = list_pre_annotations_for_run_public(db_path, run_id, limit=10)
            self.assertEqual(len(public_rows), 1)
            self.assertEqual(public_rows[0]["variant_id"], "v1")
            self.assertNotIn("details", public_rows[0])
            self.assertNotIn("rsid", public_rows[0])
            self.assertNotIn("gnomad_global_af", public_rows[0])
            self.assertNotIn("clinvar_clinical_significance", public_rows[0])
            self.assertNotIn("known_variant", public_rows[0])
            self.assertNotIn("common_variant", public_rows[0])

            clear_pre_annotations_for_run(db_path, run_id)
            self.assertEqual(list_pre_annotations_for_run(db_path, run_id), [])


if __name__ == "__main__":
    unittest.main()
