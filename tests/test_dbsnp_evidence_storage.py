import os
import sqlite3
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class DbsnpEvidenceStorageTestCase(unittest.TestCase):
    def test_upsert_list_and_clear_dbsnp_evidence_for_run(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.dbsnp_evidence import (  # noqa: E402
            clear_dbsnp_evidence_for_run,
            list_dbsnp_evidence_for_run,
            upsert_dbsnp_evidence_for_run,
        )
        from storage.runs import create_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = create_run(db_path)["run_id"]

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.executemany(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("v1", run_id, "1", 100, "A", "G", 1, "2026-03-09T00:00:00+00:00"),
                        ("v2", run_id, "2", 200, "C", "T", 2, "2026-03-09T00:00:00+00:00"),
                    ],
                )
                conn.commit()

            upsert_dbsnp_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "source": "dbsnp",
                        "outcome": "found",
                        "rsid": "rs123",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {"refsnp_id": 123},
                        "retrieved_at": "2026-03-09T00:00:01+00:00",
                    },
                    {
                        "variant_id": "v2",
                        "source": "dbsnp",
                        "outcome": "not_found",
                        "rsid": None,
                        "reason_code": "NOT_FOUND",
                        "reason_message": "No rsID found for this variant.",
                        "details": {},
                        "retrieved_at": "2026-03-09T00:00:02+00:00",
                    },
                ],
            )

            rows = list_dbsnp_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["variant_id"], "v1")
            self.assertEqual(rows[0]["variant_key"], "1:100:A>G")
            self.assertEqual(rows[0]["source"], "dbsnp")
            self.assertEqual(rows[0]["outcome"], "found")
            self.assertEqual(rows[0]["rsid"], "rs123")
            self.assertEqual(rows[1]["variant_id"], "v2")
            self.assertEqual(rows[1]["outcome"], "not_found")

            filtered = list_dbsnp_evidence_for_run(db_path, run_id, variant_id="v2", limit=10)
            self.assertEqual(len(filtered), 1)
            self.assertEqual(filtered[0]["variant_id"], "v2")

            upsert_dbsnp_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v2",
                        "source": "dbsnp",
                        "outcome": "found",
                        "rsid": "rs999",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {"refsnp_id": 999},
                        "retrieved_at": "2026-03-09T00:00:03+00:00",
                    }
                ],
            )
            updated = list_dbsnp_evidence_for_run(db_path, run_id, variant_id="v2", limit=10)
            self.assertEqual(updated[0]["outcome"], "found")
            self.assertEqual(updated[0]["rsid"], "rs999")

            clear_dbsnp_evidence_for_run(db_path, run_id)
            self.assertEqual(list_dbsnp_evidence_for_run(db_path, run_id, limit=10), [])

    def test_dbsnp_evidence_reject_run_variant_mismatch(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.dbsnp_evidence import upsert_dbsnp_evidence_for_run  # noqa: E402
        from storage.runs import create_run  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id_1 = create_run(db_path)["run_id"]
            run_id_2 = create_run(db_path)["run_id"]

            with open_connection(db_path) as conn:
                init_schema(conn)
                conn.execute(
                    """
                    INSERT INTO run_variants (
                      variant_id, run_id, chrom, pos, ref, alt, source_line, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("v1", run_id_1, "1", 1, "A", "G", 1, "2026-03-09T00:00:00+00:00"),
                )
                conn.commit()

            with self.assertRaises(sqlite3.IntegrityError) as ctx:
                upsert_dbsnp_evidence_for_run(
                    db_path,
                    run_id_2,
                    [
                        {
                            "variant_id": "v1",
                            "source": "dbsnp",
                            "outcome": "found",
                            "rsid": "rs123",
                            "reason_code": None,
                            "reason_message": None,
                            "details": {},
                            "retrieved_at": "2026-03-09T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("RUN_VARIANT_MISMATCH", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

