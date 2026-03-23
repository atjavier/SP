import os
import sqlite3
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class PredictorOutputsStorageTestCase(unittest.TestCase):
    def test_upsert_list_and_clear_predictor_outputs_for_run(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.predictor_outputs import (  # noqa: E402
            clear_predictor_outputs_for_run,
            list_predictor_outputs_for_run,
            upsert_predictor_outputs_for_run,
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
                        ("v1", run_id, "2", 2, "A", "G", 12, "2026-03-08T00:00:00+00:00"),
                        ("v2", run_id, "10", 1, "C", "T", 13, "2026-03-08T00:00:00+00:00"),
                        ("v3", run_id, "X", 5, "G", "A", 14, "2026-03-08T00:00:00+00:00"),
                    ],
                )
                conn.commit()

            created_at = "2026-03-08T00:00:01+00:00"
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
                        "reason_code": "NO_PROTEIN_CONTEXT",
                        "reason_message": "No transcript/protein context available in MVP.",
                        "details": {"note": "mvp"},
                        "created_at": created_at,
                    },
                    {
                        "variant_id": "v2",
                        "predictor_key": "sift",
                        "outcome": "computed",
                        "score": 0.12,
                        "label": "deleterious",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": created_at,
                    },
                ],
            )

            rows = list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift")
            self.assertEqual([r["variant_id"] for r in rows], ["v1", "v2"])
            self.assertEqual(rows[0]["outcome"], "not_computed")
            self.assertIsNone(rows[0]["score"])
            self.assertEqual(rows[0]["reason_code"], "NO_PROTEIN_CONTEXT")
            self.assertEqual(rows[1]["outcome"], "computed")
            self.assertEqual(rows[1]["score"], 0.12)
            self.assertEqual(rows[1]["label"], "deleterious")
            self.assertEqual(rows[1]["created_at"], created_at)

            # Ordering should follow chrom sort (2, 10, X) and pos/ref/alt.
            upsert_predictor_outputs_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v3",
                        "predictor_key": "sift",
                        "outcome": "not_applicable",
                        "score": None,
                        "label": None,
                        "reason_code": "NOT_MISSENSE",
                        "reason_message": "SIFT is only applicable to missense variants.",
                        "details": {},
                        "created_at": created_at,
                    }
                ],
            )
            ordered = list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift")
            self.assertEqual([r["variant_id"] for r in ordered], ["v1", "v2", "v3"])

            # Upsert should update in place.
            upsert_predictor_outputs_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v2",
                        "predictor_key": "sift",
                        "outcome": "computed",
                        "score": 0.34,
                        "label": "tolerated",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {"updated": True},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    }
                ],
            )
            updated = list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift")
            self.assertEqual(updated[1]["score"], 0.34)
            self.assertEqual(updated[1]["label"], "tolerated")
            self.assertTrue(updated[1]["details"]["updated"])

            clear_predictor_outputs_for_run(db_path, run_id, predictor_key="sift")
            self.assertEqual(list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift"), [])

    def test_predictor_outputs_reject_run_variant_mismatch(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.predictor_outputs import upsert_predictor_outputs_for_run  # noqa: E402
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
                    ("v1", run_id_1, "1", 1, "A", "G", 12, "2026-03-08T00:00:00+00:00"),
                )
                conn.commit()

            with self.assertRaises(sqlite3.IntegrityError) as ctx:
                upsert_predictor_outputs_for_run(
                    db_path,
                    run_id_2,
                    [
                        {
                            "variant_id": "v1",
                            "predictor_key": "sift",
                            "outcome": "not_computed",
                            "score": None,
                            "label": None,
                            "reason_code": "NO_PROTEIN_CONTEXT",
                            "reason_message": "No transcript/protein context available in MVP.",
                            "details": {},
                            "created_at": "2026-03-08T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("RUN_VARIANT_MISMATCH", str(ctx.exception))

    def test_predictor_outputs_enforce_score_invariants(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.predictor_outputs import upsert_predictor_outputs_for_run  # noqa: E402
        from storage.runs import create_run  # noqa: E402

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

            # computed requires a score
            with self.assertRaises(sqlite3.IntegrityError) as ctx_missing_score:
                upsert_predictor_outputs_for_run(
                    db_path,
                    run_id,
                    [
                        {
                            "variant_id": "v1",
                            "predictor_key": "sift",
                            "outcome": "computed",
                            "score": None,
                            "label": "tolerated",
                            "reason_code": None,
                            "reason_message": None,
                            "details": {},
                            "created_at": "2026-03-08T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("MISSING_SCORE", str(ctx_missing_score.exception))

            # non-computed must have a null score
            with self.assertRaises(sqlite3.IntegrityError) as ctx_score_must_be_null:
                upsert_predictor_outputs_for_run(
                    db_path,
                    run_id,
                    [
                        {
                            "variant_id": "v1",
                            "predictor_key": "sift",
                            "outcome": "not_computed",
                            "score": 0.1,
                            "label": None,
                            "reason_code": "NO_PROTEIN_CONTEXT",
                            "reason_message": "No transcript/protein context available in MVP.",
                            "details": {},
                            "created_at": "2026-03-08T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("SCORE_MUST_BE_NULL", str(ctx_score_must_be_null.exception))

    def test_predictor_outputs_enforce_label_and_reason_invariants(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.predictor_outputs import upsert_predictor_outputs_for_run  # noqa: E402
        from storage.runs import create_run  # noqa: E402

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

            with self.assertRaises(sqlite3.IntegrityError) as ctx_label_must_be_null:
                upsert_predictor_outputs_for_run(
                    db_path,
                    run_id,
                    [
                        {
                            "variant_id": "v1",
                            "predictor_key": "sift",
                            "outcome": "not_computed",
                            "score": None,
                            "label": "deleterious",
                            "reason_code": "NO_PROTEIN_CONTEXT",
                            "reason_message": "No transcript/protein context available in MVP.",
                            "details": {},
                            "created_at": "2026-03-08T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("LABEL_MUST_BE_NULL", str(ctx_label_must_be_null.exception))

            with self.assertRaises(sqlite3.IntegrityError) as ctx_missing_reason:
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
                            "reason_code": None,
                            "reason_message": None,
                            "details": {},
                            "created_at": "2026-03-08T00:00:01+00:00",
                        }
                    ],
                )
            self.assertIn("MISSING_REASON_CODE", str(ctx_missing_reason.exception))

    def test_predictor_outputs_pagination_and_count(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.predictor_outputs import (  # noqa: E402
            count_predictor_outputs_for_run,
            list_predictor_outputs_for_run,
            upsert_predictor_outputs_for_run,
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
                        ("v1", run_id, "1", 1, "A", "G", 12, "2026-03-08T00:00:00+00:00"),
                        ("v2", run_id, "1", 2, "C", "T", 13, "2026-03-08T00:00:00+00:00"),
                    ],
                )
                conn.commit()

            upsert_predictor_outputs_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "predictor_key": "sift",
                        "outcome": "computed",
                        "score": 0.12,
                        "label": "deleterious",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:01+00:00",
                    },
                    {
                        "variant_id": "v2",
                        "predictor_key": "sift",
                        "outcome": "computed",
                        "score": 0.34,
                        "label": "tolerated",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    },
                ],
            )

            self.assertEqual(count_predictor_outputs_for_run(db_path, run_id), 2)
            self.assertEqual(count_predictor_outputs_for_run(db_path, run_id, predictor_key="sift"), 2)

            first_page = list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift", limit=1, offset=0)
            second_page = list_predictor_outputs_for_run(db_path, run_id, predictor_key="sift", limit=1, offset=1)

            self.assertEqual(len(first_page), 1)
            self.assertEqual(len(second_page), 1)
            self.assertNotEqual(first_page[0]["variant_id"], second_page[0]["variant_id"])


if __name__ == "__main__":
    unittest.main()
