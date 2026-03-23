import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class VariantSummariesStorageTestCase(unittest.TestCase):
    def test_list_and_count_variant_summaries(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.classifications import upsert_classifications_for_run  # noqa: E402
        from storage.predictor_outputs import upsert_predictor_outputs_for_run  # noqa: E402
        from storage.dbsnp_evidence import upsert_dbsnp_evidence_for_run  # noqa: E402
        from storage.clinvar_evidence import upsert_clinvar_evidence_for_run  # noqa: E402
        from storage.variant_summaries import (  # noqa: E402
            count_variant_summaries_for_run,
            list_variant_summaries_for_run,
        )

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

            upsert_classifications_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "consequence_category": "missense",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:01+00:00",
                    },
                    {
                        "variant_id": "v2",
                        "consequence_category": "synonymous",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    },
                ],
            )

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
                        "created_at": "2026-03-08T00:00:03+00:00",
                    }
                ],
            )

            upsert_dbsnp_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "source": "dbsnp",
                        "outcome": "found",
                        "rsid": "rs1",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:04+00:00",
                    }
                ],
            )

            upsert_clinvar_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "source": "clinvar",
                        "outcome": "found",
                        "clinvar_id": "VCV1",
                        "clinical_significance": "Benign",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:05+00:00",
                    }
                ],
            )

            self.assertEqual(count_variant_summaries_for_run(db_path, run_id), 2)

            rows = list_variant_summaries_for_run(db_path, run_id, limit=10, offset=0)
            self.assertEqual(len(rows), 2)

            by_id = {row["variant_id"]: row for row in rows}
            self.assertEqual(by_id["v1"]["consequence_category"], "missense")
            self.assertTrue(by_id["v1"]["has_prediction"])
            self.assertTrue(by_id["v1"]["has_dbsnp"])
            self.assertTrue(by_id["v1"]["has_clinvar"])
            self.assertFalse(by_id["v1"]["has_gnomad"])

            self.assertEqual(by_id["v2"]["consequence_category"], "synonymous")
            self.assertFalse(by_id["v2"]["has_prediction"])
            self.assertFalse(by_id["v2"]["has_dbsnp"])
            self.assertFalse(by_id["v2"]["has_clinvar"])
            self.assertFalse(by_id["v2"]["has_gnomad"])

    def test_variant_summary_completeness_filter(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.classifications import upsert_classifications_for_run  # noqa: E402
        from storage.variant_summaries import (  # noqa: E402
            count_variant_summaries_for_run,
            list_variant_summaries_for_run,
        )

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
                        ("v3", run_id, "1", 3, "G", "A", 14, "2026-03-08T00:00:00+00:00"),
                    ],
                )
                conn.commit()

            upsert_classifications_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v1",
                        "consequence_category": "missense",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:01+00:00",
                    },
                    {
                        "variant_id": "v2",
                        "consequence_category": "synonymous",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "created_at": "2026-03-08T00:00:02+00:00",
                    },
                ],
            )

            stage_statuses = {
                "parser": "succeeded",
                "classification": "succeeded",
                "prediction": "succeeded",
                "annotation": "succeeded",
            }

            complete_rows = list_variant_summaries_for_run(
                db_path,
                run_id,
                limit=10,
                offset=0,
                completeness="complete",
                stage_statuses=stage_statuses,
                annotation_evidence_completeness="complete",
            )
            self.assertEqual({row["variant_id"] for row in complete_rows}, {"v1", "v2"})

            partial_rows = list_variant_summaries_for_run(
                db_path,
                run_id,
                limit=10,
                offset=0,
                completeness="partial",
                stage_statuses=stage_statuses,
                annotation_evidence_completeness="complete",
            )
            self.assertEqual({row["variant_id"] for row in partial_rows}, {"v3"})

            unavailable_count = count_variant_summaries_for_run(
                db_path,
                run_id,
                completeness="unavailable",
                stage_statuses=stage_statuses,
                annotation_evidence_completeness="unavailable",
            )
            self.assertEqual(unavailable_count, 2)

            failed_count = count_variant_summaries_for_run(
                db_path,
                run_id,
                completeness="failed",
                stage_statuses={**stage_statuses, "prediction": "failed"},
                annotation_evidence_completeness="complete",
            )
            self.assertEqual(failed_count, 3)


if __name__ == "__main__":
    unittest.main()
