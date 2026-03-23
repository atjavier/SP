import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class EvidenceStorageFilterTestCase(unittest.TestCase):
    def test_evidence_lists_respect_classification_filter(self):
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.runs import create_run  # noqa: E402
        from storage.classifications import upsert_classifications_for_run  # noqa: E402
        from storage.dbsnp_evidence import (  # noqa: E402
            list_dbsnp_evidence_for_run,
            upsert_dbsnp_evidence_for_run,
        )
        from storage.clinvar_evidence import (  # noqa: E402
            list_clinvar_evidence_for_run,
            upsert_clinvar_evidence_for_run,
        )
        from storage.gnomad_evidence import (  # noqa: E402
            list_gnomad_evidence_for_run,
            upsert_gnomad_evidence_for_run,
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
                    {
                        "variant_id": "v3",
                        "consequence_category": "unclassified",
                        "reason_code": "NO_VEP_MATCH",
                        "reason_message": "No classification match found.",
                        "details": {},
                        "created_at": "2026-03-08T00:00:03+00:00",
                    },
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
                    },
                    {
                        "variant_id": "v2",
                        "source": "dbsnp",
                        "outcome": "found",
                        "rsid": "rs2",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:05+00:00",
                    },
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
                        "retrieved_at": "2026-03-08T00:00:06+00:00",
                    },
                    {
                        "variant_id": "v3",
                        "source": "clinvar",
                        "outcome": "found",
                        "clinvar_id": "VCV3",
                        "clinical_significance": "Likely benign",
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:07+00:00",
                    },
                ],
            )

            upsert_gnomad_evidence_for_run(
                db_path,
                run_id,
                [
                    {
                        "variant_id": "v2",
                        "source": "gnomad",
                        "outcome": "found",
                        "gnomad_variant_id": "GN1",
                        "global_af": 0.01,
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:08+00:00",
                    },
                    {
                        "variant_id": "v3",
                        "source": "gnomad",
                        "outcome": "found",
                        "gnomad_variant_id": "GN2",
                        "global_af": 0.02,
                        "reason_code": None,
                        "reason_message": None,
                        "details": {},
                        "retrieved_at": "2026-03-08T00:00:09+00:00",
                    },
                ],
            )

            dbsnp_missense = list_dbsnp_evidence_for_run(
                db_path, run_id, classification="missense", limit=10
            )
            self.assertEqual({row["variant_id"] for row in dbsnp_missense}, {"v1"})

            clinvar_unclassified = list_clinvar_evidence_for_run(
                db_path, run_id, classification="unclassified", limit=10
            )
            self.assertEqual({row["variant_id"] for row in clinvar_unclassified}, {"v3"})

            gnomad_synonymous = list_gnomad_evidence_for_run(
                db_path, run_id, classification="synonymous", limit=10
            )
            self.assertEqual({row["variant_id"] for row in gnomad_synonymous}, {"v2"})

            dbsnp_variant_override = list_dbsnp_evidence_for_run(
                db_path, run_id, variant_id="v2", classification="missense", limit=10
            )
            self.assertEqual({row["variant_id"] for row in dbsnp_variant_override}, {"v2"})


if __name__ == "__main__":
    unittest.main()
