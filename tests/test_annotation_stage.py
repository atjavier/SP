import logging
import os
import subprocess
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from unittest.mock import patch


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


class AnnotationStageTestCase(unittest.TestCase):
    def setUp(self):
        self._original_sp_gnomad_enabled = os.environ.get("SP_GNOMAD_ENABLED")
        self._original_fail_on_evidence_error = os.environ.get("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR")
        self._original_evidence_profile = os.environ.get("SP_EVIDENCE_PROFILE")
        self._original_evidence_mode = os.environ.get("SP_EVIDENCE_MODE")
        self._original_connectivity_probe_enabled = os.environ.get("SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED")
        os.environ["SP_GNOMAD_ENABLED"] = "0"
        os.environ.pop("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR", None)
        os.environ.pop("SP_EVIDENCE_PROFILE", None)
        os.environ.pop("SP_EVIDENCE_MODE", None)
        os.environ["SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED"] = "0"

    def tearDown(self):
        if self._original_sp_gnomad_enabled is None:
            os.environ.pop("SP_GNOMAD_ENABLED", None)
        else:
            os.environ["SP_GNOMAD_ENABLED"] = self._original_sp_gnomad_enabled

        if self._original_fail_on_evidence_error is None:
            os.environ.pop("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR", None)
        else:
            os.environ["SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR"] = self._original_fail_on_evidence_error

        if self._original_evidence_profile is None:
            os.environ.pop("SP_EVIDENCE_PROFILE", None)
        else:
            os.environ["SP_EVIDENCE_PROFILE"] = self._original_evidence_profile

        if self._original_evidence_mode is None:
            os.environ.pop("SP_EVIDENCE_MODE", None)
        else:
            os.environ["SP_EVIDENCE_MODE"] = self._original_evidence_mode

        if self._original_connectivity_probe_enabled is None:
            os.environ.pop("SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED", None)
        else:
            os.environ["SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED"] = self._original_connectivity_probe_enabled

    def test_resolve_evidence_mode_decision_matrix(self):
        from pipeline.annotation_stage import _resolve_evidence_mode_decision  # noqa: E402

        cases = [
            ("online", True, {"dbsnp": False, "clinvar": False, "gnomad": False}, "online", "requested_online_online_available"),
            ("online", False, {"dbsnp": True, "clinvar": False, "gnomad": False}, "offline", "requested_online_fallback_offline"),
            ("online", False, {"dbsnp": False, "clinvar": False, "gnomad": False}, "online", "requested_online_no_valid_source"),
            ("offline", True, {"dbsnp": False, "clinvar": False, "gnomad": False}, "online", "requested_offline_fallback_online"),
            ("offline", False, {"dbsnp": True, "clinvar": False, "gnomad": False}, "offline", "requested_offline_offline_available"),
            ("offline", False, {"dbsnp": False, "clinvar": False, "gnomad": False}, "offline", "requested_offline_no_valid_source"),
            ("hybrid", True, {"dbsnp": True, "clinvar": False, "gnomad": False}, "hybrid", "requested_hybrid_both_available"),
            ("hybrid", False, {"dbsnp": True, "clinvar": False, "gnomad": False}, "offline", "requested_hybrid_online_unavailable"),
            ("hybrid", True, {"dbsnp": False, "clinvar": False, "gnomad": False}, "online", "requested_hybrid_offline_unavailable"),
            ("hybrid", False, {"dbsnp": False, "clinvar": False, "gnomad": False}, "hybrid", "requested_hybrid_no_valid_source"),
        ]

        for requested, online_available, offline_sources, expected_effective, expected_reason in cases:
            decision = _resolve_evidence_mode_decision(
                requested_mode=requested,
                online_available=online_available,
                offline_sources_configured=offline_sources,
            )
            self.assertEqual(decision.get("requested_mode"), requested)
            self.assertEqual(decision.get("effective_mode"), expected_effective)
            self.assertEqual(decision.get("decision_reason"), expected_reason)
            self.assertEqual(
                decision.get("offline_sources_configured"),
                {
                    "dbsnp": bool(offline_sources.get("dbsnp", False)),
                    "clinvar": bool(offline_sources.get("clinvar", False)),
                    "gnomad": bool(offline_sources.get("gnomad", False)),
                },
            )
            self.assertIsNotNone(decision.get("detected_at"))

    def test_detect_evidence_mode_decision_computes_online_and_offline_signals(self):
        from pipeline.annotation_stage import _detect_evidence_mode_decision  # noqa: E402

        with patch.dict(
            os.environ,
            {"SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED": "1"},
            clear=False,
        ):
            with (
                patch(
                    "pipeline.annotation_stage._local_vcf_source_state",
                    side_effect=[
                        {"configured": True, "ready": True, "reason": "ready"},
                        {"configured": True, "ready": False, "reason": "index_missing"},
                        {"configured": True, "ready": False, "reason": "path_missing"},
                    ],
                ),
                patch(
                    "pipeline.annotation_stage._probe_http_base_url",
                    side_effect=[False, True],
                ),
            ):
                decision = _detect_evidence_mode_decision(
                    requested_mode="hybrid",
                    dbsnp_local_vcf_path="/tmp/dbsnp.vcf.gz",
                    clinvar_local_vcf_path="/tmp/clinvar.vcf.gz",
                    gnomad_local_vcf_path="/tmp/gnomad.vcf.gz",
                    dbsnp_enabled=True,
                    clinvar_enabled=True,
                    gnomad_enabled=False,
                )

        self.assertEqual(decision.get("requested_mode"), "hybrid")
        self.assertEqual(decision.get("effective_mode"), "hybrid")
        self.assertEqual(decision.get("online_available"), True)
        self.assertEqual(
            decision.get("offline_sources_configured"),
            {"dbsnp": True, "clinvar": True, "gnomad": True},
        )
        self.assertEqual(
            decision.get("offline_sources_available"),
            {"dbsnp": True, "clinvar": False, "gnomad": False},
        )
        self.assertEqual(
            decision.get("offline_sources_unavailable_reason"),
            {"dbsnp": "", "clinvar": "index_missing", "gnomad": "path_missing"},
        )

    def test_detect_evidence_mode_decision_keeps_configured_sources_when_disabled(self):
        from pipeline.annotation_stage import _detect_evidence_mode_decision  # noqa: E402

        with patch.dict(
            os.environ,
            {"SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED": "1"},
            clear=False,
        ):
            with (
                patch(
                    "pipeline.annotation_stage._local_vcf_source_state",
                    side_effect=[
                        {"configured": True, "ready": True, "reason": "ready"},
                        {"configured": True, "ready": False, "reason": "index_missing"},
                        {"configured": True, "ready": False, "reason": "path_missing"},
                    ],
                ),
                patch(
                    "pipeline.annotation_stage._probe_http_base_url",
                    return_value=True,
                ),
            ):
                decision = _detect_evidence_mode_decision(
                    requested_mode="offline",
                    dbsnp_local_vcf_path="/tmp/dbsnp.vcf.gz",
                    clinvar_local_vcf_path="/tmp/clinvar.vcf.gz",
                    gnomad_local_vcf_path="/tmp/gnomad.vcf.gz",
                    dbsnp_enabled=False,
                    clinvar_enabled=True,
                    gnomad_enabled=False,
                )

        self.assertEqual(
            decision.get("offline_sources_configured"),
            {"dbsnp": True, "clinvar": True, "gnomad": True},
        )
        self.assertEqual(
            decision.get("offline_sources_available"),
            {"dbsnp": False, "clinvar": False, "gnomad": False},
        )
        self.assertEqual(
            decision.get("offline_sources_unavailable_reason"),
            {"dbsnp": "", "clinvar": "index_missing", "gnomad": "path_missing"},
        )
        self.assertEqual(decision.get("effective_mode"), "online")
        self.assertEqual(decision.get("decision_reason"), "requested_offline_fallback_online")

    def test_detect_evidence_mode_decision_skips_probe_when_disabled(self):
        from pipeline.annotation_stage import _detect_evidence_mode_decision  # noqa: E402

        with patch.dict(
            os.environ,
            {"SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED": "0"},
            clear=False,
        ):
            with (
                patch(
                    "pipeline.annotation_stage._is_local_vcf_source_ready",
                    side_effect=[False, False, False],
                ),
                patch(
                    "pipeline.annotation_stage._probe_http_base_url",
                    side_effect=AssertionError("probe should not run when disabled"),
                ),
            ):
                decision = _detect_evidence_mode_decision(
                    requested_mode="online",
                    dbsnp_local_vcf_path=None,
                    clinvar_local_vcf_path=None,
                    gnomad_local_vcf_path=None,
                    dbsnp_enabled=True,
                    clinvar_enabled=False,
                    gnomad_enabled=False,
                )

        self.assertTrue(decision.get("online_available"))

    def test_detect_evidence_mode_decision_skips_probe_for_offline_ready_request(self):
        from pipeline.annotation_stage import _detect_evidence_mode_decision  # noqa: E402

        with patch.dict(
            os.environ,
            {"SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED": "1"},
            clear=False,
        ):
            with (
                patch(
                    "pipeline.annotation_stage._local_vcf_source_state",
                    side_effect=[
                        {"configured": True, "ready": True, "reason": "ready"},
                        {"configured": True, "ready": False, "reason": "index_missing"},
                        {"configured": True, "ready": False, "reason": "path_missing"},
                    ],
                ),
                patch(
                    "pipeline.annotation_stage._probe_http_base_url",
                    side_effect=AssertionError("probe should be skipped for offline-ready requests"),
                ),
            ):
                decision = _detect_evidence_mode_decision(
                    requested_mode="offline",
                    dbsnp_local_vcf_path="/tmp/dbsnp.vcf.gz",
                    clinvar_local_vcf_path="/tmp/clinvar.vcf.gz",
                    gnomad_local_vcf_path="/tmp/gnomad.vcf.gz",
                    dbsnp_enabled=True,
                    clinvar_enabled=False,
                    gnomad_enabled=False,
                )

        self.assertEqual(decision.get("effective_mode"), "offline")
        self.assertFalse(decision.get("online_available"))
        self.assertEqual(
            decision.get("offline_sources_available"),
            {"dbsnp": True, "clinvar": False, "gnomad": False},
        )
        self.assertEqual(
            decision.get("offline_sources_unavailable_reason"),
            {"dbsnp": "", "clinvar": "index_missing", "gnomad": "path_missing"},
        )

    def test_local_vcf_source_ready_respects_directory_scan_depth_limit(self):
        from pipeline.annotation_stage import _is_local_vcf_source_ready  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            level1 = os.path.join(tmpdir, "level1")
            level2 = os.path.join(level1, "level2")
            os.makedirs(level2, exist_ok=True)
            vcf_path = os.path.join(level2, "evidence.vcf.gz")
            with open(vcf_path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write("")
            with open(f"{vcf_path}.tbi", "w", encoding="utf-8", newline="\n") as handle:
                handle.write("")

            with patch.dict(
                os.environ,
                {"SP_EVIDENCE_LOCAL_SCAN_MAX_DEPTH": "1"},
                clear=False,
            ):
                self.assertFalse(_is_local_vcf_source_ready(tmpdir))

            with patch.dict(
                os.environ,
                {"SP_EVIDENCE_LOCAL_SCAN_MAX_DEPTH": "3"},
                clear=False,
            ):
                self.assertTrue(_is_local_vcf_source_ready(tmpdir))

    def test_local_vcf_source_state_reports_scan_limit_reached(self):
        from pipeline.annotation_stage import _local_vcf_source_state  # noqa: E402

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create many files that are not valid indexed VCF candidates to trip scan limit.
            for idx in range(0, 150):
                file_path = os.path.join(tmpdir, f"dummy_{idx}.txt")
                with open(file_path, "w", encoding="utf-8", newline="\n") as handle:
                    handle.write("x")

            with patch.dict(
                os.environ,
                {"SP_EVIDENCE_LOCAL_SCAN_MAX_FILES": "100"},
                clear=False,
            ):
                state = _local_vcf_source_state(tmpdir)

        self.assertEqual(state.get("configured"), True)
        self.assertEqual(state.get("ready"), False)
        self.assertEqual(state.get("reason"), "scan_limit_reached")

    def _seed_ready_run(self, db_path: str, uploaded_at: str, *, classification_category: str = "missense") -> str:
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
            if classification_category:
                conn.execute(
                    """
                    INSERT INTO run_classifications (
                      run_id, variant_id, consequence_category, reason_code, reason_message, details_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, "v1", classification_category, None, None, "{}", uploaded_at),
                )
            conn.commit()
        return run_id

    def test_annotation_strict_block_no_valid_source_matrix(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-10T00:00:00+00:00"
        cases = [
            ("online", "online", "requested_online_no_valid_source"),
            ("offline", "offline", "requested_offline_no_valid_source"),
            ("hybrid", "hybrid", "requested_hybrid_no_valid_source"),
        ]

        for requested_mode, effective_mode, reason_code in cases:
            with self.subTest(requested_mode=requested_mode):
                with tempfile.TemporaryDirectory() as tmpdir:
                    db_path = os.path.join(tmpdir, "sp.db")
                    run_id = self._seed_ready_run(db_path, uploaded_at)
                    forced_decision = {
                        "requested_mode": requested_mode,
                        "effective_mode": effective_mode,
                        "online_available": False,
                        "offline_sources_configured": {"dbsnp": False, "clinvar": False, "gnomad": False},
                        "offline_sources_available": {"dbsnp": False, "clinvar": False, "gnomad": False},
                        "offline_sources_unavailable_reason": {"dbsnp": "", "clinvar": "", "gnomad": ""},
                        "decision_reason": reason_code,
                        "detected_at": uploaded_at,
                    }

                    with patch.dict(
                        os.environ,
                        {
                            "SP_SNPEFF_ENABLED": "0",
                            "SP_DBSNP_ENABLED": "1",
                            "SP_CLINVAR_ENABLED": "1",
                            "SP_GNOMAD_ENABLED": "1",
                            "SP_EVIDENCE_MODE": requested_mode,
                        },
                        clear=False,
                    ):
                        with (
                            patch(
                                "pipeline.annotation_stage._detect_evidence_mode_decision",
                                return_value=forced_decision,
                            ),
                            patch(
                                "pipeline.annotation_stage._fetch_dbsnp_evidence",
                                side_effect=AssertionError("dbSNP retrieval should not run"),
                            ),
                            patch(
                                "pipeline.annotation_stage._fetch_clinvar_evidence",
                                side_effect=AssertionError("ClinVar retrieval should not run"),
                            ),
                            patch(
                                "pipeline.annotation_stage._fetch_gnomad_evidence",
                                side_effect=AssertionError("gnomAD retrieval should not run"),
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

                    self.assertEqual(raised.exception.code, "EVIDENCE_SOURCES_UNAVAILABLE")
                    details = raised.exception.details or {}
                    self.assertEqual(details.get("requested_mode"), requested_mode)
                    self.assertEqual(details.get("effective_mode"), effective_mode)
                    self.assertEqual(details.get("decision_reason"), reason_code)
                    self.assertEqual(
                        sorted(details.get("missing_sources") or []),
                        ["clinvar", "dbsnp", "gnomad"],
                    )
                    self.assertEqual(details.get("blocked_outputs"), ["annotation", "reporting"])
                    self.assertIn("SP_DBSNP_LOCAL_VCF_PATH", details.get("hint") or "")
                    self.assertIn("SP_CLINVAR_LOCAL_VCF_PATH", details.get("hint") or "")
                    self.assertIn("SP_GNOMAD_LOCAL_VCF_PATH", details.get("hint") or "")

                    stage = get_stage(db_path, run_id, "annotation") or {}
                    self.assertEqual(stage.get("status"), "failed")
                    self.assertEqual((stage.get("error") or {}).get("code"), "EVIDENCE_SOURCES_UNAVAILABLE")

                    stats = stage.get("stats") or {}
                    self.assertEqual(stats.get("evidence_mode_requested"), requested_mode)
                    self.assertEqual(stats.get("evidence_mode_effective"), effective_mode)
                    self.assertEqual(stats.get("evidence_mode_decision_reason"), reason_code)
                    self.assertEqual(stats.get("strict_block_reason"), "no_valid_evidence_sources")
                    self.assertEqual(
                        sorted(stats.get("strict_missing_sources") or []),
                        ["clinvar", "dbsnp", "gnomad"],
                    )

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

    def test_annotation_persists_dbsnp_evidence_with_provenance(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.dbsnp_evidence import list_dbsnp_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(_req, timeout=10):
                del timeout
                url = _req.full_url
                if "/contextuals" in url:
                    return Response('{"data":{"spdis":[{"seq_id":"NC_000001.11","position":0,"deleted_sequence":"A","inserted_sequence":"G"}]}}')
                if "/spdi/" in url and "/rsids" in url:
                    return Response('{"data":{"rsids":[123]}}')
                raise AssertionError(f"Unexpected URL: {url}")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "3",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    result = run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["annotation"]["status"], "succeeded")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("dbsnp_found"), 1)
            self.assertEqual(stats.get("dbsnp_not_found"), 0)
            self.assertEqual(stats.get("dbsnp_errors"), 0)
            self.assertEqual(stats.get("annotation_evidence_completeness"), "partial")
            source_completeness = stats.get("evidence_source_completeness") or {}
            self.assertEqual(source_completeness.get("dbsnp"), "complete")
            self.assertEqual(source_completeness.get("clinvar"), "unavailable")
            self.assertEqual(source_completeness.get("gnomad"), "unavailable")

            rows = list_dbsnp_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source"], "dbsnp")
            self.assertEqual(rows[0]["outcome"], "found")
            self.assertEqual(rows[0]["rsid"], "rs123")
            self.assertTrue(rows[0]["retrieved_at"].endswith("+00:00"))

    def test_annotation_dbsnp_retry_then_success(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)
            attempts = {"n": 0}

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(_req, timeout=10):
                del timeout
                attempts["n"] += 1
                if attempts["n"] == 1:
                    raise HTTPError(
                        url="https://api.ncbi.nlm.nih.gov/variation/v0/test",
                        code=503,
                        msg="service unavailable",
                        hdrs=None,
                        fp=None,
                    )
                url = _req.full_url
                if "/contextuals" in url:
                    return Response('{"data":{"spdis":[{"seq_id":"NC_000001.11","position":0,"deleted_sequence":"A","inserted_sequence":"G"}]}}')
                if "/spdi/" in url and "/rsids" in url:
                    return Response('{"data":{"rsids":[456]}}')
                raise AssertionError(f"Unexpected URL: {url}")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "3",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertGreaterEqual(attempts["n"], 3)
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("dbsnp_retry_attempts"), 1)
            self.assertEqual(stats.get("dbsnp_found"), 1)

    def test_annotation_dbsnp_retry_exhaustion_fails_stage(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.dbsnp_evidence import list_dbsnp_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise HTTPError(
                    url="https://api.ncbi.nlm.nih.gov/variation/v0/test",
                    code=503,
                    msg="service unavailable",
                    hdrs=None,
                    fp=None,
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "3",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "DBSNP_RETRIEVAL_FAILED")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "DBSNP_RETRIEVAL_FAILED")
            self.assertEqual(list_dbsnp_evidence_for_run(db_path, run_id, limit=10), [])

    def test_annotation_dbsnp_timeout_path_records_error_and_succeeds(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.dbsnp_evidence import list_dbsnp_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise TimeoutError("timed out")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "0",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "2",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("dbsnp_errors"), 1)
            self.assertEqual(stats.get("dbsnp_found"), 0)
            details = stats.get("dbsnp_error_details") or {}
            self.assertIn("timeout_seconds", details)
            rows = list_dbsnp_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].get("outcome"), "error")

    def test_annotation_run_policy_stop_overrides_env_continue(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise HTTPError(
                    url="https://api.ncbi.nlm.nih.gov/variation/v0/test",
                    code=503,
                    msg="service unavailable",
                    hdrs=None,
                    fp=None,
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "0",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "2",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                            evidence_failure_policy="stop",
                        )

            self.assertEqual(raised.exception.code, "DBSNP_RETRIEVAL_FAILED")
            self.assertEqual(raised.exception.details.get("failed_source"), "dbsnp")
            self.assertIn("dbsnp", raised.exception.details.get("missing_outputs") or [])
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "DBSNP_RETRIEVAL_FAILED")
            self.assertEqual((stage.get("error") or {}).get("details", {}).get("failed_source"), "dbsnp")

    def test_annotation_run_policy_continue_overrides_env_stop(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise TimeoutError("timed out")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_DBSNP_TIMEOUT_SECONDS": "5",
                    "SP_DBSNP_RETRY_MAX_ATTEMPTS": "2",
                    "SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.dbsnp_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                        evidence_failure_policy="continue",
                    )

            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("annotation_evidence_policy"), "continue")
            self.assertIn("dbsnp", stats.get("evidence_failed_sources") or [])

    def test_annotation_persists_clinvar_evidence_with_provenance(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.clinvar_evidence import list_clinvar_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(req, timeout=10):
                del timeout
                if "esearch.fcgi" in req.full_url:
                    return Response('{"esearchresult":{"idlist":["123"]}}')
                return Response(
                    '{"result":{"uids":["123"],"123":{"uid":"123","accession":"VCV000123.2","clinical_significance":{"description":"Pathogenic"}}}}'
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "3",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    result = run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["annotation"]["status"], "succeeded")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("clinvar_found"), 1)
            self.assertEqual(stats.get("clinvar_not_found"), 0)
            self.assertEqual(stats.get("clinvar_errors"), 0)

            rows = list_clinvar_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source"], "clinvar")
            self.assertEqual(rows[0]["outcome"], "found")
            self.assertEqual(rows[0]["clinvar_id"], "VCV000123")
            self.assertEqual(rows[0]["clinical_significance"], "Pathogenic")
            self.assertTrue(rows[0]["retrieved_at"].endswith("+00:00"))

    def test_annotation_clinvar_retry_then_success(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)
            attempts = {"n": 0}

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(req, timeout=10):
                del timeout
                attempts["n"] += 1
                if attempts["n"] == 1:
                    raise HTTPError(
                        url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                        code=503,
                        msg="service unavailable",
                        hdrs=None,
                        fp=None,
                    )
                if "esearch.fcgi" in req.full_url:
                    return Response('{"esearchresult":{"idlist":["456"]}}')
                return Response(
                    '{"result":{"uids":["456"],"456":{"uid":"456","accession":"VCV000456.1","clinical_significance":{"description":"Likely benign"}}}}'
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "3",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertGreaterEqual(attempts["n"], 2)
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("clinvar_retry_attempts"), 1)
            self.assertEqual(stats.get("clinvar_found"), 1)

    def test_annotation_clinvar_retry_exhaustion_fails_stage(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from storage.clinvar_evidence import list_clinvar_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise HTTPError(
                    url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                    code=503,
                    msg="service unavailable",
                    hdrs=None,
                    fp=None,
                )

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "1",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "3",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "CLINVAR_RETRIEVAL_FAILED")
            self.assertEqual(raised.exception.details.get("missing_outputs"), ["clinvar", "gnomad"])
            self.assertEqual(raised.exception.details.get("annotation_evidence_completeness"), "unavailable")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "CLINVAR_RETRIEVAL_FAILED")
            self.assertEqual(list_clinvar_evidence_for_run(db_path, run_id, limit=10), [])

    def test_compute_evidence_completeness_from_stats(self):
        from pipeline.annotation_stage import _compute_evidence_completeness_from_stats  # noqa: E402

        source, reasons, aggregate = _compute_evidence_completeness_from_stats(
            {
                "dbsnp_enabled": True,
                "dbsnp_found": 3,
                "dbsnp_not_found": 2,
                "dbsnp_errors": 0,
                "dbsnp_variants_eligible": 5,
                "dbsnp_skipped_out_of_scope": 0,
                "clinvar_enabled": True,
                "clinvar_found": 2,
                "clinvar_not_found": 0,
                "clinvar_errors": 1,
                "clinvar_variants_eligible": 3,
                "clinvar_skipped_out_of_scope": 0,
                "gnomad_enabled": False,
                "gnomad_found": 0,
                "gnomad_not_found": 0,
                "gnomad_errors": 0,
                "gnomad_variants_eligible": 0,
                "gnomad_skipped_out_of_scope": 0,
            }
        )

        self.assertEqual(source.get("dbsnp"), "complete")
        self.assertEqual(source.get("clinvar"), "partial")
        self.assertEqual(source.get("gnomad"), "unavailable")
        self.assertEqual(reasons.get("dbsnp"), "evidence_available")
        self.assertEqual(reasons.get("clinvar"), "errors_present")
        self.assertEqual(reasons.get("gnomad"), "disabled")
        self.assertEqual(aggregate, "partial")

    def test_evidence_failure_details_preserve_partial_upstream_sources(self):
        from pipeline.annotation_stage import _evidence_failure_details  # noqa: E402

        details = _evidence_failure_details(
            {"hint": "clinvar timeout"},
            failed_source="clinvar",
            policy="stop",
            processed_source_states={"dbsnp": ("complete", "evidence_available")},
        )

        source_completeness = details.get("evidence_source_completeness") or {}
        source_reasons = details.get("evidence_source_completeness_reason") or {}
        self.assertEqual(details.get("missing_outputs"), ["clinvar", "gnomad"])
        self.assertEqual(source_completeness.get("dbsnp"), "complete")
        self.assertEqual(source_reasons.get("dbsnp"), "evidence_available")
        self.assertEqual(source_completeness.get("clinvar"), "unavailable")
        self.assertEqual(source_reasons.get("clinvar"), "failed_source_error")
        self.assertEqual(source_completeness.get("gnomad"), "unavailable")
        self.assertEqual(source_reasons.get("gnomad"), "not_executed_due_failure")
        self.assertEqual(details.get("annotation_evidence_completeness"), "partial")

    def test_annotation_clinvar_timeout_path_records_error_and_succeeds(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.clinvar_evidence import list_clinvar_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_urlopen(_req, timeout=10):
                del timeout
                raise TimeoutError("timed out")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "0",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "2",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("clinvar_errors"), 1)
            details = stats.get("clinvar_error_details") or {}
            self.assertIn("timeout_seconds", details)
            rows = list_clinvar_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].get("outcome"), "error")

    def test_annotation_clinvar_malformed_summary_records_error_and_succeeds(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.clinvar_evidence import list_clinvar_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(req, timeout=10):
                del timeout
                if "esearch.fcgi" in req.full_url:
                    return Response('{"esearchresult":{"idlist":["123"]}}')
                return Response('{"result":{"uids":["123"]}}')

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR": "0",
                    "SP_CLINVAR_TIMEOUT_SECONDS": "5",
                    "SP_CLINVAR_RETRY_MAX_ATTEMPTS": "3",
                    "SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.clinvar_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("clinvar_errors"), 1)
            details = stats.get("clinvar_error_details") or {}
            errors = details.get("errors") or []
            self.assertTrue(errors)
            self.assertEqual(errors[0].get("reason_code"), "MALFORMED_RESPONSE")
            rows = list_clinvar_evidence_for_run(db_path, run_id, limit=10)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].get("outcome"), "error")

    def test_annotation_gnomad_retrieval_records_stats(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            class Response:
                def __init__(self, body: str, code: int = 200):
                    self.status = code
                    self._body = body.encode("utf-8")

                def read(self):
                    return self._body

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            def fake_urlopen(_req, timeout=10):
                del timeout
                return Response('{"data":{"variant":{"variantId":"1-100-A-G","af":0.001}}}')

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_GNOMAD_TIMEOUT_SECONDS": "5",
                    "SP_GNOMAD_RETRY_MAX_ATTEMPTS": "2",
                    "SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
                    patch("pipeline.gnomad_client.time.sleep", return_value=None),
                ):
                    run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "succeeded")
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("gnomad_found"), 1)
            self.assertEqual(stats.get("gnomad_not_found"), 0)
            self.assertEqual(stats.get("gnomad_errors"), 0)

    def test_annotation_canceled_before_gnomad_persist_does_not_write_rows(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from pipeline.cancel_signals import request_run_cancel  # noqa: E402
        from storage.gnomad_evidence import list_gnomad_evidence_for_run  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_fetch_gnomad(*_args, **_kwargs):
                request_run_cancel(run_id)
                return {
                    "outcome": "found",
                    "gnomad_variant_id": "1-1-A-G",
                    "global_af": 0.1,
                    "reason_code": None,
                    "reason_message": None,
                    "details": {},
                    "retrieved_at": "2026-03-09T00:00:01+00:00",
                    "retry_attempts": 0,
                }

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_GNOMAD_TIMEOUT_SECONDS": "5",
                    "SP_GNOMAD_RETRY_MAX_ATTEMPTS": "2",
                    "SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.annotation_stage._fetch_gnomad_evidence", side_effect=fake_fetch_gnomad),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "RUN_CANCELED")
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "canceled")
            self.assertEqual(list_gnomad_evidence_for_run(db_path, run_id, limit=10), [])

    def test_annotation_cancel_signal_after_mark_succeeded_rolls_back_to_canceled(self):
        from pipeline.annotation_stage import StageExecutionError, run_annotation_stage  # noqa: E402
        from pipeline.cancel_signals import request_run_cancel  # noqa: E402
        from storage.gnomad_evidence import list_gnomad_evidence_for_run  # noqa: E402
        from storage.stages import get_stage as get_stage_record  # noqa: E402
        from storage.stages import mark_stage_succeeded as real_mark_stage_succeeded  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            def fake_fetch_gnomad(*_args, **_kwargs):
                return {
                    "outcome": "found",
                    "gnomad_variant_id": "1-1-A-G",
                    "global_af": 0.1,
                    "reason_code": None,
                    "reason_message": None,
                    "details": {},
                    "retrieved_at": "2026-03-09T00:00:01+00:00",
                    "retry_attempts": 0,
                }

            def wrapped_mark_stage_succeeded(*args, **kwargs):
                real_mark_stage_succeeded(*args, **kwargs)
                request_run_cancel(run_id)

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "0",
                    "SP_CLINVAR_ENABLED": "0",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_GNOMAD_TIMEOUT_SECONDS": "5",
                    "SP_GNOMAD_RETRY_MAX_ATTEMPTS": "2",
                    "SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS": "0",
                    "SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS": "0",
                },
                clear=False,
            ):
                with (
                    patch("pipeline.annotation_stage._fetch_gnomad_evidence", side_effect=fake_fetch_gnomad),
                    patch("pipeline.annotation_stage.mark_stage_succeeded", side_effect=wrapped_mark_stage_succeeded),
                ):
                    with self.assertRaises(StageExecutionError) as raised:
                        run_annotation_stage(
                            db_path,
                            run_id,
                            uploaded_at=uploaded_at,
                            logger=logging.getLogger("test"),
                            force=False,
                        )

            self.assertEqual(raised.exception.code, "RUN_CANCELED")
            stage = get_stage_record(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "canceled")
            self.assertEqual(list_gnomad_evidence_for_run(db_path, run_id, limit=10), [])

    def test_dbsnp_client_non_retryable_http_reports_zero_retries(self):
        from pipeline.dbsnp_client import DbsnpConfig, fetch_dbsnp_evidence_for_variant  # noqa: E402

        config = DbsnpConfig(
            enabled=True,
            api_base_url="https://api.ncbi.nlm.nih.gov/variation/v0",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            api_key=None,
        )

        def fake_urlopen(_req, timeout=10):
            del timeout
            raise HTTPError(
                url="https://api.ncbi.nlm.nih.gov/variation/v0/vcf/1/1/A/G/rsids",
                code=400,
                msg="bad request",
                hdrs=None,
                fp=None,
            )

        with (
            patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.dbsnp_client.time.sleep", return_value=None),
        ):
            result = fetch_dbsnp_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "error")
        self.assertEqual(result.get("reason_code"), "HTTP_ERROR")
        self.assertEqual(result.get("retry_attempts"), 0)

    def test_dbsnp_client_tries_chrom_fallback_after_422(self):
        from pipeline.dbsnp_client import DbsnpConfig, fetch_dbsnp_evidence_for_variant  # noqa: E402

        config = DbsnpConfig(
            enabled=True,
            api_base_url="https://api.ncbi.nlm.nih.gov/variation/v0",
            timeout_seconds=5,
            retry_max_attempts=2,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            api_key=None,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        calls = {"n": 0}

        def fake_urlopen(req, timeout=10):
            del timeout
            calls["n"] += 1
            if "/vcf/1/1/A/G/contextuals" in req.full_url:
                raise HTTPError(url=req.full_url, code=422, msg="unprocessable", hdrs=None, fp=None)
            if "/vcf/chr1/1/A/G/contextuals" in req.full_url:
                return Response('{"data":{"spdis":[{"seq_id":"NC_000001.11","position":0,"deleted_sequence":"A","inserted_sequence":"G"}]}}')
            if "/spdi/" in req.full_url and "/rsids" in req.full_url:
                return Response('{"data":{"rsids":[123]}}')
            raise AssertionError(f"Unexpected URL: {req.full_url}")

        with (
            patch("pipeline.dbsnp_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.dbsnp_client.time.sleep", return_value=None),
        ):
            result = fetch_dbsnp_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertGreaterEqual(calls["n"], 3)
        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("rsid"), "rs123")

    def test_gnomad_client_tries_variant_id_fallback_after_400(self):
        from pipeline.gnomad_client import GnomadConfig, fetch_gnomad_evidence_for_variant  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=2,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            min_request_interval_seconds=0,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_urlopen(req, timeout=10):
            del timeout
            payload = req.data.decode("utf-8")
            if '"variantId": "1-1-A-G"' in payload:
                raise HTTPError(url=req.full_url, code=400, msg="bad request", hdrs=None, fp=None)
            if '"variantId": "chr1-1-A-G"' in payload:
                return Response('{"data":{"variant":{"variantId":"chr1-1-A-G","af":0.123}}}')
            raise AssertionError(f"Unexpected payload: {payload}")

        with (
            patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.gnomad_client.time.sleep", return_value=None),
        ):
            result = fetch_gnomad_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("gnomad_variant_id"), "chr1-1-A-G")

    def test_gnomad_client_retries_on_retryable_graphql_error(self):
        from pipeline.gnomad_client import GnomadConfig, fetch_gnomad_evidence_for_variant  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=2,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            min_request_interval_seconds=0,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        attempts = {"n": 0}

        def fake_urlopen(_req, timeout=10):
            del timeout
            attempts["n"] += 1
            if attempts["n"] == 1:
                return Response('{"errors":[{"message":"Rate limit exceeded, try again later"}]}')
            return Response('{"data":{"variant":{"variantId":"1-1-A-G","genome":{"af":0.01}}}}')

        with (
            patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.gnomad_client.time.sleep", return_value=None),
        ):
            result = fetch_gnomad_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "found")
        self.assertIn(result.get("retry_attempts"), (0, 1))
        self.assertGreaterEqual(attempts["n"], 2)

    def test_gnomad_client_marks_schema_graphql_error(self):
        from pipeline.gnomad_client import GnomadConfig, fetch_gnomad_evidence_for_variant  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=2,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            min_request_interval_seconds=0,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_urlopen(_req, timeout=10):
            del timeout
            return Response(
                '{"errors":[{"message":"Unknown argument \\"referenceGenome\\" on field \\"Query.variant\\"."}]}'
            )

        with (
            patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.gnomad_client.time.sleep", return_value=None),
        ):
            result = fetch_gnomad_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "error")
        self.assertEqual(result.get("reason_code"), "GRAPHQL_SCHEMA_ERROR")
        self.assertEqual(result.get("retry_attempts"), 0)

    def test_gnomad_client_falls_back_to_snake_case_query(self):
        from pipeline.gnomad_client import GnomadConfig, fetch_gnomad_evidence_for_variant  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=2,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            min_request_interval_seconds=0,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        def fake_urlopen(req, timeout=10):
            del timeout
            payload = req.data.decode("utf-8")
            if "variant(variantId:" in payload:
                return Response(
                    '{"errors":[{"message":"Unknown argument \\"variantId\\" on field \\"Query.variant\\"."}]}'
                )
            if "variant(variant_id:" in payload:
                return Response('{"data":{"variant":{"variant_id":"chr1-1-A-G","joint":{"ac":1,"an":10}}}}')
            raise AssertionError(f"Unexpected payload: {payload}")

        with (
            patch("pipeline.gnomad_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.gnomad_client.time.sleep", return_value=None),
        ):
            result = fetch_gnomad_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("gnomad_variant_id"), "chr1-1-A-G")
        self.assertEqual(result.get("global_af"), 0.1)
        self.assertEqual(result.get("retry_attempts"), 0)
        self.assertEqual((result.get("details") or {}).get("query_mode"), "snake")

    def test_clinvar_client_non_retryable_http_reports_zero_retries(self):
        from pipeline.clinvar_client import ClinvarConfig, fetch_clinvar_evidence_for_variant  # noqa: E402

        config = ClinvarConfig(
            enabled=True,
            api_base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            api_key=None,
        )

        def fake_urlopen(_req, timeout=10):
            del timeout
            raise HTTPError(
                url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                code=400,
                msg="bad request",
                hdrs=None,
                fp=None,
            )

        with (
            patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.clinvar_client.time.sleep", return_value=None),
        ):
            result = fetch_clinvar_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "error")
        self.assertEqual(result.get("reason_code"), "HTTP_ERROR")
        self.assertEqual(result.get("retry_attempts"), 0)

    def test_evidence_profile_forces_missense_only(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.clinvar_evidence import list_clinvar_evidence_for_run  # noqa: E402
        from storage.dbsnp_evidence import list_dbsnp_evidence_for_run  # noqa: E402
        from storage.db import init_schema, open_connection  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at, classification_category="other")

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_EVIDENCE_PROFILE": "minimum_exome",
                },
                clear=False,
            ):
                with (
                    patch(
                        "pipeline.annotation_stage.fetch_dbsnp_evidence_for_variant",
                        side_effect=AssertionError("dbSNP lookup should be skipped"),
                    ),
                    patch(
                        "pipeline.annotation_stage.fetch_clinvar_evidence_for_variant",
                        side_effect=AssertionError("ClinVar lookup should be skipped"),
                    ),
                    patch(
                        "pipeline.annotation_stage.fetch_gnomad_evidence_for_variant",
                        side_effect=AssertionError("gnomAD lookup should be skipped"),
                    ),
                ):
                    result = run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["annotation"]["status"], "succeeded")
            stage = get_stage(db_path, run_id, "annotation") or {}
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("evidence_profile"), "predictor_only")
            self.assertEqual(stats.get("dbsnp_variants_eligible"), 0)
            self.assertEqual(stats.get("dbsnp_skipped_out_of_scope"), 1)
            self.assertEqual(stats.get("clinvar_variants_eligible"), 0)
            self.assertEqual(stats.get("clinvar_skipped_out_of_scope"), 1)
            self.assertEqual(stats.get("gnomad_variants_eligible"), 0)
            self.assertEqual(stats.get("gnomad_skipped_out_of_scope"), 1)

            self.assertEqual(len(list_dbsnp_evidence_for_run(db_path, run_id)), 0)
            self.assertEqual(len(list_clinvar_evidence_for_run(db_path, run_id)), 0)

    def test_clinvar_client_retries_transient_runtime_exception(self):
        from pipeline.clinvar_client import ClinvarConfig, fetch_clinvar_evidence_for_variant  # noqa: E402

        config = ClinvarConfig(
            enabled=True,
            api_base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0,
            retry_backoff_max_seconds=0,
            api_key=None,
        )

        class Response:
            def __init__(self, body: str, code: int = 200):
                self.status = code
                self._body = body.encode("utf-8")

            def read(self):
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        calls = {"search": 0, "summary": 0}

        def fake_urlopen(req, timeout=10):
            del timeout
            url = req.full_url
            if "esearch.fcgi" in url:
                calls["search"] += 1
                if calls["search"] == 1:
                    raise ConnectionResetError("connection reset by peer")
                return Response('{"esearchresult":{"idlist":["123"]}}')
            if "esummary.fcgi" in url:
                calls["summary"] += 1
                return Response(
                    '{"result":{"123":{"uid":"123","accession":"RCV000000001.1","clinical_significance":{"description":"Benign"}}}}'
                )
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch("pipeline.clinvar_client.urlopen", side_effect=fake_urlopen),
            patch("pipeline.clinvar_client.time.sleep", return_value=None),
        ):
            result = fetch_clinvar_evidence_for_variant(config, chrom="1", pos=1, ref="A", alt="G")

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("clinvar_id"), "RCV000000001")
        self.assertEqual(result.get("clinical_significance"), "Benign")
        self.assertEqual(result.get("retry_attempts"), 1)
        self.assertGreaterEqual(calls["search"], 2)

    def test_fetch_dbsnp_evidence_offline_uses_local_lookup_only(self):
        from pipeline.annotation_stage import _fetch_dbsnp_evidence  # noqa: E402
        from pipeline.dbsnp_client import DbsnpConfig  # noqa: E402

        config = DbsnpConfig(
            enabled=True,
            api_base_url="https://api.ncbi.nlm.nih.gov/variation/v0",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            api_key=None,
            assembly="GRCh38",
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_dbsnp_evidence_from_local_vcf",
                return_value={
                    "outcome": "found",
                    "rsid": "rs1",
                    "reason_code": None,
                    "reason_message": None,
                    "details": {"source_mode": "offline_local"},
                    "retrieved_at": "2026-03-09T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_dbsnp_evidence_for_variant",
                side_effect=AssertionError("online fetch should not be called in offline mode"),
            ),
        ):
            result = _fetch_dbsnp_evidence(
                config,
                evidence_mode="offline",
                local_vcf_path="/tmp/dbsnp.vcf.gz",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("rsid"), "rs1")
        local_mock.assert_called_once()

    def test_fetch_clinvar_evidence_offline_uses_local_lookup_only(self):
        from pipeline.annotation_stage import _fetch_clinvar_evidence  # noqa: E402
        from pipeline.clinvar_client import ClinvarConfig  # noqa: E402

        config = ClinvarConfig(
            enabled=True,
            api_base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            api_key=None,
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_clinvar_evidence_from_local_vcf",
                return_value={
                    "outcome": "found",
                    "clinvar_id": "VCV000001",
                    "clinical_significance": "Benign",
                    "reason_code": None,
                    "reason_message": None,
                    "details": {"source_mode": "offline_local"},
                    "retrieved_at": "2026-03-10T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_clinvar_evidence_for_variant",
                side_effect=AssertionError("online fetch should not be called in offline mode"),
            ),
        ):
            result = _fetch_clinvar_evidence(
                config,
                evidence_mode="offline",
                local_vcf_path="/tmp/clinvar.vcf.gz",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("clinvar_id"), "VCV000001")
        local_mock.assert_called_once()

    def test_fetch_gnomad_evidence_offline_does_not_fallback_to_online_on_local_error(self):
        from pipeline.annotation_stage import _fetch_gnomad_evidence  # noqa: E402
        from pipeline.gnomad_client import GnomadConfig  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            min_request_interval_seconds=0.0,
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_gnomad_evidence_from_local_vcf",
                return_value={
                    "outcome": "error",
                    "gnomad_variant_id": None,
                    "global_af": None,
                    "reason_code": "LOCAL_QUERY_FAILED",
                    "reason_message": "tabix failed",
                    "details": {"source_mode": "offline_local"},
                    "retrieved_at": "2026-03-10T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_gnomad_evidence_for_variant",
                side_effect=AssertionError("online fetch should not be called in offline mode"),
            ),
        ):
            result = _fetch_gnomad_evidence(
                config,
                evidence_mode="offline",
                local_vcf_path="/tmp/gnomad.vcf.bgz",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "error")
        self.assertEqual(result.get("reason_code"), "LOCAL_QUERY_FAILED")
        local_mock.assert_called_once()

    def test_fetch_dbsnp_evidence_hybrid_falls_back_to_online_with_local_attempt_details(self):
        from pipeline.annotation_stage import _fetch_dbsnp_evidence  # noqa: E402
        from pipeline.dbsnp_client import DbsnpConfig  # noqa: E402

        config = DbsnpConfig(
            enabled=True,
            api_base_url="https://api.ncbi.nlm.nih.gov/variation/v0",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            api_key=None,
            assembly="GRCh38",
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_dbsnp_evidence_from_local_vcf",
                return_value={
                    "outcome": "error",
                    "rsid": None,
                    "reason_code": "LOCAL_QUERY_FAILED",
                    "reason_message": "tabix failed",
                    "details": {"local_vcf_path": "/tmp/dbsnp"},
                    "retrieved_at": "2026-03-10T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_dbsnp_evidence_for_variant",
                return_value={
                    "outcome": "found",
                    "rsid": "rs123",
                    "reason_code": None,
                    "reason_message": None,
                    "details": {},
                    "retrieved_at": "2026-03-10T00:00:01+00:00",
                    "retry_attempts": 1,
                },
            ) as online_mock,
        ):
            result = _fetch_dbsnp_evidence(
                config,
                evidence_mode="hybrid",
                local_vcf_path="/tmp/dbsnp",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "found")
        self.assertEqual(result.get("rsid"), "rs123")
        self.assertIn("local_attempt", result.get("details") or {})
        local_mock.assert_called_once()
        online_mock.assert_called_once()

    def test_fetch_clinvar_evidence_hybrid_falls_back_to_online_with_local_attempt_details(self):
        from pipeline.annotation_stage import _fetch_clinvar_evidence  # noqa: E402
        from pipeline.clinvar_client import ClinvarConfig  # noqa: E402

        config = ClinvarConfig(
            enabled=True,
            api_base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            api_key=None,
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_clinvar_evidence_from_local_vcf",
                return_value={
                    "outcome": "error",
                    "clinvar_id": None,
                    "clinical_significance": None,
                    "reason_code": "LOCAL_QUERY_FAILED",
                    "reason_message": "tabix failed",
                    "details": {"local_vcf_path": "/tmp/clinvar"},
                    "retrieved_at": "2026-03-10T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_clinvar_evidence_for_variant",
                return_value={
                    "outcome": "not_found",
                    "clinvar_id": None,
                    "clinical_significance": None,
                    "reason_code": "NOT_FOUND",
                    "reason_message": "No ClinVar match",
                    "details": {},
                    "retrieved_at": "2026-03-10T00:00:01+00:00",
                    "retry_attempts": 1,
                },
            ) as online_mock,
        ):
            result = _fetch_clinvar_evidence(
                config,
                evidence_mode="hybrid",
                local_vcf_path="/tmp/clinvar",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "not_found")
        self.assertEqual(result.get("reason_code"), "NOT_FOUND")
        self.assertIn("local_attempt", result.get("details") or {})
        local_mock.assert_called_once()
        online_mock.assert_called_once()

    def test_fetch_gnomad_evidence_hybrid_falls_back_to_online(self):
        from pipeline.annotation_stage import _fetch_gnomad_evidence  # noqa: E402
        from pipeline.gnomad_client import GnomadConfig  # noqa: E402

        config = GnomadConfig(
            enabled=True,
            api_base_url="https://gnomad.broadinstitute.org/api",
            dataset_id="gnomad_r4",
            reference_genome="GRCh38",
            timeout_seconds=5,
            retry_max_attempts=3,
            retry_backoff_base_seconds=0.1,
            retry_backoff_max_seconds=1.0,
            min_request_interval_seconds=0.0,
        )

        with (
            patch(
                "pipeline.annotation_stage.fetch_gnomad_evidence_from_local_vcf",
                return_value={
                    "outcome": "error",
                    "gnomad_variant_id": None,
                    "global_af": None,
                    "reason_code": "LOCAL_QUERY_FAILED",
                    "reason_message": "tabix failed",
                    "details": {"local_vcf_path": "/tmp/gnomad"},
                    "retrieved_at": "2026-03-09T00:00:00+00:00",
                    "retry_attempts": 0,
                },
            ) as local_mock,
            patch(
                "pipeline.annotation_stage.fetch_gnomad_evidence_for_variant",
                return_value={
                    "outcome": "not_found",
                    "gnomad_variant_id": None,
                    "global_af": None,
                    "reason_code": "NOT_FOUND",
                    "reason_message": "No gnomAD match",
                    "details": {},
                    "retrieved_at": "2026-03-09T00:00:00+00:00",
                    "retry_attempts": 1,
                },
            ) as online_mock,
        ):
            result = _fetch_gnomad_evidence(
                config,
                evidence_mode="hybrid",
                local_vcf_path="/tmp/gnomad",
                chrom="1",
                pos=1,
                ref="A",
                alt="G",
            )

        self.assertEqual(result.get("outcome"), "not_found")
        self.assertEqual(result.get("reason_code"), "NOT_FOUND")
        self.assertIn("local_attempt", result.get("details") or {})
        local_mock.assert_called_once()
        online_mock.assert_called_once()

    def test_annotation_stage_uses_local_wrappers_in_offline_mode(self):
        from pipeline.annotation_stage import run_annotation_stage  # noqa: E402
        from storage.stages import get_stage  # noqa: E402

        uploaded_at = "2026-03-09T00:00:00+00:00"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "sp.db")
            run_id = self._seed_ready_run(db_path, uploaded_at)

            with patch.dict(
                os.environ,
                {
                    "SP_SNPEFF_ENABLED": "0",
                    "SP_DBSNP_ENABLED": "1",
                    "SP_CLINVAR_ENABLED": "1",
                    "SP_GNOMAD_ENABLED": "1",
                    "SP_EVIDENCE_MODE": "offline",
                    "SP_DBSNP_LOCAL_VCF_PATH": "/tmp/dbsnp.vcf.gz",
                    "SP_CLINVAR_LOCAL_VCF_PATH": "/tmp/clinvar.vcf.gz",
                    "SP_GNOMAD_LOCAL_VCF_PATH": "/tmp/gnomad",
                },
                clear=False,
            ):
                with (
                    patch(
                        "pipeline.annotation_stage._fetch_dbsnp_evidence",
                        return_value={
                            "outcome": "not_found",
                            "rsid": None,
                            "reason_code": "NOT_FOUND",
                            "reason_message": "none",
                            "details": {},
                            "retrieved_at": uploaded_at,
                            "retry_attempts": 0,
                        },
                    ) as dbsnp_wrapper,
                    patch(
                        "pipeline.annotation_stage._fetch_clinvar_evidence",
                        return_value={
                            "outcome": "not_found",
                            "clinvar_id": None,
                            "clinical_significance": None,
                            "reason_code": "NOT_FOUND",
                            "reason_message": "none",
                            "details": {},
                            "retrieved_at": uploaded_at,
                            "retry_attempts": 0,
                        },
                    ) as clinvar_wrapper,
                    patch(
                        "pipeline.annotation_stage._fetch_gnomad_evidence",
                        return_value={
                            "outcome": "not_found",
                            "gnomad_variant_id": None,
                            "global_af": None,
                            "reason_code": "NOT_FOUND",
                            "reason_message": "none",
                            "details": {},
                            "retrieved_at": uploaded_at,
                            "retry_attempts": 0,
                        },
                    ) as gnomad_wrapper,
                ):
                    result = run_annotation_stage(
                        db_path,
                        run_id,
                        uploaded_at=uploaded_at,
                        logger=logging.getLogger("test"),
                        force=False,
                    )

            self.assertEqual(result["annotation"]["status"], "succeeded")
            stage = get_stage(db_path, run_id, "annotation") or {}
            stats = stage.get("stats") or {}
            self.assertEqual(stats.get("evidence_mode_requested"), "offline")
            self.assertEqual(stats.get("evidence_mode_effective"), "online")
            self.assertEqual(stats.get("evidence_mode"), "online")
            dbsnp_wrapper.assert_called_once()
            clinvar_wrapper.assert_called_once()
            gnomad_wrapper.assert_called_once()


if __name__ == "__main__":
    unittest.main()
