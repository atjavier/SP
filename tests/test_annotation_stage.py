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
        os.environ["SP_GNOMAD_ENABLED"] = "0"
        os.environ.pop("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR", None)

    def tearDown(self):
        if self._original_sp_gnomad_enabled is None:
            os.environ.pop("SP_GNOMAD_ENABLED", None)
        else:
            os.environ["SP_GNOMAD_ENABLED"] = self._original_sp_gnomad_enabled

        if self._original_fail_on_evidence_error is None:
            os.environ.pop("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR", None)
        else:
            os.environ["SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR"] = self._original_fail_on_evidence_error

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
            stage = get_stage(db_path, run_id, "annotation") or {}
            self.assertEqual(stage.get("status"), "failed")
            self.assertEqual((stage.get("error") or {}).get("code"), "CLINVAR_RETRIEVAL_FAILED")
            self.assertEqual(list_clinvar_evidence_for_run(db_path, run_id, limit=10), [])

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
        self.assertEqual(result.get("retry_attempts"), 1)
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


if __name__ == "__main__":
    unittest.main()
