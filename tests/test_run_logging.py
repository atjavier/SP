import json
import os
import re
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_DIR)


class RunLoggingTestCase(unittest.TestCase):
    def _flush_logger(self, adapter) -> None:
        for handler in getattr(adapter.logger, "handlers", []):
            handler.flush()

    def _close_logger(self, adapter) -> None:
        handlers = list(getattr(adapter.logger, "handlers", []))
        for handler in handlers:
            handler.close()
            adapter.logger.removeHandler(handler)

    def test_run_logger_writes_jsonl_schema(self):
        from run_logging import build_run_logger

        temp_dir = tempfile.TemporaryDirectory()
        logger = None
        try:
            instance_dir = os.path.join(temp_dir.name, "instance")
            run_id = "run-123"
            logger = build_run_logger(run_id, instance_dir=instance_dir)
            logger.info("Run started", extra={"event": "run_start"})
            self._flush_logger(logger)

            log_path = os.path.join(instance_dir, "logs", "runs", f"{run_id}.log")
            self.assertTrue(os.path.isfile(log_path))
            with open(log_path, "r", encoding="utf-8") as handle:
                line = handle.readline().rstrip("\n")

            payload = json.loads(line)
            self.assertEqual(payload["run_id"], run_id)
            self.assertEqual(payload["event"], "run_start")
            self.assertEqual(payload["message"], "Run started")
            self.assertEqual(payload["level"], "info")
            self.assertRegex(payload["event_at"], r"\+00:00$")
        finally:
            if logger is not None:
                self._close_logger(logger)
            temp_dir.cleanup()

    def test_run_logger_redacts_vcf_content_and_strips_newlines(self):
        from run_logging import build_run_logger

        temp_dir = tempfile.TemporaryDirectory()
        logger = None
        try:
            instance_dir = os.path.join(temp_dir.name, "instance")
            run_id = "run-456"
            logger = build_run_logger(run_id, instance_dir=instance_dir)
            logger.warning(
                "Parser saw raw line\nnext line",
                extra={
                    "event": "parser_warn",
                    "details": {
                        "vcf_line": "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
                        "note": "bad\nline",
                    },
                },
            )
            self._flush_logger(logger)

            log_path = os.path.join(instance_dir, "logs", "runs", f"{run_id}.log")
            with open(log_path, "r", encoding="utf-8") as handle:
                payload = json.loads(handle.readline())

            self.assertEqual(payload["details"]["vcf_line"], "[REDACTED_VCF]")
            self.assertNotIn("\n", payload["message"])
            self.assertNotIn("\n", payload["details"]["note"])
        finally:
            if logger is not None:
                self._close_logger(logger)
            temp_dir.cleanup()

    def test_run_logger_redacts_vcf_content_in_message(self):
        from run_logging import build_run_logger

        temp_dir = tempfile.TemporaryDirectory()
        logger = None
        try:
            instance_dir = os.path.join(temp_dir.name, "instance")
            run_id = "run-789"
            logger = build_run_logger(run_id, instance_dir=instance_dir)
            logger.error(
                "#CHROM\tPOS\tREF\tALT",
                extra={"event": "parser_warn"},
            )
            self._flush_logger(logger)

            log_path = os.path.join(instance_dir, "logs", "runs", f"{run_id}.log")
            with open(log_path, "r", encoding="utf-8") as handle:
                payload = json.loads(handle.readline())

            self.assertEqual(payload["message"], "[REDACTED_VCF]")
        finally:
            if logger is not None:
                self._close_logger(logger)
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
