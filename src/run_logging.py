import json
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any

from sse import now_iso8601


REDACTED_VCF = "[REDACTED_VCF]"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3


def ensure_run_logs_dir(instance_dir: str) -> str:
    log_dir = os.path.join(instance_dir, "logs", "runs")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir


def _looks_like_vcf_line(text: str) -> bool:
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.startswith("##") or candidate.startswith("#CHROM"):
            return True
        if "\t" not in candidate:
            continue
        parts = candidate.split("\t")
        if len(parts) < 8:
            continue
        if parts[1].isdigit():
            return True
    return False


def _sanitize_text(text: str) -> str:
    cleaned = text.replace("\r", " ").replace("\n", " ").strip()
    if _looks_like_vcf_line(cleaned):
        return REDACTED_VCF
    return cleaned


def _sanitize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            value = str(value)
    if isinstance(value, str):
        if _looks_like_vcf_line(value):
            return REDACTED_VCF
        return _sanitize_text(value)
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_value(item) for item in value]
    return value


class RunLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = _sanitize_text(record.getMessage())
        payload: dict[str, Any] = {
            "event_at": now_iso8601(),
            "level": record.levelname.lower(),
            "message": message,
            "event": getattr(record, "event", "log"),
            "run_id": getattr(record, "run_id", "unknown"),
        }

        for key in ("stage_name", "status", "error_code", "error_message"):
            if hasattr(record, key):
                payload[key] = _sanitize_value(getattr(record, key))

        if hasattr(record, "details"):
            payload["details"] = _sanitize_value(getattr(record, "details"))

        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


class AutoCloseRotatingFileHandler(RotatingFileHandler):
    def emit(self, record: logging.LogRecord) -> None:
        log_dir = os.path.dirname(self.baseFilename)
        if not os.path.isdir(log_dir):
            return
        try:
            super().emit(record)
        finally:
            if self.stream:
                self.stream.close()
                self.stream = None


class RunLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(self.extra or {})
        if "extra" in kwargs and isinstance(kwargs["extra"], dict):
            extra.update(kwargs["extra"])
        kwargs["extra"] = extra
        return msg, kwargs


def build_run_logger(
    run_id: str,
    *,
    instance_dir: str,
    level: int = logging.INFO,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> logging.LoggerAdapter:
    log_dir = ensure_run_logs_dir(instance_dir)
    log_path = os.path.join(log_dir, f"{run_id}.log")
    logger_name = f"run.{run_id}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = AutoCloseRotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
            delay=True,
        )
        handler.setFormatter(RunLogFormatter())
        logger.addHandler(handler)

    return RunLoggerAdapter(logger, {"run_id": run_id})


def log_run_event(
    logger: logging.LoggerAdapter,
    event: str,
    message: str,
    *,
    level: str = "info",
    **fields: Any,
) -> None:
    payload = {"event": event}
    payload.update(fields)
    level_name = (level or "info").lower()
    log_fn = getattr(logger, level_name, logger.info)
    log_fn(message, extra=payload)


def close_run_logger(logger: logging.LoggerAdapter) -> None:
    base_logger = getattr(logger, "logger", logger)
    handlers = list(getattr(base_logger, "handlers", []))
    for handler in handlers:
        handler.close()
        base_logger.removeHandler(handler)
