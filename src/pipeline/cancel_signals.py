from __future__ import annotations

import threading


_LOCK = threading.Lock()
_REQUESTED_RUN_IDS: set[str] = set()


def request_run_cancel(run_id: str) -> None:
    key = str(run_id or "").strip()
    if not key:
        return
    with _LOCK:
        _REQUESTED_RUN_IDS.add(key)


def is_run_cancel_requested(run_id: str) -> bool:
    key = str(run_id or "").strip()
    if not key:
        return False
    with _LOCK:
        return key in _REQUESTED_RUN_IDS


def clear_run_cancel_request(run_id: str) -> None:
    key = str(run_id or "").strip()
    if not key:
        return
    with _LOCK:
        _REQUESTED_RUN_IDS.discard(key)

