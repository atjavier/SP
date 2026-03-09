from __future__ import annotations

import os
from pathlib import Path


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
        return value[1:-1]
    return value


def load_env_file(path: str | os.PathLike | None = None, *, override: bool = False) -> dict[str, str]:
    """
    Load KEY=VALUE pairs from a .env-style file into os.environ.

    - Missing files are ignored.
    - Existing environment variables win unless override=True.
    - This is a small parser intended for local development; it does not try
      to be fully compatible with every dotenv variant.
    """
    if path is None:
        path = Path(__file__).resolve().parent.parent / ".env"
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {}

    loaded: dict[str, str] = {}
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = _strip_quotes(value)
        if not override and key in os.environ:
            continue

        os.environ[key] = value
        loaded[key] = value

    return loaded

