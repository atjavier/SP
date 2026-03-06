import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager


RUNS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_inputs (
  run_id TEXT PRIMARY KEY,
  original_filename TEXT NOT NULL,
  stored_filename TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  validation_ok INTEGER NOT NULL,
  validation_errors_json TEXT NOT NULL,
  validation_warnings_json TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
"""


def ensure_parent_dir(path: str) -> None:
    if path == ":memory:":
        return
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    return sqlite3.connect(db_path)


@contextmanager
def open_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(RUNS_SCHEMA_SQL)
    conn.commit()
