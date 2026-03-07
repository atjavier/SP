import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager


SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      reference_build TEXT NOT NULL DEFAULT 'GRCh38'
    );
    """,
    """
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
    """,
    """
    CREATE TABLE IF NOT EXISTS run_stages (
      run_id TEXT NOT NULL,
      stage_name TEXT NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      input_uploaded_at TEXT,
      stats_json TEXT,
      error_code TEXT,
      error_message TEXT,
      error_details_json TEXT,
      PRIMARY KEY (run_id, stage_name),
      FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS run_variants (
      variant_id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      chrom TEXT NOT NULL,
      pos INTEGER NOT NULL,
      ref TEXT NOT NULL,
      alt TEXT NOT NULL,
      source_line INTEGER,
      created_at TEXT NOT NULL,
      UNIQUE (run_id, chrom, pos, ref, alt),
      FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_variants_run_id ON run_variants(run_id);",
)


def ensure_parent_dir(path: str) -> None:
    if path == ":memory:":
        return
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def open_connection(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    was_in_transaction = conn.in_transaction
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
    _apply_schema_migrations(conn)
    if not was_in_transaction:
        conn.commit()


def _apply_schema_migrations(conn: sqlite3.Connection) -> None:
    _ensure_runs_reference_build_column(conn)


def _ensure_runs_reference_build_column(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(runs);").fetchall()}
    if "reference_build" in columns:
        return
    conn.execute(
        "ALTER TABLE runs ADD COLUMN reference_build TEXT NOT NULL DEFAULT 'GRCh38';"
    )
