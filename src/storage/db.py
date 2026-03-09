import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager


SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      reference_build TEXT NOT NULL DEFAULT 'GRCh38',
      annotation_evidence_policy TEXT NOT NULL DEFAULT 'continue'
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
    """
    CREATE TABLE IF NOT EXISTS run_pre_annotations (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      variant_key TEXT NOT NULL,
      base_change TEXT NOT NULL,
      substitution_class TEXT NOT NULL,
      ref_class TEXT NOT NULL,
      alt_class TEXT NOT NULL,
      details_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_pre_annotations_run_id ON run_pre_annotations(run_id);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_pre_annotations_run_match_insert
    BEFORE INSERT ON run_pre_annotations
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_pre_annotations_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_pre_annotations
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS run_classifications (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      consequence_category TEXT NOT NULL,
      reason_code TEXT,
      reason_message TEXT,
      details_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_classifications_run_id ON run_classifications(run_id);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_classifications_run_match_insert
    BEFORE INSERT ON run_classifications
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_classifications_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_classifications
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_classifications_validate_insert
    BEFORE INSERT ON run_classifications
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.consequence_category NOT IN ('unclassified', 'synonymous', 'missense', 'nonsense', 'other')
          THEN RAISE(ABORT, 'INVALID_CONSEQUENCE_CATEGORY')
        END;
      SELECT
        CASE
          WHEN NEW.consequence_category = 'unclassified' AND NEW.reason_code IS NULL
          THEN RAISE(ABORT, 'MISSING_REASON_CODE')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_classifications_validate_update
    BEFORE UPDATE OF consequence_category, reason_code ON run_classifications
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.consequence_category NOT IN ('unclassified', 'synonymous', 'missense', 'nonsense', 'other')
          THEN RAISE(ABORT, 'INVALID_CONSEQUENCE_CATEGORY')
        END;
      SELECT
        CASE
          WHEN NEW.consequence_category = 'unclassified' AND NEW.reason_code IS NULL
          THEN RAISE(ABORT, 'MISSING_REASON_CODE')
        END;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS run_predictor_outputs (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      predictor_key TEXT NOT NULL,
      outcome TEXT NOT NULL,
      score REAL,
      label TEXT,
      reason_code TEXT,
      reason_message TEXT,
      details_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id, predictor_key),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_predictor_outputs_run_id ON run_predictor_outputs(run_id);",
    "CREATE INDEX IF NOT EXISTS idx_run_predictor_outputs_predictor_key ON run_predictor_outputs(predictor_key);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_predictor_outputs_run_match_insert
    BEFORE INSERT ON run_predictor_outputs
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_predictor_outputs_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_predictor_outputs
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_predictor_outputs_validate_insert
    BEFORE INSERT ON run_predictor_outputs
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('computed', 'not_applicable', 'not_computed', 'error')
          THEN RAISE(ABORT, 'INVALID_PREDICTOR_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'computed' AND NEW.score IS NULL
          THEN RAISE(ABORT, 'MISSING_SCORE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.score IS NOT NULL
          THEN RAISE(ABORT, 'SCORE_MUST_BE_NULL')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.label IS NOT NULL
          THEN RAISE(ABORT, 'LABEL_MUST_BE_NULL')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.reason_code IS NULL
          THEN RAISE(ABORT, 'MISSING_REASON_CODE')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_predictor_outputs_validate_update
    BEFORE UPDATE OF outcome, score, label, reason_code ON run_predictor_outputs
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('computed', 'not_applicable', 'not_computed', 'error')
          THEN RAISE(ABORT, 'INVALID_PREDICTOR_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'computed' AND NEW.score IS NULL
          THEN RAISE(ABORT, 'MISSING_SCORE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.score IS NOT NULL
          THEN RAISE(ABORT, 'SCORE_MUST_BE_NULL')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.label IS NOT NULL
          THEN RAISE(ABORT, 'LABEL_MUST_BE_NULL')
        END;
      SELECT
        CASE
          WHEN NEW.outcome != 'computed' AND NEW.reason_code IS NULL
          THEN RAISE(ABORT, 'MISSING_REASON_CODE')
        END;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS run_dbsnp_evidence (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      source TEXT NOT NULL,
      outcome TEXT NOT NULL,
      rsid TEXT,
      reason_code TEXT,
      reason_message TEXT,
      details_json TEXT NOT NULL,
      retrieved_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id, source),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_dbsnp_evidence_run_id ON run_dbsnp_evidence(run_id);",
    "CREATE INDEX IF NOT EXISTS idx_run_dbsnp_evidence_source ON run_dbsnp_evidence(source);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_dbsnp_evidence_run_match_insert
    BEFORE INSERT ON run_dbsnp_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_dbsnp_evidence_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_dbsnp_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_dbsnp_evidence_validate_insert
    BEFORE INSERT ON run_dbsnp_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'dbsnp'
          THEN RAISE(ABORT, 'INVALID_DBSNP_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_DBSNP_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.rsid IS NULL
          THEN RAISE(ABORT, 'MISSING_RSID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.rsid IS NOT NULL
          THEN RAISE(ABORT, 'RSID_MUST_BE_NULL')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_dbsnp_evidence_validate_update
    BEFORE UPDATE OF source, outcome, rsid ON run_dbsnp_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'dbsnp'
          THEN RAISE(ABORT, 'INVALID_DBSNP_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_DBSNP_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.rsid IS NULL
          THEN RAISE(ABORT, 'MISSING_RSID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.rsid IS NOT NULL
          THEN RAISE(ABORT, 'RSID_MUST_BE_NULL')
        END;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS run_clinvar_evidence (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      source TEXT NOT NULL,
      outcome TEXT NOT NULL,
      clinvar_id TEXT,
      clinical_significance TEXT,
      reason_code TEXT,
      reason_message TEXT,
      details_json TEXT NOT NULL,
      retrieved_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id, source),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_clinvar_evidence_run_id ON run_clinvar_evidence(run_id);",
    "CREATE INDEX IF NOT EXISTS idx_run_clinvar_evidence_source ON run_clinvar_evidence(source);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_clinvar_evidence_run_match_insert
    BEFORE INSERT ON run_clinvar_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_clinvar_evidence_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_clinvar_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_clinvar_evidence_validate_insert
    BEFORE INSERT ON run_clinvar_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'clinvar'
          THEN RAISE(ABORT, 'INVALID_CLINVAR_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_CLINVAR_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.clinvar_id IS NULL
          THEN RAISE(ABORT, 'MISSING_CLINVAR_ID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.clinvar_id IS NOT NULL
          THEN RAISE(ABORT, 'CLINVAR_ID_MUST_BE_NULL')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_clinvar_evidence_validate_update
    BEFORE UPDATE OF source, outcome, clinvar_id ON run_clinvar_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'clinvar'
          THEN RAISE(ABORT, 'INVALID_CLINVAR_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_CLINVAR_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.clinvar_id IS NULL
          THEN RAISE(ABORT, 'MISSING_CLINVAR_ID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.clinvar_id IS NOT NULL
          THEN RAISE(ABORT, 'CLINVAR_ID_MUST_BE_NULL')
        END;
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS run_gnomad_evidence (
      run_id TEXT NOT NULL,
      variant_id TEXT NOT NULL,
      source TEXT NOT NULL,
      outcome TEXT NOT NULL,
      gnomad_variant_id TEXT,
      global_af REAL,
      reason_code TEXT,
      reason_message TEXT,
      details_json TEXT NOT NULL,
      retrieved_at TEXT NOT NULL,
      PRIMARY KEY (run_id, variant_id, source),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (variant_id) REFERENCES run_variants(variant_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_run_gnomad_evidence_run_id ON run_gnomad_evidence(run_id);",
    "CREATE INDEX IF NOT EXISTS idx_run_gnomad_evidence_source ON run_gnomad_evidence(source);",
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_gnomad_evidence_run_match_insert
    BEFORE INSERT ON run_gnomad_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_gnomad_evidence_run_match_update
    BEFORE UPDATE OF run_id, variant_id ON run_gnomad_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN (SELECT run_id FROM run_variants WHERE variant_id = NEW.variant_id) != NEW.run_id
          THEN RAISE(ABORT, 'RUN_VARIANT_MISMATCH')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_gnomad_evidence_validate_insert
    BEFORE INSERT ON run_gnomad_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'gnomad'
          THEN RAISE(ABORT, 'INVALID_GNOMAD_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_GNOMAD_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.gnomad_variant_id IS NULL
          THEN RAISE(ABORT, 'MISSING_GNOMAD_VARIANT_ID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.gnomad_variant_id IS NOT NULL
          THEN RAISE(ABORT, 'GNOMAD_VARIANT_ID_MUST_BE_NULL')
        END;
    END;
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_run_gnomad_evidence_validate_update
    BEFORE UPDATE OF source, outcome, gnomad_variant_id ON run_gnomad_evidence
    FOR EACH ROW
    BEGIN
      SELECT
        CASE
          WHEN NEW.source != 'gnomad'
          THEN RAISE(ABORT, 'INVALID_GNOMAD_SOURCE')
        END;
      SELECT
        CASE
          WHEN NEW.outcome NOT IN ('found', 'not_found', 'error')
          THEN RAISE(ABORT, 'INVALID_GNOMAD_OUTCOME')
        END;
      SELECT
        CASE
          WHEN NEW.outcome = 'found' AND NEW.gnomad_variant_id IS NULL
          THEN RAISE(ABORT, 'MISSING_GNOMAD_VARIANT_ID')
        END;
      SELECT
        CASE
          WHEN NEW.outcome IN ('not_found', 'error') AND NEW.gnomad_variant_id IS NOT NULL
          THEN RAISE(ABORT, 'GNOMAD_VARIANT_ID_MUST_BE_NULL')
        END;
    END;
    """,
)

_SCHEMA_INIT_LOCK = threading.Lock()
_SCHEMA_INIT_CACHE: set[str] = set()


def _truthy_env(name: str) -> bool:
    value = (os.environ.get(name) or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _normalize_annotation_evidence_policy(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text in {"stop", "continue"}:
        return text
    return None


def _default_annotation_evidence_policy_for_migration() -> str:
    explicit = _normalize_annotation_evidence_policy(
        os.environ.get("SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT")
    )
    if explicit:
        return explicit
    legacy = os.environ.get("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR")
    if legacy is None:
        return "continue"
    return "stop" if _truthy_env("SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR") else "continue"


def ensure_parent_dir(path: str) -> None:
    if path == ":memory:":
        return
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    ensure_parent_dir(db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000;")
    if db_path != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL;")
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
    cache_key = _connection_cache_key(conn)
    if cache_key in _SCHEMA_INIT_CACHE:
        return

    with _SCHEMA_INIT_LOCK:
        if cache_key in _SCHEMA_INIT_CACHE:
            return

        was_in_transaction = conn.in_transaction
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        _apply_schema_migrations(conn)
        if not was_in_transaction:
            conn.commit()

        _SCHEMA_INIT_CACHE.add(cache_key)


def _connection_cache_key(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("PRAGMA database_list;").fetchone()
    except Exception:
        row = None
    path = (row[2] if row and len(row) > 2 else "") or ""
    if path:
        return os.path.abspath(path)
    return f":memory:{id(conn)}"


def _apply_schema_migrations(conn: sqlite3.Connection) -> None:
    _ensure_runs_reference_build_column(conn)
    _ensure_runs_annotation_evidence_policy_column(conn)
    _normalize_stage_status_vocabulary(conn)


def _normalize_stage_status_vocabulary(conn: sqlite3.Connection) -> None:
    # Legacy databases may contain stage statuses that are no longer part of the
    # accepted vocabulary (Story 3.2). Normalize them in-place so APIs never
    # surface unexpected status values.
    has_legacy = conn.execute(
        "SELECT 1 FROM run_stages WHERE status = 'blocked' LIMIT 1;"
    ).fetchone()
    if not has_legacy:
        return
    conn.execute("UPDATE run_stages SET status = 'failed' WHERE status = 'blocked';")


def _ensure_runs_reference_build_column(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(runs);").fetchall()}
    if "reference_build" in columns:
        return
    conn.execute("ALTER TABLE runs ADD COLUMN reference_build TEXT NOT NULL DEFAULT 'GRCh38';")


def _ensure_runs_annotation_evidence_policy_column(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(runs);").fetchall()}
    fallback_policy = _default_annotation_evidence_policy_for_migration()
    if "annotation_evidence_policy" not in columns:
        conn.execute(
            "ALTER TABLE runs ADD COLUMN annotation_evidence_policy TEXT NOT NULL DEFAULT 'continue';"
        )
        if fallback_policy != "continue":
            conn.execute(
                "UPDATE runs SET annotation_evidence_policy = ?",
                (fallback_policy,),
            )
        return

    has_invalid = conn.execute(
        """
        SELECT 1
        FROM runs
        WHERE annotation_evidence_policy IS NULL
           OR annotation_evidence_policy NOT IN ('stop', 'continue')
        LIMIT 1
        """
    ).fetchone()
    if not has_invalid:
        return

    conn.execute(
        """
        UPDATE runs
        SET annotation_evidence_policy = ?
        WHERE annotation_evidence_policy IS NULL
           OR annotation_evidence_policy NOT IN ('stop', 'continue')
        """,
        (fallback_policy,),
    )
