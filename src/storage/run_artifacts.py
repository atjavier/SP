import os


def artifacts_root_for_db(db_path: str) -> str:
    if db_path == ":memory:":
        raise ValueError("run artifacts require a filesystem-backed database path")
    instance_dir = os.path.dirname(os.path.abspath(db_path))
    return os.path.join(instance_dir, "artifacts")


def run_artifacts_dir(db_path: str, run_id: str) -> str:
    return os.path.join(artifacts_root_for_db(db_path), run_id)


def ensure_run_artifacts_dir(db_path: str, run_id: str) -> str:
    path = run_artifacts_dir(db_path, run_id)
    os.makedirs(path, exist_ok=True)
    return path

