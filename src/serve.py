import os


def _get_host() -> str:
    return os.environ.get("SP_HOST", "127.0.0.1")


def _get_port() -> int:
    raw = os.environ.get("SP_PORT", "8000").strip()
    try:
        port = int(raw)
    except ValueError:
        raise ValueError(f"Invalid SP_PORT: {raw!r}") from None
    if port < 1 or port > 65535:
        raise ValueError(f"SP_PORT out of range: {port}")
    return port


def main() -> None:
    import logging

    from app import create_app

    try:
        from waitress import serve
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Waitress is not installed. Install dependencies with: "
            "python -m pip install -r requirements.txt"
        ) from exc

    host = _get_host()
    port = _get_port()

    logging.basicConfig(level=logging.INFO)
    print(f"Serving SP on http://{host}:{port}/ (Waitress). Press Ctrl+C to stop.")

    app = create_app()
    try:
        serve(app, host=host, port=port, ident="SP")
    except OSError as exc:
        raise OSError(
            f"Failed to bind to {host}:{port}. "
            "If the port is already in use, set SP_PORT to another value (e.g., 8001)."
        ) from exc


if __name__ == "__main__":
    main()
