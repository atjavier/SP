# Story 1.4 Flow — Serve Mode (Waitress)

## Purpose

Run the same Flask WSGI app behind Waitress for a more reliable Windows demo mode.

## Flow (serve entrypoint → WSGI app)

1. Developer runs `python src\serve.py`.
2. `src/serve.py` reads:
   - `SP_HOST` (default `127.0.0.1`)
   - `SP_PORT` (default `8000`)
3. `src/serve.py` builds the Flask app via `create_app()` from `src/app.py`.
4. Waitress hosts the WSGI app and serves HTTP requests.
5. Browser opens `http://127.0.0.1:8000/` and the app behaves the same as dev mode.

## Key files

- Serve entrypoint: `src/serve.py`
- App factory: `src/app.py`
- Docs: `README.md`
- Tests: `tests/test_serve_mode.py`

