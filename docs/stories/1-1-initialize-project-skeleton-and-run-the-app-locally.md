# Story 1.1 Flow — App Skeleton

## Purpose

Provide a minimal Flask app that renders a single app-shell page at `GET /`.

## Flow

1. Developer runs `python src\app.py`.
2. Flask app factory `create_app()` in `src/app.py` configures defaults and registers routes.
3. Browser loads `GET /`.
4. Server renders `src/templates/index.html` (extends `src/templates/base.html`).
5. Page loads static assets (e.g., `src/static/app.css`) and shows the three UI regions:
   - Upload
   - Run Status
   - Results

## Key files

- Server: `src/app.py`
- Templates: `src/templates/base.html`, `src/templates/index.html`
- Static: `src/static/app.css`
- Tests: `tests/test_app.py`

