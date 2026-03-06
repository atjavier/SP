# SP (Story 1.1 skeleton)

## Run (Windows / PowerShell)

```powershell
py -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -r requirements.txt
python src\\app.py
```

Open `http://127.0.0.1:5000/`.

## Serve (Waitress) (Windows / PowerShell)

```powershell
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -r requirements.txt
python src\\serve.py
```

If the command appears to "hang", that's expected: the server is running and the process is blocking.

Open `http://127.0.0.1:8000/`.

Environment variables (optional):
- `SP_HOST` (default: `127.0.0.1`)
  - Use `0.0.0.0` to listen on your LAN (only if you trust the network).
- `SP_PORT` (default: `8000`)
- `SP_DB_PATH` (default: `<repo_root>\\instance\\sp.db`)
- `SECRET_KEY` (recommended for stable sessions across restarts)

Notes:
- Bootstrap loads via CDN in `src/templates/base.html` (internet required for styling in this MVP).
- `src/app.py` generates a random `SECRET_KEY` if none is set; set `SECRET_KEY` to keep sessions stable across restarts.
- Set `FLASK_DEBUG=1` to run with debug enabled.

## Run tests

```powershell
.\\.venv\\Scripts\\python.exe -m unittest discover -s tests
```
