# SP (Story 1.1 skeleton)

## Run (Windows / PowerShell)

```powershell
py -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
python -m pip install -r requirements.txt
python src\\app.py
```

Open `http://127.0.0.1:5000/`.

Notes:
- Bootstrap loads via CDN in `src/templates/base.html` (internet required for styling in this MVP).
- `src/app.py` defaults to a dev-only `SECRET_KEY=dev` and runs with `debug=True`.

## Run tests

```powershell
.\\.venv\\Scripts\\python.exe -m unittest discover -s tests
```
