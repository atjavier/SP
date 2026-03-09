# SP

## Recommended Runtime (Windows)

Use Docker for real VEP on Windows. It avoids native Windows Perl/HTS issues and keeps setup reproducible.

## Docker (Real VEP Runtime)

Prerequisites:
- Docker Desktop (WSL2 backend enabled)
- At least ~60 GB free disk (VEP cache + image + volumes)

From repo root:

```powershell
cd "C:\Users\Adrian Javier\Desktop\UPLB Docs\SP"
docker compose build
docker compose up -d sp
```

Open `http://127.0.0.1:8000/`.

Notes:
- `docker compose up -d sp` runs both init jobs (`sp-vep-init` and `sp-snpeff-init`) before starting `sp`.
- `sp-vep-init` installs:
  - Ensembl VEP (`release/115`)
  - Human cache (`homo_sapiens`, `GRCh38`)
  - AlphaMissense plugin + data + tabix index
- `sp-snpeff-init` installs SnpEff runtime + `GRCh38.86` DB.
- Runtime assets are persisted in Docker volume `vep-data` under `/opt/vep`.
- SnpEff assets are persisted in Docker volume `snpeff-data` under `/opt/snpeff`.
- App data is persisted in Docker volume `instance-data`.
- Current compose profile enables SnpEff and VEP together.

Useful commands:

```powershell
# app logs
docker compose logs -f sp

# rerun runtime setup jobs only
docker compose run --rm sp-vep-init
docker compose run --rm sp-snpeff-init

# stop app
docker compose stop sp

# remove app + volumes (destructive)
docker compose down -v
```

Troubleshooting:
- If Docker Compose fails with `unexpected character "\ufeff"` on line 1 of `.env`, re-save `.env` as UTF-8 **without BOM**.

## Runtime Contract (Docker profile)

The app container uses:
- `SP_VEP_CMD=perl`
- `SP_VEP_SCRIPT_PATH=/opt/vep/ensembl-vep/vep`
- `SP_VEP_CACHE_DIR=/opt/vep/.vep`
- `SP_VEP_PLUGIN_DIR=/opt/vep/.vep/Plugins`
- `SP_VEP_ALPHAMISSENSE_FILE=/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz`
- `SP_VEP_ASSEMBLY=GRCh38`

## Share With Test Users

Once `sp` is running on port `8000`, expose it with a tunnel:

```powershell
ngrok http 8000
```

Share the generated `https://...` URL with test users.

## Native Windows (Development / Fallback)

### Run Flask dev server

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src\app.py
```

Open `http://127.0.0.1:5000/`.

### Serve with Waitress

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src\serve.py
```

Open `http://127.0.0.1:8000/`.

If the command appears to "hang", that's expected: the server is running in the foreground.

## Environment Variables

General:
- `SP_HOST` (default: `127.0.0.1`)
- `SP_PORT` (default: `8000`)
- `SP_DB_PATH` (default: `<repo_root>\instance\sp.db`)
- `SP_MAX_UPLOAD_BYTES` (default: `52428800`)
- `SP_MAX_VCF_DECOMPRESSED_BYTES` (default: `262144000`)
- `SP_WAITRESS_THREADS` (default: `16`)
- `SECRET_KEY` (recommended for stable sessions)

Local `.env` support:
- Create `.env` at repo root (see `.env.example`).
- Real environment variables override `.env`.

SnpEff:
- `SP_SNPEFF_ENABLED` (`0` to disable)
- `SP_SNPEFF_JAR_PATH`
- `SP_SNPEFF_HOME`
- `SP_SNPEFF_GENOME` (default: `GRCh38.86`)
- `SP_SNPEFF_CONFIG_PATH`
- `SP_SNPEFF_DATA_DIR`
- `SP_JAVA_CMD` (default: `java`)
- `SP_SNPEFF_JAVA_XMX` (default: `2g`)
- `SP_SNPEFF_TIMEOUT_SECONDS` (default: `900`)
- `SP_SNPEFF_ARGS`

VEP:
- `SP_VEP_CMD` (default: `vep`)
- `SP_VEP_SCRIPT_PATH`
- `SP_VEP_CACHE_DIR` (required)
- `SP_VEP_PLUGIN_DIR`
- `SP_VEP_ALPHAMISSENSE_FILE` (required)
- `SP_VEP_FASTA_PATH`
- `SP_VEP_ASSEMBLY` (default: `GRCh38`)
- `SP_VEP_TIMEOUT_SECONDS` (default: `1200`)
- `SP_VEP_EXTRA_ARGS`

## SnpEff Setup Script (Windows)

Quick setup:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup_snpeff.ps1
```

Manual zip path:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup_snpeff.ps1 -ZipPath "C:\path\to\snpEff_latest_core.zip"
```

Windows note: prefer relative `SP_SNPEFF_DATA_DIR` under `SP_SNPEFF_HOME` (for example `./data`).

## Run Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

## Story Docs

- `docs/stories/index.md`

## Annotation Output View

- In `Results -> Annotation`, SP shows:
  - annotation stage summary stats
  - parsed annotated VCF preview as a table (`CHROM`, `POS`, `ID`, `REF`, `ALT`, etc.)
- The table is sourced from the annotation artifact file (`snpeff.annotated.vcf`) for the latest successful upload of the run.
