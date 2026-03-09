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
- If prediction fails with `ALPHAMISSENSE_NOT_AVAILABLE`, rerun:
  - `docker compose run --rm sp-vep-init`
  - then restart app: `docker compose up -d sp`

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

dbSNP:
- `SP_DBSNP_ENABLED` (default: `1`)
- `SP_DBSNP_API_BASE_URL` (default: `https://api.ncbi.nlm.nih.gov/variation/v0`)
- `SP_DBSNP_TIMEOUT_SECONDS` (default: `10`)
- `SP_DBSNP_RETRY_MAX_ATTEMPTS` (default: `3`)
- `SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS` (default: `0.5`)
- `SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS` (default: `8`)
- `SP_DBSNP_API_KEY` (optional)
- `SP_DBSNP_ASSEMBLY` (default: `GRCh38`)

ClinVar:
- `SP_CLINVAR_ENABLED` (default: `1`)
- `SP_CLINVAR_API_BASE_URL` (default: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils`)
- `SP_CLINVAR_TIMEOUT_SECONDS` (default: `10`)
- `SP_CLINVAR_RETRY_MAX_ATTEMPTS` (default: `3`)
- `SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS` (default: `0.5`)
- `SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS` (default: `8`)
- `SP_CLINVAR_API_KEY` (optional)

gnomAD:
- `SP_GNOMAD_ENABLED` (default: `1`)
- `SP_GNOMAD_API_BASE_URL` (default: `https://gnomad.broadinstitute.org/api`)
- `SP_GNOMAD_DATASET_ID` (default: `gnomad_r4`)
- `SP_GNOMAD_REFERENCE_GENOME` (default: `GRCh38`)
- `SP_GNOMAD_TIMEOUT_SECONDS` (default: `10`)
- `SP_GNOMAD_RETRY_MAX_ATTEMPTS` (default: `3`)
- `SP_GNOMAD_RETRY_BACKOFF_BASE_SECONDS` (default: `0.5`)
- `SP_GNOMAD_RETRY_BACKOFF_MAX_SECONDS` (default: `8`)
- `SP_GNOMAD_MIN_REQUEST_INTERVAL_SECONDS` (default: `1.0`)

Current gnomAD output scope:
- gnomAD retrieval is executed during annotation and reported in annotation stage stats/diagnostics.
- Dedicated persisted gnomAD evidence endpoint (`/gnomad_evidence`) is planned but not yet shipped.

Annotation evidence failure mode:
- `SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR` (default: `0`)
- When `0`, annotation continues and records per-source retrieval errors in stage stats/evidence rows.
- When `1`, any dbSNP/ClinVar/gnomAD retrieval error fails annotation stage immediately.

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
- `docs/integration-contracts.md` (single integration reference for tool/feature inputs, request formats, outputs, and diagnostics)

## Annotation Output View

- In `Results -> Annotation`, SP shows:
  - annotation stage summary stats
  - parsed annotated VCF preview as a table (`CHROM`, `POS`, `ID`, `REF`, `ALT`, etc.)
- The table is sourced from the annotation artifact file (`snpeff.annotated.vcf`) for the latest successful upload of the run.

## Pre-Annotation Behavior

- Pre-annotation is a local deterministic stage.
- It derives and persists basic context from parsed SNVs (for example `variant_key`, `base_change`, substitution class, base classes).
- It does not call external evidence sources.
- dbSNP/ClinVar/gnomAD retrieval remains in the annotation stage for final enrichment/provenance.

## Prediction Routing Behavior

- Prediction tools (SIFT, PolyPhen-2, AlphaMissense via VEP) are executed for `missense` variants.
- Variants classified as `other` are skipped by predictor execution.
- Variants outside predictor applicability may still be persisted with deterministic `not_applicable`/reason codes where applicable.
