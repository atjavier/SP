# BioEvidence (SP)

BioEvidence is the branded UI for SP. The app title/tagline shown in the UI are configurable via `APP_NAME` and `APP_TAGLINE`.

## Recommended Runtime (Windows)

Use Docker for real VEP on Windows. It avoids native Windows Perl/HTS issues and keeps setup reproducible.

## Docker (Real VEP Runtime)

Prerequisites:
- Docker Desktop (WSL2 backend enabled)
- At least ~60 GB free disk (VEP/SnpEff runtimes)
- Additional disk for local evidence DBs (can be large; see local evidence notes below)
- Optional local gnomAD v4.0 exomes: ~283 GB (plus indexes)

### From Scratch (Fresh Clone)

From repo root:

```powershell
cd "C:\Users\Adrian Javier\Desktop\UPLB Docs\SP"
docker compose build sp
docker compose up -d sp
```

Open `http://127.0.0.1:8000/`.

If init jobs fail on line 2 with `set -euo pipefail` (CRLF issue on another device), run:
`git config --local core.autocrlf false`, then `git checkout -- scripts/*.sh`, then repeat the build.

### Minimum Disk Mode (Recommended)

If you want the smallest setup, keep local evidence DB installs disabled:
- `INSTALL_DBSNP=0`
- `INSTALL_CLINVAR=0`
- `INSTALL_GNOMAD=0`
- `SP_EVIDENCE_MODE=online`

This avoids downloading local evidence files.

From repo root:

```powershell
cd "C:\Users\Adrian Javier\Desktop\UPLB Docs\SP"
docker compose build
docker compose up -d sp
```

Open `http://127.0.0.1:8000/`.

Notes:
- `docker compose up -d sp` runs init jobs (`sp-vep-init`, `sp-snpeff-init`, `sp-evidence-init`) before starting `sp`.
- `sp-vep-init` installs:
  - Ensembl VEP (`release/115`)
  - Human cache (`homo_sapiens`, `GRCh38`)
  - AlphaMissense plugin + data + tabix index
- `sp-snpeff-init` installs SnpEff runtime + `GRCh38.86` DB.
- `sp-evidence-init` installs local evidence databases:
  - dbSNP VCF + tabix index
  - ClinVar VCF + tabix index
- Runtime assets are persisted in Docker volume `vep-data` under `/opt/vep`.
- SnpEff assets are persisted in Docker volume `snpeff-data` under `/opt/snpeff`.
- Evidence assets are persisted in Docker volume `evidence-data` under `/opt/evidence`.
- App data is persisted in Docker volume `instance-data`.
- Current compose profile enables SnpEff and VEP together.
- Evidence annotation is enforced as missense-only; compose sets `SP_EVIDENCE_PROFILE=predictor_only`.
- Current compose profile enables local dbSNP + ClinVar install by default.
- Current compose profile sets `SP_EVIDENCE_MODE=hybrid` (local first, online fallback).
- If `docker compose build` fails with an image export conflict like `image "sp-local:latest": already exists`, build only once with one of these: `docker compose build sp`, `docker compose build --parallel=false`, or `COMPOSE_BAKE=false docker compose build`.
- Local evidence downloads use `aria2c` (multi-connection) when available; rebuild the image to pick up download speed improvements.

Project decision:
- gnomAD is **online by default** due to local dataset size.
- Optional local gnomAD is supported when you have enough storage (see "Optional local gnomAD v4.0" below).

## Downloaded Files (Docker Init Jobs)

These files are downloaded/extracted by `sp-vep-init`, `sp-snpeff-init`, and `sp-evidence-init`.

| Init Job | File Name / Pattern | Stored At | Significance |
|---|---|---|---|
| `sp-vep-init` | `ensembl-vep` (git checkout, branch `release/115`) | `/opt/vep/ensembl-vep` | Core VEP runtime and Ensembl Perl modules used for consequence/prediction. |
| `sp-vep-init` | `homo_sapiens_vep_115_GRCh38` cache content (installer download + extract) | `/opt/vep/.vep/homo_sapiens/115_GRCh38` | Required offline VEP cache for GRCh38 annotation/prediction. |
| `sp-vep-init` | `AlphaMissense_hg38.tsv.gz` | `/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz` | AlphaMissense score lookup data used by VEP plugin. |
| `sp-vep-init` | `AlphaMissense_hg38.tsv.gz.tbi` | `/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz.tbi` | Tabix index required for fast/random AlphaMissense queries. |
| `sp-snpeff-init` | `snpEff_latest_core.zip` | `/opt/snpeff/snpEff_latest_core.zip` | SnpEff distribution archive used to install runtime. |
| `sp-snpeff-init` | `snpEff.jar` | `/opt/snpeff/snpEff/snpEff.jar` | Main SnpEff executable used in annotation stage. |
| `sp-snpeff-init` | `snpEffectPredictor.bin` (for `GRCh38.86`) | `/opt/snpeff/snpEff/data/GRCh38.86/snpEffectPredictor.bin` | Prebuilt SnpEff genome database required for annotation. |
| `sp-evidence-init` | `dbsnp_all_grch38.vcf.gz` | `/opt/evidence/dbsnp/dbsnp_all_grch38.vcf.gz` | Local dbSNP evidence database for rsID lookup in offline/hybrid mode. |
| `sp-evidence-init` | `dbsnp_all_grch38.vcf.gz.tbi` | `/opt/evidence/dbsnp/dbsnp_all_grch38.vcf.gz.tbi` | Tabix index for fast dbSNP position queries. |
| `sp-evidence-init` | `clinvar_grch38.vcf.gz` | `/opt/evidence/clinvar/clinvar_grch38.vcf.gz` | Local ClinVar evidence database for clinical significance lookup in offline/hybrid mode. |
| `sp-evidence-init` | `clinvar_grch38.vcf.gz.tbi` | `/opt/evidence/clinvar/clinvar_grch38.vcf.gz.tbi` | Tabix index for fast ClinVar position queries. |
| `sp-evidence-init` | `evidence-manifest.env` | `/opt/evidence/evidence-manifest.env` | Records resolved local evidence paths used by the app. |
| `sp-evidence-init` (optional) | `gnomad.exomes.v4.0.sites.chr*.vcf.bgz` | `/opt/evidence/gnomad/v4.0/exomes` | Local gnomAD v4.0 exomes sites VCFs (per-chrom). |
| `sp-evidence-init` (optional) | `gnomad.exomes.v4.0.sites.chr*.vcf.bgz.tbi` | `/opt/evidence/gnomad/v4.0/exomes` | Tabix indexes for gnomAD local queries. |

### Why these are downloaded/installed

- `ensembl-vep` (runtime code) is installed because the prediction/classification stages invoke the VEP executable and Ensembl Perl modules directly.
- `homo_sapiens_vep_115_GRCh38` cache is installed because VEP runs in offline mode and needs local transcript/annotation cache data.
- `AlphaMissense_hg38.tsv.gz` + `.tbi` are installed because the AlphaMissense VEP plugin reads this local tabix-indexed file to return pathogenicity scores.
- `snpEff_latest_core.zip` is downloaded to install the SnpEff runtime (`snpEff.jar`) used by the annotation stage.
- `snpEffectPredictor.bin` (GRCh38.86) is downloaded because SnpEff requires a local genome database to annotate VCF consequences.
- `dbsnp_all_grch38.vcf.gz` + `.tbi` are downloaded so annotation can resolve rsIDs locally in `offline` mode (and in `hybrid` before API fallback).
- `clinvar_grch38.vcf.gz` + `.tbi` are downloaded so annotation can resolve ClinVar IDs/significance locally in `offline` mode (and in `hybrid` before API fallback).
- `evidence-manifest.env` is written to document the exact resolved local evidence paths that were initialized.
- gnomAD v4.0 VCFs + indexes are optional and only downloaded when `INSTALL_GNOMAD=1` (large ~283 GB).

Useful commands:

```powershell
# app logs
docker compose logs -f sp

# rerun runtime setup jobs only
docker compose run --rm sp-vep-init
docker compose run --rm sp-snpeff-init
docker compose run --rm sp-evidence-init

# stop app
docker compose stop sp

# remove app + volumes (destructive)
docker compose down -v
```

Troubleshooting:
- If Docker Compose fails with `unexpected character "\ufeff"` on line 1 of `.env`, re-save `.env` as UTF-8 **without BOM**.
- If prediction fails with `ALPHAMISSENSE_NOT_AVAILABLE`, rerun `docker compose run --rm sp-vep-init`, then restart with `docker compose up -d sp`.
- If init jobs fail on line 2 with `set -euo pipefail`, the `.sh` files were checked out with CRLF. Re-checkout with LF and rebuild with `git config --local core.autocrlf false`, `git checkout -- scripts/*.sh`, then `docker compose build --no-cache`.

## Docs + UI

- In-app Docs: `GET /docs` (linked from the sidebar).
- Sidebar is collapsible and persisted via localStorage.
- Progress/Results include glossary tooltips for key terms (keyboard accessible).

## Core Workflow

- **Start**: choose a VCF and press `Start`. The app validates first, then runs the pipeline if valid.
- **New run**: resets to a fresh run record.
- **Pipeline stages**: Parser → Pre-Annotation → Classification → Prediction → Annotation → Reporting.
- **No raw VCF retention by default**: the upload is used for parsing, then discarded; derived records remain in SQLite.

## Run Controls + Live Updates

- **Progress tab** shows run summary (run ID, status, reference build, evidence mode/policy) plus stage status.
- **Cancel run** is available while running.
- **Retry from failed stage** is available when a stage fails.
- **Live updates** are delivered via SSE for run status, stage status, variant results, and logs.
- **Run logs** surface recent, run-scoped log lines and avoid raw VCF content.

## Results Explorer

- Stage-specific tables are shown in Results as stages complete.
- Variant detail panel shows evidence + predictor outputs with provenance and completeness labels.
## Runtime Contract (Docker profile)

The app container uses:
- `SP_VEP_CMD=perl`
- `SP_VEP_SCRIPT_PATH=/opt/vep/ensembl-vep/vep`
- `SP_VEP_CACHE_DIR=/opt/vep/.vep`
- `SP_VEP_PLUGIN_DIR=/opt/vep/.vep/Plugins`
- `SP_VEP_ALPHAMISSENSE_FILE=/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz`
- `SP_VEP_ASSEMBLY=GRCh38`

Evidence runtime:
- `SP_EVIDENCE_MODE=hybrid` (`online|offline|hybrid`)
- `SP_DBSNP_LOCAL_VCF_PATH=/opt/evidence/dbsnp/dbsnp_all_grch38.vcf.gz`
- `SP_CLINVAR_LOCAL_VCF_PATH=/opt/evidence/clinvar/clinvar_grch38.vcf.gz`
- `SP_GNOMAD_LOCAL_VCF_PATH=/opt/evidence/gnomad/v4.0/exomes`

Local evidence bootstrap tuning:
- `INSTALL_DBSNP` / `INSTALL_CLINVAR` / `INSTALL_GNOMAD` (`1|0`, default `1/1/0`)
- `DBSNP_VCF_URL`, `CLINVAR_VCF_URL`
- gnomAD v4.0 controls:
  - `GNOMAD_VCF_BASE_URL`
  - `GNOMAD_FILE_PREFIX`
  - `GNOMAD_FILE_SUFFIX`
  - `GNOMAD_CHROM_LIST`

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
- `APP_NAME` (default: `BioEvidence`)
- `APP_TAGLINE` (default: `Teach and trace SNV outcomes.`)
- `SP_DB_PATH` (default: `<repo_root>\instance\sp.db`)
- `SP_MAX_UPLOAD_BYTES` (default: `52428800`)
- `SP_MAX_VCF_DECOMPRESSED_BYTES` (default: `262144000`)
- `SP_WAITRESS_THREADS` (default: `16`)
- `SECRET_KEY` (recommended for stable sessions)
- `SP_EVIDENCE_PROFILE` (forced to `predictor_only`)
  - Evidence annotation is enforced as missense-only; profile settings are ignored.
- `SP_EVIDENCE_MODE` (default: `online`)
  - `online`: remote APIs only
  - `offline`: local VCF/tabix databases only
  - `hybrid`: local first, fallback to online on local errors
- Evidence mode decision telemetry is persisted per run:
  - `evidence_mode_requested`
  - `evidence_mode_effective`
  - `evidence_online_available`
  - `evidence_offline_sources_configured` (`dbsnp|clinvar|gnomad`, path configured)
  - `evidence_mode_decision_reason`
  - `evidence_mode_detected_at`
- Additional per-source readiness diagnostics are emitted in annotation stage stats:
  - `evidence_offline_sources_available`
  - `evidence_offline_sources_unavailable_reason`
- Connectivity probe tuning:
  - `SP_EVIDENCE_CONNECTIVITY_PROBE_ENABLED` (default: `1`)
  - `SP_EVIDENCE_CONNECTIVITY_PROBE_TIMEOUT_SECONDS` (default: `1.5`)
  - `SP_EVIDENCE_CONNECTIVITY_PROBE_MAX_ATTEMPTS` (default: `1`)
- Strict no-valid-source blocking:
  - If enabled evidence sources have no valid retrieval path (online unavailable and no offline-ready local sources), annotation fails with `EVIDENCE_SOURCES_UNAVAILABLE`.
  - Failure details include `missing_sources`, `missing_outputs`, `blocked_outputs`, mode-decision fields/maps, and remediation `hint`.
  - Reporting remains `queued` for that upload because annotation did not complete successfully.

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
- `SP_VEP_BATCH_SIZE` (default: `20000`, set to `0` to disable)
- `SP_VEP_EXTRA_ARGS`

dbSNP:
- `SP_DBSNP_ENABLED` (default: `1`)
- `SP_DBSNP_API_BASE_URL` (default: `https://api.ncbi.nlm.nih.gov/variation/v0`)
- `SP_DBSNP_TIMEOUT_SECONDS` (default: `10`)
- `SP_DBSNP_RETRY_MAX_ATTEMPTS` (default: `3`)
- `SP_DBSNP_RETRY_BACKOFF_BASE_SECONDS` (default: `0.5`)
- `SP_DBSNP_RETRY_BACKOFF_MAX_SECONDS` (default: `8`)
- `SP_DBSNP_MAX_WORKERS` (default: `1`)
- `SP_DBSNP_API_KEY` (optional)
- `SP_DBSNP_ASSEMBLY` (default: `GRCh38`)
- `SP_DBSNP_LOCAL_VCF_PATH` (optional local/offline mode path)

ClinVar:
- `SP_CLINVAR_ENABLED` (default: `1`)
- `SP_CLINVAR_API_BASE_URL` (default: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils`)
- `SP_CLINVAR_TIMEOUT_SECONDS` (default: `10`)
- `SP_CLINVAR_RETRY_MAX_ATTEMPTS` (default: `3`)
- `SP_CLINVAR_RETRY_BACKOFF_BASE_SECONDS` (default: `0.5`)
- `SP_CLINVAR_RETRY_BACKOFF_MAX_SECONDS` (default: `8`)
- `SP_CLINVAR_MAX_WORKERS` (default: `1`)
- `SP_CLINVAR_API_KEY` (optional)
- `SP_CLINVAR_LOCAL_VCF_PATH` (optional local/offline mode path)

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
- `SP_GNOMAD_MAX_WORKERS` (default: `1`)
- `SP_GNOMAD_LOCAL_VCF_PATH` (optional local/offline path; directory or file)
- Local gnomAD install is disabled by default due size; enable with `INSTALL_GNOMAD=1` and v4.0 VCFs (~283 GB).

Optional local gnomAD v4.0:
- Set `INSTALL_GNOMAD=1`
- Ensure `/opt/evidence/gnomad/v4.0/exomes` contains chr1-22, X, Y VCFs + `.tbi`
- Validate with:
  - `bash scripts/validate_gnomad_v4_local.sh /opt/evidence/gnomad/v4.0/exomes`

Evidence profile behavior:
- In `minimum_exome` profile, dbSNP/ClinVar/gnomAD API calls are skipped for variants classified outside coding scope.
- In `predictor_only` profile, evidence API calls are skipped for non-`missense` variants.
- Annotation stage stats expose:
  - `*_variants_eligible`
  - `*_skipped_out_of_scope`
- This profile is intended for faster pilot runs and reduced remote API pressure.

Local evidence behavior:
- In `offline` mode, dbSNP/ClinVar lookups use local VCF/tabix sources.
- In `hybrid` mode, dbSNP/ClinVar use local-first with online fallback on local errors.
- dbSNP local lookup now normalizes chromosome aliases and also tries RefSeq contigs (for example `NC_000001.11`) to match NCBI dbSNP VCF naming.
- Variant Details -> Evidence now shows source provenance as `source (source_mode)` when available (for example `dbsnp (offline_local)`).
- Project default keeps gnomAD online-only due dataset size constraints.

Evidence mode decision behavior:
- Decision runs during annotation preflight and is shown in Progress.
- Requested `online`:
  - online available => effective `online`
  - online unavailable + offline configured => effective `offline`
- Requested `offline`:
  - offline configured => effective `offline`
  - offline unavailable + online available => effective `online`
- Requested `hybrid`:
  - both available => effective `hybrid`
  - only one available => effective `offline` or `online`
- If neither path is available, annotation hard-fails with `EVIDENCE_SOURCES_UNAVAILABLE` and downstream annotation-dependent outputs remain blocked.
- `configured` means a local source path was provided.
- `available` means the configured local source is actually ready (indexed VCF discoverable) and the source is enabled.

Current gnomAD output scope:
- gnomAD retrieval is executed during annotation and reported in annotation stage stats/diagnostics.
- Persisted gnomAD evidence is available at `GET /api/v1/runs/{run_id}/gnomad_evidence` (latest-upload stage gated, same contract style as dbSNP/ClinVar endpoints).

Annotation evidence failure mode:
- `SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT` (default: `continue`)
- Allowed values: `continue`, `stop`
- This sets the default for new runs; users can change it per-run in the UI.
- Legacy fallback: `SP_ANNOTATION_FAIL_ON_EVIDENCE_ERROR` (`0` => continue, `1` => stop) is used only when `SP_ANNOTATION_EVIDENCE_POLICY_DEFAULT` is unset.

Run settings API (annotation evidence policy):
- `POST /api/v1/runs/{run_id}/settings`
- Body:
```json
{ "annotation_evidence_policy": "continue" }
```
- Allowed values: `continue`, `stop`
- Returns `409 RUN_SETTINGS_NOT_UPDATABLE` when run status is `running`.

Annotation completeness signaling:
- Annotation stats now include explicit completeness markers:
  - `annotation_evidence_completeness`: `complete|partial|unavailable`
  - `evidence_source_completeness`: source map for `dbsnp|clinvar|gnomad`
  - `evidence_source_completeness_reason`: per-source reason code for completeness state
- Reporting stage now carries forward annotation completeness summary so final-result messaging does not silently imply full completeness when evidence is partial/unavailable.

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

## Final Result View

- In `Results -> Final`, SP shows the pipeline-level outcome plus the reporting summary (documents/findings/tables).
- The dedicated Reporting tab is removed; reporting summaries now live in the Final tab.

## Results Pagination + Filters

- Pre-annotation, classification, and prediction tables are paginated via next/previous controls.
- Classification results can be filtered by consequence category.

## Pre-Annotation Behavior

- Pre-annotation is a local deterministic stage.
- It derives and persists basic context from parsed SNVs (for example `variant_key`, `base_change`, substitution class, base classes).
- It does not call external evidence sources.
- dbSNP/ClinVar/gnomAD retrieval remains in the annotation stage for final enrichment/provenance.

## Prediction Routing Behavior

- Prediction tools (SIFT, PolyPhen-2, AlphaMissense via VEP) are executed for `missense` variants.
- Variants classified as `other` are skipped by predictor execution.
- Variants outside predictor applicability may still be persisted with deterministic `not_applicable`/reason codes where applicable.
