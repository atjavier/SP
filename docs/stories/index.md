# Story Flows

How-it-works docs and implementation records for completed and in-progress stories.

## Local story flow docs (narrative)

These are concise walkthrough docs kept under `docs/stories/`:

- `docs/stories/1-1-initialize-project-skeleton-and-run-the-app-locally.md`
- `docs/stories/1-2-create-sqlite-backed-run-records-no-raw-vcf-retention.md`
- `docs/stories/1-3-stopcancel-a-run.md`
- `docs/stories/1-4-demo-serve-mode-waitress.md`
- `docs/stories/2-1-upload-a-vcf-and-receive-validation-feedback.md`

## BMAD implementation story records (source of truth)

Current implementation story specs, status, and completion notes are tracked in:

- `_bmad-output/implementation-artifacts/`
- `_bmad-output/implementation-artifacts/sprint-status.yaml`

For the current annotation and prediction integration work, start with:

- `_bmad-output/implementation-artifacts/5-5-replace-stubbed-predictor-outputs-with-real-tool-integrations.md`
- `_bmad-output/implementation-artifacts/6-0-integrate-local-snpeff-annotation-stage-and-runtime-configuration.md`
- `_bmad-output/implementation-artifacts/6-1-retrieve-dbsnp-identifiersevidence-and-persist-results-per-variant.md`
- `_bmad-output/implementation-artifacts/9-5-automate-first-run-snpeff-setup-and-runtime-configuration.md`
- `_bmad-output/implementation-artifacts/9-6-automate-first-run-vep-and-alphamissense-setup-and-runtime-configuration.md`

## Mandatory Integration Documentation

For every new tool/feature integration story:

1. Update `docs/integration-contracts.md` with:
   - integration point and story key
   - exact request/command format
   - input contract
   - output contract (artifact/DB/API)
   - failure/retry/timeout behavior
2. Use `docs/templates/integration-entry-template.md` for new entries.
