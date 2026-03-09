# Integration Entry Template

Use this template when adding a new tool/feature integration to `docs/integration-contracts.md`.

## N) <Integration Name>

Story key(s): `<story-key>`  
Primary code: `<path1>`, `<path2>`

### Input
- `<input field/source>`
- `<required env vars>`

### Request/Command format
`<exact URL or CLI command format with params/flags>`

### Output structure
- Persistence target: `<table/artifact>`
- API target: `<endpoint or "not exposed yet">`
```json
{
  "example": "shape"
}
```

### Failure behavior
- `<error codes>`
- `<retry/timeout/backoff behavior>`

### Notes
- `<compatibility, caveats, stage-gating, stale-data rules>`

