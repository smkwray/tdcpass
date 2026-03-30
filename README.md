# tdcpass

`tdcpass` studies one question:

**When the Treasury-attributed component of deposits rises unexpectedly, do matched total bank deposits rise too, or does that increase mostly replace other deposit-creating channels?**

## Status

This repository is being prepared for an initial private release under `smkwray/tdcpass`.

Current posture:

- reproducible public-data pipeline
- quarterly, bank-only headline design
- explicit diagnostics for identification and mechanism caveats
- no published site yet

The current empirical bundle should be read as a **methods and reproducibility preview**, not as a settled pass-through or crowd-out result.

## What the repo does

The package rebuilds a quarterly public-data bundle that includes:

- a bank-only TDC treatment series
- matched total-deposit outcomes
- the non-TDC residual `other_component_qoq = total_deposits_bank_qoq - tdc_bank_only_qoq`
- expanding-window unexpected-TDC shocks
- local-projection response tables
- structural-proxy cross-checks and readiness diagnostics
- manifests for raw downloads and optional sibling-cache reuse

## What it does not do

- It does not rely on sibling repos to run.
- It does not treat the residual alone as mechanism proof.
- It does not publish a site yet.
- It does not currently claim a clean pass-through-versus-crowd-out answer.

## Environment

The repo uses an external virtualenv and external cache directories. It does not use a repo-local `.venv` or repo-local test/cache directories.

Example setup:

```bash
cp .env.example .env
set -a
source .env
set +a

python3 -m venv "$UV_PROJECT_ENVIRONMENT"
"$UV_PROJECT_ENVIRONMENT/bin/pip" install -e '.[dev]'
```

## Quick start

```bash
set -a
source .env
set +a

"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass demo
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
```

To build the live quarterly bundle:

```bash
set -a
source .env
set +a

"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run
```

## Main commands

- `tdcpass doctor`: check required repo/config files and environment visibility
- `tdcpass demo`: run the synthetic demo pipeline
- `tdcpass pipeline run`: build the live quarterly public-data bundle
- `tdcpass discover-cache`: inspect optional sibling-cache candidates

## Output policy

Generated data and build outputs are meant to be rebuilt locally. The repo does not commit:

- raw downloads
- derived quarterly datasets
- demo datasets
- output bundles
- site artifacts
- internal planning or orchestration material

## Repository layout

```text
tdcpass/
  config/
  data/
  examples/
  scripts/
  src/tdcpass/
  tests/
```

## Validation

The current local validation path is:

```bash
set -a
source .env
set +a

"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run --root /tmp/tdcpass-smoke
```

## License

MIT
