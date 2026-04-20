# Reproducibility

This repo supports two different reproducibility claims.

## 1. Rebuild the public site from committed public artifacts

This is the lighter claim. The committed `site/` directory already contains the public HTML, CSS, JavaScript, and frozen `site/data/*` artifacts used by GitHub Pages.

If you only want to inspect or serve the public release:

```bash
python3 -m http.server 8000 --directory site
```

Then open `http://127.0.0.1:8000/`.

This path does **not** rerun the analysis. It serves the already-committed public result mirror.

## 2. Rerun the public analysis pipeline from public data

This is the heavier claim. The repo is designed to rerun from public data sources, with optional sibling caches only as accelerators.

Typical setup:

```bash
cp .env.example .env
set -a
source .env
set +a

python3 -m venv "$UV_PROJECT_ENVIRONMENT"
"$UV_PROJECT_ENVIRONMENT/bin/pip" install -e '.[dev]'
```

Core checks:

```bash
set -a
source .env
set +a

"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
```

Build the live quarterly bundle:

```bash
set -a
source .env
set +a

"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run
```

## Public-data versus local accelerators

- The committed `site/data/*` mirror is the public release artifact.
- Optional sibling caches or sibling repos may speed up local rebuilds.
- The public baseline should not rely on private local planning files or ignored local bundles.

## What this repo does **not** claim

- It does not claim that residual accounting is independent mechanism proof.
- It does not claim that the broad TDC estimate and the independently confirmed non-TDC side are the same object.
- It does not claim that every exploratory diagnostic is part of the strict independently confirmed core.

## Recommended public validation path

Focused validation used for the current public bundle:

```bash
PYTHONPATH=src pytest -q \
  tests/test_contracts.py \
  tests/test_site_export_pipeline.py \
  tests/test_cli_pipeline.py \
  tests/test_pass_through_summary.py \
  tests/test_build_panel.py
```

At the time of the current public release-prep pass, that focused slice passed with `41 passed`.
