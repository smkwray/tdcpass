# Reproducibility

TDCpass builds reproducible arithmetic and boundary comparisons from public
inputs. A completed build does not establish causal identification or an
independent estimate.

## Validate the source checkout

Install the environment using the [README](README.md), then run:

```bash
set -a
source .env
set +a
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass demo
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
bash scripts/hygiene_check.sh
```

The synthetic demo writes ignored local artifacts. Unit and fixture tests verify
software behavior; they do not certify a current live-data rebuild.

## Build from public inputs

The pipeline uses Federal Reserve Financial Accounts, Treasury cash data, FRED
series, and upstream TDCest artifacts. Input specifications live in
[config/data_sources.yml](config/data_sources.yml),
[config/series_registry.yml](config/series_registry.yml), and
[config/cache_shortcuts.yml](config/cache_shortcuts.yml). The TDCest proxy is an
upstream dependency, not a quantity re-estimated independently here.

Configure `TDCEST_ROOT` for the source or artifact tree containing
`data/processed/tdc_estimates.csv`. The same directory can supply
`tdc_downstream_deposit_effect_series_panel.csv` and
`tdc_tier2_regression_series.csv` for the supplemental comparison ladder;
missing supplemental coverage remains a limit on those rows. Cache discovery
rules are separate from these canonical processed-file inputs.

Configure `FRED_API_KEY` in the ignored `.env` for API-backed FRED requests; the
pipeline also supports public FRED graph downloads. Other sibling caches are
optional accelerators. Do not commit the configuration file, credentials,
downloads, or copied source bundles.

```bash
set -a
source .env
set +a
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run \
  --root output/rebuild --reuse-mode rebuild
```

The separate root contains its own derived data, analysis outputs, and local
export bundle. It isolates the rebuild from other retained runs.
`--source-root PATH` instead materializes an existing contract bundle, while
`--raw-fixture-root PATH` supplies a frozen raw-data fixture. These are distinct
from a fresh download; retain their source identities when reporting results.

The output contract and its column descriptions are
[config/output_contract.yml](config/output_contract.yml) and
[docs/output_schema.md](docs/output_schema.md). Read the download manifests,
treatment fingerprint, provenance validation, coverage, and readiness summaries
alongside the output tables. Command success does not override a failed evidence
gate. Reproducible comparisons require a clean, provenance-checked producer and
reachable input commits.

Public sources revise observations and release layouts. Reproducing a numerical
result requires its pinned input artifacts as well as its committed producer;
installing the current package and downloading the latest data is a different
rebuild.
