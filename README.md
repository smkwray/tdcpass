# tdcpass

`tdcpass` compares quarterly Treasury Deposit Component (TDC) proxies from [tdcest](https://github.com/smkwray/tdcest) against quarterly US bank deposit changes and balance-sheet counterparts. It runs arithmetic accounting checks, comparisons across TDC definitions, and balance-sheet sensitivity tables, and writes reproducible CSV and JSON bundles under `output/`.

[Reproducibility](REPRODUCIBILITY.md) | [Output schema](docs/output_schema.md)

## What it does

Given a quarterly measured TDC series and a quarterly commercial-bank deposit change, `tdcpass` computes the arithmetic difference:

```text
other deposit change = total bank deposit change - measured TDC
```

It then builds a quarterly comparison bundle covering:

- Exact-identity parity checks between `tdcest` TDC series and deposit totals across quarters.
- Comparisons across bank-only, broad-depository, interest-adjusted, and MMF/ON RRP-adjusted TDC series.
- Balance-sheet counterpart tables pairing TDC with partial bank loan and securities transactions and nonbank holder-asset comparisons.
- Residual and local-projection comparison scripts exposed for sensitivity checks (`decompose`, `shock`, `lp`).

## Data

- Quarterly TDC series from `tdcest`: set `TDCEST_ROOT` to a directory containing `tdcest` processed outputs (`data/processed/tdc_estimates.csv` and optional sensitivity panels). If `TDCEST_ROOT` is unset, `tdcpass` looks in `../tdcest/data/processed/`.
- Public Federal Reserve (Z.1, H.8, H.4.1), Treasury Fiscal Data, and FRED series listed in `config/data_sources.yml` and `config/series_registry.yml`. Export `FRED_API_KEY` to fetch via the FRED JSON API; if unset, downloads fall back to FRED's public `fredgraph.csv` endpoint.

See `environment.example` for optional environment variables (`UV_PROJECT_ENVIRONMENT`, `TDCEST_ROOT`, `FRED_API_KEY`, and cache roots).

## Install

Requires Python 3.11 or later. From the repository root:

```bash
cp environment.example .env
set -a && source .env && set +a
make install
```

`make install` creates a virtual environment at `$HOME/venvs/tdcpass` (override with `VENV=/path/to/env`) and runs `pip install -e '.[dev]'`.

## Usage

Check required configuration and schema files (and whether `FRED_API_KEY` is set):

```bash
tdcpass doctor
```

Run the offline 96-quarter synthetic demo (writes `data/examples/synthetic_quarterly_panel.csv` and `output/demo/`):

```bash
tdcpass demo
```

Rebuild a full quarterly comparison bundle into an isolated output directory:

```bash
tdcpass pipeline run --root output/rebuild --reuse-mode rebuild
```

Validate an existing run's closeout artifacts and manifest:

```bash
tdcpass pipeline closeout --root output/rebuild
```

Additional commands include `discover-cache`, `fetch-fred`, `fetch-fiscaldata`, `decompose`, `shock`, and `lp` (`tdcpass --help` lists all options; `make doctor`, `make demo`, `make pipeline`, and `make test` wrap the common targets).

## Output

The demo writes quarterly accounting summaries, residual tables, local-projection tables, and manifests under `output/demo/`. Full pipeline runs follow the contract in `config/output_contract.yml` and schema in [docs/output_schema.md](docs/output_schema.md). Raw downloads, derived panels (`data/derived/`), caches, and generated bundles (`output/`) are local and ignored by Git.

Core quarterly panel columns:

| Description | Column |
|---|---|
| Quarterly measured TDC, banks | `tdc_bank_only_qoq` |
| Quarterly commercial-bank deposit change | `total_deposits_bank_qoq` |
| Quarterly residual deposit change | `other_component_qoq` |

## Limits

- Subtracting a measured TDC proxy from total deposit change leaves measurement error and scope differences inside the residual.
- Bank loan and securities tables cover only those partial balance-sheet counterparts and leave other bank balance-sheet items outside the comparison.
- CSV columns and command names such as `strict_*`, `shock`, and `pass_through` are interface identifiers for arithmetic and local-projection checks.
- Public statistical releases revise history, so a fresh online rebuild can differ from an earlier snapshot.

## Tests

```bash
pytest -q
bash scripts/hygiene_check.sh
```

`pytest -q` runs unit and synthetic-panel tests offline; `scripts/hygiene_check.sh` checks that local artifacts, credentials, and private paths are not tracked by Git.

## License

MIT. See [LICENSE](LICENSE).
