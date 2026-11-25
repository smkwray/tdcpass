# tdcpass

`tdcpass` compares quarterly measures of the Treasury Deposit Component (TDC), the part of the change in deposits at US banks and credit unions that comes from Treasury payments and trades in Treasury securities, against quarterly US bank deposit changes and balance-sheet counterparts. The TDC series come from [tdcest](https://github.com/smkwray/tdcest). `tdcpass` runs arithmetic accounting checks, comparisons across TDC definitions, and balance-sheet sensitivity tables, and writes reproducible CSV and JSON bundles under `output/`. A full run needs a local tdcest build; the synthetic demo runs offline without it.

[Reproducibility](REPRODUCIBILITY.md) | [Output schema](docs/output_schema.md)

## What it does

Given a quarterly measured TDC series and a quarterly commercial-bank deposit change, `tdcpass` computes the arithmetic difference:

```text
other deposit change = total bank deposit change - measured TDC
```

It then builds a quarterly comparison bundle covering:

- Exact-identity parity checks between `tdcest` TDC series and deposit totals across quarters.
- Comparisons across bank-only, broad-depository, and interest-adjusted TDC series, and series adjusted for Treasury purchases that money market funds pay for out of the Federal Reserve's overnight reverse repurchase (ON RRP) facility.
- Balance-sheet counterpart tables pairing TDC with partial bank loan and securities transactions and nonbank holder-asset comparisons.
- Residual and local-projection comparison scripts exposed for sensitivity checks (`decompose`, `shock`, `lp`).

## Data

- Quarterly TDC series from `tdcest`: set `TDCEST_ROOT` to a directory containing `tdcest` processed outputs (`data/processed/tdc_estimates.csv` and optional sensitivity panels). If `TDCEST_ROOT` is unset, `tdcpass` looks in `../tdcest/data/processed/`.
- Public series listed in `config/data_sources.yml` and `config/series_registry.yml`: Federal Reserve releases (Z.1 Financial Accounts of the United States, H.8 assets and liabilities of commercial banks, H.4.1 factors affecting reserve balances), Treasury Fiscal Data, and FRED (Federal Reserve Economic Data, from the St. Louis Fed). Export `FRED_API_KEY` to fetch through the FRED API; without it, downloads use FRED's public `fredgraph.csv` endpoint.

## Install

Requires Python 3.11 or later. From the repository root, create a virtual environment, copy the environment template (it sets `TDCEST_ROOT`, the optional `FRED_API_KEY`, and cache locations), load it into the shell, and install:

```bash
python3 -m venv $HOME/venvs/tdcpass
cp environment.example .env
set -a && source .env && set +a
make install
```

`make install` runs `pip install -e '.[dev]'` in `$HOME/venvs/tdcpass`; set `VENV=/path/to/env` to use another environment.

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
- Column and command names such as `strict_*`, `shock`, and `pass_through` are short labels for arithmetic and local-projection checks, not causal estimates.
- Public statistical releases revise history, so a fresh online rebuild can differ from an earlier snapshot.

## Tests

```bash
pytest -q
```

The tests run offline on unit fixtures and the synthetic panel.

## License

MIT. See [LICENSE](LICENSE).
