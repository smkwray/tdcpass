# TDCpass — Treasury Deposit Component comparisons

TDCpass builds quarterly comparisons between Treasury Deposit Component (TDC)
proxies and named deposit outcomes. It checks arithmetic parity, measurement
variants, and perimeter boundaries, and exports a reproducible comparison bundle.
It is not an independent estimator or causal evidence chain.

[TDCest](https://github.com/smkwray/tdcest) owns the quarterly proxy and its
measurement ladder. [EA-TDC](https://github.com/smkwray/ea-tdc) owns conditional
deposit projections and any admitted cumulative response ratio. TDCpass retains
projection and diagnostic machinery to reproduce comparisons; that machinery does
not confer independent empirical authority on the results.

## Install and run

Use Python 3.11 or newer from a source checkout. The example configuration keeps
the environment and caches outside the checkout:

```bash
cp environment.example .env
set -a
source .env
set +a
python3 -m venv "$UV_PROJECT_ENVIRONMENT"
"$UV_PROJECT_ENVIRONMENT/bin/pip" install -e '.[dev]'
```

Run the lightweight checks and synthetic example:

```bash
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass demo
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
bash scripts/hygiene_check.sh
```

`doctor` checks the public configuration and schema files. `demo` writes synthetic
data and results to ignored local directories, including
`output/demo/overview.json`.
The tests use fixtures and do not establish that a fresh live-data rebuild passes.

## What is compared

| Object | Role and boundary |
|---|---|
| `tdc_bank_only_qoq` | Imported upstream Treasury-attributed flow proxy |
| `total_deposits_bank_qoq` | Separately defined deposit outcome |
| `other_component_qoq` | Arithmetic difference between that outcome and proxy |
| Upstream measurement variants | Interest, fiscal, scope, and historical measurement comparisons |
| Loan and securities series | Partial counterpart comparisons, not a complete independent deposit component |
| Narrow holder-asset outcome | Unmatched legal-sector sensitivity for households/nonprofits and nonfinancial businesses |

**DU** is the covered domestic nonbank deposit-claim perimeter. **RS** is its
complement in the settlement system. Classification follows the claim debited or
credited at settlement, not the holder's legal sector. Producer fields containing
`ru_flow` retain their existing artifact names.

For exact Treasury-attributed deposit-settlement flows on the same clock and
perimeter, $\Delta D^P = F^{T,P} + N^P$. Subtracting a measured proxy from an
outcome is an arithmetic comparison; it does not by itself recover the exact
non-Treasury component $N^P$. Each comparison must retain its source-flow
perimeter, deposit-outcome perimeter, clock, sample, units, and vintage.

A conditional projection coefficient is separate from that accounting identity.
It is not transaction-time incidence, a materialization ratio, a retention share,
or a deposit multiplier. Direct projections of future deposit changes are not
cumulative deposit-stock responses. Ratio reporting remains gated by denominator
strength and appropriate joint inference.

The holder-asset sensitivity combines Federal Reserve Financial Accounts (Z.1)
checkable-deposit-and-currency and time-and-savings-deposit assets. Currency,
issuer coverage, timing, and perimeter differences prevent treating it as the DU
deposit outcome. The broader domestic-financial candidate includes explicit RS
positions and is not admitted as a domestic nonbank deposit measure.

## Rebuild a quarterly bundle

A live rebuild downloads public source data and consumes the canonical TDCest
outputs. Set `TDCEST_ROOT` in the ignored `.env` to the source or artifact root
containing `data/processed/tdc_estimates.csv`. Supplemental comparison files are
listed in [REPRODUCIBILITY.md](REPRODUCIBILITY.md). Set `FRED_API_KEY` when using the Federal Reserve Economic Data
(FRED) API. See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for input and output
boundaries.

Use a separate ignored run root for each rebuild:

```bash
set -a
source .env
set +a
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run \
  --root output/rebuild --reuse-mode rebuild
```

This is a full data build, not a smoke test. Public sources can revise their
history or remove series; a current download need not reproduce an earlier run.
The pipeline records manifests and readiness diagnostics. Missing coverage and
failed identification checks remain limitations even if the command completes.

## Commands and contracts

| Command | Purpose |
|---|---|
| `tdcpass doctor` | Check checkout configuration and key availability |
| `tdcpass demo` | Exercise the pipeline with synthetic data |
| `tdcpass pipeline run` | Build or materialize the quarterly bundle |
| `tdcpass pipeline closeout --root PATH` | Inspect validation and readiness from an existing run |
| `tdcpass discover-cache` | Inspect optional reusable source artifacts |

Use `--help` on a command for its options. The machine-readable output contract is
[config/output_contract.yml](config/output_contract.yml), with column and artifact
descriptions in [docs/output_schema.md](docs/output_schema.md). Historical field
names such as `strict_*`, `shock`, and `pass_through` identify stored interfaces;
they do not establish causal or independent-component claims.

## Outputs

Raw downloads, caches, derived panels, demo files, and comparison bundles are local
and ignored. Each run records its producer and input provenance. Inspect the
coverage and readiness summaries alongside the numerical tables; incomplete
corroboration and outcome coverage prevent stronger identification and ratio
claims.

## Interpretation limits

- Arithmetic closure and residual agreement are descriptive checks, not
  independent mechanism evidence.
- Loan-led comparisons cover only part of the possible deposit counterparts.
  They do not validate a complete independently measured non-TDC deposit total.
- Treasury other-checkable deposits, foreign deposits, trade flows, and Federal
  Reserve asset-purchase routing require same-perimeter settlement evidence
  before they can support stronger deposit-component claims.
- Measurement variants and time windows can change the comparison. Failed gates
  and null results must remain visible.
- A state-dependent association does not identify absorption behavior or select
  a coefficient for scenario use.

## License

[MIT](LICENSE)
