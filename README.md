# tdcpass

[Live site](https://smkwray.github.io/tdcpass/)

> In progress: this is a public methods-and-reproducibility release, and the design, estimates, and wording may still tighten.

`tdcpass` studies one question:

**When the Treasury-attributed component of deposits (TDC) rises unexpectedly, do matched total bank deposits rise too, or does that increase mostly replace other deposit-creating channels?**

Current stopping point:

- the exact baseline shows positive impact effects on matched total bank deposits and negative effects on the non-TDC residual
- medium-horizon persistence and mechanism attribution remain unsettled
- the current public counterpart preview does not show a decisive positive core creator-lending offset on impact
- foreign nontransaction pressure is the clearest currently materialized external counterpart signal
- the current robustness surface is intentionally narrow: exact-baseline treatment, control, and sample sensitivity plus period sensitivity

## Status

Current posture:

- reproducible public-data pipeline
- canonical quarterly TDC imported from `tdcest` outputs
- reusable diagnostics for identification and mechanism caveats
- committed `site/data/*` preview mirror; regenerated `output/*` stays local

The current bundle is a diagnostics-heavy methods and reproducibility release built on canonical `tdcest` TDC. The reusable core is the panel builder, outcome construction, LP stack, diagnostics, and site/export wiring around that treatment object.

Here, **TDC** means the canonically defined Treasury-attributed component of deposits from `tdcest` / `tdcsim`. It is the project’s specific treatment object, not a repo-local relabeling of literal Treasury deposit balances.

## What the repo does

The package rebuilds a quarterly public-data bundle that includes:

- canonical bank-only TDC imported from `tdcest`
- matched total-deposit outcomes
- the non-TDC residual `other_component_qoq = total_deposits_bank_qoq - tdc_bank_only_qoq`
- a rolling unexpected-treatment shock plus exploratory sensitivity variants
- public period-sensitivity tables because medium-horizon persistence differs across major usable-sample windows
- local-projection response tables
- structural-proxy cross-checks and readiness diagnostics
- creator, escape, external, and funding counterpart-channel scorecards
- manifests for raw downloads and optional local cache reuse

## What it does not do

- It does not require prebuilt local caches to run.
- It does not treat the residual alone as mechanism proof.
- It does not claim a complete additive decomposition of the non-TDC residual.
- It does not currently claim a clean pass-through-versus-crowd-out answer.
- It does not report headline pass-through or crowd-out ratios in the current release; that lane remains out of scope until the repo has a dimensionally coherent first-stage gate for raw-unit treatment responses.
- It does not treat `bill_share`-linked shock variants as co-equal headline designs; they remain exploratory stress tests only.

## What the repo can currently say

- Unexpected TDC increases are associated with higher matched total bank deposits on impact in the exact baseline.
- The increase is less than one-for-one because the non-TDC component falls.
- The current public counterpart preview does not provide a decisive positive core creator-lending offset on impact.
- Foreign nontransaction pressure is the clearest currently materialized external counterpart signal in the committed preview mirror.
- Medium-horizon persistence and mechanism attribution remain unsettled in the current release.
- The current robustness surface is intentionally narrow: exact-baseline treatment, control, and sample sensitivity plus period sensitivity.

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
- `output/*` analysis working files

The repo does commit the contract-backed `site/data/*` preview mirror that powers the public release.

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
