# tdcpass

[Live site](https://smkwray.github.io/tdcpass/)

> In progress: this is a public methods-and-reproducibility release, and the design, estimates, and wording may still tighten.

> Important: the broad TDC estimate and the independently confirmed non-TDC evidence are not the same object. The broad TDC result is the main matched-deposit empirical result; the independently confirmed non-TDC side is narrower and currently loan-led. Residual accounting is used for alignment and diagnostics, not as independent channel evidence.

`tdcpass` studies one question:

**When the Treasury Contribution to Deposits (TDC) rises unexpectedly, do matched total bank deposits rise too, or does that increase mostly replace other deposit-creating channels?**

Current stopping point:

- the exact baseline shows positive impact effects on matched total bank deposits and negative effects on the non-TDC residual
- full TDC remains the broad Treasury-attributed measure, not a validated strict deposit component
- the repo now exposes a live `tdcest` comparison ladder with corrected broad-estimate variants, while keeping that ladder outside the independently confirmed core
- the upstream DU fiscal-flow first-pass branch is intentionally not imported here because it remains exploratory proxy/residual research rather than a comparison-grade public measure
- the independently confirmed core currently excludes Treasury other-checkable and rest-of-world deposit channels
- the pre-`2002Q4` historical backfill now uses a TT&L-aware Treasury cash term; the post-`2002Q4` transaction-era headline is unchanged
- the strongest independent evidence remains narrow and loan-led, with a nonfinancial-corporate bridge kept as comparison evidence rather than direct validation
- the strict empirical expansion branch is effectively complete under current public data
- trade flows, Fed MBS / QE incidence, and offshore/correspondent routing remain outside the independently confirmed core because same-scope domestic deposit settlement incidence is not identified tightly enough in public data
- the repo is now in writeup and results-packaging mode unless new same-scope transaction or incidence evidence appears

## Start here

If you only read three things, read these:

1. **Broad TDC result:** unexpected TDC is associated with higher matched total bank deposits on impact.
2. **Independent non-TDC evidence:** the independently confirmed side is narrower, loan-led, and does not currently support a complete independently measured non-TDC deposit total.
3. **Boundary rule:** Treasury other-checkable deposits, rest-of-world deposits, trade flows, Fed MBS/QE incidence, and offshore/correspondent routing remain outside the strict independently confirmed object under current public data.

## Status

Current posture:

- reproducible public-data pipeline
- canonical quarterly TDC imported from [`smkwray/tdcest`](https://github.com/smkwray/tdcest) outputs
- reusable diagnostics for identification and mechanism caveats
- committed `site/data/*` preview mirror; regenerated `output/*` stays local

The current bundle is a diagnostics-heavy methods and reproducibility release built on canonical [`tdcest`](https://github.com/smkwray/tdcest) TDC. The reusable core is the panel builder, outcome construction, local-projection stack, diagnostics, and site/export wiring around that measure.

Here, **TDC** means the canonically defined Treasury Contribution to Deposits from [`smkwray/tdcest`](https://github.com/smkwray/tdcest) / `tdcsim`. It is the project's measured Treasury-related deposit contribution, not a relabeling of literal Treasury deposit balances.

In the upstream accounting, the preferred headline estimate is the bank-only transaction-flow series. In words, it measures reserve-user net acquisition of marketable Treasuries, minus Treasury operating-cash drain, plus positive Fed remittances only. In this repo, that imported quarterly series is the default headline treatment.

The repo also imports a selective broad-estimate comparison ladder from sibling [`smkwray/tdcest`](https://github.com/smkwray/tdcest) processed outputs:

- `tdc_tier2_bank_only_qoq`
- `tdc_tier3_bank_only_qoq`
- `tdc_tier3_broad_depository_qoq`
- `tdc_bank_receipt_historical_overlay_qoq`
- `tdc_row_mrv_nondefault_pilot_qoq`

These rows are comparison or bounded-sensitivity measures only. They do not replace the frozen strict interpretation.

The repo intentionally does **not** import upstream DU fiscal-flow first-pass rows. In current upstream form, they combine total MTS cash totals with DU-side Treasury-security proxies and coupon proxies, with fallback residual logic where direct coverage is incomplete. That makes them useful research context upstream, but not strong enough for `tdcpass`'s public comparison layer.

### Why the canonical broad headline is retained

The corrected `tdcest` ladder is important, but it is not treated as a direct replacement for the canonical broad headline.

- Tier 2 and Tier 3 change the broad estimate materially, especially through interest and fiscal corrections.
- Those corrected variants are kept as broad-estimate comparison rows rather than promoted into the independently confirmed non-TDC evidence hierarchy.
- The sign difference between the canonical headline and the corrected ladder is therefore a broad-measurement issue, not a reason to widen the strict independent object.
- Nothing in the corrected ladder promotes Treasury other-checkable deposits, rest-of-world deposits, or other excluded support channels into the independently confirmed core.

For the historical extension before transaction coverage begins, the repo now uses a regime-aware TT&L-era cash-term refinement in the backfill path. That historical refinement is separate from the modern broad-versus-strict interpretation and does not alter the transaction-era headline after `2002Q4`.

## Broad vs strict

The repo now freezes a broad-versus-strict distinction.

### Broad TDC

Full TDC is kept as the **broad Treasury-attributed measure**.

That means:

- it is mechanically coherent
- it is useful as the project's headline Treasury-related estimate
- it is **not** currently claimed to be the strict deposit component

### Strict non-TDC deposits

The strict deposit component is the narrower target:

- a mechanical or near-mechanical component inside deposits
- something that should line up with independently measured deposit-creation channels

Under current evidence:

- the independently confirmed core excludes Treasury other-checkable and rest-of-world deposit channels
- the main direct evidence is a narrow loan-led core
- mortgage lending is the strongest impact-horizon subcomponent
- the nonfinancial-corporate bridge is comparison evidence only
- the broader private-borrower bridge remains diagnostic only

This is the key release discipline of the repo:

- **full TDC** and the **strict deposit component** are not treated as interchangeable labels

### Minimal Defensible Independent Non-TDC Read

If the question is the smallest independent non-TDC measure this repo can defend, the answer is narrower than the full residual.

- minimum direct benchmark: `strict_loan_core_min_qoq`
- impact-horizon subcomponent: `strict_loan_mortgages_qoq`
- standard narrow bridge comparison: `strict_loan_core_plus_nonfinancial_corporate_qoq`
- wider private-borrower and closure-style rows: diagnostic only

In plain language: the most defensible partial independent non-TDC read is loan-led and starts with mortgages plus consumer credit, not with a full independently measured non-TDC deposit total.

## What the repo does

The package rebuilds a quarterly public-data bundle that includes:

- canonical bank-only TDC imported from [`smkwray/tdcest`](https://github.com/smkwray/tdcest)
- selective `tdcest` broad-object ladder comparisons:
  - Tier 2 interest-corrected bank-only
  - Tier 3 fiscal-corrected bank-only
  - Tier 3 fiscal-corrected broad-depository
  - historical bank-receipt overlay
  - bounded MRV ROW pilot
- matched total-deposit outcomes
- the non-TDC residual, defined as matched total deposits minus the headline TDC series
- matched-scope comparison surfaces for no-ROW, U.S.-chartered-bank, and deposits-only sensitivities
- a strict source-side direct-count lane for non-TDC deposits, led by official Z.1 loan transactions and a separate non-Treasury securities add-on
- an imported accounting-reconstruction lane for the non-TDC residual, bundled locally from the EA-TDC accounting export
- a rolling unexpected-treatment shock plus exploratory sensitivity variants
- public period-sensitivity tables because medium-horizon persistence differs across major usable-sample windows
- local-projection response tables
- structural-proxy cross-checks and readiness diagnostics
- creator, escape, external, and funding counterpart-channel scorecards
- an accounting-reconstruction cross-check that lines up closely with the non-TDC residual after normalizing the imported EA bundle scale, but still remains a secondary closure-oriented read
- a separate strict source-side comparison surface that shows how much of the non-TDC residual is covered by direct identifiable bank asset transactions before any closure-oriented reconstruction
- follow-up diagnostic surfaces for scope variants, borrower-counterpart diagnostics, and funding-offset sensitivities
- manifests for raw downloads and optional local cache reuse

## What it does not do

- It does not require prebuilt local caches to run.
- It does not treat the residual alone as mechanism proof.
- It does not claim a complete additive decomposition of the non-TDC residual.
- It does not relabel the strict source-side lane as a full accounting closure of non-TDC deposits.
- It does not currently claim a clean pass-through-versus-crowd-out answer.
- It does not report headline pass-through or crowd-out ratios in the current release.
- It does not treat bill-share-linked shock variants as co-equal headline designs; they remain exploratory stress tests only.

## What the repo can currently say

- Unexpected TDC increases are associated with higher matched total bank deposits on impact in the exact baseline.
- The increase is less than one-for-one because the non-TDC component falls.
- The strict source-side lane is a defensible independent count of directly identifiable non-Treasury bank asset support, but it is intentionally narrow and does not validate full TDC as the strict deposit component.
- The repo now distinguishes the upstream no-ROW sensitivity from a true local U.S.-chartered bank-leg-matched treatment, so scope alignment is explicit rather than implicit.
- Treasury other-checkable deposits look real in Treasury cash and reserve plumbing terms, but they are not validated as a strict in-scope deposit component.
- Rest-of-world deposits look even weaker as a strict in-scope deposit component and remain outside the strict object under current evidence.
- Trade flows and Fed MBS / QE incidence remain theoretically relevant but non-promotable under current public data because settlement incidence is not same-scope enough for the strict object.
- The `tdcest` corrected ladder is now visible as a broad-estimate comparison layer, but it does not change the strict evidence hierarchy.
- The final creator-channel search and the final DI loans n.e.c. audit do not support any new promoted strict component under current public data.
- The strict empirical branch is effectively complete unless genuinely new same-scope transaction or incidence evidence appears.

## Current Results Frame

The current release should be read in this order:

1. Full TDC is the project's broad Treasury-attributed measure.
2. The strict independent non-TDC deposits question is narrower and is not assumed to equal full TDC.
3. Treasury other-checkable and rest-of-world deposit channels remain outside the strict object because current incidence evidence does not validate them as clean in-scope deposit components.
4. The strongest current strict evidence is narrow and loan-led, with mortgage lending strongest on impact and a nonfinancial-corporate bridge kept as comparison evidence only.
5. Closure-style accounting remains descriptive only and is not independent verification.
6. Historical TT&L-era handling is a separate backfill issue: the repo now refines the pre-`2002Q4` cash term, but that does not change the modern strict-component result.
7. The `tdcest` corrected ladder is now part of the broad-estimate read only: at the latest common quarter (`2025Q4`), headline bank-only is about `68.996`, Tier 2 is about `-49.49`, Tier 3 is about `-51.57`, and Tier 3 broad-depository is about `-51.35`.

The best concise summary is: the repo can defend full TDC as a broad Treasury Contribution to Deposits object, but it cannot yet defend full TDC as the strict deposit component of deposits.

## Environment

The repo uses an external virtualenv and external cache directories. It does not use a repo-local `.venv` or repo-local test/cache directories.

For a precise public reproducibility guide, see [REPRODUCIBILITY.md](REPRODUCIBILITY.md).

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

The repo does commit the public `site/data/*` preview mirror that powers the public release.

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
