# Research design

## 1. Main estimands

### 1.1 Pass-through

For each horizon `h`, estimate the cumulative response of matched total deposit growth to an unexpected TDC shock:

\[
PT_h = \frac{\partial \sum_{j=0}^{h} \Delta D^{total}_{t+j}}{\partial Shock^{TDC}_t}
\]

### 1.2 Crowd-out

Estimate the cumulative response of the non-TDC component:

\[
CO_h = \frac{\partial \sum_{j=0}^{h} \Delta D^{other}_{t+j}}{\partial Shock^{TDC}_t}
\]

with

\[
\Delta D^{other}_t = \Delta D^{total}_t - \Delta D^{TDC}_t
\]

### 1.3 Structural channel checks

Also estimate responses for:

- bank private-credit / non-Treasury acquisition proxies,
- central-bank non-Treasury proxies,
- foreign non-Treasury proxies.

These are not just extra charts; they are a guardrail against overinterpreting the residual.

## 2. Treatment construction

### Frozen baseline shock

Use the **unexpected part** of quarterly TDC changes:

1. Predict `tdc_bank_only_qoq` using only lagged information.
2. Use an **expanding window**.
3. Store:
   - `tdc_fitted`,
   - `tdc_residual`,
   - `tdc_residual_z`,
   - model metadata.

The current baseline shock in code is the expanding-window residual from:

- `lag_tdc_bank_only_qoq`
- `lag_bill_share`
- `lag_fedfunds`
- `lag_unemployment`
- `lag_inflation`

That means `bill_share` is not only a regime overlay in the current repo state. It is also part of the baseline shock definition through `lag_bill_share`.

### Alternate shock objects

The repo also carries exploratory or sensitivity shock variants for:

- longer burn-in,
- no-bill-share residualization,
- broad-depository TDC,
- legacy total-deposit style predictor sets.

These are robustness or scope checks, not replacements for the frozen headline shock.

## 3. Estimator

### Main estimator: local projections

For each horizon `h`:

\[
Y_{t,h} = \alpha_h + \beta_h Shock^{TDC}_t + \Gamma_h X_{t-1} + u_{t,h}
\]

where:

- `Y_{t,h}` is cumulative future growth of an outcome from `t` through `t+h`,
- `Shock^{TDC}_t` is the unexpected TDC innovation,
- `X_{t-1}` are pre-treatment controls.

Use HAC/Newey-West standard errors.

### Why LP first

- easier to audit,
- easier to explain,
- robust to moderate misspecification,
- consistent with the sibling-repo style already used in related projects.

## 4. Regimes

Recommended first regime splits:

- high vs low bank absorption,
- bill-heavy vs coupon-heavy issuance mix,
- tighter vs looser balance-sheet/SLR proxy regimes.

Keep regime definitions transparent and configurable, but do not let regime availability silently define the headline sample.

## 5. Sample policy

The exported quarterly panel should preserve a **headline sample** and allow shorter-history proxy or regime columns to remain missing within that sample.

Headline sample:

- treatment: `tdc_bank_only_qoq`
- headline outcomes: `total_deposits_bank_qoq`, `other_component_qoq`
- baseline shock controls: `bill_share`, `fedfunds`, `unemployment`, `inflation`, plus their required lags

Extended coverage:

- structural proxies such as `bank_credit_private_qoq`, `cb_nonts_qoq`, and `foreign_nonts_qoq`
- regime overlays such as `bank_absorption_share`, `reserve_drain_pressure`, and `slr_tight`

These extended columns can have shorter usable history, but they should not silently truncate the baseline deposit-response sample.

## 6. Sensitivity ladder

At minimum compare:

- bank-only baseline TDC,
- alternate TDC estimate,
- broad-depository variant.

At minimum also compare outcomes:

- total bank deposits,
- non-TDC component,
- deposit-inclusive money robustness.

## 7. Things not to do first

- Do not start with the full giant mixed-frequency thesis stack.
- Do not start with bank-level micro panels.
- Do not start with black-box ML shocks.
- Do not claim precise mechanism from the residual alone.

## 8. Publication-grade evidence path

To reach a publishable version, the results should satisfy:

- accounting coherence,
- no look-ahead leakage,
- stable sign in the main sample,
- structural cross-check support,
- sensitivity stability across TDC versions.
