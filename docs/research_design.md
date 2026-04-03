# Research design

Current implementation posture:
This repository analyzes quarterly pass-through using canonical TDC imported from `tdcest`. The public bundle is a methods and reproducibility release built around that treatment object.

## 0. Source of truth

For the meaning of **TDC**, use these external source repositories as the reference:

- `tdcest` defines the canonical measured quarterly TDC series
- `tdcsim` defines the mechanism/accounting interpretation of TDC

This repo analyzes pass-through **of that TDC object**. It does not redefine TDC locally around a simpler stock-change proxy or treat it as a literal Treasury deposit stock.

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

### Headline treatment

The headline treatment is the canonical bank-only quarterly TDC series from `tdcest` or a locally reproduced equivalent.

### Shock construction

Use the **unexpected part** of canonical quarterly TDC changes:

1. Predict `tdc_bank_only_qoq` using only lagged information.
2. Use a **rolling 40-quarter window** with a ridge penalty on non-intercept terms (`ridge_alpha: 125.0`).
3. Store:
   - `tdc_fitted`,
   - `tdc_residual`,
   - `tdc_residual_z`,
   - model metadata.

The baseline shock is a rolling residual from:

- `lag_tdc_bank_only_qoq`
- `lag_fedfunds`
- `lag_unemployment`
- `lag_inflation`

That shock architecture is the current frozen headline design.

### Alternate shock objects

The repo also carries exploratory or sensitivity shock variants for:

- longer burn-in,
- no-bill-share residualization,
- broad-depository TDC,
- legacy total-deposit style predictor sets.

These are treatment-object and scope checks around the frozen rolling-macro baseline. The repo also tracks a shock-quality gate on usable-sample fit ratios, flagged-window share, and shock-to-target alignment. These variants are explicit stress tests around the frozen baseline, not co-equal headline designs.
The `bill_share`-linked variants remain in that exploratory bucket because they preserve the impact-stage sign pattern but change medium-horizon persistence enough to be read as stress tests rather than replacements for the frozen baseline.

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

For the approximate LP surfaces, use HAC/Newey-West standard errors.

For the exact identity-preserving baseline, report uncertainty from the shared nested circular block bootstrap that jointly re-estimates the headline shock and propagates uncertainty through the mechanically derived `other_component_qoq = total - tdc` object.

### Why LP first

- easier to audit,
- easier to explain,
- robust to moderate misspecification,
- consistent with the estimator style already used in related projects.

## 4. Regimes

Current live regime overlay:

- `reserve_drain_pressure`

Exploratory regime or timing inputs:

- `bill_share` remains an exploratory sensitivity input rather than a live regime export

Regime definitions should remain transparent and configurable, but they should not silently define the headline sample.

## 5. Sample policy

The exported quarterly panel should preserve a **headline sample** and allow shorter-history proxy or regime columns to remain missing within that sample.

Headline sample:

- treatment: canonical bank-only TDC from `tdcest` or an exact local reproduction
- headline outcomes: `total_deposits_bank_qoq`, `other_component_qoq`
- baseline shock controls: `lag_tdc_bank_only_qoq`, `fedfunds`, `unemployment`, and `inflation`, plus outcome-specific lag controls in the LP layer; `bill_share` remains exported but does not define the headline panel

Extended coverage:

- structural proxies such as `bank_credit_private_qoq`, `cb_nonts_qoq`, and `foreign_nonts_qoq`
- live regime overlay: `reserve_drain_pressure`
- exploratory sensitivity input: `bill_share`

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

The current public bundle keeps `period_sensitivity.csv` and `period_sensitivity_summary.json` on the preview surface because the medium-horizon pattern differs materially across the post-GFC early, pre-COVID, and COVID/post-COVID windows.

## 7. Things not to do first

- Do not start with the full giant mixed-frequency thesis stack.
- Do not start with bank-level micro panels.
- Do not start with black-box ML shocks.
- Do not claim precise mechanism from the residual alone.

## 8. Narrow public stopping point

For the current public stopping point, the results should satisfy:

- accounting coherence,
- no look-ahead leakage,
- a stable impact-stage exact-baseline sign pattern in the main sample,
- transparent sensitivity over treatment, controls, sample, and period windows,
- explicit caveats when persistence or mechanism attribution remain unsettled.

Beyond that threshold, additional mechanism work is scope expansion rather than a prerequisite for this release.
