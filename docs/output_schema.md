# Output schema

Canonical exported names follow the registry contract.
Short aliases such as `tdc_qoq` and `total_deposits_qoq` are optional compatibility aliases only and are not the frozen external artifact contract.
The live quarterly bundle is produced by `tdcpass pipeline run` and writes the bank-only contract to `data/derived/quarterly_panel.csv`, `output/*`, and mirrored `site/data/*` artifacts.
For the public release, `site/data/*` is the committed preview mirror and `output/*` remains regenerated local analysis output.

## Required files

### `data/derived/quarterly_panel.csv`

This is the canonical quarterly bank-only panel. The frozen names are the bank-only series and their residual-based treatment inputs:

- `tdc_bank_only_qoq`
- `total_deposits_bank_qoq`
- `other_component_qoq`
- `accounting_deposit_substitution_qoq`
- `accounting_bank_balance_sheet_qoq`
- `accounting_public_liquidity_qoq`
- `accounting_external_flow_qoq`
- `accounting_identity_total_qoq`
- `accounting_identity_gap_qoq`
- `strict_loan_source_qoq`
- `strict_non_treasury_securities_qoq`
- `strict_identifiable_total_qoq`
- `strict_identifiable_gap_qoq`
- `strict_loan_core_plus_nonfinancial_corporate_qoq`
- `tdc_residual_z`

Minimum columns:

- `quarter`
- `tdc_bank_only_qoq`
- `tdc_us_chartered_bank_only_qoq`
- `tdc_no_foreign_bank_sectors_qoq`
- `total_deposits_bank_qoq`
- `deposits_only_bank_qoq`
- `broad_bank_deposits_qoq`
- `other_component_qoq`
- `other_component_no_foreign_bank_sectors_qoq`
- `broad_bank_other_component_qoq`
- `tdc_domestic_bank_only_qoq`
- `tdc_no_remit_bank_only_qoq`
- `tdc_credit_union_sensitive_qoq`
- `bank_credit_private_qoq`
- `cb_nonts_qoq`
- `foreign_nonts_qoq`
- `domestic_nonfinancial_mmf_reallocation_qoq`
- `domestic_nonfinancial_repo_reallocation_qoq`
- `bill_share`
- `reserve_drain_pressure`
- `quarter_index`
- `tga_qoq`
- `reserves_qoq`
- `fedfunds`
- `unemployment`
- `inflation`
- `lag_tdc_bank_only_qoq`
- `lag_tdc_us_chartered_bank_only_qoq`
- `lag_tdc_no_foreign_bank_sectors_qoq`
- `lag_total_deposits_bank_qoq`
- `lag_deposits_only_bank_qoq`
- `lag_broad_bank_deposits_qoq`
- `lag_bank_credit_private_qoq`
- `lag_tga_qoq`
- `lag_reserves_qoq`
- `lag_bill_share`
- `lag_fedfunds`
- `lag_unemployment`
- `lag_inflation`

`bill_share` is built from quarterly Treasury auction offering amounts in FiscalData using issue dates. It remains available for exploratory shock and control sensitivities, but it is no longer part of the active live regime layer.
`reserve_drain_pressure` is a transparent transformation equal to `-lag_reserves_qoq`; it is the current headline liquidity-pressure regime split.
`quarter_index` is a monotonic quarter-order index used only for exploratory trend-stress LP diagnostics; it is not part of the headline sample gate.
`bank_credit_private_qoq` remains part of the exported panel as a structural proxy, but it can have a shorter public history than the headline sample and should not define the max-common quarterly sample used for the headline panel or shock path.

### `output/accounting/accounting_summary.csv`

Minimum columns:

- `metric`
- `value`
- `notes`

### `output/accounting/quarters_tdc_exceeds_total.csv`

Minimum columns:

- `quarter`
- `tdc_bank_only_qoq`
- `total_deposits_bank_qoq`
- `other_component_qoq`

### `output/shocks/unexpected_tdc.csv`

This file stores the strictly out-of-sample unexpected-TDC shock bundle.

Minimum columns:

- `quarter`
- `tdc_bank_only_qoq`
- `tdc_fitted`
- `tdc_residual`
- `tdc_residual_z`
- `model_name`
- `train_start_obs`
- `train_condition_number`
- `train_target_sd`
- `train_resid_sd`
- `fitted_to_target_scale_ratio`
- `shock_flag`

The shock export now carries numerical-quality diagnostics for each usable out-of-sample fit. `shock_flag` marks windows where the fitted path is badly scaled relative to the target or where the training design is numerically weak.

### `output/models/lp_irf.csv`

Minimum columns:

- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

Operational metadata now travels with the LP exports as additional columns:

- `shock_column`
- `shock_scale`
- `response_type`

This file remains the broad LP output and the approximate dynamic decomposition path because the baseline spec can still append outcome-specific lagged dependent variables.

### `output/models/lp_irf_identity_baseline.csv`

Minimum columns:

- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`
- `decomposition_mode`
- `outcome_construction`
- `inference_method`

This is the primary exact decomposition artifact for the public research sprint. It uses a common sample, common horizon transform, and common regressor matrix for `tdc_bank_only_qoq` and `total_deposits_bank_qoq`, then derives `other_component_qoq = total - tdc` mechanically at each horizon. The exported `other_component_qoq` uncertainty comes from the shared bootstrap path used to estimate the two underlying equations.

### `output/models/lp_irf_accounting_identity.csv`

Minimum columns:

- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

This file is the baseline LP slice for the imported accounting-reconstruction lane. It keeps the proxy scorecard separate from the accounting-reconstruction check by exporting only the non-TDC residual, imported accounting components, reconstructed accounting total, and the accounting closure gap.

### `output/models/lp_irf_strict_identifiable.csv`

Minimum columns:

- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

This file is the baseline LP slice for the strict source-side lane. It exports the non-TDC residual together with the direct loan-source core, the direct non-Treasury securities add-on, the combined strict identifiable total, and the remaining strict gap. It is intentionally gross and incomplete.

### `output/models/strict_corporate_bridge_secondary_comparison_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_definitions`
- `recommendation`
- `key_horizons`
- `takeaways`

This file compares the two live secondary strict candidates directly under the split-treatment framework:

- `strict_loan_core_plus_private_borrower_qoq`
- `strict_loan_core_plus_nonfinancial_corporate_qoq`

It keeps `strict_loan_core_min_qoq` fixed as the headline direct core and asks which secondary comparison is actually closer to the core-deposit-proximate residual once the small private offset block is separated out.

### `output/models/strict_component_framework_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `frozen_roles`
- `h0_snapshot`
- `classification`
- `recommendation`
- `takeaways`

This file freezes the current release-facing strict interpretation. It should make explicit which objects are evidence versus framing only: closure-oriented accounting is non-evidence for independent verification, the current full TDC remains the canonical broad Treasury Contribution to Deposits object, `TOC + ROW` are treated as a real measured support bundle with unresolved strict deposit incidence, and the strict direct-count lane is organized around a headline direct core plus bounded bridge comparisons.

### `output/models/strict_release_framing_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `release_position`
- `evidence_tiers`
- `classification`
- `h0_snapshot`
- `recommendation`
- `takeaways`

This file freezes the final release-facing rule after the TOC/ROW incidence gates. It should say explicitly that full TDC remains the broad Treasury Contribution to Deposits object, while the strict independent non-TDC object excludes both TOC and ROW under current evidence. It should also carry the reopening rule: only new scope- and timing-matched liability-incidence evidence can justify bringing either leg back into a strict direct-count candidate.

### `output/models/strict_direct_core_component_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_definitions`
- `key_horizons`
- `classification`
- `recommendation`
- `takeaways`

This file decomposes the current headline strict direct core into its two direct subcomponents, mortgages and consumer credit. It should answer whether the current bundled core is still the best strict direct benchmark or whether one of the two subcomponents is carrying nearly all of the direct-core evidence under the current non-TOC/ROW framework.

### `output/models/strict_direct_core_horizon_stability_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `horizon_winners`
- `classification`
- `recommendation`
- `takeaways`

This file turns the direct-core split into a horizon rule. It should answer whether the impact-horizon winner is stable through h4 and h8, or whether the repo should keep the bundled direct core as the multihorizon benchmark while surfacing a narrower impact-only candidate.

### `output/models/strict_additional_creator_candidate_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_groups`
- `key_horizons`
- `classification`
- `recommendation`
- `takeaways`

This file tests the next empirical question after the direct-core freeze: whether any remaining creator channels beyond the current direct core deserve strict-candidate status. It should separate broad all-bank corroborating proxies from genuine extension candidates and make clear when the best remaining channels only corroborate existing mortgage/consumer evidence rather than add a new strict component.

### `output/models/strict_di_loans_nec_measurement_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_groups`
- `classification`
- `recommendation`
- `key_horizons`
- `takeaways`

This file is the final targeted audit of the unresolved `strict_loan_di_loans_nec_qoq` bucket. It should answer whether current public data isolate any clean same-scope transaction-based subcomponent that deserves promotion into the strict direct-count object, while separating three tiers explicitly:

- same-scope transaction subcomponents
- cross-scope transaction bridges
- same-scope proxy slices

The intended release read is conservative. If the best same-scope evidence is still proxy-only and the best transaction mapping is still cross-scope, then the correct recommendation is to keep the DI bucket outside the strict object and stop empirical expansion unless a new public transaction split appears.

### `output/models/strict_results_closeout_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `release_position`
- `settled_findings`
- `evidence_tiers`
- `unresolved_questions`
- `classification`
- `recommendation`
- `takeaways`

This file is the release-ready closeout surface for the strict branch. It should consolidate the frozen broad-vs-strict framework, the final creator-channel search, and the final `DI loans n.e.c.` audit into one explicit answer about whether empirical expansion is still justified. The intended read is operational: what is now settled, what remains unresolved, and whether the repo should move to writeup and results packaging under current evidence.

### `output/models/tdcest_ladder_integration_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `classification`
- `series_roles`
- `recommendation`
- `takeaways`

This file documents the selective `tdcest` downstream integration decision. It should make clear that `tdcpass` already uses the canonical broad `tdcest` headline while newly imported Tier 2 / Tier 3 rows, the historical bank-receipt overlay, and the bounded MRV ROW pilot are comparison or bounded-sensitivity objects only and do not replace the frozen strict taxonomy.

### `output/models/tdcest_broad_object_comparison_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `latest_common_broad_comparison`
- `supplemental_surfaces`
- `classification`
- `recommendation`
- `takeaways`

This file is the release-facing broad-object comparison layer for the newly integrated `tdcest` ladder. It should summarize the latest common-quarter comparison between the canonical bank-only headline and the Tier 2 / Tier 3 corrected rows, while also surfacing the current status of the historical bank-receipt overlay and bounded MRV ROW pilot without promoting any of them into the strict direct-count object.

### `output/models/tdcest_broad_treatment_sensitivity_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `classification`
- `key_horizons`
- `recommendation`
- `takeaways`

This file quantifies whether the corrected `tdcest` broad-treatment variants materially change the broad deposit and residual LP results. It belongs on the broad-object side only: it compares the canonical bank-only headline shock with Tier 2 / Tier 3 corrected shocks and reports whether the sign-level and magnitude-level broad conclusions move materially.

### `output/models/treasury_cash_regime_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `definitions`
- `regime_windows`
- `full_sample`
- `classification`
- `recommendation`
- `takeaways`

This file audits the historical Treasury cash regime directly. It compares the current Treasury-operating-cash term against `TGA` alone versus a broader federal-cash-balance proxy that includes bank-side federal-government balances relevant to the TT&L era.

### `output/models/historical_cash_term_reestimation_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `definitions`
- `windows`
- `comparison`
- `top_adjustment_quarters`
- `classification`
- `recommendation`
- `takeaways`

This file re-estimates the historical `1990Q1` to `2002Q3` TDC backfill under an explicit TT&L-era Treasury cash-balance proxy and compares it with the current historical extension. It is a historical-only stress test; it does not alter the modern transaction-era headline.

### `output/models/toc_row_incidence_audit_summary.json`

This file is the first-pass TOC/ROW liability-incidence audit. It should answer a narrower question than the earlier treatment audit: not whether TOC or ROW are real, but whether each leg looks like it lands in the in-scope bank-deposit aggregate strongly enough to count toward a strict direct-count candidate. The intended interpretation is asymmetrical: TOC can look like real Treasury cash plumbing while still failing strict deposit incidence, and ROW can look economically real while mapping more to broader external support than to in-scope bank deposits.

### `output/models/toc_row_liability_incidence_raw_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `leg_definitions`
- `quarterly_alignment`
- `key_horizons`
- `classification`
- `recommendation`
- `takeaways`

This file is the raw-units TOC/ROW liability-incidence decision gate. It should tighten the earlier first-pass audit by using same-quarter and next-quarter raw mappings plus h0/h1 shock overlays to ask whether each measured leg lands in the in-scope bank-deposit aggregate strongly enough to be reincorporated into a strict direct-count candidate.

### `output/models/toc_validated_share_candidate_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_definitions`
- `quarterly_gate`
- `key_horizons`
- `classification`
- `recommendation`
- `takeaways`

This file is the post-gate TOC decision surface. It should combine the raw-incidence evidence with the headline direct-core benchmark and answer the narrower question: even if TOC is real, does any narrow TOC share improve the strict comparison enough to belong back inside the strict direct-count object?

### `output/models/accounting_identity_alignment.csv`

Minimum columns:

- `horizon`
- `residual_beta`
- `accounting_total_beta`
- `identity_gap_beta`
- `arithmetic_residual_minus_total_beta`
- `identity_gap_share_of_residual`
- `residual_n`
- `accounting_total_n`
- `identity_gap_n`
- `interpretation`

This file summarizes whether the imported accounting reconstruction closes most of the non-TDC response at the key LP horizons. `identity_gap_share_of_residual` is a descriptive closure diagnostic rather than a causal estimand.

### `output/models/accounting_identity_summary.json`

Required keys:

- `status`
- `source_kind`
- `headline_question`
- `estimation_path`
- `component_outcomes_present`
- `horizons`
- `takeaways`

This file explains whether the imported accounting lane is available in the current run and whether it looks like a tight closure, partial closure, or large-gap reconstruction at headline horizons.

### `output/models/strict_identifiable_alignment.csv`

Minimum columns:

- `horizon`
- `residual_beta`
- `strict_loan_source_beta`
- `strict_non_treasury_securities_beta`
- `strict_identifiable_total_beta`
- `strict_identifiable_gap_beta`
- `arithmetic_residual_minus_total_beta`
- `strict_gap_share_of_residual`
- `residual_n`
- `strict_total_n`
- `strict_gap_n`
- `interpretation`

This file summarizes how much of the non-TDC residual is covered by the strict source-side lane at headline LP horizons. A large strict gap is informative by design and should not be mechanically closed with a residual plug.

### `output/models/strict_funding_offset_alignment.csv`

Minimum columns:

- `horizon`
- `strict_identifiable_total_beta`
- `strict_funding_offset_total_beta`
- `strict_funding_offset_share_of_identifiable_total_beta`
- `strict_identifiable_net_after_funding_beta`
- `strict_gap_after_funding_beta`
- `identifiable_total_n`
- `funding_total_n`
- `net_after_funding_n`
- `gap_after_funding_n`
- `interpretation`

This file isolates the transaction-based funding-offset sensitivity so it can be compared directly against the gross strict identifiable lane without digging through the broader follow-up summary.

### `output/models/strict_identifiable_summary.json`

Required keys:

- `status`
- `source_kind`
- `headline_question`
- `estimation_path`
- `component_outcomes_present`
- `horizons`
- `takeaways`

This file describes availability and interpretation for the strict source-side lane. The strict lane is meant to be an independently measured direct-count surface, not a closure-oriented accounting decomposition.

### `output/models/strict_identifiable_followup_summary.json`

Required keys:

- `status`
- `strict_source_kind`
- `headline_question`
- `estimation_path`
- `measurement_variant_comparison`
- `scope_check_gap_assessment`
- `strict_component_diagnostics`
- `takeaways`

This file is the first follow-up diagnostic surface for the strict lane. It combines the exact identity baseline, the measurement-family treatment ladder, and the strict source-side LP slice so the repo can compare baseline bank-only TDC against domestic-bank-only and other measurement variants while also showing how much of the strict loan lane is concentrated in the broad `strict_loan_di_loans_nec_qoq` bucket.
It also carries systemwide borrower-counterpart diagnostics for the broad DI-loans-n.e.c. bucket plus a separate transaction-based funding-offset sensitivity block, both as secondary follow-up reads rather than headline strict measures. The borrower block is intentionally cross-scope: the borrower rows are systemwide F.215 liability counterparts, while `strict_loan_di_loans_nec_qoq` is the U.S.-chartered lender asset row.
When available, `recommended_measurement_comparison` should make the current policy explicit: keep the headline outcome on `total_deposits_bank_qoq` for now, standardize `us_chartered_bank_only` as the main scope-check comparison, and keep `domestic_bank_only` as the no-ROW secondary sensitivity.
When available, `scope_check_gap_assessment` should state directly how much of the current strict gap would remain if the matched-bank-leg residual shift were applied while holding the baseline direct-count strict total fixed. This is descriptive only; it isolates residual-side scope relief rather than pretending the strict lane has been re-estimated under each treatment variant.

### `output/models/scope_alignment_summary.json`

Required keys:

- `status`
- `headline_question`
- `variant_definitions`
- `deposit_concepts`
- `takeaways`

This file is the scope-alignment companion for the strict lane. It compares the current headline TDC object against the upstream no-ROW `domestic_bank_only` sensitivity and the new local U.S.-chartered bank-leg-matched treatment, and it does so for both the current total-deposits outcome and a deposits-only outcome that strips out interbank-transactions liabilities.
When available, `recommended_policy` should state the current release policy on whether to keep `total_deposits_bank_qoq` as the headline outcome, which scope-check variant to use as the standard comparison, and which alternatives remain secondary sensitivities.

### `output/models/broad_scope_system_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `usc_matched_context`
- `broad_matched_system`
- `tdc_component_audit`
- `takeaways`

This file is the first matched-scope broad-bank system surface. It keeps the broad headline TDC object, pairs it with a broad-bank deposit outcome built from U.S.-chartered banks plus foreign banking offices and banks in U.S.-affiliated areas, and compares that broad residual against a broad loans-only strict core.
It also carries a treatment-side component audit that separates no-ROW moves from “remove foreign bank-sector Treasury legs but keep ROW” moves, so the repo can distinguish rest-of-world sensitivity from foreign-bank-sector sensitivity directly.

### `output/models/treasury_operating_cash_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `quarterly_alignment`
- `key_horizons`
- `takeaways`

This file is the dedicated Treasury-operating-cash plumbing audit. It checks whether the Treasury-operating-cash leg moves like genuine TGA plumbing quarter by quarter and under the baseline shock, rather than behaving like a simple sign, timing, or object-definition bug.
`quarterly_alignment` should summarize the quarter-level relationship between `tdc_treasury_operating_cash_qoq` and `tga_qoq`, including their contemporaneous correlation, an OLS slope, lead/lag correlations, and the worst mismatch quarters.
`key_horizons` should report the direct LP responses of the Treasury-operating-cash leg, TGA, reserves, and `cb_nonts_qoq`, along with an interpretation label such as `treasury_cash_release_pattern`.

### `output/models/rest_of_world_treasury_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `quarterly_alignment`
- `key_horizons`
- `takeaways`

This file is the dedicated audit for the rest-of-world Treasury leg. It checks whether the ROW Treasury component behaves like a clean same-quarter deposit counterpart or whether it looks more like a broader external-support channel.
`quarterly_alignment` should summarize how `tdc_row_treasury_transactions_qoq` lines up with external-support counterparts such as `foreign_nonts_qoq`, `checkable_rest_of_world_bank_qoq`, and the foreign-bank interbank/deposit asset series.
`key_horizons` should report the direct LP responses of the ROW Treasury leg and those counterpart variables, along with an interpretation label such as `external_asset_support_pattern`.

### `output/models/toc_row_bundle_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `quarterly_alignment`
- `key_horizons`
- `takeaways`

This file is the combined audit for the two main suspect treatment legs together: the rest-of-world Treasury term and the Treasury-operating-cash term. It treats the signed bundle `ROW - TOC` as a single treatment block and checks whether that bundle behaves more like a broad support bundle than like a narrow deposit-liability counterpart.
`quarterly_alignment` should summarize how the combined `ROW - TOC` bundle lines up with broader counterpart bundles such as `foreign_nonts_qoq - tga_qoq`, `foreign_nonts_qoq + reserves_qoq`, and a narrower deposit-liability counterpart like `checkable_rest_of_world_bank_qoq - tga_qoq`.
`key_horizons` should report the direct LP response of the combined bundle plus those counterpart bundles, along with an interpretation label such as `broad_support_bundle_pattern`.

### `output/models/toc_row_path_split_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `path_definitions`
- `quarterly_split`
- `key_horizons`
- `takeaways`

This file is the formal path-split follow-on to the combined TOC/ROW bundle audit. It forces a direct ranking between the TGA-anchored direct-deposit path and the broader support path inside the combined TOC/ROW treatment block.
`path_definitions` should document the exact constructed bundle and the exact constructed counterpart paths.
`quarterly_split` should summarize the quarter-level ranking, including contemporaneous correlations and sign-match shares of the bundle against the broad-support, direct-deposit, and liquidity-plus-external paths, and should expose a preferred quarterly path label.
`key_horizons` should report horizon-level responses and coverage shares of the bundle for the broad-support, direct-deposit, and liquidity-plus-external paths, along with a preferred horizon-path label such as `direct_deposit_path_dominant`, `broad_support_path_dominant`, or `mixed_path_signal`.

### `output/models/toc_row_excluded_interpretation_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `comparison_definition`
- `key_horizons`
- `takeaways`

This file is the secondary comparison surface that excludes the combined TOC/ROW bundle from the treatment object without relabeling the headline treatment. It exists to answer a narrow question: how much of the current residual and strict-gap story depends on that suspect bundle.
`comparison_definition` should make the release role explicit: this is a secondary interpretation-only surface, not the headline treatment.
`key_horizons` should compare the baseline and TOC/ROW-excluded reads side by side for total deposits, the non-TDC residual, the strict identifiable total, and the strict gap share of residual, and it should include the excluded-minus-baseline deltas plus an interpretation label.

### `output/models/strict_missing_channel_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `comparison_definition`
- `key_horizons`
- `takeaways`

This file is the return path to strict-lane missing-channel work after the TOC/ROW treatment branch. It keeps the TOC/ROW-excluded treatment only as a diagnostic comparison and asks a narrower question: once the suspect treatment bundle is removed, which same-scope direct-count pieces still fail to corroborate the remaining residual?
`comparison_definition` should document that this is a strict missing-channel diagnostic rather than a new headline treatment object.
`key_horizons` should compare the baseline and TOC/ROW-excluded reads side by side for the residual, strict loan core, non-Treasury securities, strict identifiable total, funding-offset block, funding-adjusted net, and the remaining direct-count gaps.

### `output/models/strict_sign_mismatch_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `shock_alignment`
- `quarter_concentration`
- `gap_driver_alignment`
- `component_alignment`
- `interpretation`
- `takeaways`

This file audits the next branch after the strict missing-channel read. It asks why the TOC/ROW-excluded direct-count surface can turn positive while the remaining residual stays slightly negative.
`shock_alignment` should summarize how different the TOC/ROW-excluded shock is from the baseline shock on their overlapping sample, including overlap correlation, same-sign share, and the quarters with the largest shock gaps.
`quarter_concentration` should summarize how much of the absolute shock-gap mass is concentrated in the top few quarters and which period bucket carries the largest share.
`gap_driver_alignment` should summarize whether the shock-gap lines up more with the combined baseline-minus-excluded TOC/ROW target bundle or with its ROW and TOC sub-legs separately.
`component_alignment` should summarize how baseline and TOC/ROW-excluded shocks align with the main direct-count strict components, especially `strict_loan_source_qoq` and `strict_identifiable_total_qoq`.

### `output/models/strict_shock_composition_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `top_gap_quarters`
- `period_bucket_profiles`
- `trim_diagnostics`
- `interpretation`
- `takeaways`

This file takes the next step after the sign-mismatch audit. It asks whether the baseline-versus-excluded rotation is mostly a few quarter outliers, a dominant period bucket, or a broader sample-composition problem.
`top_gap_quarters` should profile the largest shock-gap quarters directly, including the combined baseline-minus-excluded TOC/ROW bundle and its ROW and signed-TOC pieces.
`period_bucket_profiles` should summarize how the absolute shock-gap mass and within-bucket rotation diagnostics break down across broad historical buckets such as `post_gfc_early`, `pre_covid`, and `covid_post`.
`trim_diagnostics` should report what happens to the rotation when the largest shock-gap quarters or the full `covid_post` bucket are dropped.

### `output/models/strict_top_gap_quarter_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `top_gap_quarters`
- `dominant_leg_summary`
- `contribution_pattern_summary`
- `interpretation`
- `takeaways`

This file takes the next step after the quarter-trim diagnostics. It decomposes the top shock-gap quarters directly and asks whether those windows are mainly TOC-driven, ROW-driven, or mixed/offsetting bundles.
`top_gap_quarters` should record the dominant leg, contribution pattern, and TOC/ROW shares for each large-gap quarter.
`dominant_leg_summary` should aggregate the weighted shock-gap mass across `row_dominant`, `toc_dominant`, and `mixed` cases.
`contribution_pattern_summary` should aggregate the weighted shock-gap mass across `reinforcing`, `offsetting`, and `single_leg` cases.

### `output/models/strict_top_gap_quarter_direction_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `top_gap_quarters`
- `gap_bundle_alignment_summary`
- `directional_driver_summary`
- `interpretation`
- `takeaways`

This file asks a narrower question inside the top-gap quarters: does the shock-gap direction line up with the TOC/ROW bundle sign, one of the legs, or actually oppose the whole bundle?
`top_gap_quarters` should record the gap-versus-bundle alignment plus the ROW-leg and TOC-leg alignment to the gap for each large-gap quarter.
`gap_bundle_alignment_summary` should aggregate the weighted shock-gap mass across `aligned`, `opposed`, and `neutral` bundle-direction cases.
`directional_driver_summary` should aggregate the weighted shock-gap mass across cases such as `row_driven_gap_direction`, `toc_driven_gap_direction`, `both_legs_align_gap`, and `both_legs_oppose_gap`.

### `output/models/strict_top_gap_inversion_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `top_gap_quarters`
- `directional_driver_context_summary`
- `residual_strict_pattern_summary`
- `interpretation`
- `takeaways`

This file moves from direction classification to quarter-level realized profiles. It asks what excluded residual, strict identifiable total, strict net after funding, and external/liquidity context sit underneath the leading inversion bucket and the single-leg exceptions.
`top_gap_quarters` should enrich each large-gap quarter with those realized quarter profiles plus a residual-versus-strict sign pattern.
`directional_driver_context_summary` should aggregate weighted means of those realized profiles inside buckets such as `both_legs_oppose_gap`, `toc_driven_gap_direction`, and `row_driven_gap_direction`.
`residual_strict_pattern_summary` should aggregate the weighted top-gap mass across sign-pair patterns such as `positive_residual_positive_strict` and `negative_residual_positive_strict`.

### `output/models/strict_top_gap_anomaly_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `anomaly_quarter`
- `peer_quarters`
- `peer_pattern_summary`
- `weighted_peer_means`
- `anomaly_vs_peer_deltas`
- `ranked_anomaly_component_deltas`
- `interpretation`
- `takeaways`

This file isolates the main within-bucket anomaly after the inversion summary lands. It compares the anomaly quarter against same-bucket peers and records how the realized excluded residual, strict total, funding-adjusted strict net, and external/liquidity context differ from the peer bucket mean.
`anomaly_quarter` should carry the full quarter-level realized profile for the anomaly.
`peer_quarters` should record the same-bucket comparison set.
`peer_pattern_summary` should summarize the residual-versus-strict sign patterns among those peers.
`weighted_peer_means` and `anomaly_vs_peer_deltas` should make the anomaly-versus-peer comparison explicit.
`ranked_anomaly_component_deltas` should sort the anomaly-minus-peer component gaps by absolute magnitude so the leading anomaly driver is easy to identify.

### `output/models/strict_top_gap_anomaly_component_split_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `anomaly_quarter`
- `peer_quarters`
- `peer_bucket_weight`
- `loan_subcomponent_deltas`
- `securities_subcomponent_deltas`
- `funding_subcomponent_deltas`
- `liquidity_external_deltas`
- `ranked_component_deltas`
- `interpretation`
- `takeaways`

This file decomposes the main within-bucket anomaly into detailed loan, securities, funding, and liquidity/external subcomponents. It should make the anomaly-versus-peer comparison concrete enough to tell whether the anomaly is mainly a loan-source contraction, a funding shift, or a weaker liquidity/external-support quarter.

### `output/models/strict_top_gap_anomaly_di_loans_split_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `anomaly_quarter`
- `peer_quarters`
- `peer_bucket_weight`
- `di_loans_nec_component_deltas`
- `dominant_borrower_component`
- `borrower_gap_row`
- `interpretation`
- `takeaways`

This file decomposes the DI-loans-n.e.c. anomaly into borrower-side counterpart buckets. It should help distinguish whether the `2009Q4` DI-loans-n.e.c. shortfall is mostly tied to domestic financial borrowers, nonfinancial corporate borrowers, rest-of-world borrowers, or a larger systemwide borrower gap.

### `output/models/strict_top_gap_anomaly_backdrop_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `anomaly_quarter`
- `peer_quarters`
- `peer_bucket_weight`
- `backdrop_rows`
- `corporate_credit_row`
- `loan_source_row`
- `reserves_row`
- `foreign_nonts_row`
- `tga_row`
- `residual_row`
- `liquidity_external_abs_to_corporate_abs_ratio`
- `interpretation`
- `takeaways`

This file compares the `2009Q4` corporate-credit shortfall directly against the liquidity/external backdrop. It should say whether the anomaly reads more like a narrow borrower/composition quarter, a broader liquidity/external-support quarter, or a combined credit-plus-liquidity quarter.

### `output/models/big_picture_synthesis_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `h0_snapshot`
- `quarter_composition`
- `supporting_case`
- `classification`
- `interpretation`
- `takeaways`

This file is the project-level synthesis layer. It should stop the repo from forcing readers to infer the main answer from separate scope, treatment, strict-lane, and quarter-specific audits. It should state explicitly whether the current problem is mostly scope mismatch, mostly TDC construction, mostly strict-lane incompleteness, or some combination.

### `output/models/treatment_object_comparison_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_objects`
- `recommendation`
- `takeaways`

This file is the treatment-object decision layer. It should compare the current headline TDC, the matched-scope comparison objects, the TOC/ROW-excluded diagnostic object, and any recommended split architecture, then say which should stay headline, which should stay diagnostic, and which redesign branch should come next.

### `output/models/split_treatment_architecture_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `series_definitions`
- `quarterly_alignment`
- `architecture_recommendation`
- `key_horizons`
- `takeaways`

This file is the explicit split-treatment layer. It should define the deposit-proximate core treatment, the TOC/ROW support bundle, and the corresponding core residual, then show that the split is mechanically exact in the panel and summarize how large the support bundle is at the key horizons.

### `output/models/core_treatment_promotion_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `series_alias_check`
- `shock_quality`
- `key_horizons`
- `strict_validation_check`
- `promotion_recommendation`
- `takeaways`

This file is the core-treatment promotion decision layer. It should say whether the split architecture should stay interpretive only or whether the deposit-proximate core should also be estimated and reported as a separate shock, using explicit first-stage quality, baseline-vs-core shock overlap, and h0 strict-lane corroboration.

### `output/models/strict_redesign_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `current_strict_problem_definition`
- `failure_modes`
- `recommended_build_order`
- `takeaways`

This file summarizes what still fails in the strict lane after the treatment side has been split into a deposit-proximate core and a TOC/ROW support bundle, and records the repo's recommended redesign order for that branch.

### `output/models/strict_loan_core_redesign_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `candidate_definitions`
- `published_roles`
- `recommendation`
- `key_horizons`
- `takeaways`

This file is the first concrete strict-redesign implementation layer. It should compare the current broad loan source against a redesigned direct-only minimum core, a private-borrower-augmented bounded comparison, and a noncore/system diagnostic subtotal under the baseline and core-deposit-proximate shocks. It also carries the published role decision for the strict loan block.

### `output/models/strict_di_bucket_role_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `release_taxonomy`
- `recommendation`
- `key_horizons`
- `takeaways`

This file is the DI-bucket role bridge. It should explain how the broad `strict_loan_di_loans_nec_qoq` bucket is interpreted after the headline-core redesign: headline direct core, standard secondary comparison, broad subtotal diagnostic, and cross-scope borrower-counterpart diagnostics.

### `output/models/strict_di_bucket_bridge_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `bridge_definitions`
- `recommendation`
- `key_horizons`
- `takeaways`

This file is the next strict-lane bridge surface after the role decision. It should treat `strict_loan_di_loans_nec_qoq` as an explicit bridge problem, measuring the U.S.-chartered DI asset response against private-borrower, noncore/system, and systemwide borrower-counterpart rows under the baseline and core-deposit-proximate shocks.

### `output/models/strict_private_borrower_bridge_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `bridge_definitions`
- `recommendation`
- `key_horizons`
- `takeaways`

This file narrows the DI bridge again, splitting the private-borrower bridge into households/nonprofits, nonfinancial-corporate, and nonfinancial-noncorporate components under the baseline and core-deposit-proximate shocks.

### `output/models/strict_nonfinancial_corporate_bridge_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `bridge_definitions`
- `recommendation`
- `key_horizons`
- `takeaways`

This file narrows the private bridge one level further, treating the nonfinancial-corporate block as the active bridge object and leaving households/nonprofits plus nonfinancial-noncorporate visible as offsets.

### `output/models/strict_private_offset_residual_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `bridge_definitions`
- `recommendation`
- `key_horizons`
- `takeaways`

This file isolates the remaining private detail after the corporate bridge, treating households/nonprofits plus nonfinancial-noncorporate as the offset block and asking whether that offset is economically meaningful or mostly a small opposing residual.

### `output/models/tdc_treatment_audit_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `component_definitions`
- `variant_definitions`
- `baseline_target`
- `construction_alignment`
- `key_horizons`
- `takeaways`

This file is the direct TDC-construction audit surface. It combines two views of the treatment object:

- direct baseline-shock responses of the TDC building blocks themselves; and
- residual shifts from removing whole treatment legs such as ROW, foreign bank sectors, or remittances.

Use this file to distinguish “which TDC component moves with the shock” from “which treatment leg materially changes the residual when omitted.”
It also records whether the imported canonical TDC series and the direct public-component reconstruction line up exactly quarter by quarter for the audited legs, so arithmetic-construction mismatches can be separated from dynamic-interpretation questions.

### `output/models/identity_measurement_ladder.csv`

Minimum columns:

- `treatment_variant`
- `treatment_role`
- `treatment_family`
- `target`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`
- `shock_column`
- `decomposition_mode`
- `outcome_construction`
- `inference_method`

This file applies the same exact identity-preserving baseline logic to the imported `tdcest` measurement variants. It is the preferred release-facing source for measurement sensitivity because it keeps the `other_component_qoq = total - tdc` accounting exact, rather than relying only on the approximate LP sensitivity ladder.

### `output/models/lp_irf_regimes.csv`

Minimum columns:

- `regime`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

### `output/models/regime_diagnostics_summary.json`

Required keys:

- `informative_regime_count`
- `stable_regime_count`
- `regimes`
- `takeaways`

This file summarizes whether the configured regime splits are balanced enough to interpret and records the key high-versus-low regime rows at headline horizons.
In the current live configuration, the release-facing regime layer uses only `reserve_drain_pressure`.
It also reports whether each regime has enough within-state shock support to avoid highly extrapolative split estimates.

### `output/models/tdc_sensitivity_ladder.csv`

Minimum columns:

- `treatment_variant`
- `treatment_role`
- `treatment_family`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

### `output/models/control_set_sensitivity.csv`

Minimum columns:

- `control_variant`
- `control_role`
- `control_columns`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

This file compares the same baseline shock under alternate LP control sets. `control_role=headline` marks the published lagged-macro baseline, `core` marks near-neighbor pre-treatment comparisons, and `exploratory` marks timing-stress or otherwise non-headline diagnostics that should not silently replace the published spec.
The current exploratory timing-stress comparison adds `quarter_index` to the lagged-macro controls to test whether the headline read is being driven by long-sample trend confounding.

### `output/models/shock_sample_sensitivity.csv`

Minimum columns:

- `sample_variant`
- `sample_role`
- `sample_filter`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

This file compares the frozen headline shock sample against explicit robustness filters. In the current release path, `sample_role=headline` is the full usable shock sample and `sample_role=exploratory` is the flagged-window trimming check. The headline estimand stays on the full usable sample; flagged-window exclusion is published as a fragility check, not as a silent replacement.

### `output/models/period_sensitivity.csv`

Minimum columns:

- `period_variant`
- `period_role`
- `start_quarter`
- `end_quarter`
- `outcome`
- `horizon`
- `beta`
- `se`
- `lower95`
- `upper95`
- `n`
- `spec_name`

This file reruns the frozen headline LP stack on explicit usable-sample subperiods. In the current release path, `all_usable` is the headline usable shock window, while `post_gfc_early`, `pre_covid`, and `covid_post` are core historical splits used to show how the total-versus-other read changes over time. Because the frozen usable shock sample begins in `2009Q1`, this file cannot identify a true 2008 GFC subperiod under the current quarterly design.

### `output/models/period_sensitivity_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `periods`
- `key_horizons`
- `takeaways`

This summary condenses `period_sensitivity.csv` to the key horizons and period windows used in interpretation. It should make clear that period sensitivity is a secondary surface rather than the primary exact identity baseline, and its horizon labels should stay CI-aware instead of inferring `crowd_out_signal` from point estimates alone.

### `output/models/total_minus_other_contrast.csv`

Minimum columns:

- `scope`
- `variant`
- `role`
- `horizon`
- `beta_total`
- `beta_other`
- `beta_implied`
- `beta_direct`
- `gap_implied_minus_direct`
- `abs_gap`
- `n_total`
- `n_other`
- `n_direct`
- `sample_mismatch_flag`

This file makes the identity check explicit at the LP layer. `beta_implied` is `beta(total_deposits_bank_qoq) - beta(other_component_qoq)` and `beta_direct` is the direct LP response of `tdc_bank_only_qoq` under the same spec slice. The file is meant to catch cases where the direct treatment response is too weak or where sample mismatches break the accounting comparison. Tiny `abs_gap` values can reflect floating-point noise and should not be treated as substantive economic contradictions.
When the exact identity-preserving baseline is available, the file can contain both `scope=baseline` for the older approximate dynamic LP path and `scope=exact_identity_baseline` for the primary exact decomposition path.

### `output/models/structural_proxy_evidence.csv`

Minimum columns:

- `scope`
- `context`
- `horizon`
- `other_outcome`
- `other_beta`
- `other_se`
- `other_lower95`
- `other_upper95`
- `other_ci_excludes_zero`
- `proxy_outcome`
- `proxy_beta`
- `proxy_se`
- `proxy_lower95`
- `proxy_upper95`
- `proxy_ci_excludes_zero`
- `other_sign`
- `proxy_sign`
- `sign_alignment`
- `evidence_label`
- `proxy_share_of_other_beta`

This file is the structural cross-check pack for the residual. It compares `other_component_qoq` to the baseline proxy outcomes at each horizon and records whether a proxy moves in the same direction, the opposite direction, or remains too weak to say much. In the current public bundle the proxy set includes bank private credit, Fed/liquidity plumbing, foreign deposits, and sign-normalized domestic nonfinancial MMF/repo reallocation channels. `proxy_share_of_other_beta` is diagnostic only and is not exact counterpart accounting.

### `output/models/structural_proxy_evidence_summary.json`

Required keys:

- `status`
- `headline_question`
- `key_horizons`
- `takeaways`

This file summarizes whether structural proxies corroborate the non-TDC residual at key horizons. It is a release gate because the residual alone is not sufficient mechanism evidence, and proxy corroboration is weaker than same-scope direct-count evidence.

### `output/models/proxy_coverage_summary.json`

Required keys:

- `status`
- `headline_question`
- `covered_channel_families`
- `major_uncovered_channel_families`
- `history_limits`
- `key_horizons`
- `published_regime_contexts`
- `release_caveat`
- `takeaways`

This file is the release-facing coverage map for the mechanism bundle. It reports how much of the non-TDC response is covered by the current public proxy bundle, where the remaining uncovered gap is still large, which channel families are covered, and which important channel families remain outside the current bundle. It is not exact counterpart accounting.

### `output/models/proxy_unit_audit.json`

Required keys:

- `status`
- `source_series`
- `derived_proxies`
- `takeaways`

This file audits the level scaling used for the FRED-based proxy inputs. It records the explicit divisor applied to each source series and the implied scale/coverage of the derived proxy channels so release review can verify that proxy weakness is not just a unit-conversion bug.

### `output/models/shock_diagnostics_summary.json`

Required keys:

- `estimand_interpretation`
- `sample_comparison`
- `impact_response_comparison`
- `treatment_variant_comparisons`
- `largest_disagreement_quarters`
- `takeaways`

This file explains what an LP coefficient means on the current shock scale and whether the baseline-versus-sensitivity disagreement reflects different treatment objects rather than a simple scaling mismatch.
It should also make clear when a variant is exploratory rather than a near-baseline robustness rung.
`treatment_variant_comparisons` should separate measurement checks from shock-design checks using `treatment_family`.
It now also reports baseline shock-quality diagnostics such as flagged windows, maximum fitted-to-target scale ratio, maximum training condition number, and the active baseline shock specification metadata.

### `output/models/headline_treatment_fingerprint.json`

Required keys:

- `treatment_freeze_status`
- `model_name`
- `target`
- `method`
- `predictors`
- `min_train_obs`
- `max_train_obs`
- `usable_sample`
- `analysis_source_commit`
- `analysis_tree`

This is the machine-readable freeze record for the current headline unexpected-TDC shock. It should match the frozen default entry in `config/shock_specs.yml`, record the `analysis_source_commit` used as the source anchor for the build, and record whether that source tree was clean or dirty at build time. That makes the committed public mirror validate against a satisfiable recorded state instead of the containing repo's later `HEAD`, while still surfacing whether the build came from uncommitted source changes.

### `output/models/provenance_validation_summary.json`

Required keys:

- `status`
- `failures`
- `analysis_source_commit_check`
- `analysis_tree_check`
- `config_hashes_check`
- `upstream_input_check`
- `spec_metadata_check`

This file is the recorded-state provenance verdict for the headline treatment fingerprint. It records whether the stored `analysis_source_commit` is present in repo history, whether the stored build tree was clean, whether the stored config hashes still match the current repo config files, and whether the upstream canonical-TDC source can be revalidated against its recorded source commit when the sibling repo is reachable. The publish path can still apply a stricter live current-HEAD gate before mirroring, but the committed public mirror should avoid an unsatisfiable “stored commit must equal containing repo HEAD” rule.

### `output/models/direct_identification_summary.json`

Required keys:

- `status`
- `headline_question`
- `estimation_path`
- `shock_definition`
- `horizon_evidence`
- `first_stage_checks`
- `sample_fragility`
- `answer_ready`
- `reasons`
- `warnings`
- `answer_ready_when`

This file is the direct identification gate for the release process. It asks whether the baseline unexpected-TDC shock moves `tdc_bank_only_qoq` itself enough to make pass-through versus crowd-out interpretable, and whether the flagged-window trimming check materially changes the headline answer. The `shock_definition` block should record the active baseline model name, predictor list, and burn-in length so headline shock redesigns remain explicit. `estimation_path` should say whether the summary is using the exact identity-preserving baseline or the approximate dynamic decomposition path. Small numeric gaps in the total-minus-other identity check are diagnostic only; the substantive release blocker is lack of clean total-versus-other response separation.

### `output/models/result_readiness_summary.json`

Required keys:

- `status`
- `estimation_path`
- `headline_assessment`
- `reasons`
- `warnings`
- `diagnostics`
- `key_estimates`
- `answer_ready_when`

This file states whether the current backend run is ready to support a deposit-response interpretation and how much mechanism weight, if any, the release bundle can carry beyond that. `estimation_path` should state whether key estimates are being read from `lp_irf_identity_baseline.csv` or the approximate broad LP path.

### `output/models/pass_through_summary.json`

Required keys:

- `status`
- `headline_question`
- `headline_answer`
- `estimation_path`
- `sample_policy`
- `baseline_horizons`
- `core_treatment_variants`
- `measurement_treatment_variants`
- `shock_design_treatment_variants`
- `core_control_variants`
- `shock_sample_variants`
- `structural_proxy_context`
- `proxy_coverage_context`
- `scope_alignment_context`
- `strict_gap_scope_check_context`
- `broad_scope_system_context`
- `tdc_treatment_audit_context`
- `treasury_operating_cash_audit_context`
- `rest_of_world_treasury_audit_context`
- `toc_row_path_split_context`
- `toc_row_excluded_interpretation_context`
- `strict_missing_channel_context`
- `strict_sign_mismatch_audit_context`
- `strict_shock_composition_context`
- `strict_top_gap_quarter_audit_context`
- `strict_top_gap_quarter_direction_context`
- `strict_top_gap_inversion_context`
- `strict_top_gap_anomaly_context`
- `strict_top_gap_anomaly_component_split_context`
- `strict_top_gap_anomaly_di_loans_split_context`
- `strict_top_gap_anomaly_backdrop_context`
- `big_picture_synthesis_context`
- `treatment_object_comparison_context`
- `split_treatment_architecture_context`
- `core_treatment_promotion_context`
- `strict_redesign_context`
- `strict_loan_core_redesign_context`
- `strict_component_framework_context`
- `strict_release_framing_context`
- `strict_direct_core_component_context`
- `strict_direct_core_horizon_stability_context`
- `strict_additional_creator_candidate_context`
- `strict_di_bucket_role_context`
- `tdcest_ladder_integration_context`
- `tdcest_broad_object_comparison_context`
- `tdcest_broad_treatment_sensitivity_context`
- `published_regime_contexts`
- `readiness_reasons`
- `readiness_warnings`

This file is the release-facing answer layer. It pairs total-deposit and non-TDC responses at headline horizons, carries the direct TDC response and total-minus-other contrast check, includes both structural-proxy direction checks and the broader proxy-coverage context, and states how flagged-window trimming should be interpreted relative to the frozen headline sample. It should explicitly separate measurement-variant treatment checks from shock-design treatment checks, identify whether the baseline read is coming from the exact identity-preserving decomposition path, and prefer `identity_measurement_ladder.csv` over the approximate LP sensitivity table for public measurement-variant comparisons when that exact artifact is available.
When available, `scope_alignment_context` should explicitly distinguish the no-ROW `domestic_bank_only` sensitivity from the true U.S.-chartered bank-leg-matched treatment and summarize how much each changes the headline residual at the key horizons.
When available, `strict_gap_scope_check_context` should state directly how much of the current strict direct-count gap would remain after applying the matched-bank-leg residual shift while holding the baseline strict total fixed. That context is descriptive only; it isolates scope-relief magnitude rather than presenting a fully re-estimated strict lane under each treatment variant.
When available, `treasury_operating_cash_audit_context` should summarize whether the Treasury-operating-cash leg behaves like a genuine TGA cash-release/cash-drain pattern and surface the compact h0 plumbing read alongside the quarter-level alignment metrics.
When available, `rest_of_world_treasury_audit_context` should summarize whether the ROW Treasury leg behaves like a clean deposit counterpart or like a broader external-support channel, and should surface both the weak same-quarter alignment and the h0 LP pattern.
When available, `toc_row_path_split_context` should summarize the key distinction inside the combined TOC/ROW bundle: quarter-by-quarter fit may look more TGA-anchored while the shock-response dominant path can still be the broader support path.
When available, `toc_row_excluded_interpretation_context` should summarize the secondary no-TOC/no-ROW comparison directly, including how much the h0 residual read changes and whether the strict direct-count gap share stays large under that excluded-treatment comparison.
When available, `strict_missing_channel_context` should summarize what still remains missing in the strict lane after that secondary TOC/ROW-excluded comparison, including the h0 residual, the strict loan core, the securities add-on, the funding-adjusted net, and the remaining gap-after-funding share.
When available, `strict_sign_mismatch_audit_context` should summarize whether the TOC/ROW-excluded shock is materially different from the baseline shock and whether that difference rotates the direct-count strict components toward positive alignment.
When available, `strict_shock_composition_context` should summarize whether the rotation survives after dropping the biggest shock-gap quarters or the full dominant period bucket.
When available, `strict_top_gap_quarter_audit_context` should summarize whether the top-gap quarters are mostly TOC-dominant, ROW-dominant, or mixed/offsetting bundles.
When available, `strict_top_gap_quarter_direction_context` should summarize whether the top-gap quarter shock-gap directions align with the TOC/ROW bundle sign, one leg, or oppose the whole bundle.
When available, `strict_top_gap_inversion_context` should summarize the realized excluded-residual versus strict-lane profile inside the leading inversion bucket and the single-leg exceptions.
When available, `strict_top_gap_anomaly_context` should summarize how the main within-bucket anomaly differs from same-bucket peers.
When available, `strict_top_gap_anomaly_component_split_context` should summarize which detailed loan, funding, securities, and liquidity/external subcomponents are most responsible for the anomaly quarter relative to peers.
When available, `strict_top_gap_anomaly_di_loans_split_context` should summarize which borrower-side DI-loans-n.e.c. counterpart bucket is most responsible for the anomaly quarter relative to peers.
When available, `strict_top_gap_anomaly_backdrop_context` should summarize how the corporate-credit shortfall compares with the reserves, foreign-NONTS, TGA, and residual backdrop in the anomaly quarter relative to peers.
When available, `big_picture_synthesis_context` should summarize the whole project state directly: scope mismatch is partial, TOC/ROW dominates the treatment-side residual issue, the independent strict lane still is not validated, and the single-quarter anomaly work is now secondary supporting evidence rather than the main active branch.
When available, `treatment_object_comparison_context` should summarize which treatment object is current headline, which are diagnostic-only comparisons, and what treatment architecture the repo should redesign next.
When available, `split_treatment_architecture_context` should summarize the explicit core-versus-support split, including the h0 support-bundle magnitude, the core residual, and the repo’s current architecture recommendation.
When available, `core_treatment_promotion_context` should summarize whether the split should remain interpretive or whether the deposit-proximate core is strong enough to be estimated and reported as a separate comparison shock.
When available, `strict_redesign_context` should summarize what still fails in the strict lane after the treatment split is fixed and what redesign sequence the repo recommends next.
When available, `strict_loan_core_redesign_context` should summarize the concrete redesigned loan-core comparison and the repo’s current published role design: `strict_loan_core_min_qoq` as the headline direct core, the current standard secondary comparison, and the old broad loan subtotal plus `strict_loan_di_loans_nec_qoq` as diagnostic-only reads.
When available, `strict_corporate_bridge_secondary_comparison_context` should summarize whether the standard secondary strict comparison should remain `strict_loan_core_plus_private_borrower_qoq` or narrow to `strict_loan_core_plus_nonfinancial_corporate_qoq`.
When available, `tdcest_ladder_integration_context` should summarize the selective integration rule directly: canonical `tdcest` bank-only remains the broad anchor, Tier 2 / Tier 3 stay broad comparison rows, the historical overlay stays historical-only, the MRV ROW branch stays bounded and nondefault, and the strict framework is unchanged.
When available, `tdcest_broad_object_comparison_context` should summarize the latest common-quarter broad-object ladder read and the live status of the historical overlay and MRV pilot, while making clear that all of these remain on the broad-object side rather than entering the strict object.
When available, `tdcest_broad_treatment_sensitivity_context` should summarize whether the corrected `tdcest` broad-treatment variants materially change the deposit and residual LP answers, while keeping that interpretation fenced to broad-object sensitivity rather than strict validation.
When available, `strict_component_framework_context` should freeze the release-facing interpretation in one place: accounting closure is non-evidence for independent verification, full TDC remains the canonical broad object, `TOC + ROW` are a measured support bundle with unresolved strict deposit incidence, `strict_loan_core_min_qoq` is the multihorizon direct core, `strict_loan_mortgages_qoq` is the impact-horizon candidate, the corporate-only bridge is the standard narrow bridge comparison, and the broader private bridge stays a wider diagnostic envelope.
When available, `strict_release_framing_context` should state the final release rule plainly: full TDC stays the broad Treasury Contribution to Deposits object, the strict independent non-TDC object excludes both TOC and ROW under current evidence, `strict_loan_core_min_qoq` remains the multihorizon direct benchmark, `strict_loan_mortgages_qoq` is an impact-horizon candidate only, and TOC/ROW should only be reconsidered if new scope- and timing-matched incidence evidence appears.
When available, `strict_direct_core_component_context` should summarize whether the current headline direct core should stay bundled or narrow further by splitting it into mortgages and consumer credit and comparing both pieces directly against the same residual object.
When available, `strict_direct_core_horizon_stability_context` should summarize whether the direct-core winner is horizon-specific. It should make clear when a narrower candidate wins at impact but the bundled direct core remains the better multihorizon benchmark.
When available, `strict_additional_creator_candidate_context` should summarize whether any remaining creator channels beyond the current direct core deserve strict-candidate status, or whether the best remaining channels are only broad validation proxies for the existing mortgage/consumer core.
When available, `toc_row_incidence_audit_context` should summarize the first-pass leg-by-leg incidence read: TOC as real reserve/TGA plumbing with only partial deposit incidence, ROW as weaker in-scope deposit incidence with stronger external-support behavior, and the combined implication that TOC/ROW should not be treated as validated strict deposit components by default.
When available, `toc_validated_share_candidate_context` should summarize whether any narrow TOC share survives both the quarterly stability gate and the direct-core comparison, or whether TOC should stay outside the strict object together with ROW.
When available, `strict_di_bucket_role_context` should summarize how the broad DI-loans-n.e.c. bucket is now interpreted: a diagnostic-only broad bucket with a bounded private-borrower secondary comparison and a cross-scope borrower-counterpart bridge, not a headline-capable direct core.
When available, `tdcest_ladder_integration_context` should summarize the selective-integration rule directly: keep the `tdcest` base headline as the broad anchor, import Tier 2 / Tier 3 as broad corrected comparisons, keep the bank-receipt overlay historical-only, keep the MRV ROW branch bounded and nondefault, and leave the frozen strict framework unchanged.

### `output/models/sample_construction_summary.json`

Required keys:

- `full_panel`
- `headline_sample`
- `usable_shock_sample`
- `shock_definition`
- `headline_sample_truncation`
- `extended_column_coverage`
- `takeaways`

This file makes the sample rule machine-readable. It records the full merged quarterly span before trimming, the narrower headline sample that defines the baseline treatment and deposit-response path, the usable shock count after treatment-model burn-in, and the shorter-history proxy or regime columns that remain partially observed inside the headline sample.
It may also include `interpretation_scope` and `mechanism_caveat` fields so the release layer can distinguish deposit-response evidence from broader mechanism attribution.

### `output/manifests/raw_downloads.json`

Required keys:

- `runs`

### `output/manifests/reused_artifacts.json`

Required keys:

- `artifacts`

### `output/manifests/pipeline_run.json`

Required keys:

- `command`
- `outputs`

### `site/data/overview.json`

Required keys:

- `headline_metrics`
- `sample`
- `main_findings`
- `caveats`
- `evidence_tiers`
- `artifacts`

Recommended content:

- `headline_metrics` should summarize headline accounting quantities and release context.
- `sample` should state the quarterly sample span and row count.
- `main_findings` and `caveats` should state that the public preview is a methods-and-reproducibility release around the frozen rolling 40-quarter ridge shock, report the usable shock span, and surface the current `not_ready` release posture honestly.
- `evidence_tiers` should classify direct data, transparent transformations, model-based estimates, and inferred counterfactuals.

### `site/data/accounting_summary.csv`

Mirror of `output/accounting/accounting_summary.csv`.

### `site/data/quarters_tdc_exceeds_total.csv`

Mirror of `output/accounting/quarters_tdc_exceeds_total.csv`.

### `site/data/unexpected_tdc.csv`

Mirror of `output/shocks/unexpected_tdc.csv`.

### `site/data/lp_irf.csv`

Mirror of `output/models/lp_irf.csv`.

### `site/data/lp_irf_identity_baseline.csv`

Mirror of `output/models/lp_irf_identity_baseline.csv`.

### `site/data/lp_irf_accounting_identity.csv`

Mirror of `output/models/lp_irf_accounting_identity.csv`.

### `site/data/lp_irf_strict_identifiable.csv`

Mirror of `output/models/lp_irf_strict_identifiable.csv`.

### `site/data/accounting_identity_alignment.csv`

Mirror of `output/models/accounting_identity_alignment.csv`.

### `site/data/accounting_identity_summary.json`

Mirror of `output/models/accounting_identity_summary.json`.

### `site/data/strict_identifiable_alignment.csv`

Mirror of `output/models/strict_identifiable_alignment.csv`.

### `site/data/strict_funding_offset_alignment.csv`

Mirror of `output/models/strict_funding_offset_alignment.csv`.

### `site/data/strict_identifiable_summary.json`

Mirror of `output/models/strict_identifiable_summary.json`.

### `site/data/strict_identifiable_followup_summary.json`

Mirror of `output/models/strict_identifiable_followup_summary.json`.

### `site/data/scope_alignment_summary.json`

Mirror of `output/models/scope_alignment_summary.json`.

### `site/data/broad_scope_system_summary.json`

Mirror of `output/models/broad_scope_system_summary.json`.

### `site/data/treasury_operating_cash_audit_summary.json`

Mirror of `output/models/treasury_operating_cash_audit_summary.json`.

### `site/data/rest_of_world_treasury_audit_summary.json`

Mirror of `output/models/rest_of_world_treasury_audit_summary.json`.

### `site/data/toc_row_bundle_audit_summary.json`

Mirror of `output/models/toc_row_bundle_audit_summary.json`.

### `site/data/toc_row_path_split_summary.json`

Mirror of `output/models/toc_row_path_split_summary.json`.

### `site/data/toc_row_excluded_interpretation_summary.json`

Mirror of `output/models/toc_row_excluded_interpretation_summary.json`.

### `site/data/strict_missing_channel_summary.json`

Mirror of `output/models/strict_missing_channel_summary.json`.

### `site/data/strict_sign_mismatch_audit_summary.json`

Mirror of `output/models/strict_sign_mismatch_audit_summary.json`.

### `site/data/strict_shock_composition_summary.json`

Mirror of `output/models/strict_shock_composition_summary.json`.

### `site/data/strict_top_gap_quarter_audit_summary.json`

Mirror of `output/models/strict_top_gap_quarter_audit_summary.json`.

### `site/data/strict_top_gap_quarter_direction_summary.json`

Mirror of `output/models/strict_top_gap_quarter_direction_summary.json`.

### `site/data/strict_top_gap_inversion_summary.json`

Mirror of `output/models/strict_top_gap_inversion_summary.json`.

### `site/data/strict_top_gap_anomaly_summary.json`

Mirror of `output/models/strict_top_gap_anomaly_summary.json`.

### `site/data/strict_top_gap_anomaly_component_split_summary.json`

Mirror of `output/models/strict_top_gap_anomaly_component_split_summary.json`.

### `site/data/strict_top_gap_anomaly_di_loans_split_summary.json`

Mirror of `output/models/strict_top_gap_anomaly_di_loans_split_summary.json`.

### `site/data/strict_top_gap_anomaly_backdrop_summary.json`

Mirror of `output/models/strict_top_gap_anomaly_backdrop_summary.json`.

### `site/data/big_picture_synthesis_summary.json`

Mirror of `output/models/big_picture_synthesis_summary.json`.

### `site/data/treatment_object_comparison_summary.json`

Mirror of `output/models/treatment_object_comparison_summary.json`.

### `site/data/split_treatment_architecture_summary.json`

Mirror of `output/models/split_treatment_architecture_summary.json`.

### `site/data/core_treatment_promotion_summary.json`

Mirror of `output/models/core_treatment_promotion_summary.json`.

### `site/data/strict_redesign_summary.json`

Mirror of `output/models/strict_redesign_summary.json`.

### `site/data/strict_loan_core_redesign_summary.json`

Mirror of `output/models/strict_loan_core_redesign_summary.json`.

### `site/data/strict_di_bucket_role_summary.json`

Mirror of `output/models/strict_di_bucket_role_summary.json`.

### `site/data/strict_di_bucket_bridge_summary.json`

Mirror of `output/models/strict_di_bucket_bridge_summary.json`.

### `site/data/strict_private_borrower_bridge_summary.json`

Mirror of `output/models/strict_private_borrower_bridge_summary.json`.

### `site/data/strict_nonfinancial_corporate_bridge_summary.json`

Mirror of `output/models/strict_nonfinancial_corporate_bridge_summary.json`.

### `site/data/strict_private_offset_residual_summary.json`

Mirror of `output/models/strict_private_offset_residual_summary.json`.

### `site/data/strict_corporate_bridge_secondary_comparison_summary.json`

Mirror of `output/models/strict_corporate_bridge_secondary_comparison_summary.json`.

### `site/data/strict_component_framework_summary.json`

Mirror of `output/models/strict_component_framework_summary.json`.

### `site/data/strict_release_framing_summary.json`

Mirror of `output/models/strict_release_framing_summary.json`.

### `site/data/strict_direct_core_component_summary.json`

Mirror of `output/models/strict_direct_core_component_summary.json`.

### `site/data/strict_direct_core_horizon_stability_summary.json`

Mirror of `output/models/strict_direct_core_horizon_stability_summary.json`.

### `site/data/strict_additional_creator_candidate_summary.json`

Mirror of `output/models/strict_additional_creator_candidate_summary.json`.

### `site/data/strict_di_loans_nec_measurement_audit_summary.json`

Mirror of `output/models/strict_di_loans_nec_measurement_audit_summary.json`.

### `site/data/strict_results_closeout_summary.json`

Mirror of `output/models/strict_results_closeout_summary.json`.

### `site/data/tdcest_ladder_integration_summary.json`

Mirror of `output/models/tdcest_ladder_integration_summary.json`.

### `site/data/tdcest_broad_object_comparison_summary.json`

Mirror of `output/models/tdcest_broad_object_comparison_summary.json`.

### `site/data/tdcest_broad_treatment_sensitivity_summary.json`

Mirror of `output/models/tdcest_broad_treatment_sensitivity_summary.json`.

### `site/data/treasury_cash_regime_audit_summary.json`

Mirror of `output/models/treasury_cash_regime_audit_summary.json`.

### `site/data/historical_cash_term_reestimation_summary.json`

Mirror of `output/models/historical_cash_term_reestimation_summary.json`.

### `site/data/toc_row_incidence_audit_summary.json`

Mirror of `output/models/toc_row_incidence_audit_summary.json`.

### `site/data/toc_row_liability_incidence_raw_summary.json`

Mirror of `output/models/toc_row_liability_incidence_raw_summary.json`.

### `site/data/toc_validated_share_candidate_summary.json`

Mirror of `output/models/toc_validated_share_candidate_summary.json`.

### `site/data/tdc_treatment_audit_summary.json`

Mirror of `output/models/tdc_treatment_audit_summary.json`.

### `site/data/identity_measurement_ladder.csv`

Mirror of `output/models/identity_measurement_ladder.csv`.

### `site/data/lp_irf_regimes.csv`

Mirror of `output/models/lp_irf_regimes.csv`.

### `site/data/regime_diagnostics_summary.json`

Mirror of `output/models/regime_diagnostics_summary.json`.

### `site/data/tdc_sensitivity_ladder.csv`

Mirror of `output/models/tdc_sensitivity_ladder.csv`.

### `site/data/control_set_sensitivity.csv`

Mirror of `output/models/control_set_sensitivity.csv`.

### `site/data/shock_sample_sensitivity.csv`

Mirror of `output/models/shock_sample_sensitivity.csv`.

### `site/data/period_sensitivity.csv`

Mirror of `output/models/period_sensitivity.csv`.

### `site/data/period_sensitivity_summary.json`

Mirror of `output/models/period_sensitivity_summary.json`.

### `site/data/total_minus_other_contrast.csv`

Mirror of `output/models/total_minus_other_contrast.csv`.

### `site/data/structural_proxy_evidence.csv`

Mirror of `output/models/structural_proxy_evidence.csv`.

### `site/data/structural_proxy_evidence_summary.json`

Mirror of `output/models/structural_proxy_evidence_summary.json`.

### `site/data/proxy_coverage_summary.json`

Mirror of `output/models/proxy_coverage_summary.json`.

### `site/data/proxy_unit_audit.json`

Mirror of `output/models/proxy_unit_audit.json`.

### `site/data/shock_diagnostics_summary.json`

Mirror of `output/models/shock_diagnostics_summary.json`.

### `site/data/headline_treatment_fingerprint.json`

Mirror of `output/models/headline_treatment_fingerprint.json`.

### `site/data/provenance_validation_summary.json`

Mirror of `output/models/provenance_validation_summary.json`.

### `site/data/direct_identification_summary.json`

Mirror of `output/models/direct_identification_summary.json`.

### `site/data/result_readiness_summary.json`

Mirror of `output/models/result_readiness_summary.json`.

### `site/data/pass_through_summary.json`

Mirror of `output/models/pass_through_summary.json`.

The pass-through summary context block now includes `strict_di_bucket_bridge_context`, `strict_private_borrower_bridge_context`, `strict_nonfinancial_corporate_bridge_context`, and `strict_private_offset_residual_context` alongside `strict_di_bucket_role_context` and `strict_loan_core_redesign_context`.

### `site/data/sample_construction_summary.json`

Mirror of `output/models/sample_construction_summary.json`.
