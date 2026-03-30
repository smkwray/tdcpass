# Output schema

Canonical exported names follow the registry contract.
Short aliases such as `tdc_qoq` and `total_deposits_qoq` are optional compatibility aliases only and are not the frozen external artifact contract.
The live quarterly bundle is produced by `tdcpass pipeline run` and writes the bank-only contract to `data/derived/quarterly_panel.csv`, `output/*`, and mirrored `site/data/*` artifacts.

## Required files

### `data/derived/quarterly_panel.csv`

This is the canonical quarterly bank-only panel. The frozen names are the bank-only series and their residual-based treatment inputs:

- `tdc_bank_only_qoq`
- `total_deposits_bank_qoq`
- `other_component_qoq`
- `tdc_residual_z`

Minimum columns:

- `quarter`
- `tdc_bank_only_qoq`
- `total_deposits_bank_qoq`
- `other_component_qoq`
- `bank_credit_private_qoq`
- `cb_nonts_qoq`
- `foreign_nonts_qoq`
- `domestic_nonfinancial_mmf_reallocation_qoq`
- `domestic_nonfinancial_repo_reallocation_qoq`
- `bill_share`
- `bank_absorption_share`
- `reserve_drain_pressure`
- `quarter_index`
- `slr_tight`
- `tga_qoq`
- `reserves_qoq`
- `fedfunds`
- `unemployment`
- `inflation`
- `lag_tdc_bank_only_qoq`
- `lag_total_deposits_bank_qoq`
- `lag_bank_credit_private_qoq`
- `lag_tga_qoq`
- `lag_reserves_qoq`
- `lag_bill_share`
- `lag_fedfunds`
- `lag_unemployment`
- `lag_inflation`

`bill_share` is built from quarterly Treasury auction offering amounts in FiscalData using issue dates. Keep the caveat attached that it is a regime overlay and not standalone mechanism proof.
`reserve_drain_pressure` is a transparent transformation equal to `-lag_reserves_qoq`; it is the current headline liquidity-pressure regime split because it preserves longer public history than `slr_tight`.
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

The shock export now carries numerical-quality diagnostics for each usable expanding-window fit. `shock_flag` marks windows where the fitted path is badly scaled relative to the target or where the training design is numerically weak.

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
In the current live configuration, the release-facing regime layer publishes only `bill_share` and `reserve_drain_pressure`; `bank_absorption_share` and `slr_tight` remain in the panel schema and diagnostics but are not part of the headline regime export.
It also reports whether each regime has enough within-state shock support to avoid highly extrapolative split estimates.

### `output/models/tdc_sensitivity_ladder.csv`

Minimum columns:

- `treatment_variant`
- `treatment_role`
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

This file makes the identity check explicit at the LP layer. `beta_implied` is `beta(total_deposits_bank_qoq) - beta(other_component_qoq)` and `beta_direct` is the direct LP response of `tdc_bank_only_qoq` under the same spec slice. The file is meant to catch cases where the direct treatment response is too weak or where sample mismatches break the accounting comparison.

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

This file summarizes whether structural proxies corroborate the non-TDC residual at key horizons. It is a release gate because the residual alone is not sufficient mechanism evidence.

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
It now also reports baseline shock-quality diagnostics such as flagged windows, maximum fitted-to-target scale ratio, maximum training condition number, and the active baseline shock specification metadata.

### `output/models/direct_identification_summary.json`

Required keys:

- `status`
- `headline_question`
- `shock_definition`
- `horizon_evidence`
- `first_stage_checks`
- `sample_fragility`
- `answer_ready`
- `reasons`
- `warnings`
- `answer_ready_when`

This file is the direct identification gate for the release process. It asks whether the baseline unexpected-TDC shock moves `tdc_bank_only_qoq` itself enough to make pass-through versus crowd-out interpretable, and whether the flagged-window trimming check materially changes the headline answer. The `shock_definition` block should record the active baseline model name, predictor list, and burn-in length so headline shock redesigns remain explicit.

### `output/models/result_readiness_summary.json`

Required keys:

- `status`
- `headline_assessment`
- `reasons`
- `warnings`
- `diagnostics`
- `key_estimates`
- `answer_ready_when`

This file states whether the current backend run is ready to support a deposit-response interpretation and how much mechanism weight, if any, the release bundle can carry beyond that.

### `output/models/pass_through_summary.json`

Required keys:

- `status`
- `headline_question`
- `headline_answer`
- `sample_policy`
- `baseline_horizons`
- `core_treatment_variants`
- `core_control_variants`
- `shock_sample_variants`
- `structural_proxy_context`
- `proxy_coverage_context`
- `published_regime_contexts`
- `readiness_reasons`
- `readiness_warnings`

This file is the release-facing answer layer. It pairs total-deposit and non-TDC responses at headline horizons, carries the direct TDC response and total-minus-other contrast check, includes both structural-proxy direction checks and the broader proxy-coverage context, and states how flagged-window trimming should be interpreted relative to the frozen headline sample.

### `output/models/sample_construction_summary.json`

Required keys:

- `full_panel`
- `headline_sample`
- `usable_shock_sample`
- `shock_definition`
- `headline_sample_truncation`
- `extended_column_coverage`
- `takeaways`

This file makes the sample rule machine-readable. It records the full merged quarterly span before trimming, the narrower headline sample that defines the baseline treatment and deposit-response path, the usable shock count after expanding-window burn-in, and the shorter-history proxy or regime columns that remain partially observed inside the headline sample.
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

- `headline_metrics` should summarize headline accounting quantities and regime context.
- `sample` should state the quarterly sample span and row count.
- `evidence_tiers` should classify direct data, transparent transformations, model-based estimates, and inferred counterfactuals.

### `site/data/accounting_summary.csv`

Mirror of `output/accounting/accounting_summary.csv`.

### `site/data/quarters_tdc_exceeds_total.csv`

Mirror of `output/accounting/quarters_tdc_exceeds_total.csv`.

### `site/data/unexpected_tdc.csv`

Mirror of `output/shocks/unexpected_tdc.csv`.

### `site/data/lp_irf.csv`

Mirror of `output/models/lp_irf.csv`.

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

### `site/data/direct_identification_summary.json`

Mirror of `output/models/direct_identification_summary.json`.

### `site/data/result_readiness_summary.json`

Mirror of `output/models/result_readiness_summary.json`.

### `site/data/pass_through_summary.json`

Mirror of `output/models/pass_through_summary.json`.

### `site/data/sample_construction_summary.json`

Mirror of `output/models/sample_construction_summary.json`.
