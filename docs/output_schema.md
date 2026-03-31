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
- `tdc_residual_z`

Minimum columns:

- `quarter`
- `tdc_bank_only_qoq`
- `total_deposits_bank_qoq`
- `other_component_qoq`
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
- `lag_total_deposits_bank_qoq`
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

This is the machine-readable freeze record for the current headline unexpected-TDC shock. It should match the frozen default entry in `config/shock_specs.yml` and record the `analysis_source_commit` whose code/config state produced the artifact. That makes the committed public mirror validate against a satisfiable recorded state instead of the containing repo's later `HEAD`.

### `output/models/provenance_validation_summary.json`

Required keys:

- `status`
- `failures`
- `analysis_source_commit_check`
- `config_hashes_check`
- `upstream_input_check`
- `spec_metadata_check`

This file is the recorded-state provenance verdict for the headline treatment fingerprint. It records whether the stored `analysis_source_commit` is present in repo history, whether the stored config hashes still match the current repo config files, and whether the upstream canonical-TDC source can be revalidated against its recorded source commit when the sibling repo is reachable. The publish path can still apply a stricter live current-HEAD gate before mirroring, but the committed public mirror should avoid an unsatisfiable “stored commit must equal containing repo HEAD” rule.

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
- `published_regime_contexts`
- `readiness_reasons`
- `readiness_warnings`

This file is the release-facing answer layer. It pairs total-deposit and non-TDC responses at headline horizons, carries the direct TDC response and total-minus-other contrast check, includes both structural-proxy direction checks and the broader proxy-coverage context, and states how flagged-window trimming should be interpreted relative to the frozen headline sample. It should explicitly separate measurement-variant treatment checks from shock-design treatment checks, identify whether the baseline read is coming from the exact identity-preserving decomposition path, and prefer `identity_measurement_ladder.csv` over the approximate LP sensitivity table for public measurement-variant comparisons when that exact artifact is available.

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

### `site/data/sample_construction_summary.json`

Mirror of `output/models/sample_construction_summary.json`.
