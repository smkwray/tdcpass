from __future__ import annotations

import json
from pathlib import Path

from tdcpass.core.yaml_utils import load_yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_output_contract_has_required_artifacts() -> None:
    payload = load_yaml(repo_root() / "config" / "output_contract.yml")
    artifacts = payload.get("artifacts", [])
    paths = {row["path"] for row in artifacts}
    expected = {
        "data/derived/quarterly_panel.csv",
        "output/accounting/accounting_summary.csv",
        "output/accounting/quarters_tdc_exceeds_total.csv",
        "output/shocks/unexpected_tdc.csv",
        "output/models/lp_irf.csv",
        "output/models/lp_irf_identity_baseline.csv",
        "output/models/lp_irf_regimes.csv",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/period_sensitivity.csv",
        "output/models/period_sensitivity_summary.json",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/headline_treatment_fingerprint.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/sample_construction_summary.json",
        "output/manifests/raw_downloads.json",
        "output/manifests/reused_artifacts.json",
        "output/manifests/pipeline_run.json",
        "site/data/overview.json",
        "site/data/accounting_summary.csv",
        "site/data/quarters_tdc_exceeds_total.csv",
        "site/data/unexpected_tdc.csv",
        "site/data/lp_irf.csv",
        "site/data/lp_irf_identity_baseline.csv",
        "site/data/lp_irf_regimes.csv",
        "site/data/regime_diagnostics_summary.json",
        "site/data/tdc_sensitivity_ladder.csv",
        "site/data/control_set_sensitivity.csv",
        "site/data/shock_sample_sensitivity.csv",
        "site/data/period_sensitivity.csv",
        "site/data/period_sensitivity_summary.json",
        "site/data/total_minus_other_contrast.csv",
        "site/data/structural_proxy_evidence.csv",
        "site/data/structural_proxy_evidence_summary.json",
        "site/data/proxy_coverage_summary.json",
        "site/data/proxy_unit_audit.json",
        "site/data/headline_treatment_fingerprint.json",
        "site/data/shock_diagnostics_summary.json",
        "site/data/direct_identification_summary.json",
        "site/data/result_readiness_summary.json",
        "site/data/pass_through_summary.json",
        "site/data/sample_construction_summary.json",
    }
    assert expected.issubset(paths)


def test_contract_freezes_canonical_aliases_and_shock_column() -> None:
    payload = load_yaml(repo_root() / "config" / "output_contract.yml")
    assert payload["canonical_aliases"]["tdc_qoq"] == "tdc_bank_only_qoq"
    assert payload["canonical_aliases"]["total_deposits_qoq"] == "total_deposits_bank_qoq"
    assert payload["shock_column"] == "tdc_residual_z"
    panel_artifact = next(item for item in payload["artifacts"] if item["path"] == "data/derived/quarterly_panel.csv")
    assert "bank_credit_private_qoq" not in panel_artifact["headline_sample_columns"]
    assert "tdc_domestic_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_no_remit_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_credit_union_sensitive_qoq" in panel_artifact["required_columns"]


def test_shock_and_lp_specs_use_canonical_names() -> None:
    shock_specs = load_yaml(repo_root() / "config" / "shock_specs.yml")
    lp_specs = load_yaml(repo_root() / "config" / "lp_specs.yml")

    default_shock = shock_specs["shocks"]["unexpected_tdc_default"]
    assert default_shock["target"] == "tdc_bank_only_qoq"
    assert default_shock["method"] == "rolling_window_ridge"
    assert default_shock["freeze_status"] == "frozen"
    assert default_shock["ridge_alpha"] == 125.0
    assert default_shock["max_train_obs"] == 40
    assert default_shock["quality_gate"]["min_usable_observations"] == 60
    assert default_shock["quality_gate"]["min_shock_target_correlation"] == 0.5
    assert default_shock["quality_gate"]["max_realized_scale_ratio_p95"] == 25.0
    assert default_shock["quality_gate"]["max_realized_scale_ratio_p99"] == 100.0
    assert default_shock["standardized_column"] == "tdc_residual_z"
    assert default_shock["fitted_column"] == "tdc_fitted"
    assert "lag_tdc_bank_only_qoq" in default_shock["predictors"]
    assert "lag_bill_share" not in default_shock["predictors"]
    assert "lag_total_deposits_bank_qoq" not in default_shock["predictors"]
    assert "lag_bank_credit_private_qoq" not in default_shock["predictors"]

    baseline = lp_specs["specs"]["baseline"]
    assert baseline["shock_column"] == "tdc_residual_z"
    assert "tdc_bank_only_qoq" in baseline["outcomes"]
    assert "total_deposits_bank_qoq" in baseline["outcomes"]
    assert baseline["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert baseline["include_lagged_outcome"] is True
    assert lp_specs["specs"]["regimes"]["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["regimes"]["include_lagged_outcome"] is True
    assert lp_specs["specs"]["regimes"]["regime_columns"] == [
        "reserve_drain_pressure",
    ]
    assert lp_specs["specs"]["sensitivity"]["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["sensitivity"]["include_lagged_outcome"] is True
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["headline_lagged_macro"]["control_role"] == "headline"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_bill"]["control_role"] == "core"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_trend"]["control_role"] == "exploratory"
    macro_factor_spec = lp_specs["specs"]["factor_control_sensitivity"]["factor_variants"]["recursive_macro_factors2"]
    assert macro_factor_spec["factor_role"] == "core"
    assert macro_factor_spec["factor_count"] == 2
    assert macro_factor_spec["min_train_obs"] == 24
    assert macro_factor_spec["source_columns"] == [
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    plumbing_factor_spec = lp_specs["specs"]["factor_control_sensitivity"]["factor_variants"][
        "recursive_macro_plumbing_factors3"
    ]
    assert plumbing_factor_spec["factor_role"] == "exploratory"
    assert plumbing_factor_spec["factor_count"] == 3
    assert plumbing_factor_spec["min_train_obs"] == 40
    assert plumbing_factor_spec["source_columns"] == [
        "lag_tga_qoq",
        "lag_reserves_qoq",
        "lag_bill_share",
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    smooth_lp = lp_specs["specs"]["smooth_lp"]
    assert smooth_lp["method"] == "gaussian_kernel"
    assert smooth_lp["bandwidth"] == 1.0
    assert smooth_lp["min_horizons"] == 4
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["all_usable_shocks"]["sample_role"] == "headline"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_flagged_shocks"]["sample_role"] == "exploratory"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_severe_scale_tail"]["sample_role"] == "exploratory"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_severe_scale_tail"]["max_value_column"] == "fitted_to_target_scale_ratio"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["all_usable"]["period_role"] == "headline"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["post_gfc_early"]["start_quarter"] == "2009Q1"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["post_gfc_early"]["end_quarter"] == "2014Q4"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["pre_covid"]["period_role"] == "core"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["covid_post"]["start_quarter"] == "2020Q1"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["baseline"]["treatment_role"] == "core"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["baseline"]["treatment_family"] == "headline"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_long_burnin"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_long_burnin"]["treatment_family"] == "shock_design"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_no_bill_share"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_billshare_macro_rolling40"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_rolling40_ols"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_billshare_expanding"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_totaldep_long_burnin"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["broad_depository"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["broad_depository"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["domestic_bank_only"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["no_remit_bank_only"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["credit_union_sensitive"]["treatment_family"] == "measurement"
    assert shock_specs["shocks"]["unexpected_tdc_domestic_bank_only"]["target"] == "tdc_domestic_bank_only_qoq"
    assert shock_specs["shocks"]["unexpected_tdc_no_remit_bank_only"]["target"] == "tdc_no_remit_bank_only_qoq"
    assert shock_specs["shocks"]["unexpected_tdc_credit_union_sensitive"]["target"] == "tdc_credit_union_sensitive_qoq"


def test_output_schema_mentions_full_bundle() -> None:
    text = (repo_root() / "docs" / "output_schema.md").read_text(encoding="utf-8")
    for needle in [
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "output/models/lp_irf_regimes.csv",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/period_sensitivity.csv",
        "output/models/period_sensitivity_summary.json",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/sample_construction_summary.json",
        "proxy_coverage_context",
        "treatment_role",
        "control_role",
        "output/manifests/raw_downloads.json",
        "site/data/tdc_sensitivity_ladder.csv",
        "site/data/control_set_sensitivity.csv",
        "site/data/shock_sample_sensitivity.csv",
        "site/data/period_sensitivity.csv",
        "site/data/period_sensitivity_summary.json",
        "site/data/total_minus_other_contrast.csv",
        "site/data/structural_proxy_evidence.csv",
        "site/data/structural_proxy_evidence_summary.json",
        "site/data/proxy_coverage_summary.json",
        "site/data/proxy_unit_audit.json",
        "site/data/regime_diagnostics_summary.json",
        "site/data/shock_diagnostics_summary.json",
        "site/data/direct_identification_summary.json",
        "site/data/result_readiness_summary.json",
        "site/data/pass_through_summary.json",
        "site/data/sample_construction_summary.json",
        "headline sample",
    ]:
        assert needle in text
