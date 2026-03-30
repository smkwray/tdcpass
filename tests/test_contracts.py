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
        "output/models/lp_irf_regimes.csv",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/manifests/raw_downloads.json",
        "output/manifests/reused_artifacts.json",
        "output/manifests/pipeline_run.json",
        "site/data/overview.json",
        "site/data/accounting_summary.csv",
        "site/data/quarters_tdc_exceeds_total.csv",
        "site/data/unexpected_tdc.csv",
        "site/data/lp_irf.csv",
        "site/data/lp_irf_regimes.csv",
        "site/data/regime_diagnostics_summary.json",
        "site/data/tdc_sensitivity_ladder.csv",
        "site/data/control_set_sensitivity.csv",
        "site/data/shock_sample_sensitivity.csv",
        "site/data/total_minus_other_contrast.csv",
        "site/data/structural_proxy_evidence.csv",
        "site/data/structural_proxy_evidence_summary.json",
        "site/data/proxy_coverage_summary.json",
        "site/data/proxy_unit_audit.json",
        "site/data/shock_diagnostics_summary.json",
        "site/data/direct_identification_summary.json",
        "site/data/result_readiness_summary.json",
        "site/data/pass_through_summary.json",
    }
    assert expected.issubset(paths)


def test_contract_freezes_canonical_aliases_and_shock_column() -> None:
    payload = load_yaml(repo_root() / "config" / "output_contract.yml")
    assert payload["canonical_aliases"]["tdc_qoq"] == "tdc_bank_only_qoq"
    assert payload["canonical_aliases"]["total_deposits_qoq"] == "total_deposits_bank_qoq"
    assert payload["shock_column"] == "tdc_residual_z"
    panel_artifact = next(item for item in payload["artifacts"] if item["path"] == "data/derived/quarterly_panel.csv")
    assert "bank_credit_private_qoq" not in panel_artifact["headline_sample_columns"]


def test_shock_and_lp_specs_use_canonical_names() -> None:
    shock_specs = load_yaml(repo_root() / "config" / "shock_specs.yml")
    lp_specs = load_yaml(repo_root() / "config" / "lp_specs.yml")

    default_shock = shock_specs["shocks"]["unexpected_tdc_default"]
    assert default_shock["target"] == "tdc_bank_only_qoq"
    assert default_shock["standardized_column"] == "tdc_residual_z"
    assert default_shock["fitted_column"] == "tdc_fitted"
    assert "lag_tdc_bank_only_qoq" in default_shock["predictors"]
    assert "lag_total_deposits_bank_qoq" not in default_shock["predictors"]
    assert "lag_bank_credit_private_qoq" not in default_shock["predictors"]

    baseline = lp_specs["specs"]["baseline"]
    assert baseline["shock_column"] == "tdc_residual_z"
    assert "tdc_bank_only_qoq" in baseline["outcomes"]
    assert "total_deposits_bank_qoq" in baseline["outcomes"]
    assert baseline["controls"] == ["lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["regimes"]["controls"] == ["lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["regimes"]["regime_columns"] == [
        "bank_absorption_share",
        "bill_share",
        "reserve_drain_pressure",
    ]
    assert lp_specs["specs"]["sensitivity"]["controls"] == ["lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["headline_lagged_macro"]["control_role"] == "headline"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_bill"]["control_role"] == "core"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_trend"]["control_role"] == "exploratory"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["all_usable_shocks"]["sample_role"] == "headline"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_flagged_shocks"]["sample_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["baseline"]["treatment_role"] == "core"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_long_burnin"]["treatment_role"] == "core"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_no_bill_share"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_totaldep_long_burnin"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["broad_depository"]["treatment_role"] == "exploratory"


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
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "proxy_coverage_context",
        "treatment_role",
        "control_role",
        "output/manifests/raw_downloads.json",
        "site/data/tdc_sensitivity_ladder.csv",
        "site/data/control_set_sensitivity.csv",
        "site/data/shock_sample_sensitivity.csv",
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
        "headline sample",
    ]:
        assert needle in text
