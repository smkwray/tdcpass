from __future__ import annotations

import pandas as pd

from tdcpass.analysis.accounting import AccountingSummary
from tdcpass.analysis.result_readiness import build_result_readiness_summary


def test_result_readiness_marks_ambiguous_run_as_not_ready() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame(
        {
            "quarter": ["2016Q1", "2016Q2", "2016Q3"],
            "tdc_residual_z": [0.1, -0.2, 0.3],
        }
    )
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.92, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 3.0, "lower95": -3.88, "upper95": 7.88, "n": 26, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 2.1, "lower95": -3.32, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 1.5, "se": 3.2, "lower95": -4.77, "upper95": 7.77, "n": 26, "spec_name": "baseline"},
        ]
    )
    regimes = pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"])
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "tdc_bank_only_qoq", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.04, "upper95": 3.96, "n": 30, "spec_name": "sensitivity"},
            {"treatment_variant": "tdc_bank_only_long_burnin", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.5, "se": 1.0, "lower95": -0.46, "upper95": 3.46, "n": 30, "spec_name": "sensitivity"},
            {"treatment_variant": "tdc_broad_depository_qoq", "treatment_role": "exploratory", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -2.96, "upper95": 0.96, "n": 30, "spec_name": "sensitivity"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=regimes,
        sensitivity=sensitivity,
    )

    assert payload["status"] == "not_ready"
    assert payload["diagnostics"]["shock_usable_obs"] == 3
    assert payload["diagnostics"]["sensitivity_core_variant_count"] == 2
    assert payload["diagnostics"]["sensitivity_exploratory_variant_count"] == 1
    assert payload["diagnostics"]["exploratory_variant_sign_disagreement"] is False
    assert payload["key_estimates"]["total_deposits_h0"]["ci_excludes_zero"] is False
    assert any("total-deposit response" in item for item in payload["reasons"])
    assert not any("Core sensitivity variants" in item for item in payload["warnings"])
    assert not any("Exploratory sensitivity variants" in item for item in payload["warnings"])


def test_result_readiness_adds_structural_proxy_reason_when_cross_checks_are_weak() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.31, "upper95": 1.09, "n": 30, "spec_name": "baseline"},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.4, "lower95": 0.22, "upper95": 1.78, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 0.3, "lower95": 0.21, "upper95": 1.39, "n": 30, "spec_name": "baseline"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
        structural_proxy_evidence={
            "status": "weak",
            "key_horizons": {
                "h0": {"interpretation": "proxy_evidence_weak"},
                "h4": {"interpretation": "proxy_evidence_weak"},
            },
        },
    )

    assert payload["diagnostics"]["structural_proxy_status"] == "weak"
    assert payload["diagnostics"]["structural_proxy_weak_key_horizons"] == 2
    assert any("Structural proxy cross-checks" in item for item in payload["reasons"])


def test_result_readiness_handles_empty_sensitivity_frame() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.92, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 2.1, "lower95": -3.32, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
    )

    assert payload["status"] == "not_ready"
    assert payload["diagnostics"]["sensitivity_variant_count"] == 0


def test_result_readiness_records_control_set_sign_disagreement() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.92, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 2.1, "lower95": -3.32, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
        ]
    )
    control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -0.96, "upper95": 2.96, "n": 30, "spec_name": "control_sensitivity"},
            {"control_variant": "lagged_macro_plus_bill", "control_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -2.96, "upper95": 0.96, "n": 30, "spec_name": "control_sensitivity"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
        control_sensitivity=control_sensitivity,
    )

    assert payload["diagnostics"]["control_set_variant_count"] == 2
    assert payload["diagnostics"]["control_set_core_variant_count"] == 2
    assert payload["diagnostics"]["control_set_exploratory_variant_count"] == 0
    assert payload["diagnostics"]["control_set_sign_disagreement"] is True
    assert any("Core control-set sensitivity variants" in item for item in payload["warnings"])


def test_result_readiness_records_only_exploratory_control_set_sign_disagreement_in_diagnostics() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.92, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 2.1, "lower95": -3.32, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
        ]
    )
    control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -0.96, "upper95": 2.96, "n": 30, "spec_name": "control_sensitivity"},
            {"control_variant": "lagged_macro_plus_bill", "control_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.1, "se": 1.0, "lower95": -0.86, "upper95": 3.06, "n": 30, "spec_name": "control_sensitivity"},
            {"control_variant": "lagged_macro_plus_trend", "control_role": "exploratory", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -2.96, "upper95": 0.96, "n": 30, "spec_name": "control_sensitivity"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
        control_sensitivity=control_sensitivity,
    )

    assert payload["diagnostics"]["control_set_variant_count"] == 3
    assert payload["diagnostics"]["control_set_core_variant_count"] == 2
    assert payload["diagnostics"]["control_set_exploratory_variant_count"] == 1
    assert payload["diagnostics"]["control_set_sign_disagreement"] is False
    assert payload["diagnostics"]["exploratory_control_set_sign_disagreement"] is True
    assert not any("Core control-set sensitivity variants" in item for item in payload["warnings"])


def test_result_readiness_warns_when_regimes_are_informative_but_support_thin() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.92, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 2.1, "lower95": -3.32, "upper95": 4.92, "n": 30, "spec_name": "baseline"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
        regime_diagnostics={"informative_regime_count": 2, "stable_regime_count": 0, "regimes": [], "takeaways": []},
    )

    assert payload["diagnostics"]["informative_regime_count"] == 2
    assert payload["diagnostics"]["stable_regime_count"] == 0
    assert any("extrapolative" in item for item in payload["warnings"])


def test_result_readiness_adds_proxy_coverage_reason_when_gap_is_large() -> None:
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )
    shocks = pd.DataFrame({"quarter": ["2016Q1"], "tdc_residual_z": [0.1]})
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.31, "upper95": 1.09, "n": 30, "spec_name": "baseline"},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.4, "lower95": 0.22, "upper95": 1.78, "n": 30, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 0.8, "se": 0.3, "lower95": 0.21, "upper95": 1.39, "n": 30, "spec_name": "baseline"},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=accounting,
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(),
        sensitivity=pd.DataFrame(),
        structural_proxy_evidence={
            "status": "supportive",
            "key_horizons": {"h0": {"interpretation": "proxy_evidence_supportive"}},
        },
        proxy_coverage_summary={
            "status": "mixed",
            "key_horizons": {
                "h0": {"coverage_label": "proxy_bundle_same_sign_but_not_decisive"},
                "h4": {"coverage_label": "proxy_bundle_uncovered_remainder_large"},
            },
            "published_regime_contexts": [{"regime": "reserve_drain"}],
        },
    )

    assert payload["diagnostics"]["proxy_coverage_status"] == "mixed"
    assert payload["diagnostics"]["proxy_coverage_large_gap_key_horizons"] == 1
    assert payload["diagnostics"]["proxy_coverage_published_regime_count"] == 1
    assert any("large uncovered remainder" in item for item in payload["reasons"])
