from __future__ import annotations

import pandas as pd

from tdcpass.analysis.accounting import AccountingSummary
from tdcpass.analysis.result_readiness import build_result_readiness_summary
from tdcpass.analysis.structural_proxy_evidence import build_structural_proxy_evidence


def test_structural_proxy_evidence_prefers_exact_identity_baseline_when_available() -> None:
    approx_lp = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 40},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 2.0, "se": 0.5, "lower95": 1.0, "upper95": 3.0, "n": 40},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -5.0, "se": 1.0, "lower95": -7.0, "upper95": -3.0, "n": 40},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 2.0, "se": 0.5, "lower95": 1.0, "upper95": 3.0, "n": 40},
        ]
    )

    frame, summary = build_structural_proxy_evidence(
        lp_irf=approx_lp,
        identity_lp_irf=identity_lp,
        horizons=(0,),
    )

    assert summary["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert summary["estimation_path"]["primary_artifact"] == "lp_irf_identity_baseline.csv"
    assert summary["key_horizons"]["h0"]["interpretation"] == "proxy_evidence_discordant"
    assert frame["other_beta"].dropna().unique().tolist() == [-5.0]


def test_result_readiness_prefers_identity_variant_artifacts_when_available() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.1, "se": 0.2, "lower95": 0.7, "upper95": 1.5, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.3, "se": 0.2, "lower95": -0.7, "upper95": 0.1, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 1.3, "se": 0.2, "lower95": 0.9, "upper95": 1.7, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 0.7, "se": 0.2, "lower95": 0.3, "upper95": 1.1, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -0.6, "se": 0.2, "lower95": -1.0, "upper95": -0.2, "n": 36},
        ]
    )
    shocks = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_residual_z": [0.2, -0.3, 0.1, -0.2],
            "shock_flag": ["", "", "", ""],
        }
    )
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0},
            {"treatment_variant": "alt", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0},
        ]
    )
    identity_sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8},
        ]
    )
    control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0},
            {"control_variant": "alt_controls", "control_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0},
        ]
    )
    identity_control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8},
        ]
    )
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.2},
        ]
    )
    identity_sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.7},
        ]
    )

    payload = build_result_readiness_summary(
        accounting_summary=AccountingSummary(
            mean_tdc=1.0,
            mean_total_deposits=0.6,
            mean_other_component=-0.4,
            share_other_negative=0.7,
            correlation_tdc_total=0.3,
            correlation_tdc_other=-0.2,
        ),
        shocks=shocks,
        lp_irf=lp_irf,
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sensitivity=sensitivity,
        identity_sensitivity=identity_sensitivity,
        control_sensitivity=control_sensitivity,
        identity_control_sensitivity=identity_control_sensitivity,
        sample_sensitivity=sample_sensitivity,
        identity_sample_sensitivity=identity_sample_sensitivity,
    )

    assert payload["estimation_path"]["treatment_variant_artifact"] == "identity_treatment_sensitivity.csv"
    assert payload["estimation_path"]["control_variant_artifact"] == "identity_control_sensitivity.csv"
    assert payload["estimation_path"]["sample_variant_artifact"] == "identity_sample_sensitivity.csv"
    assert payload["diagnostics"]["sensitivity_variant_count"] == 1
    assert payload["diagnostics"]["control_set_variant_count"] == 1
    assert payload["diagnostics"]["sample_sensitivity_variant_count"] == 2
    assert payload["diagnostics"]["control_set_sign_disagreement"] is False
    assert payload["diagnostics"]["sample_variant_sign_disagreement"] is False
