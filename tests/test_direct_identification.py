from __future__ import annotations

import pandas as pd

from tdcpass.analysis.direct_identification import (
    build_direct_identification_summary,
    build_total_minus_other_contrast,
)


def test_total_minus_other_contrast_tracks_direct_tdc_response() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
        ]
    )

    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )

    row = contrast.iloc[0].to_dict()
    assert row["scope"] == "baseline"
    assert row["beta_implied"] == 1.4
    assert row["beta_direct"] == 1.4
    assert row["gap_implied_minus_direct"] == 0.0
    assert row["identity_check_mode"] == "exact_accounting_identity"
    assert row["contrast_consistent"] is True


def test_total_minus_other_contrast_can_append_exact_identity_scope() -> None:
    approx_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.9, "se": 0.2, "lower95": 0.5, "upper95": 1.3, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.6, "se": 0.2, "lower95": -1.0, "upper95": -0.2, "n": 38},
        ]
    )

    contrast = build_total_minus_other_contrast(
        lp_irf=approx_lp,
        identity_lp_irf=identity_lp,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
        identity_check_mode="approximate_with_outcome_specific_lags",
    )

    assert set(contrast["scope"]) == {"baseline", "exact_identity_baseline"}
    exact_row = contrast[contrast["scope"] == "exact_identity_baseline"].iloc[0].to_dict()
    assert exact_row["identity_check_mode"] == "exact_identity_baseline"
    assert exact_row["beta_direct"] == 1.5
    assert exact_row["beta_implied"] == 1.5
    assert exact_row["contrast_consistent"] is True


def test_direct_identification_summary_marks_weak_first_stage_as_not_ready() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.03, "se": 0.04, "lower95": -0.05, "upper95": 0.11, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.2, "se": 1.0, "lower95": -0.76, "upper95": 3.16, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 1.1, "se": 1.0, "lower95": -0.86, "upper95": 3.06, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 0.02, "se": 0.05, "lower95": -0.08, "upper95": 0.12, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 2.0, "lower95": -1.92, "upper95": 5.92, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 2.1, "se": 2.0, "lower95": -1.82, "upper95": 6.02, "n": 36},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.2, "se": 1.0, "lower95": -0.76, "upper95": 3.16, "n": 40},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 4.0, "se": 2.0, "lower95": 0.08, "upper95": 7.92, "n": 38},
        ]
    )

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=sample_sensitivity,
    )

    assert payload["status"] == "not_ready"
    assert payload["first_stage_checks"]["tdc_ci_excludes_zero_at_h0_or_h4"] is False
    assert payload["sample_fragility"]["impact_magnitude_shift_gt_100pct"] is True
    assert any("move TDC itself" in item for item in payload["reasons"])


def test_direct_identification_suppresses_ratios_when_raw_tdc_beta_is_too_small() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.5, "se": 0.1, "lower95": 0.3, "upper95": 0.7, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 2.0, "se": 0.5, "lower95": 1.02, "upper95": 2.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 1.5, "se": 0.5, "lower95": 0.52, "upper95": 2.48, "n": 40},
        ]
    )
    raw_tdc_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.2, "se": 0.05, "lower95": 0.1, "upper95": 0.3, "n": 40},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )
    shocks = pd.DataFrame(
        {
            "quarter": ["2015Q1", "2015Q2", "2015Q3", "2015Q4"],
            "tdc_residual_z": [0.1, -0.1, 0.2, -0.2],
            "tdc_bank_only_qoq": [1.0, -1.0, 1.0, -1.0],
        }
    )

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(),
        shock_metadata={"target": "tdc_bank_only_qoq"},
        shocks=shocks,
        raw_tdc_lp=raw_tdc_lp,
    )

    assert payload["ratio_reporting_gate"]["horizons"]["h0"]["allowed"] is False
    assert payload["ratio_reporting_gate"]["horizons"]["h0"]["conditions"]["tdc_ci_excludes_zero"] is True
    assert payload["ratio_reporting_gate"]["horizons"]["h0"]["conditions"]["dimensionally_coherent_denominator_gate_resolved"] is False
    assert "out of scope in the current release" in payload["ratio_reporting_gate"]["horizons"]["h0"]["explanation"]
    assert payload["horizon_evidence"]["h0"]["pass_through_ratio_total_over_tdc"] is None
    assert payload["horizon_evidence"]["h0"]["crowd_out_ratio_neg_other_over_tdc"] is None


def test_direct_identification_downgrades_to_provisional_when_secondary_check_diverges() -> None:
    approx_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.04, "upper95": 13.96, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.04, "upper95": 4.96, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -3.96, "upper95": -0.04, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 20.0, "se": 2.0, "lower95": 16.08, "upper95": 23.92, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 5.0, "se": 2.0, "lower95": 1.08, "upper95": 8.92, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -4.0, "se": 2.0, "lower95": -7.92, "upper95": -0.08, "n": 36},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.7, "se": 0.2, "lower95": -1.1, "upper95": -0.3, "n": 38},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 2.0, "se": 0.2, "lower95": 1.6, "upper95": 2.4, "n": 34},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 0.9, "se": 0.2, "lower95": 0.5, "upper95": 1.3, "n": 34},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -1.1, "se": 0.2, "lower95": -1.5, "upper95": -0.7, "n": 34},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=approx_lp,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
        identity_check_mode="approximate_with_outcome_specific_lags",
    )
    contrast.loc[:, "contrast_consistent"] = False

    payload = build_direct_identification_summary(
        lp_irf=approx_lp,
        identity_lp_irf=identity_lp,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(),
    )

    assert payload["status"] == "provisional"
    assert payload["answer_ready"] is False
    assert any("secondary approximate dynamic path still diverges materially" in item for item in payload["warnings"])


def test_direct_identification_treats_tiny_contrast_gaps_as_numeric_noise() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.3, "lower95": 0.81, "upper95": 1.99, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.39, "se": 0.5, "lower95": -1.37, "upper95": 0.59, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 1.2, "se": 0.3, "lower95": 0.61, "upper95": 1.79, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 0.9, "se": 0.6, "lower95": -0.28, "upper95": 2.08, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -0.28, "se": 0.6, "lower95": -1.46, "upper95": 0.90, "n": 36},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )

    assert contrast["contrast_consistent"].all()

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(),
    )

    assert not any("numeric tolerance" in item for item in payload["warnings"])


def test_direct_identification_labels_lp_contrast_as_approximate_when_configured() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 10.0, "se": 1.0, "lower95": 8.04, "upper95": 11.96, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.04, "upper95": 4.96, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -3.96, "upper95": -0.04, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 15.0, "se": 2.0, "lower95": 11.08, "upper95": 18.92, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 4.0, "se": 2.0, "lower95": 0.08, "upper95": 7.92, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -3.0, "se": 2.0, "lower95": -6.92, "upper95": 0.92, "n": 36},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
        identity_check_mode="approximate_with_outcome_specific_lags",
    )
    contrast.loc[:, "contrast_consistent"] = False

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(),
    )

    assert payload["estimation_path"]["primary_decomposition_mode"] == "approximate_dynamic_decomposition"
    assert payload["estimation_path"]["approximate_dynamic_robustness"]["status"] == "primary_check"
    assert payload["contrast_check"]["identity_check_mode"] == "approximate_with_outcome_specific_lags"
    assert "outcome-specific lagged dependent variables" in payload["contrast_check"]["explanation"]
    assert any("approximate LP cross-check" in item for item in payload["warnings"])


def test_direct_identification_prefers_exact_identity_baseline_when_available() -> None:
    approx_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.04, "upper95": 13.96, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.04, "upper95": 4.96, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -3.96, "upper95": -0.04, "n": 40},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.7, "se": 0.2, "lower95": -1.1, "upper95": -0.3, "n": 38},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=approx_lp,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
        identity_check_mode="approximate_with_outcome_specific_lags",
    )
    contrast.loc[:, "contrast_consistent"] = False

    payload = build_direct_identification_summary(
        lp_irf=approx_lp,
        identity_lp_irf=identity_lp,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(),
    )

    assert payload["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert payload["estimation_path"]["approximate_dynamic_robustness"]["status"] == "divergent_secondary_check"
    assert payload["horizon_evidence"]["h0"]["tdc"]["beta"] == 1.5


def test_direct_identification_prefers_exact_sample_sensitivity_when_available() -> None:
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.7, "se": 0.2, "lower95": -1.1, "upper95": -0.3, "n": 38},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=pd.DataFrame(columns=identity_lp.columns),
        identity_lp_irf=identity_lp,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
        identity_check_mode="approximate_with_outcome_specific_lags",
    )
    payload = build_direct_identification_summary(
        lp_irf=pd.DataFrame(columns=identity_lp.columns),
        identity_lp_irf=identity_lp,
        contrast=contrast,
        sample_sensitivity=pd.DataFrame(
            [{"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 99.0, "se": 1.0, "lower95": 97.0, "upper95": 101.0, "n": 30}]
        ),
        identity_sample_sensitivity=pd.DataFrame(
            [{"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38}]
        ),
    )

    assert payload["estimation_path"]["sample_variant_artifact"] == "identity_sample_sensitivity.csv"
    assert not any("exact identity-preserving baseline is primary" in item for item in payload["warnings"])


def test_direct_identification_prefers_identity_sample_sensitivity_when_available() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.2, "se": 0.2, "lower95": 0.8, "upper95": 1.6, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.2, "se": 0.2, "lower95": -0.6, "upper95": 0.2, "n": 40},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 3.2, "se": 0.3, "lower95": 2.6, "upper95": 3.8, "n": 38},
        ]
    )
    identity_sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.2, "se": 0.2, "lower95": 0.8, "upper95": 1.6, "n": 38},
        ]
    )

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=sample_sensitivity,
        identity_sample_sensitivity=identity_sample_sensitivity,
    )

    assert payload["estimation_path"]["sample_variant_artifact"] == "identity_sample_sensitivity.csv"
    assert payload["sample_fragility"]["impact_magnitude_shift_gt_100pct"] is False
