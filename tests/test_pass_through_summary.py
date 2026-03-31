from __future__ import annotations

import pandas as pd

from tdcpass.analysis.pass_through_summary import build_pass_through_summary


def test_pass_through_summary_builds_release_answer_object() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 2.8, "se": 0.7, "lower95": 1.43, "upper95": 4.17, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 0.8, "lower95": 0.43, "upper95": 3.57, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -0.8, "se": 0.8, "lower95": -2.37, "upper95": 0.77, "n": 36},
        ]
    )
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
        ]
    )
    control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
            {"control_variant": "lagged_macro_plus_bill", "control_role": "core", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.3, "se": 0.4, "lower95": 0.52, "upper95": 2.08, "n": 40},
            {"control_variant": "lagged_macro_plus_bill", "control_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.1, "se": 0.5, "lower95": 0.12, "upper95": 2.08, "n": 40},
            {"control_variant": "lagged_macro_plus_bill", "control_role": "core", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.2, "se": 0.5, "lower95": -1.18, "upper95": 0.78, "n": 40},
        ]
    )
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
        ]
    )
    contrast = pd.DataFrame(
        [
            {"scope": "baseline", "variant": "baseline", "role": "headline", "horizon": 0, "beta_total": 1.0, "beta_other": -0.4, "beta_implied": 1.4, "beta_direct": 1.4, "gap_implied_minus_direct": 0.0, "abs_gap": 0.0, "n_total": 40, "n_other": 40, "n_direct": 40, "sample_mismatch_flag": False, "contrast_consistent": True},
            {"scope": "baseline", "variant": "baseline", "role": "headline", "horizon": 4, "beta_total": 2.0, "beta_other": -0.8, "beta_implied": 2.8, "beta_direct": 2.8, "gap_implied_minus_direct": 0.0, "abs_gap": 0.0, "n_total": 36, "n_other": 36, "n_direct": 36, "sample_mismatch_flag": False, "contrast_consistent": True},
        ]
    )
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "bank_absorption_high", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 10.0, "se": 1.0, "lower95": 8.04, "upper95": 11.96, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 100.0, "se": 10.0, "lower95": 80.4, "upper95": 119.6, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.04, "upper95": 3.96, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.2, "se": 1.0, "lower95": -1.76, "upper95": 2.16, "n": 20},
        ]
    )
    readiness = {"status": "provisional", "reasons": ["stub"], "warnings": ["warn"]}
    regime_diagnostics = {
        "regimes": [
            {
                "regime": "bank_absorption",
                "stable_for_interpretation": False,
                "stability_warnings": ["low_state_shock_support_is_thin"],
            },
            {
                "regime": "reserve_drain",
                "stable_for_interpretation": True,
                "stability_warnings": [],
            }
        ]
    }
    regime_specs = {
        "regimes": {
            "bank_absorption": {"publication_role": "diagnostic_only"},
            "reserve_drain": {"publication_role": "published"},
        }
    }

    payload = build_pass_through_summary(
        lp_irf=lp_irf,
        sensitivity=sensitivity,
        control_sensitivity=control_sensitivity,
        sample_sensitivity=sample_sensitivity,
        contrast=contrast,
        lp_irf_regimes=lp_irf_regimes,
        readiness=readiness,
        regime_diagnostics=regime_diagnostics,
        regime_specs=regime_specs,
        structural_proxy_evidence={"key_horizons": {"h0": {"interpretation": "proxy_evidence_weak"}}},
        proxy_coverage_summary={
            "status": "mixed",
            "key_horizons": {"h0": {"coverage_label": "proxy_bundle_same_sign_but_not_decisive"}},
            "published_regime_contexts": [{"regime": "reserve_drain"}],
            "release_caveat": "stub caveat",
        },
    )

    assert payload["status"] == "provisional"
    assert payload["estimation_path"]["primary_decomposition_mode"] == "approximate_dynamic_decomposition"
    assert payload["estimation_path"]["approximate_dynamic_robustness"]["status"] == "primary_check"
    assert payload["baseline_horizons"]["h0"]["assessment"] in {"crowd_out_signal", "total_up_other_unclear", "not_separated"}
    assert payload["baseline_horizons"]["h0"]["direct_tdc_response"]["beta"] == 1.4
    assert payload["baseline_horizons"]["h0"]["approximate_dynamic_contrast_consistent"] is True
    assert payload["core_treatment_variants"]
    assert payload["measurement_treatment_variants"] == []
    assert payload["shock_design_treatment_variants"] == []
    assert payload["core_control_variants"]
    assert payload["shock_sample_variants"][0]["sample_variant"] == "all_usable_shocks"
    assert payload["flagged_window_robustness"]["status"] == "not_available"
    assert payload["structural_proxy_context"]["h0"]["interpretation"] == "proxy_evidence_weak"
    assert payload["proxy_coverage_context"]["status"] == "mixed"
    assert payload["proxy_coverage_context"]["release_caveat"] == "stub caveat"
    assert [row["regime"] for row in payload["published_regime_contexts"]] == ["reserve_drain"]
    assert payload["published_regime_contexts"][0]["stable_for_interpretation"] is True
    assert payload["readiness_reasons"] == ["stub"]
    assert payload["readiness_warnings"] == ["warn"]


def test_pass_through_summary_surfaces_under_review_treatment_status() -> None:
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sensitivity=pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={
            "status": "not_ready",
            "treatment_freeze_status": "under_review",
            "treatment_candidates": [{"name": "unexpected_tdc_bank_only_macro_rolling40"}],
            "ratio_reporting_gate": {"rule": ["stub"]},
            "reasons": ["The baseline unexpected-TDC shock is not yet a credibly frozen treatment object."],
            "warnings": [],
        },
    )

    assert payload["treatment_freeze_status"] == "under_review"
    assert payload["treatment_candidates"][0]["name"] == "unexpected_tdc_bank_only_macro_rolling40"
    assert "under review" in payload["headline_answer"]


def test_pass_through_summary_prefers_exact_identity_baseline_when_available() -> None:
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.9, "se": 0.2, "lower95": 0.5, "upper95": 1.3, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.6, "se": 0.2, "lower95": -1.0, "upper95": -0.2, "n": 38},
        ]
    )
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        identity_lp_irf=identity_lp,
        sensitivity=pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
    )

    assert payload["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert payload["estimation_path"]["approximate_dynamic_robustness"]["status"] == "not_available"
    assert payload["baseline_horizons"]["h0"]["direct_tdc_response"]["beta"] == 1.5


def test_pass_through_summary_surfaces_failed_treatment_quality_gate() -> None:
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sensitivity=pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={
            "status": "not_ready",
            "treatment_freeze_status": "frozen",
            "treatment_quality_status": "fail",
            "treatment_quality_gate": {"failed_checks": ["max_realized_scale_ratio_p95"]},
            "reasons": ["The frozen baseline unexpected-TDC shock still fails its publishable shock-quality gate."],
            "warnings": [],
        },
    )

    assert payload["treatment_quality_status"] == "fail"
    assert payload["treatment_quality_gate"]["failed_checks"] == ["max_realized_scale_ratio_p95"]
    assert "quality gate" in payload["headline_answer"]


def test_pass_through_summary_promotes_flagged_window_robustness_note() -> None:
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 2.8, "se": 0.7, "lower95": 1.43, "upper95": 4.17, "n": 36},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 0.8, "lower95": 0.43, "upper95": 3.57, "n": 36},
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "other_component_qoq", "horizon": 4, "beta": -0.8, "se": 0.8, "lower95": -2.37, "upper95": 0.77, "n": 36},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.4, "lower95": 0.72, "upper95": 2.28, "n": 38},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.1, "se": 0.5, "lower95": 0.12, "upper95": 2.08, "n": 38},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.5, "se": 0.5, "lower95": -1.48, "upper95": 0.48, "n": 38},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 2.9, "se": 0.7, "lower95": 1.53, "upper95": 4.27, "n": 34},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 1.8, "se": 0.8, "lower95": 0.23, "upper95": 3.37, "n": 34},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "other_component_qoq", "horizon": 4, "beta": -0.6, "se": 0.8, "lower95": -2.17, "upper95": 0.97, "n": 34},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.45, "se": 0.4, "lower95": 0.67, "upper95": 2.23, "n": 39},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.05, "se": 0.5, "lower95": 0.07, "upper95": 2.03, "n": 39},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.45, "se": 0.5, "lower95": -1.43, "upper95": 0.53, "n": 39},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 2.85, "se": 0.7, "lower95": 1.48, "upper95": 4.22, "n": 35},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 1.9, "se": 0.8, "lower95": 0.33, "upper95": 3.47, "n": 35},
            {"sample_variant": "drop_severe_scale_tail", "sample_role": "exploratory", "sample_filter": "fitted_to_target_scale_ratio<=25.0", "outcome": "other_component_qoq", "horizon": 4, "beta": -0.7, "se": 0.8, "lower95": -2.27, "upper95": 0.87, "n": 35},
        ]
    )
    contrast = pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"])

    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sensitivity=pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=sample_sensitivity,
        contrast=contrast,
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
    )

    assert payload["flagged_window_robustness"]["status"] == "stable"
    assert payload["flagged_window_robustness"]["headline_sign_pattern_stable"] is True
    assert "does not overturn the headline h0/h4 sign pattern" in payload["flagged_window_robustness"]["note"]
