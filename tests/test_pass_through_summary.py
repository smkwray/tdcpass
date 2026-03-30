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
            {"treatment_variant": "baseline", "treatment_role": "core", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"treatment_variant": "baseline", "treatment_role": "core", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"treatment_variant": "baseline", "treatment_role": "core", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
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
    assert payload["baseline_horizons"]["h0"]["assessment"] in {"crowd_out_signal", "total_up_other_unclear", "not_separated"}
    assert payload["baseline_horizons"]["h0"]["direct_tdc_response"]["beta"] == 1.4
    assert payload["baseline_horizons"]["h0"]["contrast_consistent"] is True
    assert payload["core_treatment_variants"]
    assert payload["core_control_variants"]
    assert payload["shock_sample_variants"][0]["sample_variant"] == "all_usable_shocks"
    assert payload["structural_proxy_context"]["h0"]["interpretation"] == "proxy_evidence_weak"
    assert payload["proxy_coverage_context"]["status"] == "mixed"
    assert payload["proxy_coverage_context"]["release_caveat"] == "stub caveat"
    assert [row["regime"] for row in payload["published_regime_contexts"]] == ["reserve_drain"]
    assert payload["published_regime_contexts"][0]["stable_for_interpretation"] is True
    assert payload["readiness_reasons"] == ["stub"]
    assert payload["readiness_warnings"] == ["warn"]
