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
        counterpart_channel_scorecard={
            "status": "available",
            "legacy_private_credit_proxy_role": "coarse_legacy_creator_proxy",
            "creator_channel_outcomes_present": ["commercial_industrial_loans_qoq"],
            "key_horizons": {
                "h0": {
                    "other_component": {"beta": -0.4, "ci_excludes_zero": False},
                    "legacy_private_credit_proxy": {
                        "role": "coarse_legacy_creator_proxy",
                        "snapshot": {"beta": 0.2, "ci_excludes_zero": False},
                    },
                    "decisive_positive_creator_channels": [],
                    "decisive_negative_creator_channels": [],
                    "decisive_positive_asset_purchase_channels": ["agency_gse_backed_securities_bank_qoq"],
                    "decisive_positive_retention_support_channels": ["on_rrp_reallocation_qoq"],
                    "decisive_negative_retention_support_channels": [],
                    "escape_support_context": {
                        "interpretation": "deposit_retention_support_signal",
                        "decisive_positive_channels": ["on_rrp_reallocation_qoq"],
                    },
                    "asset_purchase_plumbing_context": {
                        "interpretation": "treasury_drain_context",
                        "treasury_drain_signal": True,
                    },
                    "proxy_coverage_label": "proxy_bundle_same_sign_but_not_decisive",
                },
                "h4": {
                    "other_component": {"beta": -0.8, "ci_excludes_zero": False},
                    "legacy_private_credit_proxy": {
                        "role": "coarse_legacy_creator_proxy",
                        "snapshot": {"beta": 0.1, "ci_excludes_zero": False},
                    },
                    "decisive_positive_creator_channels": ["auto_loans_qoq"],
                    "decisive_negative_creator_channels": [],
                    "proxy_coverage_label": None,
                },
            },
            "takeaways": ["stub counterpart"],
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
    assert payload["counterpart_channel_context"]["artifact"] == "counterpart_channel_scorecard.json"
    assert payload["counterpart_channel_context"]["legacy_private_credit_proxy_role"] == "coarse_legacy_creator_proxy"
    assert payload["counterpart_channel_context"]["key_horizons"]["h4"]["decisive_positive_creator_channels"] == ["auto_loans_qoq"]
    assert payload["counterpart_channel_context"]["key_horizons"]["h0"]["decisive_positive_asset_purchase_channels"] == [
        "agency_gse_backed_securities_bank_qoq"
    ]
    assert payload["counterpart_channel_context"]["key_horizons"]["h0"]["decisive_positive_retention_support_channels"] == [
        "on_rrp_reallocation_qoq"
    ]
    assert (
        payload["counterpart_channel_context"]["key_horizons"]["h0"]["escape_support_context"]["interpretation"]
        == "deposit_retention_support_signal"
    )
    assert (
        payload["counterpart_channel_context"]["key_horizons"]["h0"]["asset_purchase_plumbing_context"]["interpretation"]
        == "treasury_drain_context"
    )
    assert payload["scope_alignment_context"] == {}
    assert payload["toc_row_excluded_interpretation_context"] == {}
    assert payload["strict_missing_channel_context"] == {}
    assert payload["strict_sign_mismatch_audit_context"] == {}
    assert payload["strict_shock_composition_context"] == {}
    assert payload["strict_top_gap_quarter_audit_context"] == {}
    assert payload["strict_top_gap_quarter_direction_context"] == {}
    assert payload["strict_top_gap_inversion_context"] == {}
    assert payload["strict_top_gap_anomaly_context"] == {}
    assert payload["strict_top_gap_anomaly_component_split_context"] == {}
    assert payload["strict_top_gap_anomaly_di_loans_split_context"] == {}
    assert payload["strict_top_gap_anomaly_backdrop_context"] == {}
    assert payload["big_picture_synthesis_context"] == {}
    assert payload["treatment_object_comparison_context"] == {}
    assert payload["split_treatment_architecture_context"] == {}
    assert payload["core_treatment_promotion_context"] == {}
    assert payload["strict_redesign_context"] == {}
    assert payload["strict_private_offset_residual_context"] == {}
    assert payload["strict_corporate_bridge_secondary_comparison_context"] == {}
    assert payload["strict_component_framework_context"] == {}
    assert payload["strict_release_framing_context"] == {}
    assert payload["strict_direct_core_component_context"] == {}
    assert payload["strict_direct_core_horizon_stability_context"] == {}
    assert payload["strict_additional_creator_candidate_context"] == {}
    assert payload["strict_di_loans_nec_measurement_audit_context"] == {}
    assert payload["strict_results_closeout_context"] == {}
    assert payload["tdcest_ladder_integration_context"] == {}
    assert payload["tdcest_broad_object_comparison_context"] == {}
    assert payload["tdcest_broad_treatment_sensitivity_context"] == {}
    assert payload["toc_row_incidence_audit_context"] == {}
    assert payload["toc_row_liability_incidence_raw_context"] == {}
    assert "coarse legacy creator proxy" in payload["mechanism_caveat"]
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


def test_pass_through_summary_prefers_exact_measurement_ladder_when_available() -> None:
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        identity_measurement_ladder=pd.DataFrame(
            [
                {
                    "treatment_variant": "domestic_bank_only",
                    "treatment_role": "exploratory",
                    "treatment_family": "measurement",
                    "target": "tdc_domestic_bank_only_qoq",
                    "outcome": "tdc_domestic_bank_only_qoq",
                    "horizon": 0,
                    "beta": 1.2,
                    "se": 0.2,
                    "lower95": 0.8,
                    "upper95": 1.6,
                    "n": 30,
                    "spec_name": "identity_measurement_ladder",
                    "shock_column": "tdc_domestic_bank_only_residual_z",
                    "decomposition_mode": "exact_identity_baseline",
                    "outcome_construction": "estimated_common_design",
                    "inference_method": "bootstrap",
                },
                {
                    "treatment_variant": "domestic_bank_only",
                    "treatment_role": "exploratory",
                    "treatment_family": "measurement",
                    "target": "tdc_domestic_bank_only_qoq",
                    "outcome": "total_deposits_bank_qoq",
                    "horizon": 0,
                    "beta": 0.6,
                    "se": 0.2,
                    "lower95": 0.2,
                    "upper95": 1.0,
                    "n": 30,
                    "spec_name": "identity_measurement_ladder",
                    "shock_column": "tdc_domestic_bank_only_residual_z",
                    "decomposition_mode": "exact_identity_baseline",
                    "outcome_construction": "estimated_common_design",
                    "inference_method": "bootstrap",
                },
                {
                    "treatment_variant": "domestic_bank_only",
                    "treatment_role": "exploratory",
                    "treatment_family": "measurement",
                    "target": "tdc_domestic_bank_only_qoq",
                    "outcome": "other_component_qoq",
                    "horizon": 0,
                    "beta": -0.6,
                    "se": 0.2,
                    "lower95": -1.0,
                    "upper95": -0.2,
                    "n": 30,
                    "spec_name": "identity_measurement_ladder",
                    "shock_column": "tdc_domestic_bank_only_residual_z",
                    "decomposition_mode": "exact_identity_baseline",
                    "outcome_construction": "derived_total_minus_tdc",
                    "inference_method": "bootstrap",
                },
            ]
        ),
        sensitivity=pd.DataFrame(
            [
                {
                    "treatment_variant": "domestic_bank_only",
                    "treatment_role": "exploratory",
                    "treatment_family": "measurement",
                    "outcome": "total_deposits_bank_qoq",
                    "horizon": 0,
                    "beta": -9.0,
                    "se": 1.0,
                    "lower95": -11.0,
                    "upper95": -7.0,
                    "n": 30,
                    "spec_name": "baseline",
                }
            ]
        ),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
    )

    assert payload["estimation_path"]["measurement_variant_artifact"] == "identity_measurement_ladder.csv"
    assert payload["measurement_treatment_variants"][0]["variant"] == "domestic_bank_only"
    assert payload["measurement_treatment_variants"][0]["horizons"]["h0"]["assessment"] == "crowd_out_signal"


def test_pass_through_summary_surfaces_scope_alignment_context() -> None:
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sensitivity=pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
        scope_alignment_summary={
            "status": "available",
            "recommended_policy": {
                "preferred_scope_check_variant": "us_chartered_bank_only",
                "secondary_scope_sensitivity_variant": "domestic_bank_only",
            },
            "variant_definitions": {
                "baseline": {"target": "tdc_bank_only_qoq"},
                "domestic_bank_only": {"target": "tdc_domestic_bank_only_qoq"},
                "us_chartered_bank_only": {"target": "tdc_us_chartered_bank_only_qoq"},
            },
            "deposit_concepts": {
                "total_deposits_including_interbank": {
                    "key_horizons": {
                        "h0": {
                            "baseline": {"residual_response": {"beta": -72.7}},
                            "variants": {
                                "domestic_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 20.0}
                                },
                                "us_chartered_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 9.6}
                                },
                            },
                        },
                        "h4": {
                            "baseline": {"residual_response": {"beta": -110.8}},
                            "variants": {
                                "domestic_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 83.5}
                                },
                                "us_chartered_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 63.6}
                                },
                            },
                        },
                    }
                }
            },
            "takeaways": ["scope stub"],
        },
        strict_identifiable_followup_summary={
            "status": "available",
            "scope_check_gap_assessment": {
                "assumption": "descriptive only",
                "key_horizons": {
                    "h0": {
                        "baseline_strict_gap_beta": -75.6,
                        "variant_gap_assessments": {
                            "us_chartered_bank_only": {
                                "remaining_share_of_baseline_strict_gap": 0.92,
                                "relief_share_of_baseline_strict_gap": 0.08,
                            },
                            "domestic_bank_only": {
                                "remaining_share_of_baseline_strict_gap": 0.78,
                            },
                        },
                    }
                },
            },
        },
        tdc_treatment_audit_summary={
            "status": "available",
            "construction_alignment": {"status": "available"},
            "key_horizons": {
                "h0": {
                    "dominant_signed_component": "rest_of_world_treasury_transactions",
                    "variant_removal_diagnostics": {
                        "no_toc_bank_only": {"residual_shift_vs_baseline_beta": -4.0},
                        "domestic_bank_only": {"residual_shift_vs_baseline_beta": 20.0},
                        "no_foreign_bank_sectors": {"residual_shift_vs_baseline_beta": -0.2},
                    },
                }
            },
            "takeaways": ["tdc audit stub"],
        },
        treasury_operating_cash_audit_summary={
            "status": "available",
            "quarterly_alignment": {
                "contemporaneous_corr_tga_vs_toc": 0.95,
                "ols_tga_on_toc": {"slope": 0.93, "intercept": 0.5, "r2": 0.9},
                "sign_match_share_tga_vs_toc": 0.76,
            },
            "key_horizons": {
                "h0": {
                    "treasury_operating_cash_response": {
                        "beta": -70.4,
                        "se": 3.0,
                        "lower95": -76.0,
                        "upper95": -64.8,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "treasury_operating_cash_signed_contribution_beta": 70.4,
                    "tga_response": {
                        "beta": -63.7,
                        "se": 3.0,
                        "lower95": -69.0,
                        "upper95": -58.4,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "reserves_response": {
                        "beta": 89.3,
                        "se": 4.0,
                        "lower95": 81.5,
                        "upper95": 97.1,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "cb_nonts_response": {
                        "beta": 26.5,
                        "se": 4.5,
                        "lower95": 17.7,
                        "upper95": 35.3,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "toc_minus_tga_beta_gap": -6.7,
                    "interpretation": "treasury_cash_release_pattern",
                }
            },
            "takeaways": ["toc audit stub"],
        },
        rest_of_world_treasury_audit_summary={
            "status": "available",
            "quarterly_alignment": {
                "counterparts": {
                    "foreign_nonts_qoq": {
                        "contemporaneous_corr": -0.17,
                        "lead_lag_correlations": {"shift_+0q": -0.17},
                    },
                    "checkable_rest_of_world_bank_qoq": {
                        "contemporaneous_corr": 0.02,
                        "lead_lag_correlations": {"shift_+0q": 0.02},
                    },
                }
            },
            "key_horizons": {
                "h0": {
                    "rest_of_world_treasury_response": {
                        "beta": 11.8,
                        "se": 6.0,
                        "lower95": 0.0,
                        "upper95": 23.6,
                        "n": 38,
                        "ci_excludes_zero": False,
                    },
                    "foreign_nonts_response": {
                        "beta": 26.6,
                        "se": 6.0,
                        "lower95": 14.8,
                        "upper95": 38.4,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "checkable_rest_of_world_bank_response": {
                        "beta": 0.22,
                        "se": 0.5,
                        "lower95": -0.8,
                        "upper95": 1.24,
                        "n": 38,
                        "ci_excludes_zero": False,
                    },
                    "interbank_transactions_foreign_banks_liability_response": {
                        "beta": 2.7,
                        "se": 3.0,
                        "lower95": -3.2,
                        "upper95": 8.6,
                        "n": 38,
                        "ci_excludes_zero": False,
                    },
                    "interbank_transactions_foreign_banks_asset_response": {
                        "beta": 5.0,
                        "se": 2.7,
                        "lower95": -0.4,
                        "upper95": 10.4,
                        "n": 38,
                        "ci_excludes_zero": False,
                    },
                    "deposits_at_foreign_banks_asset_response": {
                        "beta": 0.03,
                        "se": 0.2,
                        "lower95": -0.4,
                        "upper95": 0.45,
                        "n": 38,
                        "ci_excludes_zero": False,
                    },
                    "interpretation": "external_asset_support_pattern",
                }
            },
            "takeaways": ["row audit stub"],
        },
        toc_row_path_split_summary={
            "status": "available",
            "quarterly_split": {
                "preferred_quarterly_path": "direct_deposit_path_dominant",
                "bundle_contemporaneous_corr": {
                    "broad_support_path": 0.75,
                    "direct_deposit_path": 0.86,
                    "liquidity_external_path": 0.20,
                    "direct_minus_broad_gap": 0.11,
                },
            },
            "key_horizons": {
                "h0": {
                    "preferred_horizon_path": "broad_support_path_dominant",
                    "bundle_response": {
                        "beta": 82.2,
                        "se": 8.0,
                        "lower95": 66.5,
                        "upper95": 97.9,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "broad_support_path_response": {
                        "beta": 87.8,
                        "se": 8.5,
                        "lower95": 71.1,
                        "upper95": 104.5,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "direct_deposit_path_response": {
                        "beta": 61.4,
                        "se": 8.1,
                        "lower95": 45.5,
                        "upper95": 77.3,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "liquidity_external_path_response": {
                        "beta": 115.5,
                        "se": 10.0,
                        "lower95": 95.9,
                        "upper95": 135.1,
                        "n": 38,
                        "ci_excludes_zero": True,
                    },
                    "direct_minus_broad_beta_gap": -26.4,
                }
            },
            "takeaways": ["toc/row split stub"],
        },
        toc_row_excluded_interpretation_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "baseline": {
                        "residual_response": {"beta": -72.7},
                        "strict_gap_share_of_residual": 0.92,
                    },
                    "toc_row_excluded": {
                        "residual_response": {"beta": -5.5},
                        "strict_gap_share_of_residual": 0.48,
                    },
                    "excluded_minus_baseline_beta": {"residual_response": 67.2},
                    "interpretation": "toc_row_exclusion_materially_relaxes_residual_and_strict_gap",
                }
            },
            "takeaways": ["toc/row excluded stub"],
        },
        strict_missing_channel_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "baseline": {
                        "residual_response": {"beta": -72.7},
                    },
                    "toc_row_excluded": {
                        "residual_response": {"beta": -5.5},
                        "strict_headline_direct_core_response": {"beta": -3.3},
                        "strict_loan_source_response": {"beta": -1.1},
                        "strict_loan_core_plus_private_borrower_response": {"beta": 2.4},
                        "strict_loan_noncore_system_response": {"beta": -4.8},
                        "strict_non_treasury_securities_response": {"beta": 0.2},
                        "strict_identifiable_net_after_funding_response": {"beta": -0.9},
                        "strict_gap_after_funding_share_of_residual_abs": 0.82,
                    },
                    "interpretation": "toc_row_exclusion_relaxes_residual_but_missing_channels_still_dominate",
                }
            },
            "takeaways": ["strict missing stub"],
        },
        strict_sign_mismatch_audit_summary={
            "status": "available",
            "shock_alignment": {
                "shock_corr": 0.42,
                "same_sign_share": 0.72,
            },
            "quarter_concentration": {
                "top5_abs_gap_share": 0.64,
                "dominant_period_bucket": "covid_post",
            },
            "gap_driver_alignment": {
                "shock_gap_driver_correlations": {
                    "baseline_minus_excluded_target_qoq": 0.88,
                }
            },
            "component_alignment": {
                "strict_loan_core_min_qoq": {
                    "baseline_shock_corr": -0.38,
                    "toc_row_excluded_shock_corr": 0.19,
                },
                "strict_identifiable_total_qoq": {
                    "baseline_shock_corr": -0.11,
                    "toc_row_excluded_shock_corr": 0.19,
                },
            },
            "interpretation": "excluded_shock_rotates_toward_positive_direct_count_channels",
            "takeaways": ["strict sign mismatch stub"],
        },
        strict_shock_composition_summary={
            "status": "available",
            "top_gap_quarters": [{"quarter": "2020Q1", "period_bucket": "covid_post"}],
            "period_bucket_profiles": [{"period_bucket": "covid_post", "abs_gap_share": 0.61}],
            "trim_diagnostics": {
                "drop_top5_gap_quarters": {
                    "shock_corr": 0.57,
                    "same_sign_share": 0.79,
                    "interpretation": "excluded_shock_moderately_aligned_but_distinct",
                },
                "drop_covid_post": {
                    "shock_corr": 0.66,
                    "same_sign_share": 0.82,
                    "interpretation": "excluded_shock_close_to_baseline",
                },
            },
            "interpretation": "rotation_is_mostly_covid_post_specific",
            "takeaways": ["strict shock composition stub"],
        },
        strict_top_gap_quarter_audit_summary={
            "status": "available",
            "top_gap_quarters": [{"quarter": "2020Q3", "dominant_leg": "mixed", "contribution_pattern": "offsetting"}],
            "dominant_leg_summary": [{"dominant_leg": "mixed", "abs_gap_share": 0.58}],
            "contribution_pattern_summary": [{"contribution_pattern": "offsetting", "abs_gap_share": 0.62}],
            "interpretation": "top_gap_quarters_are_mixed_or_offsetting_toc_row_bundles",
            "takeaways": ["strict top-gap stub"],
        },
        strict_top_gap_quarter_direction_summary={
            "status": "available",
            "top_gap_quarters": [{"quarter": "2020Q3", "gap_alignment_to_bundle": "opposed", "directional_driver": "toc_driven_gap_direction"}],
            "gap_bundle_alignment_summary": [{"gap_alignment_to_bundle": "opposed", "abs_gap_share": 0.63}],
            "directional_driver_summary": [{"directional_driver": "toc_driven_gap_direction", "abs_gap_share": 0.55}],
            "interpretation": "top_gap_gap_direction_often_opposes_bundle_sign",
            "takeaways": ["strict top-gap direction stub"],
        },
        strict_top_gap_inversion_summary={
            "status": "available",
            "top_gap_quarters": [{"quarter": "2020Q3", "directional_driver": "toc_driven_gap_direction", "excluded_other_component_qoq": -217.3, "strict_identifiable_total_qoq": 89.0}],
            "directional_driver_context_summary": [{"directional_driver": "both_legs_oppose_gap", "abs_gap_share": 0.57, "weighted_mean_excluded_other_component_qoq": 177.9, "weighted_mean_strict_identifiable_total_qoq": 352.1}],
            "residual_strict_pattern_summary": [{"residual_strict_pattern": "positive_residual_positive_strict", "abs_gap_share": 0.47}],
            "interpretation": "both_leg_inversion_quarters_still_tend_to_show_positive_residual_and_positive_strict_support",
            "takeaways": ["strict top-gap inversion stub"],
        },
        strict_top_gap_anomaly_summary={
            "status": "available",
            "anomaly_quarter": {"quarter": "2009Q4", "excluded_other_component_qoq": 73.6, "strict_identifiable_total_qoq": -68.4},
            "peer_quarters": [{"quarter": "2020Q1"}, {"quarter": "2021Q1"}],
            "peer_pattern_summary": [{"residual_strict_pattern": "positive_residual_positive_strict", "abs_gap_share": 0.82}],
            "weighted_peer_means": {"strict_identifiable_total_qoq": 352.1},
            "anomaly_vs_peer_deltas": {"strict_identifiable_total_qoq": -420.5, "strict_loan_source_qoq": -359.7},
            "ranked_anomaly_component_deltas": [
                {"metric": "strict_identifiable_total_qoq", "anomaly_minus_peer_delta": -420.5, "abs_delta": 420.5}
            ],
            "interpretation": "anomaly_flips_strict_total_negative_while_peer_bucket_stays_positive",
            "takeaways": ["strict top-gap anomaly stub"],
        },
        strict_top_gap_anomaly_component_split_summary={
            "status": "available",
            "anomaly_quarter": {"quarter": "2009Q4"},
            "loan_subcomponent_deltas": [
                {"metric": "strict_loan_di_loans_nec_qoq", "label": "DI loans n.e.c.", "anomaly_minus_peer_delta": -352.5}
            ],
            "securities_subcomponent_deltas": [
                {"metric": "strict_non_treasury_corporate_foreign_bonds_qoq", "label": "Corporate and foreign bonds", "anomaly_minus_peer_delta": -71.3}
            ],
            "funding_subcomponent_deltas": [
                {"metric": "strict_funding_fedfunds_repo_qoq", "label": "Fed funds / repo funding", "anomaly_minus_peer_delta": -141.2}
            ],
            "liquidity_external_deltas": [
                {"metric": "reserves_qoq", "label": "Reserves", "anomaly_minus_peer_delta": -469.1}
            ],
            "ranked_component_deltas": [
                {"metric": "reserves_qoq", "anomaly_minus_peer_delta": -469.1, "abs_delta": 469.1}
            ],
            "interpretation": "anomaly_is_di_loans_nec_contraction_with_weaker_liquidity_and_external_support",
            "takeaways": ["strict top-gap anomaly component split stub"],
        },
        strict_top_gap_anomaly_di_loans_split_summary={
            "status": "available",
            "anomaly_quarter": {"quarter": "2009Q4"},
            "di_loans_nec_component_deltas": [
                {"metric": "strict_di_loans_nec_domestic_financial_qoq", "label": "Domestic financial", "anomaly_minus_peer_delta": -280.4}
            ],
            "dominant_borrower_component": {
                "metric": "strict_di_loans_nec_domestic_financial_qoq",
                "label": "Domestic financial",
                "anomaly_minus_peer_delta": -280.4,
            },
            "borrower_gap_row": {
                "metric": "strict_di_loans_nec_systemwide_borrower_gap_qoq",
                "label": "Systemwide borrower gap",
                "anomaly_minus_peer_delta": 15.2,
            },
            "interpretation": "di_loans_nec_anomaly_is_domestic_financial_shortfall",
            "takeaways": ["strict top-gap anomaly di-loans split stub"],
        },
        strict_top_gap_anomaly_backdrop_summary={
            "status": "available",
            "anomaly_quarter": {"quarter": "2009Q4"},
            "corporate_credit_row": {"metric": "strict_di_loans_nec_nonfinancial_corporate_qoq", "anomaly_minus_peer_delta": -345.9},
            "loan_source_row": {"metric": "strict_loan_source_qoq", "anomaly_minus_peer_delta": -359.7},
            "reserves_row": {"metric": "reserves_qoq", "anomaly_minus_peer_delta": -469.1},
            "foreign_nonts_row": {"metric": "foreign_nonts_qoq", "anomaly_minus_peer_delta": -331.4},
            "tga_row": {"metric": "tga_qoq", "anomaly_minus_peer_delta": 263.4},
            "residual_row": {"metric": "other_component_no_toc_no_row_bank_only_qoq", "anomaly_minus_peer_delta": -127.4},
            "liquidity_external_abs_to_corporate_abs_ratio": 2.31,
            "interpretation": "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall",
            "takeaways": ["strict top-gap anomaly backdrop stub"],
        },
        big_picture_synthesis_summary={
            "status": "available",
            "h0_snapshot": {
                "toc_row_excluded_residual_beta": -5.5,
                "toc_row_excluded_strict_identifiable_total_beta": 10.8,
                "us_chartered_scope_relief_beta": 9.6,
            },
            "quarter_composition": {"dominant_period_bucket": "covid_post"},
            "supporting_case": {
                "anomaly_quarter": "2009Q4",
                "interpretation": "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall",
            },
            "classification": {
                "scope_issue_status": "real_but_partial",
                "treatment_issue_status": "toc_row_dominant",
                "independent_lane_status": "not_validated",
            },
            "interpretation": "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated",
            "takeaways": ["big picture synthesis stub"],
        },
        treatment_object_comparison_summary={
            "status": "available",
            "candidate_objects": [
                {"candidate": "baseline_full_tdc"},
                {"candidate": "us_chartered_bank_leg_match"},
                {"candidate": "toc_row_excluded_core"},
            ],
            "recommendation": {
                "recommended_next_branch": "split_core_plus_support_bundle",
                "headline_decision_now": "keep current headline provisional and do not promote the TOC_ROW_excluded object",
            },
            "takeaways": ["treatment object comparison stub"],
        },
        split_treatment_architecture_summary={
            "status": "available",
            "series_definitions": {
                "baseline_treatment": "tdc_bank_only_qoq",
                "core_deposit_proximate_treatment": "tdc_core_deposit_proximate_bank_only_qoq",
                "support_bundle_treatment": "tdc_toc_row_support_bundle_qoq",
            },
            "quarterly_alignment": {
                "status": "available",
                "tdc_identity": {"quarterly_alignment": "exact", "max_abs_gap_beta": 0.0},
                "residual_identity": {"quarterly_alignment": "exact", "max_abs_gap_beta": 0.0},
            },
            "architecture_recommendation": {
                "recommended_next_branch": "split_core_plus_support_bundle",
                "headline_decision_now": "keep current headline provisional and do not promote the TOC_ROW_excluded object",
            },
            "key_horizons": {
                "h0": {
                    "support_bundle_beta": 67.2,
                    "core_deposit_proximate_residual_response": {"beta": -5.5},
                    "horizon_preferred_path": "broad_support_path_dominant",
                }
            },
            "takeaways": ["split treatment stub"],
        },
        core_treatment_promotion_summary={
            "status": "available",
            "shock_quality": {
                "baseline_vs_core_overlap": {
                    "status": "available",
                    "shock_corr": 0.42,
                    "same_sign_share": 0.72,
                }
            },
            "strict_validation_check": {
                "status": "available",
                "h0_core_residual_beta": -5.5,
                "h0_strict_identifiable_total_beta": 10.8,
                "h0_sign_match": False,
            },
            "promotion_recommendation": {
                "status": "keep_interpretive_only",
                "current_release_role": "keep split architecture interpretive and diagnostic",
            },
            "key_horizons": {"h0": {"core_residual_response": {"beta": -5.5}}},
            "takeaways": ["core treatment promotion stub"],
        },
        strict_redesign_summary={
            "status": "available",
            "current_strict_problem_definition": {
                "h0_core_residual_beta": -5.5,
                "h0_toc_row_excluded_strict_identifiable_total_beta": 10.8,
            },
            "failure_modes": {
                "scope_mismatch_not_primary": {"h0_remaining_share_of_baseline_strict_gap": 0.92},
                "loan_bucket_shape": {"h0_dominant_loan_component": "strict_loan_consumer_credit_qoq"},
                "funding_offset_instability": {"h0_funding_offset_share_of_identifiable_total_beta": 0.78},
            },
            "recommended_build_order": [{"step": "redesign_strict_loan_core_before_adding_more_channels"}],
            "takeaways": ["strict redesign stub"],
        },
        strict_loan_core_redesign_summary={
            "status": "available",
            "published_roles": {
                "headline_direct_core": {"series": "strict_loan_core_min_qoq"},
                "standard_secondary_comparison": {"series": "strict_loan_core_plus_private_borrower_qoq"},
                "di_bucket_diagnostic": {"series": "strict_loan_di_loans_nec_qoq"},
            },
            "recommendation": {
                "release_headline_candidate": "strict_loan_core_min_qoq",
                "standard_secondary_candidate": "strict_loan_core_plus_private_borrower_qoq",
                "diagnostic_di_bucket": "strict_loan_di_loans_nec_qoq",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "core_residual_response": {"beta": -5.5},
                        "current_broad_loan_source_response": {"beta": 5.6},
                        "redesigned_direct_min_core_response": {"beta": -10.7},
                        "private_borrower_augmented_core_response": {"beta": 8.5},
                        "noncore_system_diagnostic_response": {"beta": 4.3},
                    }
                }
            },
            "takeaways": ["strict loan-core redesign stub"],
        },
        strict_di_bucket_role_summary={
            "status": "available",
            "release_taxonomy": {
                "headline_direct_core": {"series": "strict_loan_core_min_qoq"},
                "standard_secondary_comparison": {"series": "strict_loan_core_plus_private_borrower_qoq"},
                "di_bucket_diagnostic": {"series": "strict_loan_di_loans_nec_qoq"},
            },
            "recommendation": {
                "headline_direct_core": "strict_loan_core_min_qoq",
                "standard_secondary_comparison": "strict_loan_core_plus_private_borrower_qoq",
                "diagnostic_di_bucket": "strict_loan_di_loans_nec_qoq",
            },
            "key_horizons": {
                "h0": {
                    "core_residual_response": {"beta": -5.5},
                    "headline_direct_core_response": {"beta": -10.7},
                    "standard_secondary_comparison_response": {"beta": 8.5},
                    "broad_loan_subtotal_response": {"beta": 5.6},
                    "dominant_borrower_component": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                }
            },
            "takeaways": ["strict di role stub"],
        },
        strict_di_bucket_bridge_summary={
            "status": "available",
            "bridge_definitions": {
                "di_asset": "strict_loan_di_loans_nec_qoq",
                "private_borrower_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
            },
            "recommendation": {
                "next_branch": "build_counterpart_alignment_surface",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "di_asset_response": {"beta": -2.4},
                        "private_borrower_bridge_response": {"beta": 1.1},
                        "noncore_system_bridge_response": {"beta": 0.5},
                        "bridge_residual_beta": -4.0,
                        "interpretation": "cross_scope_bridge_residual_large",
                    }
                }
            },
            "takeaways": ["strict di bridge stub"],
        },
        strict_private_borrower_bridge_summary={
            "status": "available",
            "bridge_definitions": {
                "private_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
                "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
            },
            "recommendation": {
                "next_branch": "build_nonfinancial_corporate_bridge_surface",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "private_bridge_response": {"beta": 20.8},
                        "households_nonprofits_response": {"beta": 1.3},
                        "nonfinancial_corporate_response": {"beta": 20.8},
                        "nonfinancial_noncorporate_response": {"beta": -1.9},
                        "dominant_private_component": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                    }
                }
            },
            "takeaways": ["strict private bridge stub"],
        },
        strict_nonfinancial_corporate_bridge_summary={
            "status": "available",
            "bridge_definitions": {
                "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
            },
            "recommendation": {
                "next_branch": "assess_household_and_nonfinancial_noncorporate_offset_residual",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "private_bridge_response": {"beta": 20.8},
                        "nonfinancial_corporate_response": {"beta": 20.8},
                        "households_nonprofits_response": {"beta": 1.3},
                        "nonfinancial_noncorporate_response": {"beta": -1.9},
                    }
                }
            },
            "takeaways": ["strict nonfinancial corporate bridge stub"],
        },
        strict_private_offset_residual_summary={
            "status": "available",
            "bridge_definitions": {
                "private_offset_total": "strict_di_loans_nec_private_offset_residual_qoq",
            },
            "recommendation": {
                "next_branch": "assess_corporate_bridge_secondary_comparison_role",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "private_offset_total_response": {"beta": -0.57},
                        "private_bridge_response": {"beta": 20.8},
                        "households_nonprofits_response": {"beta": 1.3},
                        "nonfinancial_noncorporate_response": {"beta": -1.9},
                    }
                }
            },
            "takeaways": ["strict private offset stub"],
        },
        strict_corporate_bridge_secondary_comparison_summary={
            "status": "available",
            "candidate_definitions": {
                "headline_direct_core": "strict_loan_core_min_qoq",
                "core_plus_private_bridge": "strict_loan_core_plus_private_borrower_qoq",
                "core_plus_nonfinancial_corporate": "strict_loan_core_plus_nonfinancial_corporate_qoq",
            },
            "recommendation": {
                "status": "narrow_secondary_to_corporate_bridge",
                "standard_secondary_candidate": "strict_loan_core_plus_nonfinancial_corporate_qoq",
                "secondary_comparison_retained_for_diagnostics": "strict_loan_core_plus_private_borrower_qoq",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "core_residual_response": {"beta": -5.5},
                        "headline_direct_core_response": {"beta": -10.7},
                        "core_plus_private_bridge_response": {"beta": 8.5},
                        "core_plus_nonfinancial_corporate_response": {"beta": -6.0},
                    }
                }
            },
            "takeaways": ["strict corporate bridge secondary stub"],
        },
        strict_component_framework_summary={
            "status": "available",
            "frozen_roles": {
                "accounting_lane_role": "non_evidence_for_independent_verification",
                "headline_direct_core": "strict_loan_core_min_qoq",
                "standard_secondary_comparison": "strict_loan_core_plus_private_borrower_qoq",
            },
            "classification": {
                "framework_state": "frozen_for_release_framing",
                "external_critique_readiness": "ready_for_gpt_pro_critique",
            },
            "recommendation": {
                "status": "strict_release_framing_finalized",
                "next_branch": "only_reopen_toc_or_row_if_new_incidence_evidence_appears",
            },
            "h0_snapshot": {
                "toc_row_support_bundle_beta": 65.4,
                "core_residual_beta": -5.5,
                "headline_direct_core_beta": -10.7,
                "standard_secondary_beta": 8.5,
                "narrowing_diagnostic_beta": 9.4,
            },
            "takeaways": ["strict framework stub"],
        },
        strict_release_framing_summary={
            "status": "available",
            "release_position": {
                "full_tdc_release_role": "broad_treasury_attributed_object_only",
                "strict_object_rule": "exclude_toc_and_row_under_current_evidence",
            },
            "classification": {
                "release_state": "strict_release_framing_finalized",
            },
            "recommendation": {
                "status": "strict_release_framing_finalized",
                "reopen_rule": "reopen_only_if_new_scope_and_timing_matched_incidence_evidence_appears",
            },
            "h0_snapshot": {
                "toc_row_support_bundle_beta": 65.4,
                "core_residual_beta": -5.5,
                "headline_direct_core_beta": -10.7,
                "toc_deposits_only_share": 0.66,
                "row_checkable_share": 0.02,
            },
            "takeaways": ["strict release framing stub"],
        },
        strict_direct_core_component_summary={
            "status": "available",
            "candidate_definitions": {
                "headline_direct_core": "strict_loan_core_min_qoq",
                "mortgages_only_candidate": "strict_loan_mortgages_qoq",
                "consumer_credit_only_candidate": "strict_loan_consumer_credit_qoq",
            },
            "classification": {
                "h0_dominant_component": "strict_loan_consumer_credit_qoq",
            },
            "recommendation": {
                "status": "keep_bundled_direct_core",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "residual_response": {"beta": -5.5},
                        "mortgages_response": {"beta": -1.2},
                        "consumer_credit_response": {"beta": -9.4},
                        "direct_core_response": {"beta": -10.7},
                    }
                }
            },
            "takeaways": ["strict direct core component stub"],
        },
        strict_direct_core_horizon_stability_summary={
            "status": "available",
            "horizon_winners": {
                "h0": "strict_loan_mortgages_qoq",
                "h4": "strict_loan_core_min_qoq",
                "h8": "strict_loan_core_min_qoq",
            },
            "classification": {
                "impact_winner": "strict_loan_mortgages_qoq",
                "medium_horizon_winner": "strict_loan_core_min_qoq",
                "long_horizon_winner": "strict_loan_core_min_qoq",
                "recommendation_status": "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate",
            },
            "recommendation": {
                "status": "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate",
                "impact_candidate": "strict_loan_mortgages_qoq",
                "multihorizon_candidate": "strict_loan_core_min_qoq",
                "next_branch": "keep_bundled_direct_core_but_surface_mortgages_as_impact_candidate",
            },
            "takeaways": ["strict direct core horizon stub"],
        },
        strict_additional_creator_candidate_summary={
            "status": "available",
            "candidate_groups": {
                "validation_proxies": ["closed_end_residential_loans_qoq"],
                "extension_candidates": ["commercial_industrial_loans_qoq"],
            },
            "classification": {
                "h0_best_validation_proxy": "closed_end_residential_loans_qoq",
                "h0_best_extension_candidate": "commercial_industrial_loans_qoq",
                "recommendation_status": "no_additional_extension_candidate_supported",
            },
            "recommendation": {
                "status": "no_additional_extension_candidate_supported",
                "best_validation_proxy": "closed_end_residential_loans_qoq",
                "best_extension_candidate": "commercial_industrial_loans_qoq",
                "next_branch": "freeze_creator_search_and_only_reopen_if_new_same_scope_channel_appears",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "validation_proxies": {
                            "best_candidate": {
                                "outcome": "closed_end_residential_loans_qoq",
                                "response": {"beta": -5.1},
                            }
                        },
                        "extension_candidates": {
                            "best_candidate": {
                                "outcome": "commercial_industrial_loans_qoq",
                                "response": {"beta": -0.7},
                            }
                        },
                    }
                }
            },
            "takeaways": ["strict additional creator stub"],
        },
        strict_di_loans_nec_measurement_audit_summary={
            "status": "available",
            "candidate_groups": {
                "same_scope_transaction_subcomponents": [],
                "cross_scope_transaction_bridges": ["strict_di_loans_nec_nonfinancial_corporate_qoq"],
                "same_scope_proxies": ["loans_to_nondepository_financial_institutions_qoq"],
            },
            "classification": {
                "same_scope_transaction_subcomponent_status": "not_available_from_current_public_data",
                "h0_best_cross_scope_transaction_bridge": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                "h0_best_same_scope_proxy": "loans_to_nondepository_financial_institutions_qoq",
                "promotion_gate": "no_promotable_same_scope_transaction_subcomponent_supported",
            },
            "recommendation": {
                "status": "no_promotable_same_scope_transaction_subcomponent_supported",
                "next_branch": "freeze_framework_and_move_to_writeup_if_no_new_public_transaction_split_appears",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "cross_scope_transaction_bridges": {
                            "target_response": {"beta": 17.6},
                            "best_candidate": {
                                "outcome": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                                "response": {"beta": 20.8},
                            },
                        },
                        "same_scope_proxies": {
                            "best_candidate": {
                                "outcome": "loans_to_nondepository_financial_institutions_qoq",
                                "response": {"beta": 5.7},
                            },
                        },
                    }
                }
            },
            "takeaways": ["strict di measurement audit stub"],
        },
        strict_results_closeout_summary={
            "status": "available",
            "release_position": {
                "headline_direct_benchmark": "strict_loan_core_min_qoq",
                "standard_bridge_comparison": "strict_loan_core_plus_nonfinancial_corporate_qoq",
            },
            "settled_findings": ["strict closeout settled stub"],
            "evidence_tiers": {"independent_evidence": ["strict_loan_core_min_qoq"]},
            "unresolved_questions": ["strict closeout unresolved stub"],
            "classification": {
                "branch_state": "strict_empirical_expansion_effectively_complete",
                "closeout_readiness": "writeup_ready_under_current_evidence",
            },
            "recommendation": {
                "status": "move_to_writeup_and_results_packaging",
                "next_branch": "writeup_results_and_release_packaging",
            },
            "h0_snapshot": {
                "core_residual_beta": -5.5,
                "headline_direct_core_beta": -10.66,
                "standard_bridge_beta": 9.44,
            },
            "takeaways": ["strict closeout stub"],
        },
    )

    assert payload["scope_alignment_context"]["status"] == "available"
    assert payload["scope_alignment_context"]["artifact"] == "scope_alignment_summary.json"
    assert payload["scope_alignment_context"]["recommended_release_comparison"]["preferred_scope_check_variant"] == "us_chartered_bank_only"
    assert "20.00 less negative" in payload["scope_alignment_context"]["headline_read"]
    assert "9.60 less negative" in payload["scope_alignment_context"]["headline_read"]
    assert payload["scope_alignment_context"]["key_horizons"]["h4"]["us_chartered_bank_only_residual_delta"] == 63.6
    assert payload["scope_alignment_context"]["takeaways"] == ["scope stub"]
    assert payload["strict_gap_scope_check_context"]["status"] == "available"
    assert payload["strict_gap_scope_check_context"]["artifact"] == "strict_identifiable_followup_summary.json"
    assert "about 0.92 of the baseline strict gap remains" in payload["strict_gap_scope_check_context"]["headline_read"]
    assert payload["strict_gap_scope_check_context"]["key_horizons"]["h0"]["us_chartered_relief_share_of_baseline_strict_gap"] == 0.08
    assert payload["tdc_treatment_audit_context"]["status"] == "available"
    assert payload["tdc_treatment_audit_context"]["artifact"] == "tdc_treatment_audit_summary.json"
    assert "rest_of_world_treasury_transactions" in payload["tdc_treatment_audit_context"]["headline_read"]
    assert "-4.00 versus 20.00" in payload["tdc_treatment_audit_context"]["headline_read"]
    assert payload["tdc_treatment_audit_context"]["key_horizons"]["h0"]["no_toc_residual_shift_vs_baseline_beta"] == -4.0
    assert payload["tdc_treatment_audit_context"]["key_horizons"]["h0"]["rest_of_world_residual_shift_vs_baseline_beta"] == 20.0
    assert payload["treasury_operating_cash_audit_context"]["status"] == "available"
    assert payload["treasury_operating_cash_audit_context"]["artifact"] == "treasury_operating_cash_audit_summary.json"
    assert "TOC ≈ -70.40" in payload["treasury_operating_cash_audit_context"]["headline_read"]
    assert "corr ≈ 0.95" in payload["treasury_operating_cash_audit_context"]["headline_read"]
    assert payload["treasury_operating_cash_audit_context"]["quarterly_alignment"]["contemporaneous_corr_tga_vs_toc"] == 0.95
    assert payload["treasury_operating_cash_audit_context"]["key_horizons"]["h0"]["interpretation"] == "treasury_cash_release_pattern"
    assert payload["rest_of_world_treasury_audit_context"]["status"] == "available"
    assert payload["rest_of_world_treasury_audit_context"]["artifact"] == "rest_of_world_treasury_audit_summary.json"
    assert "ROW ≈ 11.80" in payload["rest_of_world_treasury_audit_context"]["headline_read"]
    assert "foreign NONTS corr ≈ -0.17" in payload["rest_of_world_treasury_audit_context"]["headline_read"]
    assert payload["rest_of_world_treasury_audit_context"]["quarterly_alignment"]["foreign_nonts_contemporaneous_corr"] == -0.17
    assert payload["rest_of_world_treasury_audit_context"]["key_horizons"]["h0"]["interpretation"] == "external_asset_support_pattern"
    assert payload["toc_row_path_split_context"]["status"] == "available"
    assert payload["toc_row_path_split_context"]["artifact"] == "toc_row_path_split_summary.json"
    assert "direct_deposit_path_dominant" in payload["toc_row_path_split_context"]["headline_read"]
    assert "broad_support_path_dominant" in payload["toc_row_path_split_context"]["headline_read"]
    assert payload["toc_row_path_split_context"]["quarterly_split"]["preferred_quarterly_path"] == "direct_deposit_path_dominant"
    assert payload["toc_row_path_split_context"]["key_horizons"]["h0"]["preferred_horizon_path"] == "broad_support_path_dominant"
    assert payload["toc_row_path_split_context"]["key_horizons"]["h0"]["direct_minus_broad_beta_gap"] == -26.4
    assert payload["toc_row_path_split_context"]["takeaways"] == ["toc/row split stub"]
    assert payload["toc_row_excluded_interpretation_context"]["status"] == "available"
    assert payload["toc_row_excluded_interpretation_context"]["artifact"] == "toc_row_excluded_interpretation_summary.json"
    assert "-72.70" in payload["toc_row_excluded_interpretation_context"]["headline_read"]
    assert "-5.50" in payload["toc_row_excluded_interpretation_context"]["headline_read"]
    assert payload["toc_row_excluded_interpretation_context"]["key_horizons"]["h0"]["baseline_strict_gap_share_of_residual"] == 0.92
    assert payload["toc_row_excluded_interpretation_context"]["key_horizons"]["h0"]["toc_row_excluded_strict_gap_share_of_residual"] == 0.48
    assert payload["toc_row_excluded_interpretation_context"]["takeaways"] == ["toc/row excluded stub"]
    assert payload["strict_missing_channel_context"]["status"] == "available"
    assert payload["strict_missing_channel_context"]["artifact"] == "strict_missing_channel_summary.json"
    assert "-5.50" in payload["strict_missing_channel_context"]["headline_read"]
    assert "-3.30" in payload["strict_missing_channel_context"]["headline_read"]
    assert "-1.10" in payload["strict_missing_channel_context"]["headline_read"]
    assert payload["strict_missing_channel_context"]["key_horizons"]["h0"]["toc_row_excluded_strict_gap_after_funding_share_of_residual_abs"] == 0.82
    assert payload["strict_missing_channel_context"]["takeaways"] == ["strict missing stub"]
    assert payload["strict_sign_mismatch_audit_context"]["status"] == "available"
    assert payload["strict_sign_mismatch_audit_context"]["artifact"] == "strict_sign_mismatch_audit_summary.json"
    assert "0.42" in payload["strict_sign_mismatch_audit_context"]["headline_read"]
    assert "0.72" in payload["strict_sign_mismatch_audit_context"]["headline_read"]
    assert "0.64" in payload["strict_sign_mismatch_audit_context"]["headline_read"]
    assert payload["strict_sign_mismatch_audit_context"]["interpretation"] == "excluded_shock_rotates_toward_positive_direct_count_channels"
    assert payload["strict_sign_mismatch_audit_context"]["component_alignment"]["strict_loan_core_min_qoq"]["baseline_shock_corr"] == -0.38
    assert payload["strict_sign_mismatch_audit_context"]["quarter_concentration"]["dominant_period_bucket"] == "covid_post"
    assert (
        payload["strict_sign_mismatch_audit_context"]["gap_driver_alignment"]["shock_gap_driver_correlations"][
            "baseline_minus_excluded_target_qoq"
        ]
        == 0.88
    )
    assert payload["strict_sign_mismatch_audit_context"]["takeaways"] == ["strict sign mismatch stub"]
    assert payload["strict_shock_composition_context"]["status"] == "available"
    assert payload["strict_shock_composition_context"]["artifact"] == "strict_shock_composition_summary.json"
    assert "covid_post" in payload["strict_shock_composition_context"]["headline_read"]
    assert "0.57" in payload["strict_shock_composition_context"]["headline_read"]
    assert payload["strict_shock_composition_context"]["interpretation"] == "rotation_is_mostly_covid_post_specific"
    assert payload["strict_shock_composition_context"]["top_gap_quarters"][0]["quarter"] == "2020Q1"
    assert payload["strict_shock_composition_context"]["takeaways"] == ["strict shock composition stub"]
    assert payload["strict_top_gap_quarter_audit_context"]["status"] == "available"
    assert payload["strict_top_gap_quarter_audit_context"]["artifact"] == "strict_top_gap_quarter_audit_summary.json"
    assert "mixed" in payload["strict_top_gap_quarter_audit_context"]["headline_read"]
    assert "0.58" in payload["strict_top_gap_quarter_audit_context"]["headline_read"]
    assert payload["strict_top_gap_quarter_audit_context"]["interpretation"] == "top_gap_quarters_are_mixed_or_offsetting_toc_row_bundles"
    assert payload["strict_top_gap_quarter_audit_context"]["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert payload["strict_top_gap_quarter_audit_context"]["takeaways"] == ["strict top-gap stub"]
    assert payload["strict_top_gap_quarter_direction_context"]["status"] == "available"
    assert payload["strict_top_gap_quarter_direction_context"]["artifact"] == "strict_top_gap_quarter_direction_summary.json"
    assert "opposed" in payload["strict_top_gap_quarter_direction_context"]["headline_read"]
    assert "0.63" in payload["strict_top_gap_quarter_direction_context"]["headline_read"]
    assert payload["strict_top_gap_quarter_direction_context"]["interpretation"] == "top_gap_gap_direction_often_opposes_bundle_sign"
    assert payload["strict_top_gap_quarter_direction_context"]["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert payload["strict_top_gap_quarter_direction_context"]["takeaways"] == ["strict top-gap direction stub"]
    assert payload["strict_top_gap_inversion_context"]["status"] == "available"
    assert payload["strict_top_gap_inversion_context"]["artifact"] == "strict_top_gap_inversion_summary.json"
    assert "both_legs_oppose_gap" in payload["strict_top_gap_inversion_context"]["headline_read"]
    assert "177.90" in payload["strict_top_gap_inversion_context"]["headline_read"]
    assert payload["strict_top_gap_inversion_context"]["interpretation"] == "both_leg_inversion_quarters_still_tend_to_show_positive_residual_and_positive_strict_support"
    assert payload["strict_top_gap_inversion_context"]["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert payload["strict_top_gap_inversion_context"]["takeaways"] == ["strict top-gap inversion stub"]
    assert payload["strict_top_gap_anomaly_context"]["status"] == "available"
    assert payload["strict_top_gap_anomaly_context"]["artifact"] == "strict_top_gap_anomaly_summary.json"
    assert "2009Q4" in payload["strict_top_gap_anomaly_context"]["headline_read"]
    assert "-359.70" in payload["strict_top_gap_anomaly_context"]["headline_read"]
    assert "-420.50" in payload["strict_top_gap_anomaly_context"]["headline_read"]
    assert payload["strict_top_gap_anomaly_context"]["interpretation"] == "anomaly_flips_strict_total_negative_while_peer_bucket_stays_positive"
    assert payload["strict_top_gap_anomaly_context"]["anomaly_quarter"]["quarter"] == "2009Q4"
    assert (
        payload["strict_top_gap_anomaly_context"]["ranked_anomaly_component_deltas"][0]["metric"]
        == "strict_identifiable_total_qoq"
    )
    assert payload["strict_top_gap_anomaly_context"]["takeaways"] == ["strict top-gap anomaly stub"]
    assert payload["strict_top_gap_anomaly_component_split_context"]["status"] == "available"
    assert payload["strict_top_gap_anomaly_component_split_context"]["artifact"] == "strict_top_gap_anomaly_component_split_summary.json"
    assert "DI loans n.e.c." in payload["strict_top_gap_anomaly_component_split_context"]["headline_read"]
    assert "-352.50" in payload["strict_top_gap_anomaly_component_split_context"]["headline_read"]
    assert payload["strict_top_gap_anomaly_component_split_context"]["interpretation"] == "anomaly_is_di_loans_nec_contraction_with_weaker_liquidity_and_external_support"
    assert payload["strict_top_gap_anomaly_component_split_context"]["loan_subcomponent_deltas"][0]["metric"] == "strict_loan_di_loans_nec_qoq"
    assert payload["strict_top_gap_anomaly_component_split_context"]["takeaways"] == ["strict top-gap anomaly component split stub"]
    assert payload["strict_top_gap_anomaly_di_loans_split_context"]["status"] == "available"
    assert payload["strict_top_gap_anomaly_di_loans_split_context"]["artifact"] == "strict_top_gap_anomaly_di_loans_split_summary.json"
    assert "Domestic financial" in payload["strict_top_gap_anomaly_di_loans_split_context"]["headline_read"]
    assert "-280.40" in payload["strict_top_gap_anomaly_di_loans_split_context"]["headline_read"]
    assert payload["strict_top_gap_anomaly_di_loans_split_context"]["interpretation"] == "di_loans_nec_anomaly_is_domestic_financial_shortfall"
    assert payload["strict_top_gap_anomaly_di_loans_split_context"]["dominant_borrower_component"]["metric"] == "strict_di_loans_nec_domestic_financial_qoq"
    assert payload["strict_top_gap_anomaly_di_loans_split_context"]["takeaways"] == ["strict top-gap anomaly di-loans split stub"]
    assert payload["strict_top_gap_anomaly_backdrop_context"]["status"] == "available"
    assert payload["strict_top_gap_anomaly_backdrop_context"]["artifact"] == "strict_top_gap_anomaly_backdrop_summary.json"
    assert "-345.90" in payload["strict_top_gap_anomaly_backdrop_context"]["headline_read"]
    assert "-469.10" in payload["strict_top_gap_anomaly_backdrop_context"]["headline_read"]
    assert payload["strict_top_gap_anomaly_backdrop_context"]["interpretation"] == "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall"
    assert payload["strict_top_gap_anomaly_backdrop_context"]["corporate_credit_row"]["metric"] == "strict_di_loans_nec_nonfinancial_corporate_qoq"
    assert payload["strict_top_gap_anomaly_backdrop_context"]["takeaways"] == ["strict top-gap anomaly backdrop stub"]
    assert payload["big_picture_synthesis_context"]["status"] == "available"
    assert payload["big_picture_synthesis_context"]["artifact"] == "big_picture_synthesis_summary.json"
    assert "-5.50" in payload["big_picture_synthesis_context"]["headline_read"]
    assert "10.80" in payload["big_picture_synthesis_context"]["headline_read"]
    assert payload["big_picture_synthesis_context"]["classification"]["treatment_issue_status"] == "toc_row_dominant"
    assert payload["big_picture_synthesis_context"]["interpretation"] == "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated"
    assert payload["big_picture_synthesis_context"]["takeaways"] == ["big picture synthesis stub"]
    assert payload["treatment_object_comparison_context"]["status"] == "available"
    assert payload["treatment_object_comparison_context"]["artifact"] == "treatment_object_comparison_summary.json"
    assert "split_core_plus_support_bundle" in payload["treatment_object_comparison_context"]["headline_read"]
    assert payload["treatment_object_comparison_context"]["recommendation"]["recommended_next_branch"] == "split_core_plus_support_bundle"
    assert payload["treatment_object_comparison_context"]["candidate_objects"][2]["candidate"] == "toc_row_excluded_core"
    assert payload["treatment_object_comparison_context"]["takeaways"] == ["treatment object comparison stub"]
    assert payload["split_treatment_architecture_context"]["status"] == "available"
    assert payload["split_treatment_architecture_context"]["artifact"] == "split_treatment_architecture_summary.json"
    assert "67.20" in payload["split_treatment_architecture_context"]["headline_read"]
    assert payload["split_treatment_architecture_context"]["architecture_recommendation"]["recommended_next_branch"] == "split_core_plus_support_bundle"
    assert payload["split_treatment_architecture_context"]["series_definitions"]["support_bundle_treatment"] == "tdc_toc_row_support_bundle_qoq"
    assert payload["split_treatment_architecture_context"]["takeaways"] == ["split treatment stub"]
    assert payload["core_treatment_promotion_context"]["status"] == "available"
    assert payload["core_treatment_promotion_context"]["artifact"] == "core_treatment_promotion_summary.json"
    assert "0.42" in payload["core_treatment_promotion_context"]["headline_read"]
    assert "0.72" in payload["core_treatment_promotion_context"]["headline_read"]
    assert payload["core_treatment_promotion_context"]["promotion_recommendation"]["status"] == "keep_interpretive_only"
    assert payload["core_treatment_promotion_context"]["takeaways"] == ["core treatment promotion stub"]
    assert payload["strict_redesign_context"]["status"] == "available"
    assert payload["strict_redesign_context"]["artifact"] == "strict_redesign_summary.json"
    assert "0.92" in payload["strict_redesign_context"]["headline_read"]
    assert "10.80" in payload["strict_redesign_context"]["headline_read"]
    assert payload["strict_redesign_context"]["recommended_build_order"][0]["step"] == "redesign_strict_loan_core_before_adding_more_channels"
    assert payload["strict_redesign_context"]["takeaways"] == ["strict redesign stub"]
    assert payload["strict_loan_core_redesign_context"]["status"] == "available"
    assert payload["strict_loan_core_redesign_context"]["artifact"] == "strict_loan_core_redesign_summary.json"
    assert "direct minimum core ≈ -10.70" in payload["strict_loan_core_redesign_context"]["headline_read"]
    assert "standard secondary comparison = `strict_loan_core_plus_private_borrower_qoq`" in payload["strict_loan_core_redesign_context"]["headline_read"]
    assert payload["strict_loan_core_redesign_context"]["published_roles"]["di_bucket_diagnostic"]["series"] == "strict_loan_di_loans_nec_qoq"
    assert payload["strict_loan_core_redesign_context"]["takeaways"] == ["strict loan-core redesign stub"]
    assert payload["strict_di_bucket_role_context"]["status"] == "available"
    assert payload["strict_di_bucket_role_context"]["artifact"] == "strict_di_bucket_role_summary.json"
    assert "standard secondary comparison ≈ 8.50" in payload["strict_di_bucket_role_context"]["headline_read"]
    assert "strict_di_loans_nec_nonfinancial_corporate_qoq" in payload["strict_di_bucket_role_context"]["headline_read"]
    assert payload["strict_di_bucket_role_context"]["release_taxonomy"]["di_bucket_diagnostic"]["series"] == "strict_loan_di_loans_nec_qoq"
    assert payload["strict_di_bucket_role_context"]["takeaways"] == ["strict di role stub"]
    assert payload["strict_di_bucket_bridge_context"]["status"] == "available"
    assert payload["strict_di_bucket_bridge_context"]["artifact"] == "strict_di_bucket_bridge_summary.json"
    assert "DI asset ≈ -2.40" in payload["strict_di_bucket_bridge_context"]["headline_read"]
    assert "bridge residual ≈ -4.00" in payload["strict_di_bucket_bridge_context"]["headline_read"]
    assert payload["strict_di_bucket_bridge_context"]["recommendation"]["next_branch"] == "build_counterpart_alignment_surface"
    assert payload["strict_di_bucket_bridge_context"]["takeaways"] == ["strict di bridge stub"]
    assert payload["strict_private_borrower_bridge_context"]["status"] == "available"
    assert payload["strict_private_borrower_bridge_context"]["artifact"] == "strict_private_borrower_bridge_summary.json"
    assert "private total ≈ 20.80" in payload["strict_private_borrower_bridge_context"]["headline_read"]
    assert "strict_di_loans_nec_nonfinancial_corporate_qoq" in payload["strict_private_borrower_bridge_context"]["headline_read"]
    assert payload["strict_private_borrower_bridge_context"]["recommendation"]["next_branch"] == "build_nonfinancial_corporate_bridge_surface"
    assert payload["strict_private_borrower_bridge_context"]["takeaways"] == ["strict private bridge stub"]
    assert payload["strict_nonfinancial_corporate_bridge_context"]["status"] == "available"
    assert payload["strict_nonfinancial_corporate_bridge_context"]["artifact"] == "strict_nonfinancial_corporate_bridge_summary.json"
    assert "nonfinancial corporate ≈ 20.80" in payload["strict_nonfinancial_corporate_bridge_context"]["headline_read"]
    assert payload["strict_nonfinancial_corporate_bridge_context"]["recommendation"]["next_branch"] == "assess_household_and_nonfinancial_noncorporate_offset_residual"
    assert payload["strict_nonfinancial_corporate_bridge_context"]["takeaways"] == ["strict nonfinancial corporate bridge stub"]
    assert payload["strict_private_offset_residual_context"]["status"] == "available"
    assert payload["strict_private_offset_residual_context"]["artifact"] == "strict_private_offset_residual_summary.json"
    assert "offset total ≈ -0.57" in payload["strict_private_offset_residual_context"]["headline_read"]
    assert payload["strict_private_offset_residual_context"]["recommendation"]["next_branch"] == "assess_corporate_bridge_secondary_comparison_role"
    assert payload["strict_private_offset_residual_context"]["takeaways"] == ["strict private offset stub"]
    assert payload["strict_corporate_bridge_secondary_comparison_context"]["status"] == "available"
    assert payload["strict_corporate_bridge_secondary_comparison_context"]["artifact"] == "strict_corporate_bridge_secondary_comparison_summary.json"
    assert "core + nonfinancial corporate ≈ -6.00" in payload["strict_corporate_bridge_secondary_comparison_context"]["headline_read"]
    assert (
        payload["strict_corporate_bridge_secondary_comparison_context"]["recommendation"]["standard_secondary_candidate"]
        == "strict_loan_core_plus_nonfinancial_corporate_qoq"
    )
    assert payload["strict_corporate_bridge_secondary_comparison_context"]["takeaways"] == ["strict corporate bridge secondary stub"]
    assert payload["strict_component_framework_context"]["status"] == "available"
    assert payload["strict_component_framework_context"]["artifact"] == "strict_component_framework_summary.json"
    assert "TOC/ROW support bundle ≈ 65.40" in payload["strict_component_framework_context"]["headline_read"]
    assert (
        payload["strict_component_framework_context"]["frozen_roles"]["standard_secondary_comparison"]
        == "strict_loan_core_plus_private_borrower_qoq"
    )
    assert (
        payload["strict_component_framework_context"]["recommendation"]["next_branch"]
        == "only_reopen_toc_or_row_if_new_incidence_evidence_appears"
    )
    assert payload["strict_component_framework_context"]["takeaways"] == ["strict framework stub"]
    assert payload["strict_release_framing_context"]["status"] == "available"
    assert payload["strict_release_framing_context"]["artifact"] == "strict_release_framing_summary.json"
    assert "TOC and ROW stay outside the strict object" in payload["strict_release_framing_context"]["headline_read"]
    assert (
        payload["strict_release_framing_context"]["release_position"]["strict_object_rule"]
        == "exclude_toc_and_row_under_current_evidence"
    )
    assert (
        payload["strict_release_framing_context"]["recommendation"]["status"]
        == "strict_release_framing_finalized"
    )
    assert payload["strict_release_framing_context"]["takeaways"] == ["strict release framing stub"]
    assert payload["strict_direct_core_component_context"]["status"] == "available"
    assert payload["strict_direct_core_component_context"]["artifact"] == "strict_direct_core_component_summary.json"
    assert "consumer credit ≈ -9.40" in payload["strict_direct_core_component_context"]["headline_read"]
    assert (
        payload["strict_direct_core_component_context"]["classification"]["h0_dominant_component"]
        == "strict_loan_consumer_credit_qoq"
    )
    assert (
        payload["strict_direct_core_component_context"]["recommendation"]["status"]
        == "keep_bundled_direct_core"
    )
    assert payload["strict_direct_core_component_context"]["takeaways"] == ["strict direct core component stub"]
    assert payload["strict_direct_core_horizon_stability_context"]["status"] == "available"
    assert (
        payload["strict_direct_core_horizon_stability_context"]["artifact"]
        == "strict_direct_core_horizon_stability_summary.json"
    )
    assert "h0 = `strict_loan_mortgages_qoq`" in payload["strict_direct_core_horizon_stability_context"]["headline_read"]
    assert (
        payload["strict_direct_core_horizon_stability_context"]["classification"]["recommendation_status"]
        == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
    )
    assert (
        payload["strict_direct_core_horizon_stability_context"]["recommendation"]["multihorizon_candidate"]
        == "strict_loan_core_min_qoq"
    )
    assert payload["strict_direct_core_horizon_stability_context"]["takeaways"] == ["strict direct core horizon stub"]
    assert payload["strict_additional_creator_candidate_context"]["status"] == "available"
    assert (
        payload["strict_additional_creator_candidate_context"]["artifact"]
        == "strict_additional_creator_candidate_summary.json"
    )
    assert "best broad validation proxy = `closed_end_residential_loans_qoq`" in payload["strict_additional_creator_candidate_context"]["headline_read"]
    assert (
        payload["strict_additional_creator_candidate_context"]["classification"]["h0_best_extension_candidate"]
        == "commercial_industrial_loans_qoq"
    )
    assert (
        payload["strict_additional_creator_candidate_context"]["recommendation"]["status"]
        == "no_additional_extension_candidate_supported"
    )
    assert payload["strict_additional_creator_candidate_context"]["takeaways"] == ["strict additional creator stub"]
    assert payload["strict_di_loans_nec_measurement_audit_context"]["status"] == "available"
    assert (
        payload["strict_di_loans_nec_measurement_audit_context"]["artifact"]
        == "strict_di_loans_nec_measurement_audit_summary.json"
    )
    assert "do not isolate a promotable same-scope transaction split" in payload["strict_di_loans_nec_measurement_audit_context"]["headline_read"]
    assert (
        payload["strict_di_loans_nec_measurement_audit_context"]["classification"]["h0_best_same_scope_proxy"]
        == "loans_to_nondepository_financial_institutions_qoq"
    )
    assert (
        payload["strict_di_loans_nec_measurement_audit_context"]["recommendation"]["status"]
        == "no_promotable_same_scope_transaction_subcomponent_supported"
    )
    assert payload["strict_di_loans_nec_measurement_audit_context"]["takeaways"] == ["strict di measurement audit stub"]
    assert payload["strict_results_closeout_context"]["status"] == "available"
    assert payload["strict_results_closeout_context"]["artifact"] == "strict_results_closeout_summary.json"
    assert "core residual ≈ -5.50" in payload["strict_results_closeout_context"]["headline_read"]
    assert (
        payload["strict_results_closeout_context"]["classification"]["branch_state"]
        == "strict_empirical_expansion_effectively_complete"
    )
    assert (
        payload["strict_results_closeout_context"]["recommendation"]["status"]
        == "move_to_writeup_and_results_packaging"
    )
    assert payload["strict_results_closeout_context"]["takeaways"] == ["strict closeout stub"]


def test_pass_through_summary_surfaces_tdcest_ladder_integration_context() -> None:
    empty_lp = pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_variant = pd.DataFrame(columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_control = pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_sample = pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_contrast = pd.DataFrame(columns=["scope", "variant", "role", "horizon", "beta_total", "beta_other", "beta_implied", "beta_direct", "gap_implied_minus_direct", "abs_gap", "n_total", "n_other", "n_direct", "sample_mismatch_flag", "contrast_consistent"])
    payload = build_pass_through_summary(
        lp_irf=empty_lp,
        sensitivity=empty_variant,
        control_sensitivity=empty_control,
        sample_sensitivity=empty_sample,
        contrast=empty_contrast,
        lp_irf_regimes=pd.DataFrame(),
        readiness={"status": "provisional", "reasons": [], "warnings": []},
        tdcest_ladder_integration_summary={
            "status": "available",
            "classification": {"decision": "selective_integration_not_wholesale_pivot"},
            "recommendation": {"status": "import_selected_tdcest_ladder_rows_only"},
            "series_roles": [
                {"series_key": "tdc_tier2_bank_only_qoq", "latest_value": -38.09},
                {"series_key": "tdc_tier3_bank_only_qoq", "latest_value": -40.16},
                {"series_key": "tdc_bank_receipt_historical_overlay_qoq", "latest_nonzero_quarter": "2024Q4"},
                {"series_key": "tdc_row_mrv_nondefault_pilot_qoq", "latest_nonzero_quarter": "2025Q3"},
            ],
            "takeaways": ["tdcest ladder stub"],
        },
    )

    assert payload["tdcest_ladder_integration_context"]["status"] == "available"
    assert payload["tdcest_ladder_integration_context"]["artifact"] == "tdcest_ladder_integration_summary.json"
    assert "latest Tier 2 bank-only comparison ≈ -38.09" in payload["tdcest_ladder_integration_context"]["headline_read"]
    assert (
        payload["tdcest_ladder_integration_context"]["classification"]["decision"]
        == "selective_integration_not_wholesale_pivot"
    )
    assert (
        payload["tdcest_ladder_integration_context"]["recommendation"]["status"]
        == "import_selected_tdcest_ladder_rows_only"
    )
    assert payload["tdcest_ladder_integration_context"]["takeaways"] == ["tdcest ladder stub"]


def test_pass_through_summary_surfaces_tdcest_broad_object_comparison_context() -> None:
    empty_lp = pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_treatment = pd.DataFrame(
        columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_control = pd.DataFrame(
        columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_sample = pd.DataFrame(
        columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_contrast = pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"])
    empty_regimes = pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    payload = build_pass_through_summary(
        lp_irf=empty_lp,
        sensitivity=empty_treatment,
        control_sensitivity=empty_control,
        sample_sensitivity=empty_sample,
        contrast=empty_contrast,
        lp_irf_regimes=empty_regimes,
        readiness={"status": "not_ready", "reasons": [], "warnings": []},
        tdcest_broad_object_comparison_summary={
            "status": "available",
            "headline_question": "stub",
            "estimation_path": {"summary_artifact": "tdcest_broad_object_comparison_summary.json"},
            "latest_common_broad_comparison": {
                "quarter": "2025Q4",
                "headline_bank_only_beta": 68.996,
                "tier2_bank_only_beta": -38.086565167702794,
                "tier3_bank_only_beta": -40.1625295472228,
                "tier3_broad_depository_beta": -39.94452954722279,
            },
            "supplemental_surfaces": {
                "historical_bank_receipt_overlay": {"latest_nonzero_quarter": "2024Q4"},
                "row_mrv_nondefault_pilot": {"latest_nonzero_quarter": "2025Q3"},
            },
            "classification": {"role": "broad_object_comparison_only"},
            "recommendation": {"status": "use_as_broad_object_comparison_layer_only"},
            "takeaways": ["tdcest broad comparison stub"],
        },
    )

    assert payload["tdcest_broad_object_comparison_context"]["status"] == "available"
    assert (
        payload["tdcest_broad_object_comparison_context"]["artifact"]
        == "tdcest_broad_object_comparison_summary.json"
    )
    assert "Latest common broad-object comparison 2025Q4" in payload["tdcest_broad_object_comparison_context"]["headline_read"]
    assert (
        payload["tdcest_broad_object_comparison_context"]["classification"]["role"]
        == "broad_object_comparison_only"
    )
    assert (
        payload["tdcest_broad_object_comparison_context"]["recommendation"]["status"]
        == "use_as_broad_object_comparison_layer_only"
    )
    assert payload["tdcest_broad_object_comparison_context"]["takeaways"] == ["tdcest broad comparison stub"]


def test_pass_through_summary_surfaces_tdcest_broad_treatment_sensitivity_context() -> None:
    empty_lp = pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_treatment = pd.DataFrame(
        columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_control = pd.DataFrame(
        columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_sample = pd.DataFrame(
        columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    empty_contrast = pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"])
    empty_regimes = pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    payload = build_pass_through_summary(
        lp_irf=empty_lp,
        sensitivity=empty_treatment,
        control_sensitivity=empty_control,
        sample_sensitivity=empty_sample,
        contrast=empty_contrast,
        lp_irf_regimes=empty_regimes,
        readiness={"status": "not_ready", "reasons": [], "warnings": []},
        tdcest_broad_treatment_sensitivity_summary={
            "status": "available",
            "headline_question": "stub",
            "estimation_path": {"summary_artifact": "tdcest_broad_treatment_sensitivity_summary.json"},
            "classification": {"headline_direction_status": "unchanged_across_corrected_broad_variants"},
            "key_horizons": {
                "h0": {
                    "baseline": {
                        "total_deposits": {"beta": 1.0},
                        "other_component": {"beta": -0.4},
                    },
                    "variants": {
                        "tier2_bank_only": {
                            "total_deposits": {"beta": 0.8},
                            "other_component": {"beta": -0.3},
                        },
                        "tier3_bank_only": {
                            "total_deposits": {"beta": 0.7},
                            "other_component": {"beta": -0.2},
                        },
                    },
                }
            },
            "recommendation": {"status": "use_as_broad_object_sensitivity_only"},
            "takeaways": ["tdcest broad treatment stub"],
        },
    )

    assert payload["tdcest_broad_treatment_sensitivity_context"]["status"] == "available"
    assert (
        payload["tdcest_broad_treatment_sensitivity_context"]["artifact"]
        == "tdcest_broad_treatment_sensitivity_summary.json"
    )
    assert "baseline h0 total ≈ 1.00" in payload["tdcest_broad_treatment_sensitivity_context"]["headline_read"]
    assert (
        payload["tdcest_broad_treatment_sensitivity_context"]["classification"]["headline_direction_status"]
        == "unchanged_across_corrected_broad_variants"
    )
    assert (
        payload["tdcest_broad_treatment_sensitivity_context"]["recommendation"]["status"]
        == "use_as_broad_object_sensitivity_only"
    )
    assert payload["tdcest_broad_treatment_sensitivity_context"]["takeaways"] == ["tdcest broad treatment stub"]


def test_pass_through_summary_surfaces_insufficient_history_tdcest_broad_treatment_context() -> None:
    empty_lp = pd.DataFrame(columns=["outcome", "horizon", "beta", "se", "lower95", "upper95", "n"])
    empty_treatment = pd.DataFrame(
        columns=["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]
    )
    payload = build_pass_through_summary(
        lp_irf=empty_lp,
        sensitivity=empty_treatment,
        control_sensitivity=pd.DataFrame(columns=["control_variant", "control_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        sample_sensitivity=pd.DataFrame(columns=["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "not_ready", "reasons": [], "warnings": []},
        tdcest_broad_treatment_sensitivity_summary={
            "status": "insufficient_history",
            "reason": "corrected_tdcest_variants_do_not_clear_current_shock_history_gate",
            "recommendation": {"status": "use_tdcest_ladder_as_level_comparison_only"},
            "takeaways": ["baseline still available", "history too short"],
            "exploratory_short_history": {
                "baseline": {
                    "total_deposits": {"beta": 1.1},
                    "other_component": {"beta": -0.9},
                },
                "variants": {
                    "tier2_bank_only": {
                        "total_deposits": {"beta": 0.8},
                        "other_component": {"beta": -0.7},
                    }
                },
            },
        },
    )

    assert payload["tdcest_broad_treatment_sensitivity_context"]["status"] == "insufficient_history"
    assert (
        payload["tdcest_broad_treatment_sensitivity_context"]["recommendation"]["status"]
        == "use_tdcest_ladder_as_level_comparison_only"
    )
    assert "Tier 2 is total" in payload["tdcest_broad_treatment_sensitivity_context"]["headline_read"]
    assert payload["tdcest_broad_treatment_sensitivity_context"]["exploratory_short_history"]["baseline"]["total_deposits"]["beta"] == 1.1


def test_pass_through_summary_prefers_exact_variant_artifacts_when_available() -> None:
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.5, "se": 0.2, "lower95": 1.1, "upper95": 1.9, "n": 38},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.7, "se": 0.2, "lower95": -1.1, "upper95": -0.3, "n": 38},
        ]
    )
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(columns=identity_lp.columns),
        identity_lp_irf=identity_lp,
        sensitivity=pd.DataFrame(
            [{"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 99.0, "se": 1.0, "lower95": 97.0, "upper95": 101.0, "n": 30}]
        ),
        identity_sensitivity=pd.DataFrame(
            [{"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "target": "tdc_bank_only_qoq", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38, "spec_name": "identity_treatment_sensitivity", "shock_column": "tdc_residual_z", "decomposition_mode": "exact_identity_baseline", "outcome_construction": "estimated_common_design", "inference_method": "bootstrap"}]
        ),
        control_sensitivity=pd.DataFrame(
            [{"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 99.0, "se": 1.0, "lower95": 97.0, "upper95": 101.0, "n": 30}]
        ),
        identity_control_sensitivity=pd.DataFrame(
            [{"control_variant": "headline_lagged_macro", "control_role": "headline", "control_columns": "lag_tdc_bank_only_qoq|lag_fedfunds", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38, "spec_name": "identity_control_sensitivity", "shock_column": "tdc_residual_z", "shock_scale": "rolling_oos_standard_deviation", "response_type": "cumulative_sum_h0_to_h", "decomposition_mode": "exact_identity_baseline", "outcome_construction": "estimated_common_design", "inference_method": "bootstrap"}]
        ),
        sample_sensitivity=pd.DataFrame(
            [{"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 99.0, "se": 1.0, "lower95": 97.0, "upper95": 101.0, "n": 30}]
        ),
        identity_sample_sensitivity=pd.DataFrame(
            [{"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 38, "spec_name": "identity_sample_sensitivity", "shock_column": "tdc_residual_z", "shock_scale": "rolling_oos_standard_deviation", "response_type": "cumulative_sum_h0_to_h", "decomposition_mode": "exact_identity_baseline", "outcome_construction": "estimated_common_design", "inference_method": "bootstrap"}]
        ),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
    )

    assert payload["estimation_path"]["treatment_variant_artifact"] == "identity_treatment_sensitivity.csv"
    assert payload["estimation_path"]["control_variant_artifact"] == "identity_control_sensitivity.csv"
    assert payload["estimation_path"]["sample_variant_artifact"] == "identity_sample_sensitivity.csv"


def test_pass_through_summary_uses_cooler_provisional_headline_for_exact_baseline() -> None:
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

    assert "impact-stage sign pattern" in payload["headline_answer"]
    assert "suggestive of crowd-out" in payload["headline_answer"]
    assert "mechanism attribution remain unsettled" in payload["headline_answer"]
    assert "out of scope in the current release" in payload["headline_answer"]


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


def test_pass_through_summary_prefers_identity_variant_artifacts_when_available() -> None:
    payload = build_pass_through_summary(
        lp_irf=pd.DataFrame(
            [
                {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.1, "se": 0.2, "lower95": 0.7, "upper95": 1.5, "n": 40},
                {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.3, "upper95": 1.1, "n": 40},
                {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
            ]
        ),
        sensitivity=pd.DataFrame(
            [
                {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -9.0, "se": 1.0, "lower95": -11.0, "upper95": -7.0, "n": 40},
                {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 40},
            ]
        ),
        identity_sensitivity=pd.DataFrame(
            [
                {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.3, "upper95": 1.1, "n": 40},
                {"treatment_variant": "baseline", "treatment_role": "core", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
            ]
        ),
        control_sensitivity=pd.DataFrame(
            [
                {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 40},
                {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 40},
            ]
        ),
        identity_control_sensitivity=pd.DataFrame(
            [
                {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 40},
                {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.3, "se": 0.2, "lower95": -0.7, "upper95": 0.1, "n": 40},
            ]
        ),
        sample_sensitivity=pd.DataFrame(
            [
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.1, "se": 0.2, "lower95": 0.7, "upper95": 1.5, "n": 40},
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 40},
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "other_component_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.0, "upper95": 5.0, "n": 40},
            ]
        ),
        identity_sample_sensitivity=pd.DataFrame(
            [
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.1, "se": 0.2, "lower95": 0.7, "upper95": 1.5, "n": 40},
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.3, "upper95": 1.1, "n": 40},
                {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
            ]
        ),
        contrast=pd.DataFrame(columns=["scope", "variant", "role", "horizon", "gap_implied_minus_direct", "contrast_consistent"]),
        lp_irf_regimes=pd.DataFrame(columns=["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"]),
        readiness={"status": "provisional", "warnings": [], "reasons": []},
    )

    assert payload["estimation_path"]["treatment_variant_artifact"] == "identity_treatment_sensitivity.csv"
    assert payload["estimation_path"]["control_variant_artifact"] == "identity_control_sensitivity.csv"
    assert payload["estimation_path"]["sample_variant_artifact"] == "identity_sample_sensitivity.csv"
    assert payload["core_treatment_variants"][0]["horizons"]["h0"]["total_deposits"]["beta"] == 0.7
    assert payload["core_control_variants"][0]["horizons"]["h0"]["total_deposits"]["beta"] == 0.8
    assert payload["shock_sample_variants"][0]["horizons"]["h0"]["total_deposits"]["beta"] == 0.7


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
