from __future__ import annotations

from tdcpass.analysis.big_picture_synthesis import build_big_picture_synthesis_summary


def test_big_picture_synthesis_classifies_treatment_side_problem_and_unvalidated_strict_lane() -> None:
    payload = build_big_picture_synthesis_summary(
        scope_alignment_summary={
            "status": "available",
            "deposit_concepts": {
                "total_deposits_including_interbank": {
                    "key_horizons": {
                        "h0": {
                            "variants": {
                                "domestic_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 19.97}
                                },
                                "us_chartered_bank_only": {
                                    "differences_vs_baseline_beta": {"residual_response": 9.63}
                                },
                            }
                        }
                    }
                }
            },
        },
        broad_scope_system_summary={
            "status": "available",
            "broad_matched_system": {"key_horizons": {"h0": {"broad_strict_gap_share_of_residual": 0.81}}},
        },
        tdc_treatment_audit_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "baseline_residual_response": {"beta": -72.74},
                    "variant_removal_diagnostics": {
                        "domestic_bank_only": {"residual_shift_vs_baseline_beta": 19.97},
                        "no_foreign_bank_sectors": {"residual_shift_vs_baseline_beta": -0.17},
                        "no_toc_bank_only": {"residual_shift_vs_baseline_beta": 50.33},
                        "no_toc_no_row_bank_only": {"residual_shift_vs_baseline_beta": 67.24},
                    },
                }
            },
        },
        toc_row_excluded_interpretation_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "toc_row_excluded": {
                        "residual_response": {"beta": -5.50},
                        "strict_gap_share_of_residual": 2.96,
                    },
                    "interpretation": "toc_row_exclusion_removes_most_residual_but_strict_gap_remains",
                }
            },
        },
        strict_missing_channel_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "toc_row_excluded": {
                        "strict_identifiable_total_response": {"beta": 10.83},
                        "strict_identifiable_net_after_funding_response": {"beta": 9.90},
                        "strict_gap_after_funding_share_of_residual_abs": 2.84,
                    },
                    "interpretation": "toc_row_exclusion_exposes_sign_mismatch_in_direct_counts",
                }
            },
        },
        strict_sign_mismatch_audit_summary={
            "status": "available",
            "shock_alignment": {"shock_corr": 0.42, "same_sign_share": 0.72},
            "quarter_concentration": {"top5_abs_gap_share": 0.33, "dominant_period_bucket": "covid_post"},
        },
        strict_top_gap_anomaly_backdrop_summary={
            "status": "available",
            "interpretation": "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall",
            "liquidity_external_abs_to_corporate_abs_ratio": 2.31,
        },
    )

    assert payload["status"] == "available"
    assert (
        payload["interpretation"]
        == "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated"
    )
    assert payload["classification"]["scope_issue_status"] == "real_but_partial"
    assert payload["classification"]["treatment_issue_status"] == "toc_row_dominant"
    assert payload["classification"]["independent_lane_status"] == "not_validated"
    assert payload["h0_snapshot"]["toc_row_excluded_residual_beta"] == -5.50
    assert payload["h0_snapshot"]["toc_row_excluded_strict_identifiable_total_beta"] == 10.83
    assert payload["quarter_composition"]["dominant_period_bucket"] == "covid_post"
    assert payload["supporting_case"]["anomaly_quarter"] == "2009Q4"
    assert len(payload["takeaways"]) >= 5
