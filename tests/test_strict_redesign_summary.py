from __future__ import annotations

from tdcpass.analysis.strict_redesign_summary import build_strict_redesign_summary


def test_strict_redesign_summary_prioritizes_loan_core_redesign_after_treatment_split() -> None:
    payload = build_strict_redesign_summary(
        strict_identifiable_followup_summary={
            "status": "available",
            "scope_check_gap_assessment": {
                "key_horizons": {
                    "h0": {
                        "variant_gap_assessments": {
                            "us_chartered_bank_only": {
                                "remaining_share_of_baseline_strict_gap": 0.92,
                                "relief_share_of_baseline_strict_gap": 0.08,
                            }
                        }
                    }
                }
            },
            "strict_component_diagnostics": {
                "key_horizons": {
                    "h0": {
                        "dominant_loan_component": "strict_loan_consumer_credit_qoq",
                        "strict_loan_di_loans_nec_share_of_loan_source_beta": 0.01,
                        "strict_identifiable_total": {"beta": -2.1},
                        "strict_identifiable_gap": {"beta": -75.6},
                    }
                }
            },
            "funding_offset_sensitivity": {
                "key_horizons": {"h0": {"strict_funding_offset_share_of_identifiable_total_beta": 0.78}}
            },
        },
        strict_missing_channel_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "toc_row_excluded": {
                        "residual_response": {"beta": -5.5},
                        "strict_identifiable_total_response": {"beta": 10.8},
                        "strict_identifiable_net_after_funding_response": {"beta": 9.9},
                        "strict_gap_share_of_residual_abs": 2.96,
                        "strict_gap_after_funding_share_of_residual_abs": 2.80,
                    },
                    "interpretation": "toc_row_exclusion_exposes_sign_mismatch_in_direct_counts",
                }
            },
        },
        split_treatment_architecture_summary={
            "status": "available",
            "architecture_recommendation": {"recommended_next_branch": "split_core_plus_support_bundle"},
            "key_horizons": {
                "h0": {
                    "support_bundle_beta": 65.4,
                    "core_deposit_proximate_target_response": {"beta": 14.9},
                    "core_deposit_proximate_residual_response": {"beta": -5.5},
                }
            },
        },
        core_treatment_promotion_summary={
            "status": "available",
            "promotion_recommendation": {"status": "keep_interpretive_only"},
            "strict_validation_check": {
                "h0_core_residual_beta": -5.5,
                "h0_strict_identifiable_total_beta": 10.8,
                "h0_gap_after_funding_beta": -15.6,
                "h0_sign_match": False,
            },
        },
    )

    assert payload["status"] == "available"
    assert (
        payload["current_strict_problem_definition"]["label"]
        == "treatment_split_fixed_but_core_direct_counts_still_point_the_wrong_way"
    )
    assert payload["failure_modes"]["scope_mismatch_not_primary"]["status"] == "not_primary"
    assert payload["failure_modes"]["sign_mismatch_under_core_residual"]["status"] == "confirmed"
    assert payload["failure_modes"]["loan_bucket_shape"]["status"] == "loan_core_not_h0_di_loans_nec_concentrated"
    assert payload["failure_modes"]["funding_offset_instability"]["status"] == "material"
    assert payload["recommended_build_order"][0]["step"] == "redesign_strict_loan_core_before_adding_more_channels"
    assert any("DI-loans-n.e.c." in takeaway for takeaway in payload["takeaways"])
    assert any("Funding offsets" in takeaway for takeaway in payload["takeaways"])
