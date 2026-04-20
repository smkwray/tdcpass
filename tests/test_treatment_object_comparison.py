from __future__ import annotations

from tdcpass.analysis.treatment_object_comparison import build_treatment_object_comparison_summary


def test_treatment_object_comparison_recommends_split_architecture() -> None:
    payload = build_treatment_object_comparison_summary(
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
                        "residual_response": {"beta": -5.51},
                        "strict_gap_share_of_residual": 2.96,
                    }
                }
            },
        },
        strict_missing_channel_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "toc_row_excluded": {
                        "strict_identifiable_total_response": {"beta": 10.83},
                        "strict_gap_after_funding_share_of_residual_abs": 2.84,
                    }
                }
            },
        },
        strict_sign_mismatch_audit_summary={
            "status": "available",
            "shock_alignment": {"shock_corr": 0.42, "same_sign_share": 0.72},
            "quarter_concentration": {"top5_abs_gap_share": 0.33, "dominant_period_bucket": "covid_post"},
        },
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["recommended_next_branch"] == "split_core_plus_support_bundle"
    assert payload["candidate_objects"][0]["candidate"] == "baseline_full_tdc"
    assert payload["candidate_objects"][2]["candidate"] == "toc_row_excluded_core"
    assert payload["candidate_objects"][2]["h0_strict_identifiable_total_beta"] == 10.83
    assert "TOC/ROW-excluded object is diagnostic only" in payload["takeaways"][2]
