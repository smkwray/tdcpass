from __future__ import annotations

import pandas as pd

from tdcpass.analysis.split_treatment_architecture import build_split_treatment_architecture_summary


def test_split_treatment_architecture_summary_builds_explicit_core_and_support_bundle() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2000Q1", "2000Q2"],
            "tdc_bank_only_qoq": [10.0, 12.0],
            "tdc_core_deposit_proximate_bank_only_qoq": [4.0, 5.0],
            "tdc_toc_row_support_bundle_qoq": [6.0, 7.0],
            "other_component_qoq": [-8.0, -9.0],
            "other_component_core_deposit_proximate_bank_only_qoq": [-2.0, -2.0],
        }
    )

    payload = build_split_treatment_architecture_summary(
        shocked=shocked,
        tdc_treatment_audit_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "baseline_tdc_response": {"beta": 82.2},
                    "baseline_residual_response": {"beta": -72.7},
                    "direct_component_responses": {
                        "rest_of_world_treasury_transactions": {"signed_contribution_beta": 11.8},
                        "treasury_operating_cash_drain": {"signed_contribution_beta": -55.4},
                    },
                    "variant_removal_diagnostics": {
                        "no_toc_no_row_bank_only": {
                            "target_response": {"beta": 14.9},
                            "residual_response": {"beta": -5.5},
                            "residual_shift_vs_baseline_beta": 67.2,
                        }
                    },
                }
            },
        },
        toc_row_path_split_summary={
            "status": "available",
            "quarterly_split": {"preferred_quarterly_path": "direct_deposit_path_dominant"},
            "key_horizons": {
                "h0": {
                    "preferred_horizon_path": "broad_support_path_dominant",
                    "broad_support_path_response": {"beta": 87.8},
                    "direct_deposit_path_response": {"beta": 61.4},
                }
            },
        },
        treatment_object_comparison_summary={
            "status": "available",
            "recommendation": {
                "recommended_next_branch": "split_core_plus_support_bundle",
                "headline_decision_now": "keep current headline provisional and do not promote the TOC_ROW_excluded object",
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["series_definitions"]["core_deposit_proximate_treatment"] == "tdc_core_deposit_proximate_bank_only_qoq"
    assert payload["series_definitions"]["support_bundle_treatment"] == "tdc_toc_row_support_bundle_qoq"
    assert payload["quarterly_alignment"]["tdc_identity"]["quarterly_alignment"] == "exact"
    assert payload["quarterly_alignment"]["residual_identity"]["quarterly_alignment"] == "exact"
    assert payload["key_horizons"]["h0"]["support_bundle_beta"] == 67.3
    assert payload["key_horizons"]["h0"]["support_bundle_residual_shift_vs_baseline_beta"] == 67.2
    assert abs(payload["key_horizons"]["h0"]["direct_support_bundle_signed_beta"] + 43.6) < 1e-12
    assert payload["architecture_recommendation"]["recommended_next_branch"] == "split_core_plus_support_bundle"
    assert "deposit-proximate core" in payload["takeaways"][0]
