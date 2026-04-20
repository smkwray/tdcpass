from __future__ import annotations

from tdcpass.analysis.toc_validated_share_candidate_summary import (
    build_toc_validated_share_candidate_summary,
)


def test_toc_validated_share_candidate_summary_rejects_narrow_toc_reincorporation() -> None:
    payload = build_toc_validated_share_candidate_summary(
        toc_row_liability_incidence_raw_summary={
            "status": "available",
            "quarterly_alignment": {
                "toc_leg": {
                    "best_in_scope_corr": 0.25,
                    "best_support_corr": 0.95,
                    "in_scope_counterparts": {
                        "deposits_only_bank_qoq": {"same_quarter_sign_match_share": 0.53},
                        "checkable_private_domestic_bank_qoq": {"same_quarter_sign_match_share": 0.50},
                    },
                }
            },
            "key_horizons": {
                "h0": {
                    "toc_leg": {
                        "leg_response": {"beta": 70.44},
                        "counterpart_share_of_leg_beta": {
                            "total_deposits_bank_qoq": 0.79,
                            "deposits_only_bank_qoq": 0.66,
                            "checkable_private_domestic_bank_qoq": 0.41,
                        },
                    }
                }
            },
        },
        strict_component_framework_summary={
            "status": "available",
            "h0_snapshot": {
                "core_residual_beta": -5.51,
                "headline_direct_core_beta": -10.66,
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["classification"]["quarterly_stability_gate"] == "fails"
    assert payload["classification"]["direct_core_fit_gate"] == "fails"
    assert payload["classification"]["decision"] == "keep_toc_outside_strict_object_under_current_evidence"
    assert payload["recommendation"]["toc_rule"] == "keep_outside_strict_object"
    best = payload["key_horizons"]["h0"]["best_candidate"]
    assert best["label"] == "h0_private_checkable_share_candidate"
    assert best["improves_vs_core_residual_gap"] is False
