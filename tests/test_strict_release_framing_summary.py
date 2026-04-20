from __future__ import annotations

from tdcpass.analysis.strict_release_framing_summary import build_strict_release_framing_summary


def test_strict_release_framing_freezes_toc_and_row_outside_strict_object() -> None:
    payload = build_strict_release_framing_summary(
        strict_component_framework_summary={
            "status": "available",
            "frozen_roles": {
                "headline_direct_core": "strict_loan_core_min_qoq",
                "multihorizon_direct_core": "strict_loan_core_min_qoq",
                "impact_horizon_candidate": "strict_loan_mortgages_qoq",
                "standard_secondary_comparison": "strict_loan_core_plus_nonfinancial_corporate_qoq",
                "narrowing_diagnostic": "strict_loan_core_plus_private_borrower_qoq",
            },
            "h0_snapshot": {
                "toc_row_support_bundle_beta": 65.4,
                "core_residual_beta": -5.51,
                "headline_direct_core_beta": -10.66,
                "standard_secondary_beta": 9.44,
            },
        },
        toc_row_liability_incidence_raw_summary={
            "status": "available",
            "classification": {"decision_gate": "full_reincorporation_not_supported"},
            "key_horizons": {
                "h0": {
                    "toc_leg": {
                        "counterpart_share_of_leg_beta": {
                            "deposits_only_bank_qoq": 0.66,
                            "reserves_qoq": 1.26,
                        }
                    },
                    "row_leg": {
                        "counterpart_share_of_leg_beta": {
                            "checkable_rest_of_world_bank_qoq": 0.02,
                            "foreign_nonts_qoq": 2.26,
                        }
                    },
                }
            },
        },
        toc_validated_share_candidate_summary={
            "status": "available",
            "classification": {"decision": "keep_toc_outside_strict_object_under_current_evidence"},
            "key_horizons": {
                "h0": {
                    "best_candidate": {
                        "implied_residual_beta": -34.16,
                        "abs_gap_to_direct_core": 23.50,
                    }
                }
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["release_position"]["strict_object_rule"] == "exclude_toc_and_row_under_current_evidence"
    assert payload["release_position"]["toc_rule"] == "outside_strict_object"
    assert payload["release_position"]["row_rule"] == "outside_strict_object"
    assert payload["release_position"]["multihorizon_direct_benchmark"] == "strict_loan_core_min_qoq"
    assert payload["release_position"]["impact_horizon_candidate"] == "strict_loan_mortgages_qoq"
    assert payload["classification"]["release_state"] == "strict_release_framing_finalized"
    assert payload["classification"]["raw_incidence_gate"] == "full_reincorporation_not_supported"
    assert payload["classification"]["toc_narrow_share_decision"] == "keep_toc_outside_strict_object_under_current_evidence"
    assert payload["recommendation"]["status"] == "strict_release_framing_finalized"
    assert payload["recommendation"]["strict_rule"] == "exclude_toc_and_row_from_strict_object"
    assert payload["recommendation"]["reopen_rule"] == "reopen_only_if_new_scope_and_timing_matched_incidence_evidence_appears"
    assert payload["h0_snapshot"]["toc_deposits_only_share"] == 0.66
    assert payload["h0_snapshot"]["row_external_share"] == 2.26
    assert any("TOC and ROW stay outside the strict object" in item for item in payload["takeaways"])
    assert any("impact-horizon candidate = `strict_loan_mortgages_qoq`" in item for item in payload["takeaways"])
