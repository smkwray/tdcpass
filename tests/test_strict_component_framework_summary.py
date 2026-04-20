from __future__ import annotations

from tdcpass.analysis.strict_component_framework_summary import build_strict_component_framework_summary


def test_strict_component_framework_freezes_current_release_roles() -> None:
    payload = build_strict_component_framework_summary(
        big_picture_synthesis_summary={
            "status": "available",
            "interpretation": "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated",
        },
        split_treatment_architecture_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "support_bundle_beta": 65.4,
                    "core_deposit_proximate_residual_response": {"beta": -5.5},
                }
            },
        },
        core_treatment_promotion_summary={
            "status": "available",
            "promotion_recommendation": {"status": "keep_interpretive_only"},
        },
        strict_loan_core_redesign_summary={
            "status": "available",
            "recommendation": {"release_headline_candidate": "strict_loan_core_min_qoq"},
        },
        strict_corporate_bridge_secondary_comparison_summary={
            "status": "available",
            "recommendation": {
                "standard_secondary_candidate": "strict_loan_core_plus_nonfinancial_corporate_qoq",
                "secondary_comparison_retained_for_diagnostics": "strict_loan_core_plus_private_borrower_qoq",
                "fit_preferred_secondary_candidate": "strict_loan_core_plus_private_borrower_qoq",
                "private_offset_role": "diagnostic_only",
            },
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "headline_direct_core_response": {"beta": -10.66},
                        "core_plus_private_bridge_response": {"beta": 8.49},
                        "core_plus_nonfinancial_corporate_response": {"beta": 9.44},
                    }
                }
            },
        },
        toc_row_incidence_audit_summary={
            "status": "available",
            "classification": {
                "bundle_role": "measured_support_bundle_with_unresolved_strict_deposit_incidence",
            },
        },
        toc_row_liability_incidence_raw_summary={
            "status": "available",
            "classification": {"decision_gate": "full_reincorporation_not_supported"},
            "recommendation": {
                "status": "raw_incidence_binary_gate_completed",
                "next_branch": "decide_whether_any_validated_toc_share_belongs_in_strict_object",
            },
        },
        toc_validated_share_candidate_summary={
            "status": "available",
            "classification": {"decision": "keep_toc_outside_strict_object_under_current_evidence"},
            "recommendation": {
                "status": "toc_candidate_gate_completed",
                "toc_rule": "keep_outside_strict_object",
                "next_branch": "finalize_release_framing_that_toc_and_row_stay_outside_strict_object",
            },
        },
        strict_direct_core_horizon_stability_summary={
            "status": "available",
            "horizon_winners": {
                "h0": "strict_loan_mortgages_qoq",
                "h4": "strict_loan_core_min_qoq",
                "h8": "strict_loan_core_min_qoq",
            },
            "classification": {
                "recommendation_status": "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate",
            },
            "recommendation": {
                "impact_candidate": "strict_loan_mortgages_qoq",
                "multihorizon_candidate": "strict_loan_core_min_qoq",
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["frozen_roles"]["accounting_lane_role"] == "non_evidence_for_independent_verification"
    assert payload["frozen_roles"]["headline_direct_core"] == "strict_loan_core_min_qoq"
    assert payload["frozen_roles"]["multihorizon_direct_core"] == "strict_loan_core_min_qoq"
    assert payload["frozen_roles"]["impact_horizon_candidate"] == "strict_loan_mortgages_qoq"
    assert payload["frozen_roles"]["standard_secondary_comparison"] == "strict_loan_core_plus_nonfinancial_corporate_qoq"
    assert payload["frozen_roles"]["narrowing_diagnostic"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["frozen_roles"]["toc_row_role"] == "measured_support_bundle_with_unresolved_strict_deposit_incidence"
    assert payload["frozen_roles"]["toc_narrow_share_role"] == "not_reincorporated_under_current_evidence"
    assert payload["classification"]["framework_state"] == "external_critique_incorporated_and_toc_candidate_gate_built"
    assert payload["classification"]["external_critique_readiness"] == "critique_incorporated"
    assert payload["classification"]["raw_incidence_decision_gate"] == "full_reincorporation_not_supported"
    assert payload["classification"]["toc_narrow_share_decision"] == "keep_toc_outside_strict_object_under_current_evidence"
    assert (
        payload["classification"]["direct_core_horizon_rule"]
        == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
    )
    assert payload["recommendation"]["status"] == "strict_release_framing_finalized"
    assert payload["recommendation"]["toc_release_role"] == "keep_outside_strict_object"
    assert payload["recommendation"]["multihorizon_direct_core"] == "strict_loan_core_min_qoq"
    assert payload["recommendation"]["impact_horizon_candidate"] == "strict_loan_mortgages_qoq"
    assert payload["recommendation"]["next_branch"] == "only_reopen_toc_or_row_if_new_incidence_evidence_appears"
    assert payload["h0_snapshot"]["toc_row_support_bundle_beta"] == 65.4
    assert payload["h0_snapshot"]["core_residual_beta"] == -5.5
    assert payload["h0_snapshot"]["headline_direct_core_beta"] == -10.66
    assert payload["h0_snapshot"]["standard_secondary_beta"] == 9.44
    assert payload["h0_snapshot"]["narrowing_diagnostic_beta"] == 8.49
    assert any("Closure-oriented accounting remains out of the independent-evidence tier" in item for item in payload["takeaways"])
    assert any("impact-horizon candidate = `strict_loan_mortgages_qoq`" in item for item in payload["takeaways"])
