from __future__ import annotations

from tdcpass.analysis.strict_results_closeout_summary import build_strict_results_closeout_summary


def test_strict_results_closeout_moves_repo_to_writeup() -> None:
    payload = build_strict_results_closeout_summary(
        strict_release_framing_summary={
            "status": "available",
            "release_position": {
                "full_tdc_release_role": "broad_treasury_attributed_object_only",
                "strict_object_rule": "exclude_toc_and_row_under_current_evidence",
                "headline_direct_benchmark": "strict_loan_core_min_qoq",
                "impact_horizon_candidate": "strict_loan_mortgages_qoq",
                "standard_bridge_comparison": "strict_loan_core_plus_nonfinancial_corporate_qoq",
                "diagnostic_envelope": "strict_loan_core_plus_private_borrower_qoq",
            },
            "evidence_tiers": {"independent_evidence": ["strict_loan_core_min_qoq"]},
        },
        strict_component_framework_summary={
            "status": "available",
            "frozen_roles": {
                "narrowing_diagnostic": "strict_loan_core_plus_private_borrower_qoq",
            },
            "h0_snapshot": {
                "toc_row_support_bundle_beta": 65.4,
                "core_residual_beta": -5.51,
                "headline_direct_core_beta": -10.66,
                "standard_secondary_beta": 9.44,
            },
        },
        strict_di_loans_nec_measurement_audit_summary={
            "status": "available",
            "classification": {
                "same_scope_transaction_subcomponent_status": "not_available_from_current_public_data",
                "h0_best_cross_scope_transaction_bridge": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                "h0_best_same_scope_proxy": "loans_to_nondepository_financial_institutions_qoq",
            },
            "recommendation": {
                "status": "no_promotable_same_scope_transaction_subcomponent_supported",
            },
        },
        strict_additional_creator_candidate_summary={
            "status": "available",
            "classification": {
                "h0_best_extension_candidate": "cre_multifamily_loans_qoq",
            },
            "recommendation": {
                "status": "no_additional_extension_candidate_supported",
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["classification"]["branch_state"] == "strict_empirical_expansion_effectively_complete"
    assert payload["classification"]["closeout_readiness"] == "writeup_ready_under_current_evidence"
    assert payload["recommendation"]["status"] == "move_to_writeup_and_results_packaging"
    assert payload["release_position"]["headline_direct_benchmark"] == "strict_loan_core_min_qoq"
    assert payload["release_position"]["standard_bridge_comparison"] == "strict_loan_core_plus_nonfinancial_corporate_qoq"
    assert any("Full TDC remains the broad Treasury-attributed object" in row for row in payload["settled_findings"])
    assert any("best cross-scope bridge = `strict_di_loans_nec_nonfinancial_corporate_qoq`" in row for row in payload["takeaways"])
