from __future__ import annotations

from tdcpass.analysis.strict_di_bucket_role_summary import build_strict_di_bucket_role_summary


def test_strict_di_bucket_role_summary_builds_release_taxonomy_from_upstream_surfaces() -> None:
    payload = build_strict_di_bucket_role_summary(
        strict_loan_core_redesign_summary={
            "status": "available",
            "published_roles": {
                "headline_direct_core": {"series": "strict_loan_core_min_qoq"},
                "standard_secondary_comparison": {"series": "strict_loan_core_plus_private_borrower_qoq"},
                "broad_loan_subtotal_diagnostic": {"series": "strict_loan_source_qoq"},
                "di_bucket_diagnostic": {"series": "strict_loan_di_loans_nec_qoq"},
                "noncore_system_diagnostic": {"series": "strict_loan_noncore_system_qoq"},
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
                        "redesigned_direct_min_core_response": {"beta": -10.7},
                        "private_borrower_augmented_core_response": {"beta": 8.5},
                        "current_broad_loan_source_response": {"beta": 5.6},
                        "noncore_system_diagnostic_response": {"beta": 4.3},
                    }
                }
            },
        },
        strict_identifiable_followup_summary={
            "status": "available",
            "di_loans_nec_borrower_diagnostics": {
                "key_horizons": {
                    "h0": {
                        "strict_loan_di_loans_nec": {"beta": -12.0},
                        "dominant_borrower_component": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                        "us_chartered_share_of_systemwide_liability_beta": 0.42,
                        "systemwide_borrower_gap_share_of_systemwide_liability_beta": 0.08,
                    }
                }
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["status"] == "keep_di_bucket_diagnostic_only"
    assert payload["release_taxonomy"]["headline_direct_core"]["series"] == "strict_loan_core_min_qoq"
    assert payload["release_taxonomy"]["standard_secondary_comparison"]["series"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["release_taxonomy"]["di_bucket_diagnostic"]["series"] == "strict_loan_di_loans_nec_qoq"
    assert payload["key_horizons"]["h0"]["private_borrower_increment_beta"] == 19.2
    assert payload["key_horizons"]["h0"]["dominant_borrower_component"] == "strict_di_loans_nec_nonfinancial_corporate_qoq"
    assert payload["takeaways"][0].startswith("This surface fixes the published role")
