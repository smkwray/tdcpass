from __future__ import annotations

from tdcpass.analysis.strict_direct_core_horizon_stability_summary import (
    build_strict_direct_core_horizon_stability_summary,
)


def test_strict_direct_core_horizon_stability_summary_flags_horizon_specific_winner() -> None:
    payload = build_strict_direct_core_horizon_stability_summary(
        strict_direct_core_component_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "core_deposit_proximate": {
                        "candidate_abs_gap_to_residual_beta": {
                            "strict_loan_mortgages_qoq": 0.46,
                            "strict_loan_consumer_credit_qoq": 1.16,
                            "strict_loan_core_min_qoq": 5.16,
                        }
                    }
                },
                "h4": {
                    "core_deposit_proximate": {
                        "candidate_abs_gap_to_residual_beta": {
                            "strict_loan_mortgages_qoq": 20.34,
                            "strict_loan_consumer_credit_qoq": 28.68,
                            "strict_loan_core_min_qoq": 4.29,
                        }
                    }
                },
                "h8": {
                    "core_deposit_proximate": {
                        "candidate_abs_gap_to_residual_beta": {
                            "strict_loan_mortgages_qoq": 44.87,
                            "strict_loan_consumer_credit_qoq": 57.60,
                            "strict_loan_core_min_qoq": 33.41,
                        }
                    }
                },
            },
        }
    )

    assert payload["status"] == "available"
    assert payload["horizon_winners"]["h0"] == "strict_loan_mortgages_qoq"
    assert payload["horizon_winners"]["h4"] == "strict_loan_core_min_qoq"
    assert payload["horizon_winners"]["h8"] == "strict_loan_core_min_qoq"
    assert (
        payload["recommendation"]["status"]
        == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
    )
    assert payload["recommendation"]["impact_candidate"] == "strict_loan_mortgages_qoq"
    assert payload["recommendation"]["multihorizon_candidate"] == "strict_loan_core_min_qoq"
