from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tdcpass.analysis.strict_di_loans_nec_measurement_audit_summary import (
    build_strict_di_loans_nec_measurement_audit_summary,
)


def test_strict_di_loans_nec_measurement_audit_rejects_promotion() -> None:
    baseline_lp = pd.DataFrame(
        [
            {"outcome": "strict_loan_di_loans_nec_qoq", "horizon": 0, "beta": -20.0, "se": 1.0, "lower95": -22.0, "upper95": -18.0, "n": 68},
            {"outcome": "strict_di_loans_nec_private_domestic_borrower_qoq", "horizon": 0, "beta": -18.0, "se": 1.0, "lower95": -20.0, "upper95": -16.0, "n": 68},
            {"outcome": "loans_to_nondepository_financial_institutions_qoq", "horizon": 0, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 68},
        ]
    )
    core_lp = pd.DataFrame(
        [
            {"outcome": "strict_loan_di_loans_nec_qoq", "horizon": 0, "beta": 17.6, "se": 1.0, "lower95": 15.6, "upper95": 19.6, "n": 68},
            {"outcome": "strict_di_loans_nec_private_domestic_borrower_qoq", "horizon": 0, "beta": 20.8, "se": 1.0, "lower95": 18.8, "upper95": 22.8, "n": 68},
            {"outcome": "strict_di_loans_nec_nonfinancial_corporate_qoq", "horizon": 0, "beta": 20.8, "se": 1.0, "lower95": 18.8, "upper95": 22.8, "n": 68},
            {"outcome": "strict_di_loans_nec_noncore_system_borrower_qoq", "horizon": 0, "beta": 4.1, "se": 1.0, "lower95": 2.1, "upper95": 6.1, "n": 68},
            {"outcome": "strict_di_loans_nec_systemwide_liability_total_qoq", "horizon": 0, "beta": 23.3, "se": 1.0, "lower95": 21.3, "upper95": 25.3, "n": 68},
            {"outcome": "loans_to_commercial_banks_qoq", "horizon": 0, "beta": 1.2, "se": 1.0, "lower95": -0.8, "upper95": 3.2, "n": 68},
            {"outcome": "loans_to_nondepository_financial_institutions_qoq", "horizon": 0, "beta": 5.7, "se": 1.0, "lower95": 3.7, "upper95": 7.7, "n": 68},
            {"outcome": "loans_for_purchasing_or_carrying_securities_qoq", "horizon": 0, "beta": 0.8, "se": 1.0, "lower95": -1.2, "upper95": 2.8, "n": 68},
        ]
    )

    with patch(
        "tdcpass.analysis.strict_di_loans_nec_measurement_audit_summary.run_local_projections",
        side_effect=[baseline_lp, core_lp],
    ):
        payload = build_strict_di_loans_nec_measurement_audit_summary(
            shocked=pd.DataFrame(
                {
                    "quarter": ["2020Q1"],
                    "strict_loan_di_loans_nec_qoq": [0.0],
                    "strict_di_loans_nec_private_domestic_borrower_qoq": [0.0],
                    "strict_di_loans_nec_nonfinancial_corporate_qoq": [0.0],
                    "strict_di_loans_nec_noncore_system_borrower_qoq": [0.0],
                    "strict_di_loans_nec_systemwide_liability_total_qoq": [0.0],
                    "loans_to_commercial_banks_qoq": [0.0],
                    "loans_to_nondepository_financial_institutions_qoq": [0.0],
                    "loans_for_purchasing_or_carrying_securities_qoq": [0.0],
                }
            ),
            baseline_lp_spec={"controls": [], "horizons": [0], "cumulative": True, "nw_lags": 1},
            baseline_shock_spec={"standardized_column": "tdc_residual_z", "predictors": []},
            core_shock_spec={
                "standardized_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "predictors": [],
            },
            strict_release_framing_summary={"status": "available"},
            strict_di_bucket_bridge_summary={"status": "available"},
            horizons=(0,),
        )

    assert payload["status"] == "available"
    assert payload["classification"]["same_scope_transaction_subcomponent_status"] == "not_available_from_current_public_data"
    assert payload["classification"]["h0_best_cross_scope_transaction_bridge"] == "strict_di_loans_nec_private_domestic_borrower_qoq"
    assert payload["classification"]["h0_best_same_scope_proxy"] == "loans_to_nondepository_financial_institutions_qoq"
    assert payload["recommendation"]["status"] == "no_promotable_same_scope_transaction_subcomponent_supported"
