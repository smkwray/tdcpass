from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_corporate_bridge_secondary_comparison_summary import (
    build_strict_corporate_bridge_secondary_comparison_summary,
)


def test_strict_corporate_bridge_secondary_comparison_summary_can_narrow_secondary_role() -> None:
    quarter_count = 24
    shock = [1.0 if idx % 2 == 0 else -1.0 for idx in range(quarter_count)]
    core_shock = [0.8 if idx % 2 == 0 else -0.8 for idx in range(quarter_count)]
    core_other_component = [(-10.0 + 0.1 * idx) if idx % 2 == 0 else (10.0 - 0.1 * idx) for idx in range(quarter_count)]
    other_component = [value * 1.5 for value in core_other_component]
    direct_core = [value * 1.8 for value in core_other_component]
    core_plus_corporate = [value * 1.02 for value in core_other_component]
    core_plus_private = [value * 0.55 for value in core_other_component]
    private_offset = [private - corp for private, corp in zip(core_plus_private, core_plus_corporate)]
    shocked = pd.DataFrame(
        {
            "quarter": pd.period_range("2000Q1", periods=quarter_count, freq="Q").astype(str),
            "tdc_residual_z": shock,
            "tdc_core_deposit_proximate_bank_only_residual_z": core_shock,
            "other_component_qoq": other_component,
            "other_component_core_deposit_proximate_bank_only_qoq": core_other_component,
            "strict_loan_core_min_qoq": direct_core,
            "strict_loan_core_plus_private_borrower_qoq": core_plus_private,
            "strict_loan_core_plus_nonfinancial_corporate_qoq": core_plus_corporate,
            "strict_di_loans_nec_private_offset_residual_qoq": private_offset,
            "lag_tdc_bank_only_qoq": [0.0] * quarter_count,
            "lag_tdc_core_deposit_proximate_bank_only_qoq": [0.0] * quarter_count,
            "lag_fedfunds": [0.0] * quarter_count,
            "lag_unemployment": [0.0] * quarter_count,
            "lag_inflation": [0.0] * quarter_count,
            "fedfunds": [1.0] * quarter_count,
            "unemployment": [5.0] * quarter_count,
            "inflation": [2.0] * quarter_count,
            "lag_other_component_qoq": [0.0] * quarter_count,
            "lag_other_component_core_deposit_proximate_bank_only_qoq": [0.0] * quarter_count,
            "lag_strict_loan_core_min_qoq": [0.0] * quarter_count,
            "lag_strict_loan_core_plus_private_borrower_qoq": [0.0] * quarter_count,
            "lag_strict_loan_core_plus_nonfinancial_corporate_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_private_offset_residual_qoq": [0.0] * quarter_count,
        }
    )

    payload = build_strict_corporate_bridge_secondary_comparison_summary(
        shocked=shocked,
        baseline_lp_spec={
            "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
            "horizons": [0],
            "cumulative": False,
            "include_lagged_outcome": True,
            "nw_lags": 1,
        },
        baseline_shock_spec={
            "standardized_column": "tdc_residual_z",
            "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        },
        core_shock_spec={
            "standardized_column": "tdc_core_deposit_proximate_bank_only_residual_z",
            "predictors": [
                "lag_tdc_core_deposit_proximate_bank_only_qoq",
                "lag_fedfunds",
                "lag_unemployment",
                "lag_inflation",
            ],
        },
        strict_private_offset_residual_summary={"status": "available"},
        horizons=(0,),
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["status"] == "promote_corporate_bridge_for_strict_role"
    assert payload["recommendation"]["standard_secondary_candidate"] == "strict_loan_core_plus_nonfinancial_corporate_qoq"
    assert payload["recommendation"]["secondary_comparison_retained_for_diagnostics"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["recommendation"]["role_decision_basis"] == "strict_design_over_fit_heuristic"
    h0 = payload["key_horizons"]["h0"]["core_deposit_proximate"]
    assert h0["candidate_abs_gap_to_core_residual_beta"]["core_plus_nonfinancial_corporate"] is not None
    assert (
        h0["candidate_abs_gap_to_core_residual_beta"]["core_plus_nonfinancial_corporate"]
        <= h0["candidate_abs_gap_to_core_residual_beta"]["core_plus_private_bridge"]
    )
    assert "strict design rule" in payload["takeaways"][-2]
