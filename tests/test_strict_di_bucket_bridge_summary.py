from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_di_bucket_bridge_summary import build_strict_di_bucket_bridge_summary


def test_strict_di_bucket_bridge_summary_surfaces_bridge_residual_and_next_branch() -> None:
    quarter_count = 20
    shock = [1.0 if idx % 2 == 0 else -1.0 for idx in range(quarter_count)]
    core_shock = [0.9 if idx % 2 == 0 else -0.9 for idx in range(quarter_count)]
    di_asset = [4.0 if idx % 2 == 0 else -4.0 for idx in range(quarter_count)]
    private_bridge = [0.8 if idx % 2 == 0 else -0.8 for idx in range(quarter_count)]
    noncore_bridge = [0.2 if idx % 2 == 0 else -0.2 for idx in range(quarter_count)]
    borrower_total = [1.0 if idx % 2 == 0 else -1.0 for idx in range(quarter_count)]
    liability_total = [2.0 if idx % 2 == 0 else -2.0 for idx in range(quarter_count)]
    borrower_gap = [0.0] * quarter_count
    other_advances = [0.1 if idx % 2 == 0 else -0.1 for idx in range(quarter_count)]
    other_component = [(-5.0 + 0.1 * idx) if idx % 2 == 0 else (5.0 - 0.1 * idx) for idx in range(quarter_count)]
    core_other_component = [value * 0.5 for value in other_component]

    shocked = pd.DataFrame(
        {
            "quarter": pd.period_range("2000Q1", periods=quarter_count, freq="Q").astype(str),
            "tdc_residual_z": shock,
            "tdc_core_deposit_proximate_bank_only_residual_z": core_shock,
            "other_component_qoq": other_component,
            "other_component_core_deposit_proximate_bank_only_qoq": core_other_component,
            "strict_loan_di_loans_nec_qoq": di_asset,
            "strict_di_loans_nec_private_domestic_borrower_qoq": private_bridge,
            "strict_di_loans_nec_noncore_system_borrower_qoq": noncore_bridge,
            "strict_di_loans_nec_systemwide_liability_total_qoq": liability_total,
            "strict_di_loans_nec_systemwide_borrower_total_qoq": borrower_total,
            "strict_di_loans_nec_systemwide_borrower_gap_qoq": borrower_gap,
            "strict_loan_other_advances_qoq": other_advances,
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
            "lag_strict_loan_di_loans_nec_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_private_domestic_borrower_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_noncore_system_borrower_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_systemwide_liability_total_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_systemwide_borrower_total_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_systemwide_borrower_gap_qoq": [0.0] * quarter_count,
            "lag_strict_loan_other_advances_qoq": [0.0] * quarter_count,
        }
    )

    payload = build_strict_di_bucket_bridge_summary(
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
        strict_di_bucket_role_summary={"status": "available"},
        horizons=(0,),
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["status"] == "bridge_surface_first"
    assert payload["recommendation"]["next_branch"] == "build_counterpart_alignment_surface"
    assert payload["bridge_definitions"]["di_asset"] == "strict_loan_di_loans_nec_qoq"
    h0_core = payload["key_horizons"]["h0"]["core_deposit_proximate"]
    assert h0_core["di_asset_response"] is not None
    assert h0_core["bridge_residual_beta"] is not None
    assert h0_core["interpretation"] == "cross_scope_bridge_residual_large"
    assert h0_core["us_chartered_di_asset_share_of_systemwide_liability_beta"] == 2.0
    assert payload["takeaways"][0].startswith("This surface converts the broad DI-loans-n.e.c.")
