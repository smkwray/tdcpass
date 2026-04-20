from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_private_borrower_bridge_summary import build_strict_private_borrower_bridge_summary


def test_strict_private_borrower_bridge_summary_identifies_dominant_private_component() -> None:
    quarter_count = 20
    shock = [1.0 if idx % 2 == 0 else -1.0 for idx in range(quarter_count)]
    core_shock = [0.9 if idx % 2 == 0 else -0.9 for idx in range(quarter_count)]
    households = [0.5 if idx % 2 == 0 else -0.5 for idx in range(quarter_count)]
    corporate = [2.5 if idx % 2 == 0 else -2.5 for idx in range(quarter_count)]
    noncorporate = [-0.3 if idx % 2 == 0 else 0.3 for idx in range(quarter_count)]
    other_component = [(-4.0 + 0.1 * idx) if idx % 2 == 0 else (4.0 - 0.1 * idx) for idx in range(quarter_count)]
    core_other_component = [value * 0.5 for value in other_component]

    shocked = pd.DataFrame(
        {
            "quarter": pd.period_range("2000Q1", periods=quarter_count, freq="Q").astype(str),
            "tdc_residual_z": shock,
            "tdc_core_deposit_proximate_bank_only_residual_z": core_shock,
            "other_component_qoq": other_component,
            "other_component_core_deposit_proximate_bank_only_qoq": core_other_component,
            "strict_di_loans_nec_households_nonprofits_qoq": households,
            "strict_di_loans_nec_nonfinancial_corporate_qoq": corporate,
            "strict_di_loans_nec_nonfinancial_noncorporate_qoq": noncorporate,
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
            "lag_strict_di_loans_nec_households_nonprofits_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_nonfinancial_corporate_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_nonfinancial_noncorporate_qoq": [0.0] * quarter_count,
        }
    )

    payload = build_strict_private_borrower_bridge_summary(
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
        strict_di_bucket_bridge_summary={"status": "available"},
        horizons=(0,),
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["status"] == "private_bridge_split_first"
    assert payload["recommendation"]["next_branch"] == "build_nonfinancial_corporate_bridge_surface"
    h0_core = payload["key_horizons"]["h0"]["core_deposit_proximate"]
    assert h0_core["private_bridge_response"] is not None
    assert h0_core["dominant_private_component"] == "strict_di_loans_nec_nonfinancial_corporate_qoq"
    assert h0_core["nonfinancial_corporate_share_of_private_bridge_beta"] is not None
    assert payload["takeaways"][0].startswith("This surface narrows the DI-bucket bridge")
