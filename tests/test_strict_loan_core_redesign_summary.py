from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_loan_core_redesign_summary import build_strict_loan_core_redesign_summary


def test_strict_loan_core_redesign_summary_compares_direct_core_against_broad_loan_source() -> None:
    quarter_count = 20
    shock = [1.0 if idx % 2 == 0 else -1.0 for idx in range(quarter_count)]
    core_shock = [0.9 if idx % 2 == 0 else -0.9 for idx in range(quarter_count)]
    other_component = [(-5.0 + 0.1 * idx) if idx % 2 == 0 else (5.0 - 0.1 * idx) for idx in range(quarter_count)]
    core_other_component = [value * 0.4 for value in other_component]
    strict_loan_source = [(-4.0 + 0.08 * idx) if idx % 2 else (4.0 - 0.08 * idx) for idx in range(quarter_count)]
    strict_loan_core_min = [value * 0.25 for value in strict_loan_source]
    strict_private_augmented = [value * 0.375 for value in strict_loan_source]
    strict_noncore_system = [value * 0.625 for value in strict_loan_source]
    private_borrower = [value * 0.125 for value in strict_loan_source]
    noncore_borrower = [value * 0.5 for value in strict_loan_source]
    shocked = pd.DataFrame(
        {
            "quarter": pd.period_range("2000Q1", periods=quarter_count, freq="Q").astype(str),
            "tdc_residual_z": shock,
            "tdc_core_deposit_proximate_bank_only_residual_z": core_shock,
            "other_component_qoq": other_component,
            "other_component_core_deposit_proximate_bank_only_qoq": core_other_component,
            "strict_loan_source_qoq": strict_loan_source,
            "strict_loan_core_min_qoq": strict_loan_core_min,
            "strict_loan_core_plus_private_borrower_qoq": strict_private_augmented,
            "strict_loan_noncore_system_qoq": strict_noncore_system,
            "strict_di_loans_nec_private_domestic_borrower_qoq": private_borrower,
            "strict_di_loans_nec_noncore_system_borrower_qoq": noncore_borrower,
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
            "lag_strict_loan_source_qoq": [0.0] * quarter_count,
            "lag_strict_loan_core_min_qoq": [0.0] * quarter_count,
            "lag_strict_loan_core_plus_private_borrower_qoq": [0.0] * quarter_count,
            "lag_strict_loan_noncore_system_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_private_domestic_borrower_qoq": [0.0] * quarter_count,
            "lag_strict_di_loans_nec_noncore_system_borrower_qoq": [0.0] * quarter_count,
        }
    )

    payload = build_strict_loan_core_redesign_summary(
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
        strict_redesign_summary={"status": "available"},
        horizons=(0,),
    )

    assert payload["status"] == "available"
    assert payload["recommendation"]["status"] == "promote_direct_core_role_design"
    assert payload["recommendation"]["release_headline_candidate"] == "strict_loan_core_min_qoq"
    assert payload["recommendation"]["standard_secondary_candidate"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["recommendation"]["diagnostic_di_bucket"] == "strict_loan_di_loans_nec_qoq"
    assert payload["recommendation"]["diagnostic_augmented_candidate"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["published_roles"]["headline_direct_core"]["series"] == "strict_loan_core_min_qoq"
    assert payload["published_roles"]["standard_secondary_comparison"]["series"] == "strict_loan_core_plus_private_borrower_qoq"
    assert payload["published_roles"]["di_bucket_diagnostic"]["series"] == "strict_loan_di_loans_nec_qoq"
    assert payload["candidate_definitions"]["noncore_system_diagnostic"] == "strict_loan_noncore_system_qoq"
    h0_core = payload["key_horizons"]["h0"]["core_deposit_proximate"]
    assert h0_core["core_residual_response"] is not None
    assert h0_core["redesigned_direct_min_core_response"] is not None
    assert h0_core["candidate_abs_gap_to_core_residual_beta"]["redesigned_direct_min_core"] is not None
    assert "direct minimum core" in payload["takeaways"][1]
    assert "standard secondary comparison" in payload["takeaways"][-1]
