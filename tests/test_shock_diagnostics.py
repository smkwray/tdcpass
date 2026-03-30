from __future__ import annotations

import pandas as pd

from tdcpass.analysis.shock_diagnostics import build_shock_diagnostics_summary


def test_shock_diagnostics_summary_flags_weak_alignment_and_sign_disagreement() -> None:
    shocks = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_residual_z": [1.0, -1.0, 1.0, -1.0],
            "tdc_broad_depository_residual_z": [1.0, 1.0, -1.0, -1.0],
            "tdc_bank_only_qoq": [0.2, -0.2, 0.2, -0.2],
            "tdc_broad_depository_qoq": [10.0, 10.0, -10.0, -10.0],
            "total_deposits_bank_qoq": [5.0, -5.0, 5.0, -5.0],
        }
    )
    sensitivity = pd.DataFrame(
        [
            {
                "treatment_variant": "baseline",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 2.0,
                "se": 1.0,
                "lower95": 0.04,
                "upper95": 3.96,
                "n": 4,
                "spec_name": "sensitivity",
                "treatment_role": "core",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "treatment_variant": "broad_depository",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -1.0,
                "se": 1.0,
                "lower95": -2.96,
                "upper95": 0.96,
                "n": 4,
                "spec_name": "sensitivity",
                "treatment_role": "exploratory",
                "shock_column": "tdc_broad_depository_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
        ]
    )

    payload = build_shock_diagnostics_summary(shocks=shocks, sensitivity=sensitivity)

    assert payload["estimand_interpretation"]["shock_scale"] == "per_one_rolling_out_of_sample_standard_deviation"
    assert payload["sample_comparison"]["overlap_observations"] == 4
    assert payload["impact_response_comparison"]["baseline_total_deposits_h0"]["shock_column"] == "tdc_residual_z"
    assert payload["impact_response_comparison"]["broad_depository_total_deposits_h0"]["treatment_role"] == "exploratory"
    assert payload["treatment_variant_comparisons"][0]["treatment_variant"] == "broad_depository"
    assert payload["shock_quality"]["flagged_observations"] == 0
    assert payload["largest_disagreement_quarters"]
    assert any("different treatment objects" in item for item in payload["takeaways"])
    assert any("classified as exploratory" in item for item in payload["takeaways"])
