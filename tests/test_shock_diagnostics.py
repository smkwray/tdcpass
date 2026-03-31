from __future__ import annotations

import pandas as pd

from tdcpass.analysis.shock_diagnostics import build_shock_diagnostics_summary


def test_shock_diagnostics_summary_flags_weak_alignment_and_sign_disagreement() -> None:
    shocks = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4", "2021Q1"],
            "tdc_residual_z": [1.0, -1.0, 1.0, -1.0, 0.5],
            "tdc_residual": [0.2, -0.2, 0.2, -0.2, 0.1],
            "train_target_sd": [0.5, 0.5, 0.5, 0.5, 0.5],
            "fitted_to_target_scale_ratio": [1.0, 1.1, 1.2, 1.3, 150.0],
            "fitted_to_train_target_sd_ratio": [0.8, 0.9, 1.0, 1.1, 4.0],
            "shock_flag": ["", "", "", "", "scale_ratio"],
            "tdc_broad_depository_residual_z": [1.0, 1.0, -1.0, -1.0, None],
            "tdc_bank_only_qoq": [0.2, -0.2, 0.2, -0.2, 0.01],
            "tdc_broad_depository_qoq": [10.0, 10.0, -10.0, -10.0, None],
            "total_deposits_bank_qoq": [5.0, -5.0, 5.0, -5.0, 1.0],
            "lag_fedfunds": [1.0, 1.0, 1.0, 1.0, 1.0],
            "lag_unemployment": [5.0, 5.0, 5.0, 5.0, 5.0],
            "lag_inflation": [2.0, 2.0, 2.0, 2.0, 2.0],
            "tdc_bank_only_macro_rolling40_residual_z": [0.8, -0.8, 0.9, -0.9, 0.4],
            "tdc_bank_only_macro_rolling40_residual": [0.15, -0.15, 0.18, -0.18, 0.08],
            "tdc_bank_only_macro_rolling40_train_target_sd": [0.4, 0.4, 0.4, 0.4, 0.4],
            "tdc_bank_only_macro_rolling40_fitted_to_target_scale_ratio": [0.9, 1.0, 1.1, 1.2, 2.0],
            "tdc_bank_only_macro_rolling40_fitted_to_train_target_sd_ratio": [0.4, 0.5, 0.6, 0.7, 0.8],
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

    payload = build_shock_diagnostics_summary(
        shocks=shocks,
        sensitivity=sensitivity,
        baseline_shock_spec={
            "freeze_status": "under_review",
            "residual_column": "tdc_residual",
            "quality_gate": {
                "min_usable_observations": 5,
                "min_shock_target_correlation": 0.25,
                "max_flagged_share": 0.1,
                "max_realized_scale_ratio_p95": 25.0,
            },
        },
        shock_specs={
            "unexpected_tdc_bank_only_macro_rolling40": {
                "candidate_role": "repair_candidate",
                "model_name": "unexpected_tdc_bank_only_macro_rolling40",
                "standardized_column": "tdc_bank_only_macro_rolling40_residual_z",
                "residual_column": "tdc_bank_only_macro_rolling40_residual",
                "target": "tdc_bank_only_qoq",
                "method": "rolling_window_ols",
                "min_train_obs": 24,
                "max_train_obs": 40,
                "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
            }
        },
        lp_controls=["lag_fedfunds", "lag_unemployment", "lag_inflation"],
    )

    assert payload["treatment_freeze_status"] == "under_review"
    assert payload["baseline_usable_sample"]["rows"] == 5
    assert payload["treatment_candidates"][0]["usable_sample"]["rows"] == 5
    assert "h0" in payload["treatment_candidates"][0]["raw_unit_tdc_lp"]
    assert "h0" in payload["raw_unit_tdc_lp_response"]
    assert payload["estimand_interpretation"]["shock_scale"] == "per_one_rolling_out_of_sample_standard_deviation"
    assert payload["sample_comparison"]["overlap_observations"] == 4
    assert payload["impact_response_comparison"]["baseline_total_deposits_h0"]["shock_column"] == "tdc_residual_z"
    assert payload["impact_response_comparison"]["broad_depository_total_deposits_h0"]["treatment_role"] == "exploratory"
    assert payload["treatment_variant_comparisons"][0]["treatment_variant"] == "broad_depository"
    assert payload["shock_quality"]["flagged_observations"] == 1
    assert payload["treatment_quality_status"] == "fail"
    assert payload["treatment_quality_gate"]["failed_checks"] == ["max_flagged_share", "max_realized_scale_ratio_p95"]
    assert "p50" in payload["shock_quality"]["realized_scale_ratio_quantiles"]
    assert payload["severe_realized_scale_tail_audit"]["threshold"] == 25.0
    assert payload["severe_realized_scale_tail_audit"]["tail_rows"] == 1
    assert payload["severe_realized_scale_tail_audit"]["quarters"][0]["quarter"] == "2021Q1"
    assert payload["largest_disagreement_quarters"]
    assert any("still under review" in item for item in payload["takeaways"])
    assert any("different treatment objects" in item for item in payload["takeaways"])
    assert any("classified as exploratory" in item for item in payload["takeaways"])
    assert any("publishable quality gate" in item for item in payload["takeaways"])
    assert any("tail quarters" in item for item in payload["takeaways"])
    assert any("scale-ratio flagged windows" in item for item in payload["takeaways"])
