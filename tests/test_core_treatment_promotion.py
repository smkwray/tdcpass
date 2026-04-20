from __future__ import annotations

import pandas as pd

from tdcpass.analysis.core_treatment_promotion import build_core_treatment_promotion_summary


def test_core_treatment_promotion_summary_keeps_split_interpretive_when_strict_sign_fails() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2000Q1", "2000Q2", "2000Q3", "2000Q4"],
            "tdc_bank_only_qoq": [10.0, 8.0, -4.0, 6.0],
            "tdc_core_deposit_proximate_bank_only_qoq": [3.0, 2.0, -1.0, 0.5],
            "tdc_no_toc_no_row_bank_only_qoq": [3.0, 2.0, -1.0, 0.5],
            "tdc_residual_z": [0.9, -0.6, 0.4, -0.3],
            "tdc_core_deposit_proximate_bank_only_residual_z": [0.6, -0.2, -0.1, 0.3],
            "tdc_core_deposit_proximate_bank_only_train_target_sd": [2.0, 2.0, 2.0, 2.0],
            "tdc_core_deposit_proximate_bank_only_fitted_to_target_scale_ratio": [1.1, 1.2, 1.0, 0.9],
            "tdc_core_deposit_proximate_bank_only_train_condition_number": [12.0, 11.0, 14.0, 10.0],
            "tdc_core_deposit_proximate_bank_only_shock_flag": ["", "", "", ""],
            "tdc_residual": [0.3, -0.2, 0.1, -0.1],
            "tdc_train_target_sd": [4.0, 4.0, 4.0, 4.0],
            "fitted_to_target_scale_ratio": [1.0, 1.2, 1.1, 0.8],
            "train_condition_number": [20.0, 18.0, 21.0, 19.0],
            "shock_flag": ["", "", "", ""],
        }
    )
    identity_treatment_sensitivity = pd.DataFrame(
        [
            {
                "treatment_variant": "baseline",
                "treatment_role": "core",
                "treatment_family": "headline",
                "target": "tdc_bank_only_qoq",
                "outcome": "tdc_bank_only_qoq",
                "horizon": 0,
                "beta": 12.0,
                "se": 1.0,
                "lower95": 10.0,
                "upper95": 14.0,
                "n": 30,
                "shock_column": "tdc_residual_z",
                "outcome_construction": "estimated_common_design",
            },
            {
                "treatment_variant": "baseline",
                "treatment_role": "core",
                "treatment_family": "headline",
                "target": "tdc_bank_only_qoq",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -61.0,
                "se": 1.0,
                "lower95": -63.0,
                "upper95": -59.0,
                "n": 30,
                "shock_column": "tdc_residual_z",
                "outcome_construction": "estimated_common_design",
            },
            {
                "treatment_variant": "baseline",
                "treatment_role": "core",
                "treatment_family": "headline",
                "target": "tdc_bank_only_qoq",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -73.0,
                "se": 1.0,
                "lower95": -75.0,
                "upper95": -71.0,
                "n": 30,
                "shock_column": "tdc_residual_z",
                "outcome_construction": "derived_total_minus_tdc",
            },
            {
                "treatment_variant": "core_deposit_proximate",
                "treatment_role": "exploratory",
                "treatment_family": "measurement",
                "target": "tdc_core_deposit_proximate_bank_only_qoq",
                "outcome": "tdc_core_deposit_proximate_bank_only_qoq",
                "horizon": 0,
                "beta": 4.0,
                "se": 1.0,
                "lower95": 2.0,
                "upper95": 6.0,
                "n": 30,
                "shock_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "outcome_construction": "estimated_common_design",
            },
            {
                "treatment_variant": "core_deposit_proximate",
                "treatment_role": "exploratory",
                "treatment_family": "measurement",
                "target": "tdc_core_deposit_proximate_bank_only_qoq",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -1.5,
                "se": 1.0,
                "lower95": -3.5,
                "upper95": 0.5,
                "n": 30,
                "shock_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "outcome_construction": "estimated_common_design",
            },
            {
                "treatment_variant": "core_deposit_proximate",
                "treatment_role": "exploratory",
                "treatment_family": "measurement",
                "target": "tdc_core_deposit_proximate_bank_only_qoq",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -5.5,
                "se": 1.0,
                "lower95": -7.5,
                "upper95": -3.5,
                "n": 30,
                "shock_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "outcome_construction": "derived_total_minus_tdc",
            },
        ]
    )
    shock_specs = {
        "unexpected_tdc_default": {
            "target": "tdc_bank_only_qoq",
            "standardized_column": "tdc_residual_z",
            "predictors": ["lag_tdc_bank_only_qoq"],
            "model_name": "unexpected_tdc_default",
            "target_sd_column": "tdc_train_target_sd",
            "scale_ratio_column": "fitted_to_target_scale_ratio",
            "condition_number_column": "train_condition_number",
            "flag_column": "shock_flag",
        },
        "unexpected_tdc_core_deposit_proximate_bank_only": {
            "target": "tdc_core_deposit_proximate_bank_only_qoq",
            "standardized_column": "tdc_core_deposit_proximate_bank_only_residual_z",
            "predictors": ["lag_tdc_core_deposit_proximate_bank_only_qoq"],
            "model_name": "unexpected_tdc_core_deposit_proximate_bank_only",
            "target_sd_column": "tdc_core_deposit_proximate_bank_only_train_target_sd",
            "scale_ratio_column": "tdc_core_deposit_proximate_bank_only_fitted_to_target_scale_ratio",
            "condition_number_column": "tdc_core_deposit_proximate_bank_only_train_condition_number",
            "flag_column": "tdc_core_deposit_proximate_bank_only_shock_flag",
        },
        "unexpected_tdc_no_toc_no_row_bank_only": {
            "target": "tdc_no_toc_no_row_bank_only_qoq",
        },
    }
    split_summary = {
        "status": "available",
        "key_horizons": {
            "h0": {
                "core_deposit_proximate_residual_response": {"beta": -5.5},
                "core_deposit_proximate_target_response": {"beta": 4.0},
            }
        },
    }
    strict_missing = {
        "status": "available",
        "key_horizons": {
            "h0": {
                "toc_row_excluded": {
                    "comparison_target": "tdc_no_toc_no_row_bank_only_qoq",
                    "comparison_residual_outcome": "other_component_no_toc_no_row_bank_only_qoq",
                    "strict_identifiable_total_response": {"beta": 10.8},
                    "strict_gap_after_funding_response": {"beta": -9.9},
                }
            }
        },
    }

    payload = build_core_treatment_promotion_summary(
        shocked=shocked,
        identity_treatment_sensitivity=identity_treatment_sensitivity,
        shock_specs=shock_specs,
        split_treatment_architecture_summary=split_summary,
        strict_missing_channel_summary=strict_missing,
    )

    assert payload["status"] == "available"
    assert payload["series_alias_check"]["status"] == "available"
    assert payload["series_alias_check"]["max_abs_gap_beta"] == 0.0
    assert payload["shock_quality"]["core_deposit_proximate"]["target"] == "tdc_core_deposit_proximate_bank_only_qoq"
    assert payload["shock_quality"]["baseline_vs_core_overlap"]["status"] == "available"
    assert payload["key_horizons"]["h0"]["core_residual_response"]["beta"] == -5.5
    assert payload["strict_validation_check"]["h0_strict_identifiable_total_beta"] == 10.8
    assert payload["strict_validation_check"]["h0_sign_match"] is False
    assert payload["promotion_recommendation"]["status"] == "keep_interpretive_only"
    assert "semantic alias" in payload["takeaways"][-1] or "interpretive" in payload["takeaways"][-1]
