from __future__ import annotations

import pandas as pd

from tdcpass.analysis.factor_control_diagnostics import build_factor_control_diagnostics_summary


def test_factor_control_diagnostics_flags_severe_history_loss() -> None:
    control_sensitivity = pd.DataFrame(
        [
            {
                "control_variant": "headline_lagged_macro",
                "control_role": "headline",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -2.5,
                "lower95": -12.5,
                "upper95": 7.5,
                "n": 261,
            },
            {
                "control_variant": "headline_lagged_macro",
                "control_role": "headline",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -7.0,
                "lower95": -16.6,
                "upper95": 2.4,
                "n": 261,
            },
            {
                "control_variant": "headline_lagged_macro",
                "control_role": "headline",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 4,
                "beta": 13.5,
                "lower95": -0.6,
                "upper95": 27.7,
                "n": 257,
            },
            {
                "control_variant": "headline_lagged_macro",
                "control_role": "headline",
                "outcome": "other_component_qoq",
                "horizon": 4,
                "beta": 10.0,
                "lower95": -3.4,
                "upper95": 23.5,
                "n": 257,
            },
        ]
    )
    factor_control_sensitivity = pd.DataFrame(
        [
            {
                "factor_variant": "recursive_macro_plumbing_factors3",
                "factor_role": "core",
                "factor_columns": "f1|f2|f3",
                "source_columns": "lag_tga_qoq|lag_reserves_qoq",
                "factor_count": 3,
                "min_train_obs": 40,
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -12.4,
                "lower95": -34.1,
                "upper95": 9.2,
                "n": 51,
            },
            {
                "factor_variant": "recursive_macro_plumbing_factors3",
                "factor_role": "core",
                "factor_columns": "f1|f2|f3",
                "source_columns": "lag_tga_qoq|lag_reserves_qoq",
                "factor_count": 3,
                "min_train_obs": 40,
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -12.9,
                "lower95": -34.5,
                "upper95": 8.8,
                "n": 51,
            },
            {
                "factor_variant": "recursive_macro_plumbing_factors3",
                "factor_role": "core",
                "factor_columns": "f1|f2|f3",
                "source_columns": "lag_tga_qoq|lag_reserves_qoq",
                "factor_count": 3,
                "min_train_obs": 40,
                "outcome": "total_deposits_bank_qoq",
                "horizon": 4,
                "beta": 4.3,
                "lower95": -46.9,
                "upper95": 55.5,
                "n": 47,
            },
            {
                "factor_variant": "recursive_macro_plumbing_factors3",
                "factor_role": "core",
                "factor_columns": "f1|f2|f3",
                "source_columns": "lag_tga_qoq|lag_reserves_qoq",
                "factor_count": 3,
                "min_train_obs": 40,
                "outcome": "other_component_qoq",
                "horizon": 4,
                "beta": 4.1,
                "lower95": -47.1,
                "upper95": 55.3,
                "n": 47,
            },
        ]
    )

    payload = build_factor_control_diagnostics_summary(
        control_sensitivity=control_sensitivity,
        factor_control_sensitivity=factor_control_sensitivity,
    )

    assert payload["status"] == "short_history"
    variant = payload["factor_variants"][0]
    assert variant["coverage_label"] == "severe_history_loss"
    assert variant["min_key_horizon_n_ratio"] < 0.35
    assert variant["key_horizons"]["h0"]["total_deposits_bank_qoq"]["n_drop_vs_baseline"] == 210


def test_factor_control_diagnostics_handles_empty_factor_frame() -> None:
    payload = build_factor_control_diagnostics_summary(
        control_sensitivity=pd.DataFrame(),
        factor_control_sensitivity=pd.DataFrame(),
    )

    assert payload["status"] == "no_factor_rows"
    assert payload["factor_variants"] == []


def test_factor_control_diagnostics_prefers_core_adequate_over_exploratory_short_history() -> None:
    control_sensitivity = pd.DataFrame(
        [
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -2.5, "lower95": -12.5, "upper95": 7.5, "n": 261},
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "lower95": -16.6, "upper95": 2.4, "n": 261},
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 13.5, "lower95": -0.6, "upper95": 27.7, "n": 257},
            {"control_variant": "headline_lagged_macro", "control_role": "headline", "outcome": "other_component_qoq", "horizon": 4, "beta": 10.0, "lower95": -3.4, "upper95": 23.5, "n": 257},
        ]
    )
    factor_control_sensitivity = pd.DataFrame(
        [
            {"factor_variant": "recursive_macro_factors2", "factor_role": "core", "factor_columns": "f1|f2", "source_columns": "lag_fedfunds|lag_unemployment|lag_inflation", "factor_count": 2, "min_train_obs": 24, "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -2.4, "lower95": -12.4, "upper95": 7.6, "n": 261},
            {"factor_variant": "recursive_macro_factors2", "factor_role": "core", "factor_columns": "f1|f2", "source_columns": "lag_fedfunds|lag_unemployment|lag_inflation", "factor_count": 2, "min_train_obs": 24, "outcome": "other_component_qoq", "horizon": 0, "beta": -7.1, "lower95": -16.7, "upper95": 2.5, "n": 261},
            {"factor_variant": "recursive_macro_factors2", "factor_role": "core", "factor_columns": "f1|f2", "source_columns": "lag_fedfunds|lag_unemployment|lag_inflation", "factor_count": 2, "min_train_obs": 24, "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 11.7, "lower95": -3.5, "upper95": 27.0, "n": 257},
            {"factor_variant": "recursive_macro_factors2", "factor_role": "core", "factor_columns": "f1|f2", "source_columns": "lag_fedfunds|lag_unemployment|lag_inflation", "factor_count": 2, "min_train_obs": 24, "outcome": "other_component_qoq", "horizon": 4, "beta": 8.3, "lower95": -6.2, "upper95": 22.8, "n": 257},
            {"factor_variant": "recursive_macro_plumbing_factors3", "factor_role": "exploratory", "factor_columns": "f1|f2|f3", "source_columns": "lag_tga_qoq|lag_reserves_qoq", "factor_count": 3, "min_train_obs": 40, "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -12.4, "lower95": -34.1, "upper95": 9.2, "n": 51},
            {"factor_variant": "recursive_macro_plumbing_factors3", "factor_role": "exploratory", "factor_columns": "f1|f2|f3", "source_columns": "lag_tga_qoq|lag_reserves_qoq", "factor_count": 3, "min_train_obs": 40, "outcome": "other_component_qoq", "horizon": 0, "beta": -12.9, "lower95": -34.5, "upper95": 8.8, "n": 51},
            {"factor_variant": "recursive_macro_plumbing_factors3", "factor_role": "exploratory", "factor_columns": "f1|f2|f3", "source_columns": "lag_tga_qoq|lag_reserves_qoq", "factor_count": 3, "min_train_obs": 40, "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 4.3, "lower95": -46.9, "upper95": 55.5, "n": 47},
            {"factor_variant": "recursive_macro_plumbing_factors3", "factor_role": "exploratory", "factor_columns": "f1|f2|f3", "source_columns": "lag_tga_qoq|lag_reserves_qoq", "factor_count": 3, "min_train_obs": 40, "outcome": "other_component_qoq", "horizon": 4, "beta": 4.1, "lower95": -47.1, "upper95": 55.3, "n": 47},
        ]
    )

    payload = build_factor_control_diagnostics_summary(
        control_sensitivity=control_sensitivity,
        factor_control_sensitivity=factor_control_sensitivity,
    )

    assert payload["status"] == "core_adequate"
    assert any("core factor-control specification preserves" in item for item in payload["takeaways"])
    assert any("exploratory factor-control variants" in item for item in payload["takeaways"])
