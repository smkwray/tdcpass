from __future__ import annotations

import pandas as pd

from tdcpass.analysis.period_sensitivity import build_period_sensitivity_summary


def test_build_period_sensitivity_summary_materializes_key_horizons() -> None:
    frame = pd.DataFrame(
        [
            {
                "period_variant": "all_usable",
                "period_role": "headline",
                "start_quarter": "2009Q1",
                "end_quarter": "2025Q4",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 40.0,
                "se": 10.0,
                "lower95": 20.4,
                "upper95": 59.6,
                "n": 68,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "period_variant": "all_usable",
                "period_role": "headline",
                "start_quarter": "2009Q1",
                "end_quarter": "2025Q4",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -50.0,
                "se": 12.0,
                "lower95": -73.52,
                "upper95": -26.48,
                "n": 68,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "period_variant": "all_usable",
                "period_role": "headline",
                "start_quarter": "2009Q1",
                "end_quarter": "2025Q4",
                "outcome": "tdc_bank_only_qoq",
                "horizon": 0,
                "beta": 90.0,
                "se": 10.0,
                "lower95": 70.4,
                "upper95": 109.6,
                "n": 68,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "period_variant": "covid_post",
                "period_role": "core",
                "start_quarter": "2020Q1",
                "end_quarter": "2025Q4",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 100.0,
                "se": 15.0,
                "lower95": 70.6,
                "upper95": 129.4,
                "n": 24,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "period_variant": "covid_post",
                "period_role": "core",
                "start_quarter": "2020Q1",
                "end_quarter": "2025Q4",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -80.0,
                "se": 20.0,
                "lower95": -119.2,
                "upper95": -40.8,
                "n": 24,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "period_variant": "covid_post",
                "period_role": "core",
                "start_quarter": "2020Q1",
                "end_quarter": "2025Q4",
                "outcome": "tdc_bank_only_qoq",
                "horizon": 0,
                "beta": 180.0,
                "se": 30.0,
                "lower95": 121.2,
                "upper95": 238.8,
                "n": 24,
                "spec_name": "period_sensitivity",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
        ]
    )

    payload = build_period_sensitivity_summary(frame)

    assert payload["status"] == "materialized"
    assert {item["period_variant"] for item in payload["periods"]} == {"all_usable", "covid_post"}
    assert payload["key_horizons"]["all_usable"]["h0"]["assessment"] == "crowd_out_signal"
    assert payload["key_horizons"]["covid_post"]["h0"]["assessment"] == "crowd_out_signal"
    assert any("COVID/post-COVID" in item for item in payload["takeaways"])


def test_build_period_sensitivity_summary_handles_empty_frame() -> None:
    payload = build_period_sensitivity_summary(pd.DataFrame())

    assert payload["status"] == "unavailable"
    assert payload["periods"] == []
