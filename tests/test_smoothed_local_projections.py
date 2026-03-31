from __future__ import annotations

import numpy as np
import pandas as pd

from tdcpass.analysis.smoothed_local_projections import (
    build_smoothed_lp_diagnostics_summary,
    build_smoothed_lp_irf,
)


def test_smoothed_lp_irf_reduces_path_roughness_on_noisy_series() -> None:
    horizons = list(range(9))
    raw_beta = [0.0, 2.5, -0.5, 5.0, 3.0, 7.0, 5.5, 8.0, 7.5]
    lp_irf = pd.DataFrame(
        {
            "outcome": ["total_deposits_bank_qoq"] * len(horizons),
            "horizon": horizons,
            "beta": raw_beta,
            "se": [1.0] * len(horizons),
            "lower95": [beta - 1.96 for beta in raw_beta],
            "upper95": [beta + 1.96 for beta in raw_beta],
            "n": [100] * len(horizons),
            "spec_name": ["baseline"] * len(horizons),
            "shock_column": ["tdc_residual_z"] * len(horizons),
            "shock_scale": ["rolling_oos_standard_deviation"] * len(horizons),
            "response_type": ["cumulative_sum_h0_to_h"] * len(horizons),
        }
    )

    smoothed = build_smoothed_lp_irf(lp_irf, degree=3, ridge_alpha=1.0)

    assert not smoothed.empty
    assert smoothed["horizon"].tolist() == horizons
    assert {"raw_beta", "adjustment", "smooth_method", "bandwidth"}.issubset(smoothed.columns)
    assert set(smoothed["smooth_method"]) == {"gaussian_kernel"}
    raw_roughness = float(np.abs(np.diff(lp_irf["beta"].to_numpy(dtype=float))).sum())
    smooth_roughness = float(np.abs(np.diff(smoothed["beta"].to_numpy(dtype=float))).sum())
    assert smooth_roughness < raw_roughness


def test_smoothed_lp_diagnostics_reports_stable_key_horizon_signs() -> None:
    smoothed_lp = pd.DataFrame(
        [
            {
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": -2.4,
                "se": 1.0,
                "lower95": -4.36,
                "upper95": -0.44,
                "raw_beta": -2.5,
                "raw_se": 1.0,
                "raw_lower95": -4.46,
                "raw_upper95": -0.54,
                "adjustment": 0.1,
                "n": 261,
                "smooth_method": "gaussian_kernel",
                "bandwidth": 1.0,
                "degree": 3,
                "ridge_alpha": 1.0,
                "spec_name": "smoothed_lp",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "outcome": "total_deposits_bank_qoq",
                "horizon": 4,
                "beta": 11.7,
                "se": 2.0,
                "lower95": 7.78,
                "upper95": 15.62,
                "raw_beta": 13.5,
                "raw_se": 2.0,
                "raw_lower95": 9.58,
                "raw_upper95": 17.42,
                "adjustment": -1.8,
                "n": 257,
                "smooth_method": "gaussian_kernel",
                "bandwidth": 1.0,
                "degree": 3,
                "ridge_alpha": 1.0,
                "spec_name": "smoothed_lp",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -7.0,
                "se": 1.0,
                "lower95": -8.96,
                "upper95": -5.04,
                "raw_beta": -7.1,
                "raw_se": 1.0,
                "raw_lower95": -9.06,
                "raw_upper95": -5.14,
                "adjustment": 0.1,
                "n": 261,
                "smooth_method": "gaussian_kernel",
                "bandwidth": 1.0,
                "degree": 3,
                "ridge_alpha": 1.0,
                "spec_name": "smoothed_lp",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
            {
                "outcome": "other_component_qoq",
                "horizon": 4,
                "beta": 8.3,
                "se": 2.0,
                "lower95": 4.38,
                "upper95": 12.22,
                "raw_beta": 10.0,
                "raw_se": 2.0,
                "raw_lower95": 6.08,
                "raw_upper95": 13.92,
                "adjustment": -1.7,
                "n": 257,
                "smooth_method": "gaussian_kernel",
                "bandwidth": 1.0,
                "degree": 3,
                "ridge_alpha": 1.0,
                "spec_name": "smoothed_lp",
                "shock_column": "tdc_residual_z",
                "shock_scale": "rolling_oos_standard_deviation",
                "response_type": "cumulative_sum_h0_to_h",
            },
        ]
    )

    payload = build_smoothed_lp_diagnostics_summary(smoothed_lp)

    assert payload["status"] == "stable"
    assert payload["key_horizons"]["h0"]["total_deposits_bank_qoq"]["raw_sign"] == "negative"
    assert payload["key_horizons"]["h4"]["total_deposits_bank_qoq"]["smoothed_sign"] == "positive"
