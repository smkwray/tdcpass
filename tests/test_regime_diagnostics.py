from __future__ import annotations

import pandas as pd

from tdcpass.analysis.local_projections import run_regime_split_local_projections
from tdcpass.analysis.regime_diagnostics import build_regime_diagnostics_summary


def test_regime_lp_supports_median_thresholds() -> None:
    n = 30
    frame = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in range(n)],
            "tdc_residual_z": [float(i) for i in range(n)],
            "total_deposits_bank_qoq": [float(i + 1) for i in range(n)],
            "bill_share": [0.40 + 0.01 * i for i in range(n)],
            "fedfunds": [5.0 for _ in range(n)],
            "unemployment": [4.0 for _ in range(n)],
            "inflation": [2.0 for _ in range(n)],
        }
    )

    out = run_regime_split_local_projections(
        frame,
        shock_col="tdc_residual_z",
        outcome_cols=["total_deposits_bank_qoq"],
        controls=["fedfunds", "unemployment", "inflation"],
        horizons=[0],
        nw_lags=1,
        cumulative=True,
        regime_definitions={"bill_heavy": {"column": "bill_share", "threshold": "median", "type": "threshold"}},
    )

    assert set(out["regime"]) == {"bill_heavy_high", "bill_heavy_low"}


def test_regime_diagnostics_summary_reports_informative_splits() -> None:
    panel = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in range(40)],
            "bank_absorption_share": [0.1 + 0.01 * i for i in range(40)],
            "bill_share": [0.5 + 0.005 * i for i in range(40)],
            "reserve_drain_pressure": [20.0 - float(i) for i in range(40)],
            "tdc_residual_z": [float(i - 20) / 5.0 for i in range(40)],
            "fedfunds": [5.0] * 40,
            "unemployment": [4.0] * 40,
            "inflation": [2.0] * 40,
        }
    )
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "bank_absorption_high", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20},
            {"regime": "bank_absorption_high", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 1.0, "lower95": 0.0, "upper95": 4.0, "n": 18},
            {"regime": "bank_absorption_low", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 18},
            {"regime": "reserve_drain_high", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.5, "se": 1.0, "lower95": -0.5, "upper95": 3.5, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -0.2, "se": 1.0, "lower95": -2.2, "upper95": 1.8, "n": 20},
        ]
    )
    regime_specs = {
        "regimes": {
            "bank_absorption": {"column": "bank_absorption_share", "threshold": "median", "type": "threshold"},
            "bill_heavy": {"column": "bill_share", "threshold": "median", "type": "threshold"},
            "reserve_drain": {"column": "reserve_drain_pressure", "threshold": "median", "type": "threshold"},
        }
    }

    payload = build_regime_diagnostics_summary(
        panel=panel,
        regime_specs=regime_specs,
        selected_regime_columns={"bank_absorption_share", "bill_share", "reserve_drain_pressure"},
        lp_irf_regimes=lp_irf_regimes,
        shock_column="tdc_residual_z",
        controls=["fedfunds", "unemployment", "inflation"],
    )

    assert payload["informative_regime_count"] == 2
    assert payload["stable_regime_count"] == 2
    assert any(item["regime"] == "bank_absorption" and item["informative"] for item in payload["regimes"])
    assert any(item["regime"] == "reserve_drain" and item["informative"] for item in payload["regimes"])
    assert all("stable_for_interpretation" in item for item in payload["regimes"])
    assert payload["takeaways"]
