from __future__ import annotations

import pandas as pd

from tdcpass.analysis.treasury_operating_cash_audit import (
    build_treasury_operating_cash_audit_summary,
)


def test_treasury_operating_cash_audit_summary_surfaces_alignment_and_h0_pattern(
    monkeypatch,
) -> None:
    def _fake_run_local_projections(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "outcome": "tdc_treasury_operating_cash_qoq",
                    "horizon": 0,
                    "beta": -7.0,
                    "se": 0.5,
                    "lower95": -8.0,
                    "upper95": -6.0,
                    "n": 12,
                },
                {
                    "outcome": "tga_qoq",
                    "horizon": 0,
                    "beta": -6.5,
                    "se": 0.5,
                    "lower95": -7.5,
                    "upper95": -5.5,
                    "n": 12,
                },
                {
                    "outcome": "reserves_qoq",
                    "horizon": 0,
                    "beta": 8.5,
                    "se": 0.5,
                    "lower95": 7.5,
                    "upper95": 9.5,
                    "n": 12,
                },
                {
                    "outcome": "cb_nonts_qoq",
                    "horizon": 0,
                    "beta": 2.0,
                    "se": 0.5,
                    "lower95": 1.0,
                    "upper95": 3.0,
                    "n": 12,
                },
            ]
        )

    monkeypatch.setattr(
        "tdcpass.analysis.treasury_operating_cash_audit.run_local_projections",
        _fake_run_local_projections,
    )

    shocked = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_treasury_operating_cash_qoq": [-1.0, -2.0, 1.0, 2.0],
            "tga_qoq": [-1.1, -2.1, 1.1, 2.1],
            "reserves_qoq": [1.2, 2.2, -1.2, -2.2],
            "cb_nonts_qoq": [0.6, 1.0, -0.4, -0.8],
        }
    )

    payload = build_treasury_operating_cash_audit_summary(
        shocked=shocked,
        baseline_lp_spec={
            "shock_column": "tdc_residual_z",
            "controls": [],
            "horizons": [0, 1, 4, 8],
            "nw_lags": 4,
            "cumulative": True,
        },
    )

    assert payload["status"] == "available"
    assert payload["quarterly_alignment"]["contemporaneous_corr_tga_vs_toc"] > 0.99
    assert payload["quarterly_alignment"]["ols_tga_on_toc"]["slope"] > 0.8
    assert payload["key_horizons"]["h0"]["interpretation"] == "treasury_cash_release_pattern"
    assert payload["key_horizons"]["h0"]["treasury_operating_cash_signed_contribution_beta"] == 7.0
    assert "correlation" in payload["takeaways"][1]
