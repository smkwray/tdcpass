from __future__ import annotations

import pandas as pd

from tdcpass.analysis.rest_of_world_treasury_audit import (
    build_rest_of_world_treasury_audit_summary,
)


def test_rest_of_world_treasury_audit_summary_surfaces_external_pattern(
    monkeypatch,
) -> None:
    def _fake_run_local_projections(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "outcome": "tdc_row_treasury_transactions_qoq",
                    "horizon": 0,
                    "beta": 11.0,
                    "se": 1.0,
                    "lower95": 9.0,
                    "upper95": 13.0,
                    "n": 20,
                },
                {
                    "outcome": "foreign_nonts_qoq",
                    "horizon": 0,
                    "beta": 22.0,
                    "se": 2.0,
                    "lower95": 18.0,
                    "upper95": 26.0,
                    "n": 20,
                },
                {
                    "outcome": "checkable_rest_of_world_bank_qoq",
                    "horizon": 0,
                    "beta": 0.4,
                    "se": 0.5,
                    "lower95": -0.6,
                    "upper95": 1.4,
                    "n": 20,
                },
                {
                    "outcome": "interbank_transactions_foreign_banks_liability_qoq",
                    "horizon": 0,
                    "beta": 1.0,
                    "se": 0.5,
                    "lower95": 0.0,
                    "upper95": 2.0,
                    "n": 20,
                },
                {
                    "outcome": "interbank_transactions_foreign_banks_asset_qoq",
                    "horizon": 0,
                    "beta": 5.0,
                    "se": 0.8,
                    "lower95": 3.4,
                    "upper95": 6.6,
                    "n": 20,
                },
                {
                    "outcome": "deposits_at_foreign_banks_asset_qoq",
                    "horizon": 0,
                    "beta": 0.1,
                    "se": 0.2,
                    "lower95": -0.3,
                    "upper95": 0.5,
                    "n": 20,
                },
            ]
        )

    monkeypatch.setattr(
        "tdcpass.analysis.rest_of_world_treasury_audit.run_local_projections",
        _fake_run_local_projections,
    )

    shocked = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_row_treasury_transactions_qoq": [1.0, -1.0, 2.0, -2.0],
            "foreign_nonts_qoq": [0.2, 0.3, -0.1, -0.2],
            "checkable_rest_of_world_bank_qoq": [0.1, -0.1, 0.1, -0.1],
            "interbank_transactions_foreign_banks_liability_qoq": [0.2, -0.2, 0.3, -0.3],
            "interbank_transactions_foreign_banks_asset_qoq": [0.3, -0.3, 0.4, -0.4],
            "deposits_at_foreign_banks_asset_qoq": [0.0, 0.1, -0.1, 0.0],
        }
    )

    payload = build_rest_of_world_treasury_audit_summary(
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
    assert "foreign_nonts_qoq" in payload["quarterly_alignment"]["counterparts"]
    assert payload["key_horizons"]["h0"]["interpretation"] == "external_asset_support_pattern"
    assert payload["key_horizons"]["h0"]["rest_of_world_treasury_response"]["beta"] == 11.0
    assert payload["key_horizons"]["h0"]["checkable_rest_of_world_bank_response"]["beta"] == 0.4
    assert "simple same-quarter liability counterpart" in payload["takeaways"][1]
