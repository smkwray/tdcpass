from __future__ import annotations

import pandas as pd

from tdcpass.analysis.toc_row_bundle_audit import build_toc_row_bundle_audit_summary


def test_toc_row_bundle_audit_summary_surfaces_broad_support_pattern(monkeypatch) -> None:
    def _fake_run_local_projections(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"outcome": "tdc_toc_row_bundle_qoq", "horizon": 0, "beta": 82.0, "se": 3.0, "lower95": 76.0, "upper95": 88.0, "n": 20},
                {"outcome": "tdc_row_treasury_transactions_qoq", "horizon": 0, "beta": 12.0, "se": 2.0, "lower95": 8.0, "upper95": 16.0, "n": 20},
                {"outcome": "tdc_treasury_operating_cash_qoq", "horizon": 0, "beta": -70.0, "se": 4.0, "lower95": -78.0, "upper95": -62.0, "n": 20},
                {
                    "outcome": "toc_row_broad_support_counterpart_qoq",
                    "horizon": 0,
                    "beta": 64.0,
                    "se": 4.0,
                    "lower95": 56.0,
                    "upper95": 72.0,
                    "n": 20,
                },
                {
                    "outcome": "toc_row_liquidity_external_counterpart_qoq",
                    "horizon": 0,
                    "beta": 118.0,
                    "se": 5.0,
                    "lower95": 108.0,
                    "upper95": 128.0,
                    "n": 20,
                },
                {
                    "outcome": "toc_row_direct_deposit_counterpart_qoq",
                    "horizon": 0,
                    "beta": 9.0,
                    "se": 1.0,
                    "lower95": 7.0,
                    "upper95": 11.0,
                    "n": 20,
                },
                {"outcome": "foreign_nonts_qoq", "horizon": 0, "beta": 27.0, "se": 3.0, "lower95": 21.0, "upper95": 33.0, "n": 20},
                {"outcome": "tga_qoq", "horizon": 0, "beta": -37.0, "se": 3.0, "lower95": -43.0, "upper95": -31.0, "n": 20},
                {"outcome": "reserves_qoq", "horizon": 0, "beta": 91.0, "se": 4.0, "lower95": 83.0, "upper95": 99.0, "n": 20},
                {"outcome": "checkable_rest_of_world_bank_qoq", "horizon": 0, "beta": 2.0, "se": 0.5, "lower95": 1.0, "upper95": 3.0, "n": 20},
            ]
        )

    monkeypatch.setattr(
        "tdcpass.analysis.toc_row_bundle_audit.run_local_projections",
        _fake_run_local_projections,
    )

    shocked = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_row_treasury_transactions_qoq": [1.0, -1.0, 2.0, -2.0],
            "tdc_treasury_operating_cash_qoq": [-3.0, 2.0, -4.0, 3.0],
            "foreign_nonts_qoq": [2.0, -1.5, 1.0, -0.5],
            "tga_qoq": [-1.0, 1.0, -2.0, 1.5],
            "reserves_qoq": [4.0, -3.0, 5.0, -4.0],
            "checkable_rest_of_world_bank_qoq": [0.2, -0.1, 0.1, -0.2],
        }
    )

    payload = build_toc_row_bundle_audit_summary(
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
    assert "toc_row_broad_support_counterpart_qoq" in payload["quarterly_alignment"]["counterparts"]
    assert payload["key_horizons"]["h0"]["interpretation"] == "broad_support_bundle_pattern"
    assert payload["key_horizons"]["h0"]["toc_row_bundle_response"]["beta"] == 82.0
    assert payload["key_horizons"]["h0"]["direct_deposit_counterpart_response"]["beta"] == 9.0
    assert "broad support bundle" in payload["takeaways"][0]
