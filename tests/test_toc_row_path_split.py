from __future__ import annotations

import pandas as pd

from tdcpass.analysis.toc_row_path_split import build_toc_row_path_split_summary


def test_toc_row_path_split_summary_surfaces_direct_path_dominance(monkeypatch) -> None:
    def _fake_run_local_projections(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"outcome": "tdc_toc_row_bundle_qoq", "horizon": 0, "beta": 80.0, "se": 3.0, "lower95": 74.0, "upper95": 86.0, "n": 20},
                {
                    "outcome": "toc_row_broad_support_counterpart_qoq",
                    "horizon": 0,
                    "beta": 58.0,
                    "se": 3.0,
                    "lower95": 52.0,
                    "upper95": 64.0,
                    "n": 20,
                },
                {
                    "outcome": "toc_row_direct_deposit_counterpart_qoq",
                    "horizon": 0,
                    "beta": 71.0,
                    "se": 2.0,
                    "lower95": 67.0,
                    "upper95": 75.0,
                    "n": 20,
                },
                {
                    "outcome": "toc_row_liquidity_external_counterpart_qoq",
                    "horizon": 0,
                    "beta": 95.0,
                    "se": 4.0,
                    "lower95": 87.0,
                    "upper95": 103.0,
                    "n": 20,
                },
            ]
        )

    monkeypatch.setattr(
        "tdcpass.analysis.toc_row_path_split.run_local_projections",
        _fake_run_local_projections,
    )

    shocked = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3", "2020Q4"],
            "tdc_row_treasury_transactions_qoq": [1.0, -1.0, 2.0, -2.0],
            "tdc_treasury_operating_cash_qoq": [-3.0, 2.0, -4.0, 3.0],
            "foreign_nonts_qoq": [0.0, 0.3, -0.2, 0.1],
            "tga_qoq": [-0.9, 0.6, -1.1, 0.8],
            "reserves_qoq": [4.0, -2.0, 5.0, -3.0],
            "checkable_rest_of_world_bank_qoq": [0.8, -0.7, 1.1, -0.9],
        }
    )

    payload = build_toc_row_path_split_summary(
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
    assert payload["path_definitions"]["bundle"] == "tdc_toc_row_bundle_qoq"
    assert payload["quarterly_split"]["preferred_quarterly_path"] in {"mixed_path_signal", "direct_deposit_path_dominant"}
    assert payload["key_horizons"]["h0"]["preferred_horizon_path"] == "direct_deposit_path_dominant"
    assert payload["key_horizons"]["h0"]["coverage_share_of_bundle_beta"]["direct_deposit_path"] == 71.0 / 80.0
