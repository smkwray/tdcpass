from __future__ import annotations

import numpy as np
import pandas as pd

from tdcpass.analysis.toc_row_liability_incidence_raw_summary import (
    build_toc_row_liability_incidence_raw_summary,
)


def test_toc_row_liability_incidence_raw_summary_flags_partial_toc_and_weak_row() -> None:
    quarters = pd.period_range("2010Q1", periods=28, freq="Q")
    shock = np.linspace(-1.5, 1.5, len(quarters))
    toc_signed = 40.0 + 20.0 * shock
    row_leg = 2.0 * shock + 12.0 * np.sin(np.linspace(0.0, 6.0 * np.pi, len(quarters)))
    frame = pd.DataFrame(
        {
            "quarter": quarters.astype(str),
            "tdc_residual_z": shock,
            "tdc_treasury_operating_cash_qoq": -toc_signed,
            "tdc_row_treasury_transactions_qoq": row_leg,
            "total_deposits_bank_qoq": 8.0 + 0.45 * toc_signed,
            "deposits_only_bank_qoq": 4.0 + 0.30 * toc_signed,
            "checkable_private_domestic_bank_qoq": 2.0 + 0.18 * toc_signed,
            "reserves_qoq": 5.0 + 1.25 * toc_signed,
            "tga_qoq": -(3.0 + 0.92 * toc_signed),
            "cb_nonts_qoq": 2.0 + 0.40 * toc_signed,
            "checkable_rest_of_world_bank_qoq": 0.15 * row_leg,
            "foreign_nonts_qoq": 1.0 + 2.10 * row_leg,
            "interbank_transactions_foreign_banks_asset_qoq": 0.6 * row_leg,
            "interbank_transactions_foreign_banks_liability_qoq": 0.2 * row_leg,
            "deposits_at_foreign_banks_asset_qoq": 0.05 * row_leg,
        }
    )

    payload = build_toc_row_liability_incidence_raw_summary(
        shocked=frame,
        baseline_lp_spec={
            "shock_column": "tdc_residual_z",
            "controls": [],
            "horizons": [0, 1],
            "nw_lags": 1,
            "cumulative": True,
        },
    )

    assert payload["status"] == "available"
    assert payload["classification"]["toc_leg_status"] == "partial_in_scope_deposit_incidence_support_channels_still_dominate"
    assert (
        payload["classification"]["row_leg_status"]
        == "weak_in_scope_deposit_incidence_external_or_interbank_channels_dominate"
    )
    assert payload["classification"]["decision_gate"] == "full_reincorporation_not_supported"
    assert payload["recommendation"]["next_branch"] == "decide_whether_any_validated_toc_share_belongs_in_strict_object"
    assert payload["quarterly_alignment"]["toc_leg"]["best_in_scope_corr"] is not None
    assert payload["quarterly_alignment"]["row_leg"]["best_support_corr"] is not None
