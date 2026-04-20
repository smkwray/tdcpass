from __future__ import annotations

from pathlib import Path

import pandas as pd

from tdcpass.analysis.historical_cash_term_reestimation_summary import (
    build_historical_cash_term_reestimation_summary,
)


def test_historical_cash_term_reestimation_summary_adjusts_pre_transaction_history(tmp_path: Path) -> None:
    source = tmp_path / "tdc_estimates.csv"
    pd.DataFrame(
        {
            "date": ["1999-03-31", "1999-06-30", "2002-09-30", "2002-12-31"],
            "tdc_bank_only_extended_1990": [10000.0, 12000.0, 14000.0, 16000.0],
        }
    ).to_csv(source, index=False)

    shocked = pd.DataFrame(
        {
            "quarter": ["1999Q1", "1999Q2", "2002Q3", "2002Q4"],
            "tdc_treasury_operating_cash_qoq": [10.0, 12.0, 14.0, 16.0],
            "federal_govt_cash_balance_proxy_qoq": [20.0, 18.0, 21.0, 16.0],
        }
    )

    summary = build_historical_cash_term_reestimation_summary(
        shocked=shocked,
        canonical_tdc_source_path=source,
    )

    assert summary["status"] == "available"
    assert summary["windows"]["historical_backfill_window"]["end"] == "2002Q3"
    assert summary["comparison"]["historical_backfill_window"]["mean_abs_adjustment"] == 7.666666666666667
    top = summary["top_adjustment_quarters"][0]
    assert top["quarter"] == "1999Q1"
    assert top["candidate_minus_current_tdc_qoq"] == -10.0
