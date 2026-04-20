from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_shock_composition import build_strict_shock_composition_summary


def test_build_strict_shock_composition_summary_reports_trim_diagnostics() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q1", "2009Q2", "2009Q3", "2020Q1", "2020Q2", "2020Q3", "2021Q1", "2021Q2"],
            "tdc_residual_z": [1.2, 1.0, -0.8, 2.6, 1.8, 0.7, 2.1, -0.4],
            "tdc_no_toc_no_row_bank_only_residual_z": [-0.3, 0.2, 0.9, 5.0, 2.7, -1.6, 0.4, 0.1],
            "tdc_bank_only_qoq": [12.0, 10.0, -5.0, 220.0, 160.0, 40.0, 180.0, -20.0],
            "tdc_no_toc_no_row_bank_only_qoq": [3.0, 4.0, -2.0, -120.0, -40.0, 10.0, -60.0, -8.0],
            "tdc_row_treasury_transactions_qoq": [6.0, 4.0, -1.0, -80.0, -25.0, 18.0, 70.0, -5.0],
            "tdc_treasury_operating_cash_qoq": [-3.0, -2.0, 2.0, 260.0, 175.0, 12.0, -170.0, 7.0],
            "strict_loan_source_qoq": [-1.0, -0.8, 0.6, 2.2, 1.9, -0.5, 0.8, 0.2],
            "strict_identifiable_total_qoq": [-0.4, -0.3, 0.5, 2.7, 2.3, -0.1, 0.9, 0.3],
        }
    )

    summary = build_strict_shock_composition_summary(shocked=shocked)

    assert summary["status"] == "available"
    assert summary["interpretation"] in {
        "no_rotation_detected_in_full_sample",
        "rotation_persists_after_top_quarter_and_covid_post_trims",
        "rotation_is_mostly_covid_post_specific",
        "rotation_concentrated_in_top_gap_quarters",
        "rotation_fragile_under_both_top_quarter_and_covid_post_trims",
    }
    assert summary["top_gap_quarters"][0]["quarter"] == "2020Q1"
    assert summary["period_bucket_profiles"][0]["period_bucket"] == "covid_post"
    assert "drop_top5_gap_quarters" in summary["trim_diagnostics"]
    assert "drop_covid_post" in summary["trim_diagnostics"]
    assert any("Dropping the five largest shock-gap quarters" in takeaway for takeaway in summary["takeaways"])
