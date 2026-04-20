from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_quarter_audit import build_strict_top_gap_quarter_audit_summary


def test_build_strict_top_gap_quarter_audit_summary_profiles_top_gap_quarters() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q2", "2009Q4", "2017Q1", "2020Q1", "2020Q3", "2021Q1", "2021Q3"],
            "tdc_residual_z": [0.3, -0.3, 2.4, 3.8, 0.6, 5.6, -1.2],
            "tdc_no_toc_no_row_bank_only_residual_z": [4.0, -3.4, -0.7, 13.1, -8.9, 0.6, 1.0],
            "tdc_bank_only_qoq": [85.0, 210.0, 380.0, 225.0, 40.0, 770.0, -55.0],
            "tdc_no_toc_no_row_bank_only_qoq": [15.0, 1.0, 1.0, -142.0, 22.0, -57.0, -25.0],
            "tdc_row_treasury_transactions_qoq": [119.0, 127.0, 72.0, -256.0, 78.0, 221.0, -42.0],
            "tdc_treasury_operating_cash_qoq": [49.0, -82.0, -307.0, 111.0, 60.0, -607.0, -12.0],
        }
    )

    summary = build_strict_top_gap_quarter_audit_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["interpretation"] in {
        "top_gap_quarters_are_mixed_or_offsetting_toc_row_bundles",
        "top_gap_quarters_are_mostly_row_dominant",
        "top_gap_quarters_are_mostly_toc_dominant",
        "top_gap_quarters_have_no_single_dominant_leg",
    }
    assert summary["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert summary["top_gap_quarters"][0]["dominant_leg"] in {"row_dominant", "toc_dominant", "mixed"}
    assert summary["top_gap_quarters"][0]["contribution_pattern"] in {"reinforcing", "offsetting", "single_leg"}
    assert summary["dominant_leg_summary"][0]["dominant_leg"] in {"row_dominant", "toc_dominant", "mixed"}
    assert summary["contribution_pattern_summary"][0]["contribution_pattern"] in {
        "reinforcing",
        "offsetting",
        "single_leg",
    }
    assert any("top-gap quarter" in takeaway for takeaway in summary["takeaways"])
