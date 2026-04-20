from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_quarter_direction import build_strict_top_gap_quarter_direction_summary


def test_build_strict_top_gap_quarter_direction_summary_profiles_gap_direction() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q2", "2009Q4", "2020Q1", "2020Q3", "2021Q1", "2021Q3"],
            "tdc_residual_z": [0.3, -0.3, 3.8, 0.6, 5.6, -1.2],
            "tdc_no_toc_no_row_bank_only_residual_z": [4.0, -3.4, 13.1, -8.9, 0.6, 1.0],
            "tdc_bank_only_qoq": [85.0, 210.0, 225.0, 40.0, 770.0, -55.0],
            "tdc_no_toc_no_row_bank_only_qoq": [15.0, 1.0, -142.0, 22.0, -57.0, -25.0],
            "tdc_row_treasury_transactions_qoq": [119.0, 127.0, -256.0, 78.0, 221.0, -42.0],
            "tdc_treasury_operating_cash_qoq": [49.0, -82.0, 111.0, 60.0, -607.0, -12.0],
        }
    )

    summary = build_strict_top_gap_quarter_direction_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["interpretation"] in {
        "top_gap_gap_direction_often_opposes_both_toc_and_row_legs",
        "top_gap_gap_direction_often_opposes_bundle_sign",
        "top_gap_gap_direction_is_mostly_row_driven_gap_direction",
        "top_gap_gap_direction_is_mostly_toc_driven_gap_direction",
        "top_gap_gap_direction_is_mixed_across_quarters",
    }
    assert summary["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert summary["top_gap_quarters"][0]["gap_alignment_to_bundle"] in {"aligned", "opposed", "neutral"}
    assert summary["top_gap_quarters"][0]["directional_driver"] in {
        "row_driven_gap_direction",
        "toc_driven_gap_direction",
        "both_legs_align_gap",
        "both_legs_oppose_gap",
        "neutral_or_single_leg",
    }
    assert summary["gap_bundle_alignment_summary"][0]["gap_alignment_to_bundle"] in {"aligned", "opposed", "neutral"}
    assert summary["directional_driver_summary"][0]["directional_driver"] in {
        "row_driven_gap_direction",
        "toc_driven_gap_direction",
        "both_legs_align_gap",
        "both_legs_oppose_gap",
        "neutral_or_single_leg",
    }
    assert any("directional-driver" in takeaway for takeaway in summary["takeaways"])
