from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_inversion import build_strict_top_gap_inversion_summary


def test_build_strict_top_gap_inversion_summary_profiles_realized_inversion_context() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q2", "2009Q4", "2020Q1", "2020Q3", "2021Q1", "2021Q3"],
            "tdc_residual_z": [0.3, -0.3, 3.8, 0.6, 5.6, -1.2],
            "tdc_no_toc_no_row_bank_only_residual_z": [4.0, -3.4, 13.1, -8.9, 0.6, 1.0],
            "tdc_bank_only_qoq": [85.0, 210.0, 225.0, 40.0, 770.0, -55.0],
            "tdc_no_toc_no_row_bank_only_qoq": [15.0, 1.0, -142.0, 22.0, -57.0, -25.0],
            "tdc_row_treasury_transactions_qoq": [119.0, 127.0, -256.0, 78.0, 221.0, -42.0],
            "tdc_treasury_operating_cash_qoq": [49.0, -82.0, 111.0, 60.0, -607.0, -12.0],
            "other_component_no_toc_no_row_bank_only_qoq": [-361.0, 74.0, 172.0, -217.0, 256.0, 5.0],
            "strict_identifiable_total_qoq": [-27.0, -68.0, 516.0, 89.0, 314.0, 4.0],
            "strict_identifiable_net_after_funding_qoq": [51.0, 97.0, 339.0, 136.0, 339.0, 3.0],
            "foreign_nonts_qoq": [-97.0, 43.0, 559.0, -42.0, 30.0, 2.0],
            "tga_qoq": [41.0, 81.0, 22.0, 55.0, -565.0, 0.0],
            "reserves_qoq": [-103.0, 142.0, 535.0, -79.0, 754.0, 1.0],
        }
    )

    summary = build_strict_top_gap_inversion_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["interpretation"] in {
        "both_leg_inversion_quarters_still_tend_to_show_positive_residual_and_positive_strict_support",
        "top_gap_inversion_quarters_often_show_negative_residual_but_positive_strict_support",
        "top_gap_inversion_quarters_often_show_joint_negative_residual_and_strict_support",
        "top_gap_inversion_profiles_are_mixed",
    }
    assert summary["top_gap_quarters"][0]["quarter"] == "2020Q3"
    assert summary["top_gap_quarters"][0]["residual_strict_pattern"] in {
        "positive_residual_positive_strict",
        "negative_residual_positive_strict",
        "negative_residual_negative_strict",
        "positive_residual_negative_strict",
        "zero_residual_zero_strict",
    }
    assert summary["directional_driver_context_summary"][0]["directional_driver"] in {
        "row_driven_gap_direction",
        "toc_driven_gap_direction",
        "both_legs_align_gap",
        "both_legs_oppose_gap",
        "neutral_or_single_leg",
    }
    assert summary["residual_strict_pattern_summary"][0]["residual_strict_pattern"] in {
        "positive_residual_positive_strict",
        "negative_residual_positive_strict",
        "negative_residual_negative_strict",
        "positive_residual_negative_strict",
        "zero_residual_zero_strict",
    }
    assert any("TOC-driven exception" in takeaway or "leading inversion bucket" in takeaway for takeaway in summary["takeaways"])
