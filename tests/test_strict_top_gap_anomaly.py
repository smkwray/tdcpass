from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly import build_strict_top_gap_anomaly_summary


def test_build_strict_top_gap_anomaly_summary_profiles_main_within_bucket_exception() -> None:
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
            "strict_loan_source_qoq": [-21.0, -55.0, 350.0, 44.0, 304.0, 3.0],
            "strict_non_treasury_securities_qoq": [-6.0, -13.0, 166.0, 45.0, 10.0, 1.0],
            "strict_funding_offset_total_qoq": [-78.0, -165.0, 177.0, -47.0, -25.0, 1.0],
            "strict_identifiable_total_qoq": [-27.0, -68.0, 516.0, 89.0, 314.0, 4.0],
            "strict_identifiable_net_after_funding_qoq": [51.0, 97.0, 339.0, 136.0, 339.0, 3.0],
            "foreign_nonts_qoq": [-97.0, 43.0, 559.0, -42.0, 30.0, 2.0],
            "tga_qoq": [41.0, 81.0, 22.0, 55.0, -565.0, 0.0],
            "reserves_qoq": [-103.0, 142.0, 535.0, -79.0, 754.0, 1.0],
        }
    )

    summary = build_strict_top_gap_anomaly_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["anomaly_quarter"]["quarter"] == "2009Q4"
    assert summary["peer_quarters"][0]["quarter"] in {"2020Q1", "2021Q1"}
    assert summary["interpretation"] in {
        "anomaly_flips_strict_total_negative_mainly_through_loan_contraction_relative_to_peers",
        "anomaly_flips_strict_total_negative_while_peer_bucket_stays_positive",
        "anomaly_is_mainly_pre_funding_not_post_funding",
        "anomaly_not_classified",
    }
    assert "strict_identifiable_total_qoq" in summary["weighted_peer_means"]
    assert "strict_identifiable_total_qoq" in summary["anomaly_vs_peer_deltas"]
    assert summary["ranked_anomaly_component_deltas"][0]["metric"] in {
        "strict_identifiable_total_qoq",
        "strict_loan_source_qoq",
        "reserves_qoq",
        "foreign_nonts_qoq",
    }
    assert summary["peer_pattern_summary"][0]["residual_strict_pattern"] in {
        "positive_residual_positive_strict",
        "negative_residual_positive_strict",
        "negative_residual_negative_strict",
        "positive_residual_negative_strict",
    }
    assert any("anomaly quarter" in takeaway.lower() or "same-bucket peers" in takeaway.lower() for takeaway in summary["takeaways"])
