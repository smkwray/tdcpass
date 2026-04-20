from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_sign_mismatch_audit import build_strict_sign_mismatch_audit_summary


def test_build_strict_sign_mismatch_audit_summary_detects_rotation() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q1", "2009Q2", "2009Q3", "2009Q4", "2010Q1"],
            "tdc_residual_z": [2.0, 1.5, -1.0, -0.5, 0.8],
            "tdc_no_toc_no_row_bank_only_residual_z": [-1.0, 0.4, 1.2, -0.3, 0.1],
            "tdc_bank_only_qoq": [12.0, 9.0, -4.0, -2.0, 5.0],
            "tdc_no_toc_no_row_bank_only_qoq": [3.0, 2.0, -1.0, -0.5, 0.5],
            "tdc_row_treasury_transactions_qoq": [4.0, 3.0, -0.5, 0.2, 1.0],
            "tdc_treasury_operating_cash_qoq": [-5.0, -4.0, 2.5, 1.3, -3.5],
            "other_component_qoq": [-4.0, -3.0, 2.0, 1.0, -1.0],
            "other_component_no_toc_no_row_bank_only_qoq": [-0.5, -0.2, 0.6, 0.2, -0.1],
            "total_deposits_bank_qoq": [2.0, 1.0, 0.5, -0.5, 1.5],
            "strict_loan_core_min_qoq": [-1.0, -0.8, 0.7, 0.5, 0.3],
            "strict_loan_source_qoq": [-2.0, -1.5, 1.0, 0.7, 0.5],
            "strict_non_treasury_securities_qoq": [1.0, 0.8, -0.4, -0.2, 0.1],
            "strict_identifiable_total_qoq": [-1.0, -0.7, 0.9, 0.5, 0.4],
            "strict_identifiable_net_after_funding_qoq": [-1.2, -0.8, 0.7, 0.3, 0.2],
        }
    )
    summary = build_strict_sign_mismatch_audit_summary(
        shocked=shocked,
        strict_missing_channel_summary={
            "key_horizons": {
                "h0": {
                    "toc_row_excluded": {
                        "residual_response": {"beta": -0.5},
                        "strict_identifiable_total_response": {"beta": 1.0},
                    }
                }
            }
        },
    )

    assert summary["status"] == "available"
    assert summary["interpretation"] == "excluded_shock_rotates_toward_positive_direct_count_channels"
    assert summary["shock_alignment"]["overlap_rows"] == 5
    assert abs(summary["quarter_concentration"]["top5_abs_gap_share"] - 1.0) < 1e-12
    assert summary["gap_driver_alignment"]["dominant_driver_by_abs_corr"] == "baseline_minus_excluded_target_qoq"
    assert summary["component_alignment"]["strict_loan_core_min_qoq"]["baseline_shock_corr"] < 0
    assert summary["component_alignment"]["strict_loan_core_min_qoq"]["toc_row_excluded_shock_corr"] > 0
    assert any("sign mismatch" in takeaway for takeaway in summary["takeaways"])
