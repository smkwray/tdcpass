from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly_di_loans_split import (
    build_strict_top_gap_anomaly_di_loans_split_summary,
)


def test_build_strict_top_gap_anomaly_di_loans_split_summary_identifies_borrower_side_driver() -> None:
    shocked = pd.DataFrame(
        {
            "quarter": ["2009Q2", "2009Q4", "2020Q1", "2020Q3", "2021Q1"],
            "tdc_residual_z": [0.3, -0.3, 3.8, 0.6, 5.6],
            "tdc_no_toc_no_row_bank_only_residual_z": [4.0, -3.4, 13.1, -8.9, 0.6],
            "tdc_bank_only_qoq": [85.0, 210.0, 225.0, 40.0, 770.0],
            "tdc_no_toc_no_row_bank_only_qoq": [15.0, 1.0, -142.0, 22.0, -57.0],
            "tdc_row_treasury_transactions_qoq": [119.0, 127.0, -256.0, 78.0, 221.0],
            "tdc_treasury_operating_cash_qoq": [49.0, -82.0, 111.0, 60.0, -607.0],
            "other_component_no_toc_no_row_bank_only_qoq": [-361.0, 74.0, 172.0, -217.0, 256.0],
            "strict_identifiable_total_qoq": [-27.0, -68.0, 516.0, 89.0, 314.0],
            "strict_identifiable_net_after_funding_qoq": [51.0, 97.0, 339.0, 136.0, 339.0],
            "foreign_nonts_qoq": [-97.0, 43.0, 559.0, -42.0, 30.0],
            "tga_qoq": [41.0, 81.0, 22.0, 55.0, -565.0],
            "reserves_qoq": [-103.0, 142.0, 535.0, -79.0, 754.0],
            "strict_loan_source_qoq": [-21.0, -55.0, 350.0, 44.0, 304.0],
            "strict_non_treasury_securities_qoq": [-6.0, -13.0, 166.0, 45.0, 10.0],
            "strict_funding_offset_total_qoq": [-8.0, -166.0, 157.0, -47.0, 12.0],
            "strict_loan_di_loans_nec_qoq": [-11.0, -59.0, 377.0, 33.0, 182.0],
            "strict_di_loans_nec_systemwide_liability_total_qoq": [-12.0, -63.0, 395.0, 32.0, 170.0],
            "strict_di_loans_nec_households_nonprofits_qoq": [-1.0, -2.0, 8.0, 1.0, 7.0],
            "strict_di_loans_nec_nonfinancial_corporate_qoq": [-2.0, -6.0, 35.0, 4.0, 18.0],
            "strict_di_loans_nec_nonfinancial_noncorporate_qoq": [-1.0, -1.5, 5.0, 1.0, 4.0],
            "strict_di_loans_nec_state_local_qoq": [-0.5, -0.7, 2.0, 0.5, 3.0],
            "strict_di_loans_nec_domestic_financial_qoq": [-4.0, -42.0, 300.0, 20.0, 120.0],
            "strict_di_loans_nec_rest_of_world_qoq": [-1.5, -3.0, 20.0, 5.5, 18.0],
            "strict_di_loans_nec_systemwide_borrower_total_qoq": [-10.0, -55.2, 370.0, 32.0, 170.0],
            "strict_di_loans_nec_systemwide_borrower_gap_qoq": [-2.0, -7.8, 25.0, 0.0, 0.0],
        }
    )

    summary = build_strict_top_gap_anomaly_di_loans_split_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["anomaly_quarter"]["quarter"] == "2009Q4"
    assert summary["dominant_borrower_component"]["metric"] in {
        "strict_di_loans_nec_domestic_financial_qoq",
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
    }
    assert summary["dominant_borrower_component"]["anomaly_minus_peer_delta"] < 0.0
    assert summary["borrower_gap_row"]["metric"] == "strict_di_loans_nec_systemwide_borrower_gap_qoq"
    assert summary["di_loans_nec_component_deltas"][0]["metric"] in {
        "strict_di_loans_nec_domestic_financial_qoq",
        "strict_di_loans_nec_systemwide_liability_total_qoq",
        "strict_loan_di_loans_nec_qoq",
    }
