from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly_component_split import (
    build_strict_top_gap_anomaly_component_split_summary,
)


def test_build_strict_top_gap_anomaly_component_split_summary_identifies_di_loans_nec_shortfall() -> None:
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
            "strict_loan_source_qoq": [-21.0, -55.0, 350.0, 44.0, 304.0],
            "strict_loan_mortgages_qoq": [-8.0, -51.0, 20.0, 6.0, -12.0],
            "strict_loan_consumer_credit_qoq": [-2.0, -5.0, -50.0, 3.0, -70.0],
            "strict_loan_di_loans_nec_qoq": [-11.0, -59.0, 377.0, 33.0, 182.0],
            "strict_loan_other_advances_qoq": [0.0, 0.0, 3.0, 2.0, 2.0],
            "strict_non_treasury_agency_gse_qoq": [5.0, 97.0, 170.0, 4.0, 140.0],
            "strict_non_treasury_municipal_qoq": [1.0, 0.0, 28.0, 2.0, 9.0],
            "strict_non_treasury_corporate_foreign_bonds_qoq": [-12.0, -50.0, 140.0, 39.0, -18.0],
            "strict_non_treasury_securities_qoq": [-6.0, -13.0, 166.0, 45.0, 10.0],
            "strict_funding_fedfunds_repo_qoq": [10.0, -120.0, 60.0, -3.0, -50.0],
            "strict_funding_debt_securities_qoq": [2.0, -4.0, 7.0, 1.0, 12.0],
            "strict_funding_fhlb_advances_qoq": [-20.0, -42.0, 90.0, -45.0, 50.0],
            "strict_funding_offset_total_qoq": [-8.0, -166.0, 157.0, -47.0, 12.0],
            "strict_identifiable_total_qoq": [-27.0, -68.0, 516.0, 89.0, 314.0],
            "strict_identifiable_net_after_funding_qoq": [51.0, 97.0, 339.0, 136.0, 339.0],
            "foreign_nonts_qoq": [-97.0, 43.0, 559.0, -42.0, 30.0],
            "tga_qoq": [41.0, 81.0, 22.0, 55.0, -565.0],
            "reserves_qoq": [-103.0, 142.0, 535.0, -79.0, 754.0],
        }
    )

    summary = build_strict_top_gap_anomaly_component_split_summary(shocked=shocked, limit=5)

    assert summary["status"] == "available"
    assert summary["anomaly_quarter"]["quarter"] == "2009Q4"
    assert summary["interpretation"] in {
        "anomaly_is_di_loans_nec_contraction_with_weaker_liquidity_and_external_support",
        "anomaly_is_loan_led_with_secondary_liquidity_external_gap",
    }
    assert summary["loan_subcomponent_deltas"][0]["metric"] == "strict_loan_di_loans_nec_qoq"
    assert summary["loan_subcomponent_deltas"][0]["anomaly_minus_peer_delta"] < 0.0
    assert summary["liquidity_external_deltas"][0]["metric"] in {"reserves_qoq", "foreign_nonts_qoq", "tga_qoq"}
    assert summary["ranked_component_deltas"][0]["metric"] in {
        "reserves_qoq",
        "strict_loan_di_loans_nec_qoq",
        "foreign_nonts_qoq",
    }
