from __future__ import annotations

import pandas as pd

from tdcpass.analysis import strict_missing_channel_summary


def _strict_lp_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "se": 1.0, "lower95": -9.0, "upper95": -5.0, "n": 20},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 0, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 20},
            {"outcome": "strict_loan_source_qoq", "horizon": 0, "beta": -1.2, "se": 1.0, "lower95": -3.2, "upper95": 0.8, "n": 20},
            {"outcome": "strict_loan_core_plus_private_borrower_qoq", "horizon": 0, "beta": 0.4, "se": 1.0, "lower95": -1.6, "upper95": 2.4, "n": 20},
            {"outcome": "strict_loan_noncore_system_qoq", "horizon": 0, "beta": -1.6, "se": 1.0, "lower95": -3.6, "upper95": 0.4, "n": 20},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 0, "beta": 0.6, "se": 1.0, "lower95": -1.4, "upper95": 2.6, "n": 20},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -0.6, "se": 1.0, "lower95": -2.6, "upper95": 1.4, "n": 20},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 0, "beta": -6.4, "se": 1.0, "lower95": -8.4, "upper95": -4.4, "n": 20},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 0, "beta": 0.8, "se": 1.0, "lower95": -1.2, "upper95": 2.8, "n": 20},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 0, "beta": -1.4, "se": 1.0, "lower95": -3.4, "upper95": 0.6, "n": 20},
            {"outcome": "strict_gap_after_funding_qoq", "horizon": 0, "beta": -5.6, "se": 1.0, "lower95": -7.6, "upper95": -3.6, "n": 20},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 19},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 4, "beta": -0.8, "se": 1.0, "lower95": -2.8, "upper95": 1.2, "n": 19},
            {"outcome": "strict_loan_source_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19},
            {"outcome": "strict_loan_core_plus_private_borrower_qoq", "horizon": 4, "beta": 0.2, "se": 1.0, "lower95": -1.8, "upper95": 2.2, "n": 19},
            {"outcome": "strict_loan_noncore_system_qoq", "horizon": 4, "beta": -1.7, "se": 1.0, "lower95": -3.7, "upper95": 0.3, "n": 19},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 4, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 19},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 4, "beta": -1.5, "se": 1.0, "lower95": -3.5, "upper95": 0.5, "n": 19},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 4, "beta": -6.5, "se": 1.0, "lower95": -8.5, "upper95": -4.5, "n": 19},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 4, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 19},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19},
            {"outcome": "strict_gap_after_funding_qoq", "horizon": 4, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 19},
        ]
    )


def test_build_strict_missing_channel_summary_highlights_remaining_gap(monkeypatch) -> None:
    excluded_lp = pd.DataFrame(
        [
            {"outcome": "other_component_no_toc_no_row_bank_only_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 0, "beta": -0.3, "se": 1.0, "lower95": -2.3, "upper95": 1.7, "n": 20},
            {"outcome": "strict_loan_source_qoq", "horizon": 0, "beta": -0.6, "se": 1.0, "lower95": -2.6, "upper95": 1.4, "n": 20},
            {"outcome": "strict_loan_core_plus_private_borrower_qoq", "horizon": 0, "beta": 0.8, "se": 1.0, "lower95": -1.2, "upper95": 2.8, "n": 20},
            {"outcome": "strict_loan_noncore_system_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 0, "beta": 0.4, "se": 1.0, "lower95": -1.6, "upper95": 2.4, "n": 20},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -0.2, "se": 1.0, "lower95": -2.2, "upper95": 1.8, "n": 20},
            {"outcome": "strict_identifiable_gap_no_toc_no_row_qoq", "horizon": 0, "beta": -1.8, "se": 1.0, "lower95": -3.8, "upper95": 0.2, "n": 20},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 0, "beta": 0.6, "se": 1.0, "lower95": -1.4, "upper95": 2.6, "n": 20},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 0, "beta": -0.8, "se": 1.0, "lower95": -2.8, "upper95": 1.2, "n": 20},
            {"outcome": "strict_gap_after_funding_no_toc_no_row_qoq", "horizon": 0, "beta": -1.2, "se": 1.0, "lower95": -3.2, "upper95": 0.8, "n": 20},
            {"outcome": "other_component_no_toc_no_row_bank_only_qoq", "horizon": 4, "beta": -3.0, "se": 1.0, "lower95": -5.0, "upper95": -1.0, "n": 19},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 4, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 19},
            {"outcome": "strict_loan_source_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 19},
            {"outcome": "strict_loan_core_plus_private_borrower_qoq", "horizon": 4, "beta": 0.3, "se": 1.0, "lower95": -1.7, "upper95": 2.3, "n": 19},
            {"outcome": "strict_loan_noncore_system_qoq", "horizon": 4, "beta": -0.8, "se": 1.0, "lower95": -2.8, "upper95": 1.2, "n": 19},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 4, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 19},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 4, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 19},
            {"outcome": "strict_identifiable_gap_no_toc_no_row_qoq", "horizon": 4, "beta": -2.5, "se": 1.0, "lower95": -4.5, "upper95": -0.5, "n": 19},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 4, "beta": 0.4, "se": 1.0, "lower95": -1.6, "upper95": 2.4, "n": 19},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 4, "beta": -0.9, "se": 1.0, "lower95": -2.9, "upper95": 1.1, "n": 19},
            {"outcome": "strict_gap_after_funding_no_toc_no_row_qoq", "horizon": 4, "beta": -2.1, "se": 1.0, "lower95": -4.1, "upper95": -0.1, "n": 19},
        ]
    )

    monkeypatch.setattr(
        strict_missing_channel_summary,
        "run_local_projections",
        lambda *args, **kwargs: excluded_lp.copy(),
    )

    summary = strict_missing_channel_summary.build_strict_missing_channel_summary(
        strict_lp_irf=_strict_lp_fixture(),
        shocked=pd.DataFrame(
            {
                "quarter": ["2000Q1"],
                "other_component_no_toc_no_row_bank_only_qoq": [0.5],
                "strict_identifiable_total_qoq": [0.1],
                "strict_identifiable_net_after_funding_qoq": [0.1],
            }
        ),
        baseline_lp_spec={
            "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds"],
            "horizons": [0, 4],
            "cumulative": True,
            "nw_lags": 4,
        },
        baseline_shock_spec={"target": "tdc_bank_only_qoq", "predictors": ["lag_tdc_bank_only_qoq"]},
        excluded_shock_spec={
            "target": "tdc_no_toc_no_row_bank_only_qoq",
            "standardized_column": "tdc_no_toc_no_row_bank_only_residual_z",
            "predictors": ["lag_tdc_no_toc_no_row_bank_only_qoq"],
        },
        horizons=(0, 4),
    )

    assert summary["status"] == "available"
    assert summary["comparison_definition"]["release_role"] == "strict_missing_channel_diagnostic"
    h0 = summary["key_horizons"]["h0"]
    assert h0["toc_row_excluded"]["strict_headline_direct_core_share_of_residual_abs"] == 0.15
    assert h0["toc_row_excluded"]["strict_loan_share_of_residual_abs"] == 0.3
    assert h0["toc_row_excluded"]["strict_gap_after_funding_share_of_residual_abs"] == 0.6
    assert h0["interpretation"] == "toc_row_exclusion_relaxes_residual_but_missing_channels_still_dominate"
    assert abs(h0["excluded_minus_baseline_beta"]["strict_headline_direct_core_response"] - 0.2) < 1e-12
    assert abs(h0["excluded_minus_baseline_beta"]["strict_identifiable_total_response"] - 0.4) < 1e-12
    assert any("headline direct core ≈ -0.30" in takeaway for takeaway in summary["takeaways"])
    assert any("current broad loan source ≈ -0.60" in takeaway for takeaway in summary["takeaways"])
    assert any("gap-after-funding share ≈ 0.60" in takeaway for takeaway in summary["takeaways"])
