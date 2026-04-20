from __future__ import annotations

import pandas as pd

from tdcpass.analysis import toc_row_excluded_interpretation


def test_build_toc_row_excluded_interpretation_summary_compares_baseline_and_excluded_reads(monkeypatch) -> None:
    baseline_identity = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 20},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 20},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "se": 1.0, "lower95": -9.0, "upper95": -5.0, "n": 20},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 14.0, "se": 1.0, "lower95": 12.0, "upper95": 16.0, "n": 19},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 6.0, "se": 1.0, "lower95": 4.0, "upper95": 8.0, "n": 19},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 19},
        ]
    )
    excluded_identity = pd.DataFrame(
        [
            {"outcome": "tdc_no_toc_no_row_bank_only_qoq", "horizon": 0, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 20},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.2, "se": 1.0, "lower95": 3.2, "upper95": 7.2, "n": 20},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -2.8, "se": 1.0, "lower95": -4.8, "upper95": -0.8, "n": 20},
            {"outcome": "tdc_no_toc_no_row_bank_only_qoq", "horizon": 4, "beta": 9.0, "se": 1.0, "lower95": 7.0, "upper95": 11.0, "n": 19},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 6.5, "se": 1.0, "lower95": 4.5, "upper95": 8.5, "n": 19},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -2.5, "se": 1.0, "lower95": -4.5, "upper95": -0.5, "n": 19},
        ]
    )
    strict_baseline = pd.DataFrame(
        [
            {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 0, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 20},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 4, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 19},
        ]
    )
    strict_excluded = pd.DataFrame(
        [
            {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -0.8, "se": 1.0, "lower95": -2.8, "upper95": 1.2, "n": 20},
            {"outcome": "strict_identifiable_gap_no_toc_no_row_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 4, "beta": -1.5, "se": 1.0, "lower95": -3.5, "upper95": 0.5, "n": 19},
            {"outcome": "strict_identifiable_gap_no_toc_no_row_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 19},
        ]
    )

    identity_calls = {"count": 0}

    def _identity_stub(*args, **kwargs):
        identity_calls["count"] += 1
        return baseline_identity.copy() if identity_calls["count"] == 1 else excluded_identity.copy()

    lp_calls = {"count": 0}

    def _lp_stub(*args, **kwargs):
        lp_calls["count"] += 1
        return strict_baseline.copy() if lp_calls["count"] == 1 else strict_excluded.copy()

    monkeypatch.setattr(toc_row_excluded_interpretation, "build_identity_baseline_irf", _identity_stub)
    monkeypatch.setattr(toc_row_excluded_interpretation, "run_local_projections", _lp_stub)

    summary = toc_row_excluded_interpretation.build_toc_row_excluded_interpretation_summary(
        shocked=pd.DataFrame(
            {
                "quarter": ["2000Q1"],
                "strict_identifiable_total_qoq": [1.0],
                "other_component_no_toc_no_row_bank_only_qoq": [0.5],
            }
        ),
        baseline_lp_spec={
            "shock_column": "tdc_residual_z",
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
        bootstrap_reps=0,
    )

    assert summary["status"] == "available"
    h0 = summary["key_horizons"]["h0"]
    assert h0["baseline"]["residual_response"]["beta"] == -7.0
    assert h0["toc_row_excluded"]["residual_response"]["beta"] == -2.8
    assert abs(h0["excluded_minus_baseline_beta"]["residual_response"] - 4.2) < 1e-12
    assert abs(h0["baseline"]["strict_gap_share_of_residual"] - (6.0 / 7.0)) < 1e-12
    assert abs(h0["toc_row_excluded"]["strict_gap_share_of_residual"] - (2.0 / 2.8)) < 1e-12
    assert h0["interpretation"] == "toc_row_exclusion_materially_relaxes_residual_and_strict_gap"
    assert summary["comparison_definition"]["release_role"] == "secondary_interpretation_only"
    assert any("excluding TOC/ROW changes the non-TDC residual response from about -7.00 to about -2.80" in takeaway for takeaway in summary["takeaways"])
    assert any(
        "strict direct-count gap share" in takeaway and "0.86" in takeaway and "0.71" in takeaway
        for takeaway in summary["takeaways"]
    )
