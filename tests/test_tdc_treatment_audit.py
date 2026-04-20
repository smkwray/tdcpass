from __future__ import annotations

import pandas as pd

from tdcpass.analysis import tdc_treatment_audit


def test_build_tdc_treatment_audit_summary_reports_component_and_variant_reads(monkeypatch) -> None:
    component_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 20},
            {"outcome": "tdc_fed_treasury_transactions_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.0, "upper95": 4.0, "n": 20},
            {"outcome": "tdc_us_chartered_treasury_transactions_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.0, "upper95": 5.0, "n": 20},
            {"outcome": "tdc_foreign_offices_treasury_transactions_qoq", "horizon": 0, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 20},
            {"outcome": "tdc_affiliated_areas_treasury_transactions_qoq", "horizon": 0, "beta": 0.25, "se": 1.0, "lower95": -1.75, "upper95": 2.25, "n": 20},
            {"outcome": "tdc_row_treasury_transactions_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 20},
            {"outcome": "tdc_treasury_operating_cash_qoq", "horizon": 0, "beta": 1.5, "se": 1.0, "lower95": -0.5, "upper95": 3.5, "n": 20},
            {"outcome": "tdc_fed_remit_positive_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 20},
        ]
    )
    audit_ladder = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 20},
            {"treatment_variant": "baseline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 20},
            {"treatment_variant": "baseline", "outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "se": 1.0, "lower95": -9.0, "upper95": -5.0, "n": 20},
            {"treatment_variant": "domestic_bank_only", "outcome": "tdc_domestic_bank_only_qoq", "horizon": 0, "beta": 10.0, "se": 1.0, "lower95": 8.0, "upper95": 12.0, "n": 20},
            {"treatment_variant": "domestic_bank_only", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.5, "se": 1.0, "lower95": 3.5, "upper95": 7.5, "n": 20},
            {"treatment_variant": "domestic_bank_only", "outcome": "other_component_qoq", "horizon": 0, "beta": -4.5, "se": 1.0, "lower95": -6.5, "upper95": -2.5, "n": 20},
            {"treatment_variant": "no_foreign_bank_sectors", "outcome": "tdc_no_foreign_bank_sectors_qoq", "horizon": 0, "beta": 9.0, "se": 1.0, "lower95": 7.0, "upper95": 11.0, "n": 20},
            {"treatment_variant": "no_foreign_bank_sectors", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.1, "se": 1.0, "lower95": 3.1, "upper95": 7.1, "n": 20},
            {"treatment_variant": "no_foreign_bank_sectors", "outcome": "other_component_qoq", "horizon": 0, "beta": -3.9, "se": 1.0, "lower95": -5.9, "upper95": -1.9, "n": 20},
            {"treatment_variant": "no_toc_bank_only", "outcome": "tdc_no_toc_bank_only_qoq", "horizon": 0, "beta": 13.5, "se": 1.0, "lower95": 11.5, "upper95": 15.5, "n": 20},
            {"treatment_variant": "no_toc_bank_only", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.2, "se": 1.0, "lower95": 3.2, "upper95": 7.2, "n": 20},
            {"treatment_variant": "no_toc_bank_only", "outcome": "other_component_qoq", "horizon": 0, "beta": -8.3, "se": 1.0, "lower95": -10.3, "upper95": -6.3, "n": 20},
            {"treatment_variant": "no_toc_no_row_bank_only", "outcome": "tdc_no_toc_no_row_bank_only_qoq", "horizon": 0, "beta": 8.5, "se": 1.0, "lower95": 6.5, "upper95": 10.5, "n": 20},
            {"treatment_variant": "no_toc_no_row_bank_only", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.3, "se": 1.0, "lower95": 3.3, "upper95": 7.3, "n": 20},
            {"treatment_variant": "no_toc_no_row_bank_only", "outcome": "other_component_qoq", "horizon": 0, "beta": -3.2, "se": 1.0, "lower95": -5.2, "upper95": -1.2, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "outcome": "tdc_us_chartered_bank_only_qoq", "horizon": 0, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.2, "se": 1.0, "lower95": 3.2, "upper95": 7.2, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "outcome": "other_component_qoq", "horizon": 0, "beta": -2.8, "se": 1.0, "lower95": -4.8, "upper95": -0.8, "n": 20},
            {"treatment_variant": "no_remit_bank_only", "outcome": "tdc_no_remit_bank_only_qoq", "horizon": 0, "beta": 11.0, "se": 1.0, "lower95": 9.0, "upper95": 13.0, "n": 20},
            {"treatment_variant": "no_remit_bank_only", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 20},
            {"treatment_variant": "no_remit_bank_only", "outcome": "other_component_qoq", "horizon": 0, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 20},
        ]
    )

    monkeypatch.setattr(tdc_treatment_audit, "run_local_projections", lambda *args, **kwargs: component_lp.copy())
    monkeypatch.setattr(tdc_treatment_audit, "build_identity_variant_ladder", lambda *args, **kwargs: audit_ladder.copy())

    summary = tdc_treatment_audit.build_tdc_treatment_audit_summary(
        shocked=pd.DataFrame(
            {
                "quarter": ["2000Q1"],
                "tdc_bank_only_qoq": [11.0],
                "tdc_domestic_bank_only_qoq": [6.0],
                "tdc_no_foreign_bank_sectors_qoq": [10.25],
                "tdc_no_toc_bank_only_qoq": [11.75],
                "tdc_no_toc_no_row_bank_only_qoq": [6.75],
                "tdc_no_remit_bank_only_qoq": [10.0],
                "tdc_fed_treasury_transactions_qoq": [2.0],
                "tdc_us_chartered_treasury_transactions_qoq": [3.0],
                "tdc_foreign_offices_treasury_transactions_qoq": [0.5],
                "tdc_affiliated_areas_treasury_transactions_qoq": [0.25],
                "tdc_row_treasury_transactions_qoq": [5.0],
                "tdc_treasury_operating_cash_qoq": [0.75],
                "tdc_fed_remit_positive_qoq": [1.0],
            }
        ),
        baseline_lp_spec={
            "shock_column": "tdc_residual_z",
            "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds"],
            "horizons": [0],
            "cumulative": True,
            "nw_lags": 4,
        },
        baseline_shock_spec={"target": "tdc_bank_only_qoq"},
        shock_specs={
            "unexpected_tdc_default": {"standardized_column": "tdc_residual_z", "target": "tdc_bank_only_qoq", "predictors": ["lag_tdc_bank_only_qoq"]},
            "unexpected_tdc_domestic_bank_only": {"standardized_column": "tdc_domestic_bank_only_residual_z", "target": "tdc_domestic_bank_only_qoq", "predictors": ["lag_tdc_domestic_bank_only_qoq"]},
            "unexpected_tdc_no_foreign_bank_sectors": {"standardized_column": "tdc_no_foreign_bank_sectors_residual_z", "target": "tdc_no_foreign_bank_sectors_qoq", "predictors": ["lag_tdc_no_foreign_bank_sectors_qoq"]},
            "unexpected_tdc_no_toc_bank_only": {"standardized_column": "tdc_no_toc_bank_only_residual_z", "target": "tdc_no_toc_bank_only_qoq", "predictors": ["lag_tdc_no_toc_bank_only_qoq"]},
            "unexpected_tdc_no_toc_no_row_bank_only": {"standardized_column": "tdc_no_toc_no_row_bank_only_residual_z", "target": "tdc_no_toc_no_row_bank_only_qoq", "predictors": ["lag_tdc_no_toc_no_row_bank_only_qoq"]},
            "unexpected_tdc_us_chartered_bank_only": {"standardized_column": "tdc_us_chartered_bank_only_residual_z", "target": "tdc_us_chartered_bank_only_qoq", "predictors": ["lag_tdc_us_chartered_bank_only_qoq"]},
            "unexpected_tdc_no_remit_bank_only": {"standardized_column": "tdc_no_remit_bank_only_residual_z", "target": "tdc_no_remit_bank_only_qoq", "predictors": ["lag_tdc_no_remit_bank_only_qoq"]},
        },
        horizons=(0,),
        bootstrap_reps=0,
    )

    assert summary["status"] == "available"
    h0 = summary["key_horizons"]["h0"]
    assert h0["dominant_signed_component"] == "rest_of_world_treasury_transactions"
    assert abs(h0["direct_component_responses"]["treasury_operating_cash_drain"]["signed_contribution_beta"] + 1.5) < 1e-12
    assert abs(h0["foreign_bank_sectors_signed_beta_sum"] - 0.75) < 1e-12
    assert h0["largest_residual_shift_variant"] == "us_chartered_bank_only"
    assert abs(h0["variant_removal_diagnostics"]["domestic_bank_only"]["residual_shift_vs_baseline_beta"] - 2.5) < 1e-12
    assert abs(h0["variant_removal_diagnostics"]["no_toc_bank_only"]["residual_shift_vs_baseline_beta"] + 1.3) < 1e-12
    assert abs(h0["variant_removal_diagnostics"]["no_toc_no_row_bank_only"]["residual_shift_vs_baseline_beta"] - 3.8) < 1e-12
    assert summary["construction_alignment"]["status"] == "available"
    assert summary["construction_alignment"]["rows"]["treasury_operating_cash_leg"]["quarterly_alignment"] == "exact"
    assert summary["construction_alignment"]["rows"]["no_toc_no_row_variant"]["quarterly_alignment"] == "exact"
    assert abs(summary["construction_alignment"]["rows"]["treasury_operating_cash_leg"]["max_abs_gap_beta"]) < 1e-12
    assert any("removing only ROW shifts the residual by about 2.50" in takeaway for takeaway in summary["takeaways"])
    assert any("removing Treasury operating cash from the treatment shifts the residual by about -1.30" in takeaway for takeaway in summary["takeaways"])
    assert any("removing Treasury operating cash and ROW together shifts the residual by about 3.80" in takeaway for takeaway in summary["takeaways"])
