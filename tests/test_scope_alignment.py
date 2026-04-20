from __future__ import annotations

import pandas as pd
import pytest

from tdcpass.analysis import scope_alignment


def test_build_scope_alignment_summary_materializes_both_deposit_concepts(monkeypatch) -> None:
    ladder_rows = pd.DataFrame(
        [
            {
                "treatment_variant": "baseline",
                "target": "tdc_bank_only_qoq",
                "outcome": "tdc_bank_only_qoq",
                "horizon": 0,
                "beta": 10.0,
                "se": 1.0,
                "lower95": 8.0,
                "upper95": 12.0,
                "n": 20,
            },
            {
                "treatment_variant": "baseline",
                "target": "tdc_bank_only_qoq",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 4.0,
                "se": 1.0,
                "lower95": 2.0,
                "upper95": 6.0,
                "n": 20,
            },
            {
                "treatment_variant": "baseline",
                "target": "tdc_bank_only_qoq",
                "outcome": "deposits_only_bank_qoq",
                "horizon": 0,
                "beta": 3.0,
                "se": 1.0,
                "lower95": 1.0,
                "upper95": 5.0,
                "n": 20,
            },
            {
                "treatment_variant": "baseline",
                "target": "tdc_bank_only_qoq",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -6.0,
                "se": 1.0,
                "lower95": -8.0,
                "upper95": -4.0,
                "n": 20,
            },
            {
                "treatment_variant": "domestic_bank_only",
                "target": "tdc_domestic_bank_only_qoq",
                "outcome": "tdc_domestic_bank_only_qoq",
                "horizon": 0,
                "beta": 9.0,
                "se": 1.0,
                "lower95": 7.0,
                "upper95": 11.0,
                "n": 20,
            },
            {
                "treatment_variant": "domestic_bank_only",
                "target": "tdc_domestic_bank_only_qoq",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 4.5,
                "se": 1.0,
                "lower95": 2.5,
                "upper95": 6.5,
                "n": 20,
            },
            {
                "treatment_variant": "domestic_bank_only",
                "target": "tdc_domestic_bank_only_qoq",
                "outcome": "deposits_only_bank_qoq",
                "horizon": 0,
                "beta": 3.4,
                "se": 1.0,
                "lower95": 1.4,
                "upper95": 5.4,
                "n": 20,
            },
            {
                "treatment_variant": "domestic_bank_only",
                "target": "tdc_domestic_bank_only_qoq",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -4.5,
                "se": 1.0,
                "lower95": -6.5,
                "upper95": -2.5,
                "n": 20,
            },
            {
                "treatment_variant": "us_chartered_bank_only",
                "target": "tdc_us_chartered_bank_only_qoq",
                "outcome": "tdc_us_chartered_bank_only_qoq",
                "horizon": 0,
                "beta": 8.0,
                "se": 1.0,
                "lower95": 6.0,
                "upper95": 10.0,
                "n": 20,
            },
            {
                "treatment_variant": "us_chartered_bank_only",
                "target": "tdc_us_chartered_bank_only_qoq",
                "outcome": "total_deposits_bank_qoq",
                "horizon": 0,
                "beta": 5.2,
                "se": 1.0,
                "lower95": 3.2,
                "upper95": 7.2,
                "n": 20,
            },
            {
                "treatment_variant": "us_chartered_bank_only",
                "target": "tdc_us_chartered_bank_only_qoq",
                "outcome": "deposits_only_bank_qoq",
                "horizon": 0,
                "beta": 3.8,
                "se": 1.0,
                "lower95": 1.8,
                "upper95": 5.8,
                "n": 20,
            },
            {
                "treatment_variant": "us_chartered_bank_only",
                "target": "tdc_us_chartered_bank_only_qoq",
                "outcome": "other_component_qoq",
                "horizon": 0,
                "beta": -2.8,
                "se": 1.0,
                "lower95": -4.8,
                "upper95": -0.8,
                "n": 20,
            },
        ]
    )
    deposits_only_rows = ladder_rows.copy()
    deposits_only_rows.loc[deposits_only_rows["outcome"] == "total_deposits_bank_qoq", "outcome"] = "deposits_only_bank_qoq"
    deposits_only_rows.loc[
        (deposits_only_rows["treatment_variant"] == "baseline") & (deposits_only_rows["outcome"] == "other_component_qoq"),
        ["beta", "lower95", "upper95"],
    ] = [-7.0, -9.0, -5.0]
    deposits_only_rows.loc[
        (deposits_only_rows["treatment_variant"] == "domestic_bank_only")
        & (deposits_only_rows["outcome"] == "other_component_qoq"),
        ["beta", "lower95", "upper95"],
    ] = [-5.6, -7.6, -3.6]
    deposits_only_rows.loc[
        (deposits_only_rows["treatment_variant"] == "us_chartered_bank_only")
        & (deposits_only_rows["outcome"] == "other_component_qoq"),
        ["beta", "lower95", "upper95"],
    ] = [-4.2, -6.2, -2.2]
    shocked = pd.DataFrame({"quarter": ["2000Q1"], "tdc_bank_only_qoq": [1.0]})
    lp_specs = {
        "specs": {
            "sensitivity": {
                "horizons": [0],
                "cumulative": True,
                "identity_bootstrap_reps": 0,
                "identity_bootstrap_block_length": 4,
            }
        }
    }
    shock_specs = {
        "unexpected_tdc_default": {
            "standardized_column": "tdc_residual_z",
            "target": "tdc_bank_only_qoq",
            "predictors": ["lag_tdc_bank_only_qoq"],
        },
        "unexpected_tdc_domestic_bank_only": {
            "standardized_column": "tdc_domestic_bank_only_residual_z",
            "target": "tdc_domestic_bank_only_qoq",
            "predictors": ["lag_tdc_domestic_bank_only_qoq"],
        },
        "unexpected_tdc_us_chartered_bank_only": {
            "standardized_column": "tdc_us_chartered_bank_only_residual_z",
            "target": "tdc_us_chartered_bank_only_qoq",
            "predictors": ["lag_tdc_us_chartered_bank_only_qoq"],
        },
    }

    def fake_identity_variant_ladder(
        _df: pd.DataFrame,
        *,
        total_outcome_col: str,
        **_kwargs,
    ) -> pd.DataFrame:
        if total_outcome_col == "total_deposits_bank_qoq":
            return ladder_rows.copy()
        if total_outcome_col == "deposits_only_bank_qoq":
            return deposits_only_rows.copy()
        raise AssertionError(f"unexpected total outcome: {total_outcome_col}")

    monkeypatch.setattr(scope_alignment, "build_identity_variant_ladder", fake_identity_variant_ladder)

    summary = scope_alignment.build_scope_alignment_summary(
        shocked=shocked,
        lp_specs=lp_specs,
        shock_specs=shock_specs,
        horizons=(0,),
    )

    assert summary["status"] == "available"
    assert set(summary["deposit_concepts"]) == {
        "total_deposits_including_interbank",
        "deposits_only_ex_interbank",
    }
    total_h0 = summary["deposit_concepts"]["total_deposits_including_interbank"]["key_horizons"]["h0"]
    assert total_h0["variants"]["domestic_bank_only"]["differences_vs_baseline_beta"]["residual_response"] == pytest.approx(1.5)
    assert total_h0["variants"]["us_chartered_bank_only"]["differences_vs_baseline_beta"]["residual_response"] == pytest.approx(3.2)
    deposits_only_h0 = summary["deposit_concepts"]["deposits_only_ex_interbank"]["key_horizons"]["h0"]
    assert deposits_only_h0["variants"]["domestic_bank_only"]["differences_vs_baseline_beta"]["residual_response"] == pytest.approx(1.4)
    assert deposits_only_h0["variants"]["us_chartered_bank_only"]["differences_vs_baseline_beta"]["residual_response"] == pytest.approx(2.8)
    assert summary["recommended_policy"]["headline_outcome"] == "total_deposits_bank_qoq"
    assert summary["recommended_policy"]["preferred_scope_check_variant"] == "us_chartered_bank_only"
    assert summary["recommended_policy"]["secondary_scope_sensitivity_variant"] == "domestic_bank_only"
    assert any("domestic_bank_only" in takeaway for takeaway in summary["takeaways"])
    assert any("U.S.-chartered bank-leg-matched" in takeaway for takeaway in summary["takeaways"])
