from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tdcpass.analysis.strict_additional_creator_candidate_summary import (
    build_strict_additional_creator_candidate_summary,
)


def test_strict_additional_creator_candidate_summary_rejects_extension_promotion() -> None:
    baseline_lp = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -70.0, "se": 1.0, "lower95": -72.0, "upper95": -68.0, "n": 68},
            {"outcome": "closed_end_residential_loans_qoq", "horizon": 0, "beta": -20.0, "se": 1.0, "lower95": -22.0, "upper95": -18.0, "n": 68},
            {"outcome": "commercial_industrial_loans_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 68},
        ]
    )
    core_lp = pd.DataFrame(
        [
            {"outcome": "other_component_core_deposit_proximate_bank_only_qoq", "horizon": 0, "beta": -5.5, "se": 1.0, "lower95": -7.5, "upper95": -3.5, "n": 68},
            {"outcome": "closed_end_residential_loans_qoq", "horizon": 0, "beta": -5.1, "se": 1.0, "lower95": -7.1, "upper95": -3.1, "n": 68},
            {"outcome": "commercial_industrial_loans_qoq", "horizon": 0, "beta": -0.7, "se": 1.0, "lower95": -2.7, "upper95": 1.3, "n": 68},
        ]
    )

    with patch(
        "tdcpass.analysis.strict_additional_creator_candidate_summary.run_local_projections",
        side_effect=[baseline_lp, core_lp],
    ):
        payload = build_strict_additional_creator_candidate_summary(
            shocked=pd.DataFrame(
                {
                    "quarter": ["2020Q1"],
                    "closed_end_residential_loans_qoq": [0.0],
                    "commercial_industrial_loans_qoq": [0.0],
                }
            ),
            baseline_lp_spec={"controls": [], "horizons": [0], "cumulative": True, "nw_lags": 1},
            baseline_shock_spec={"standardized_column": "tdc_residual_z", "predictors": []},
            core_shock_spec={
                "standardized_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "predictors": [],
                "name": "unexpected_tdc_core_deposit_proximate_bank_only",
            },
            strict_release_framing_summary={"status": "available"},
            strict_direct_core_horizon_stability_summary={
                "status": "available",
                "recommendation": {"impact_candidate": "strict_loan_mortgages_qoq"},
            },
            horizons=(0,),
        )

    assert payload["status"] == "available"
    assert payload["classification"]["h0_best_validation_proxy"] == "closed_end_residential_loans_qoq"
    assert payload["classification"]["h0_best_extension_candidate"] == "commercial_industrial_loans_qoq"
    assert payload["recommendation"]["status"] == "no_additional_extension_candidate_supported"
    assert payload["recommendation"]["impact_horizon_candidate"] == "strict_loan_mortgages_qoq"
