from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tdcpass.analysis.strict_direct_core_component_summary import (
    build_strict_direct_core_component_summary,
)


def test_strict_direct_core_component_summary_can_flag_consumer_credit_review() -> None:
    baseline_lp = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -72.7, "se": 1.0, "lower95": -74.7, "upper95": -70.7, "n": 68},
            {"outcome": "strict_loan_mortgages_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 68},
            {"outcome": "strict_loan_consumer_credit_qoq", "horizon": 0, "beta": -70.0, "se": 1.0, "lower95": -72.0, "upper95": -68.0, "n": 68},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 0, "beta": -72.0, "se": 1.0, "lower95": -74.0, "upper95": -70.0, "n": 68},
        ]
    )
    core_lp = pd.DataFrame(
        [
            {"outcome": "other_component_core_deposit_proximate_bank_only_qoq", "horizon": 0, "beta": -5.5, "se": 1.0, "lower95": -7.5, "upper95": -3.5, "n": 68},
            {"outcome": "strict_loan_mortgages_qoq", "horizon": 0, "beta": -1.2, "se": 1.0, "lower95": -3.2, "upper95": 0.8, "n": 68},
            {"outcome": "strict_loan_consumer_credit_qoq", "horizon": 0, "beta": -5.2, "se": 1.0, "lower95": -7.2, "upper95": -3.2, "n": 68},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 0, "beta": -6.4, "se": 1.0, "lower95": -8.4, "upper95": -4.4, "n": 68},
        ]
    )

    with patch(
        "tdcpass.analysis.strict_direct_core_component_summary.run_local_projections",
        side_effect=[baseline_lp, core_lp],
    ):
        payload = build_strict_direct_core_component_summary(
            shocked=pd.DataFrame({"quarter": ["2020Q1"]}),
            baseline_lp_spec={"controls": [], "horizons": [0], "cumulative": True, "nw_lags": 1},
            baseline_shock_spec={"standardized_column": "tdc_residual_z", "predictors": []},
            core_shock_spec={
                "standardized_column": "tdc_core_deposit_proximate_bank_only_residual_z",
                "predictors": [],
                "name": "unexpected_tdc_core_deposit_proximate_bank_only",
            },
            strict_release_framing_summary={"status": "available"},
            horizons=(0,),
        )

    assert payload["status"] == "available"
    assert payload["classification"]["h0_dominant_component"] == "strict_loan_consumer_credit_qoq"
    assert payload["recommendation"]["status"] == "consumer_credit_only_candidate_deserves_review"
    assert payload["recommendation"]["headline_direct_core"] == "strict_loan_core_min_qoq"
    assert any("consumer credit" in item.lower() for item in payload["takeaways"])
