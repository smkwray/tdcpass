from __future__ import annotations

import pandas as pd

from tdcpass.analysis.deposit_component_scorecard import build_deposit_component_scorecard


def test_deposit_component_scorecard_prefers_exact_baseline_trio_and_lists_components() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "checkable_deposits_bank_qoq", "horizon": 0, "beta": -20.0, "se": 5.0, "lower95": -29.8, "upper95": -10.2, "n": 68},
            {"outcome": "time_savings_deposits_bank_qoq", "horizon": 0, "beta": -5.0, "se": 4.0, "lower95": -12.84, "upper95": 2.84, "n": 68},
            {"outcome": "commercial_industrial_loans_qoq", "horizon": 0, "beta": 18.0, "se": 6.0, "lower95": 6.24, "upper95": 29.76, "n": 68},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 120.0, "se": 30.0, "lower95": 61.2, "upper95": 178.8, "n": 68},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 55.0, "se": 25.0, "lower95": 6.0, "upper95": 104.0, "n": 68},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -65.0, "se": 30.0, "lower95": -123.8, "upper95": -6.2, "n": 68},
        ]
    )

    payload = build_deposit_component_scorecard(
        lp_irf=lp_irf,
        identity_lp_irf=identity_lp,
        proxy_coverage_summary={
            "major_uncovered_channel_families": ["domestic_public_sector_and_wholesale_deposit_channels"],
            "key_horizons": {
                "h0": {
                    "proxy_bundle_beta_sum": 15.0,
                    "unexplained_beta": -80.0,
                    "unexplained_share_of_other_beta": 1.23,
                    "coverage_label": "proxy_bundle_uncovered_remainder_large",
                }
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert payload["key_horizons"]["h0"]["tdc"]["beta"] == 120.0
    assert payload["key_horizons"]["h0"]["z1_deposit_components"]["checkable_deposits_bank_qoq"]["beta"] == -20.0
    assert payload["key_horizons"]["h0"]["creator_lending_channels"]["commercial_industrial_loans_qoq"]["beta"] == 18.0
    assert payload["key_horizons"]["h0"]["proxy_coverage_label"] == "proxy_bundle_uncovered_remainder_large"
    assert payload["component_outcomes_present"] == [
        "checkable_deposits_bank_qoq",
        "time_savings_deposits_bank_qoq",
    ]
    assert payload["creator_channel_outcomes_present"] == [
        "commercial_industrial_loans_qoq",
    ]
