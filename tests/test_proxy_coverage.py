from __future__ import annotations

import pandas as pd

from tdcpass.analysis.proxy_coverage import build_proxy_coverage_summary


def test_proxy_coverage_summary_quantifies_baseline_and_published_regime_gaps() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 10.0, "se": 1.0, "lower95": 8.0, "upper95": 12.0, "n": 40},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 4.0, "se": 2.0, "lower95": 0.1, "upper95": 7.9, "n": 40},
            {"outcome": "cb_nonts_qoq", "horizon": 0, "beta": 3.0, "se": 2.0, "lower95": -0.9, "upper95": 6.9, "n": 40},
            {"outcome": "foreign_nonts_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.9, "upper95": 4.9, "n": 40},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": 0.5, "se": 2.0, "lower95": -3.4, "upper95": 4.4, "n": 40},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 0.5, "se": 2.0, "lower95": -3.4, "upper95": 4.4, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 40.0, "se": 5.0, "lower95": 30.0, "upper95": 50.0, "n": 36},
            {"outcome": "bank_credit_private_qoq", "horizon": 4, "beta": 2.0, "se": 2.0, "lower95": -1.9, "upper95": 5.9, "n": 36},
            {"outcome": "cb_nonts_qoq", "horizon": 4, "beta": -1.0, "se": 2.0, "lower95": -4.9, "upper95": 2.9, "n": 36},
            {"outcome": "foreign_nonts_qoq", "horizon": 4, "beta": 0.0, "se": 2.0, "lower95": -3.9, "upper95": 3.9, "n": 36},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 4, "beta": 1.0, "se": 2.0, "lower95": -2.9, "upper95": 4.9, "n": 36},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 4, "beta": 0.0, "se": 2.0, "lower95": -3.9, "upper95": 3.9, "n": 36},
        ]
    )
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "reserve_drain_high", "outcome": "other_component_qoq", "horizon": 0, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 4.5, "se": 1.0, "lower95": 2.5, "upper95": 6.5, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "cb_nonts_qoq", "horizon": 0, "beta": 2.5, "se": 1.0, "lower95": 0.5, "upper95": 4.5, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "foreign_nonts_qoq", "horizon": 0, "beta": 0.8, "se": 1.0, "lower95": -1.2, "upper95": 2.8, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "other_component_qoq", "horizon": 0, "beta": 4.0, "se": 1.0, "lower95": 2.0, "upper95": 6.0, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "cb_nonts_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "foreign_nonts_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
            {"regime": "reserve_drain_low", "outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 0.1, "se": 1.0, "lower95": -1.9, "upper95": 2.1, "n": 20},
        ]
    )

    payload = build_proxy_coverage_summary(
        lp_irf=lp_irf,
        lp_irf_regimes=lp_irf_regimes,
        regime_diagnostics={
            "regimes": [
                {
                    "regime": "reserve_drain",
                    "publication_role": "published",
                    "stable_for_interpretation": True,
                    "stability_warnings": [],
                }
            ]
        },
        proxy_unit_audit={
            "derived_proxies": [
                {"proxy_outcome": "bank_credit_private_qoq", "start_quarter": "2009Q4", "non_missing_obs": 65}
            ]
        },
        horizons=(0, 4),
    )

    assert payload["status"] == "mixed"
    assert payload["key_horizons"]["h0"]["coverage_label"] == "proxy_bundle_mostly_covers_other"
    assert payload["key_horizons"]["h4"]["coverage_label"] == "proxy_bundle_uncovered_remainder_large"
    assert payload["key_horizons"]["h4"]["unexplained_share_of_other_beta"] > 0.9
    assert payload["published_regime_contexts"][0]["regime"] == "reserve_drain"
    assert payload["published_regime_contexts"][0]["horizons"]["h0"]["high"]["coverage_label"] == "proxy_bundle_mostly_covers_other"
    assert payload["history_limits"][0]["proxy_outcome"] == "bank_credit_private_qoq"
    assert "does not exhaust" in payload["release_caveat"]
