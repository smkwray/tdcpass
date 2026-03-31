from __future__ import annotations

import pandas as pd

from tdcpass.analysis.research_dashboard import build_research_dashboard_summary


def test_research_dashboard_combines_backend_extensions() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -2.5, "lower95": -12.5, "upper95": 7.5, "n": 261},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "lower95": -16.6, "upper95": 2.4, "n": 261},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 13.5, "lower95": -0.6, "upper95": 27.7, "n": 257},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 10.0, "lower95": -3.4, "upper95": 23.5, "n": 257},
        ]
    )
    lp_irf_state_dependence = pd.DataFrame(
        [
            {"state_variant": "bank_absorption", "state_label": "low", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -20.0, "lower95": -38.0, "upper95": -2.0, "n": 261},
            {"state_variant": "bank_absorption", "state_label": "high", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 10.0, "lower95": 4.0, "upper95": 16.0, "n": 261},
            {"state_variant": "bank_absorption", "state_label": "low", "outcome": "other_component_qoq", "horizon": 0, "beta": -22.0, "lower95": -40.0, "upper95": -4.0, "n": 261},
            {"state_variant": "bank_absorption", "state_label": "high", "outcome": "other_component_qoq", "horizon": 0, "beta": 4.0, "lower95": -1.0, "upper95": 9.0, "n": 261},
            {"state_variant": "reserve_drain", "state_label": "low", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": -13.0, "lower95": -48.0, "upper95": 22.0, "n": 47},
            {"state_variant": "reserve_drain", "state_label": "high", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": -44.0, "lower95": -118.0, "upper95": 29.0, "n": 47},
            {"state_variant": "reserve_drain", "state_label": "low", "outcome": "other_component_qoq", "horizon": 4, "beta": -17.0, "lower95": -51.0, "upper95": 17.0, "n": 47},
            {"state_variant": "reserve_drain", "state_label": "high", "outcome": "other_component_qoq", "horizon": 4, "beta": -49.0, "lower95": -122.0, "upper95": 24.0, "n": 47},
        ]
    )
    factor_control_sensitivity = pd.DataFrame(
        [
            {"factor_variant": "recursive_macro_factors2", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -2.4, "lower95": -12.4, "upper95": 7.6, "n": 261},
            {"factor_variant": "recursive_macro_factors2", "outcome": "other_component_qoq", "horizon": 0, "beta": -7.1, "lower95": -16.7, "upper95": 2.5, "n": 261},
            {"factor_variant": "recursive_macro_factors2", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 11.7, "lower95": -3.5, "upper95": 27.0, "n": 257},
            {"factor_variant": "recursive_macro_factors2", "outcome": "other_component_qoq", "horizon": 4, "beta": 8.3, "lower95": -6.2, "upper95": 22.8, "n": 257},
        ]
    )

    payload = build_research_dashboard_summary(
        readiness={"status": "not_ready"},
        direct_identification={"status": "not_ready"},
        shock_diagnostics={"treatment_quality_status": "pass"},
        lp_irf=lp_irf,
        smoothed_lp_diagnostics={
            "status": "stable",
            "key_horizons": {
                "h0": {
                    "total_deposits_bank_qoq": {"raw_beta": -2.5, "smoothed_beta": -1.9},
                    "other_component_qoq": {"raw_beta": -7.0, "smoothed_beta": -5.9},
                },
                "h4": {
                    "total_deposits_bank_qoq": {"raw_beta": 13.5, "smoothed_beta": 13.7},
                    "other_component_qoq": {"raw_beta": 10.0, "smoothed_beta": 11.1},
                },
            },
        },
        lp_irf_state_dependence=lp_irf_state_dependence,
        factor_control_sensitivity=factor_control_sensitivity,
        factor_control_diagnostics={
            "status": "core_adequate",
            "factor_variants": [
                {
                    "factor_variant": "recursive_macro_factors2",
                    "factor_role": "core",
                    "min_key_horizon_n_ratio": 1.0,
                }
            ],
        },
        proxy_factor_summary={
            "status": "weak",
            "key_horizons": {
                "h0": {"families": {"funding_side": {"family_label": "other_component_not_decisive"}}},
                "h4": {"families": {"asset_side": {"family_label": "other_component_not_decisive"}}},
            },
        },
        state_proxy_factor_summary={
            "status": "published_supportive",
            "regimes": [
                {
                    "regime": "bank_absorption",
                    "stable_for_interpretation": True,
                    "publication_role": "diagnostic_only",
                    "horizons": {
                        "h0": {
                            "high": {"families": {"asset_side": {"family_label": "same_direction_not_decisive"}}},
                            "low": {"families": {"asset_side": {"family_label": "supportive"}}},
                        },
                        "h4": {
                            "high": {"families": {"funding_side": {"family_label": "opposite_direction"}}},
                            "low": {"families": {"funding_side": {"family_label": "weak"}}},
                        },
                    },
                }
            ],
            "published_supportive_contexts": ["reserve_drain_low_h0:funding_side"],
            "diagnostic_only_supportive_contexts": ["bank_absorption_low_h0:asset_side"],
        },
    )

    assert payload["status"] == "not_ready"
    assert payload["status_board"]["smoothed_lp"] == "stable"
    assert payload["status_board"]["factor_controls"] == "core_adequate"
    assert payload["status_board"]["state_proxy_factors"] == "published_supportive"
    assert payload["best_core_factor_variant"] == "recursive_macro_factors2"
    assert payload["key_horizons"]["h0"]["baseline"]["total_deposits"]["sign"] == "negative"
    assert payload["key_horizons"]["h4"]["best_core_factor_control"]["factor_variant"] == "recursive_macro_factors2"
    assert payload["key_horizons"]["h0"]["state_dependence"]["bank_absorption"]["total_deposits_bank_qoq"]["high"]["sign"] == "positive"
    assert payload["key_horizons"]["h0"]["proxy_families"]["funding_side"]["family_label"] == "other_component_not_decisive"
    assert payload["key_horizons"]["h0"]["state_proxy_contexts"][0]["regime"] == "bank_absorption"
    assert any("published regime-state contexts" in item for item in payload["takeaways"])
    assert any("backend extensions" in item for item in payload["takeaways"])
