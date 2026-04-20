from __future__ import annotations

import pandas as pd

from tdcpass.analysis.tdcest_broad_treatment_sensitivity_summary import (
    build_tdcest_broad_treatment_sensitivity_summary,
)


def test_tdcest_broad_treatment_sensitivity_summary_reports_variant_moves() -> None:
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
            {"treatment_variant": "tier2_bank_only", "treatment_family": "measurement", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.8, "se": 0.2, "lower95": 0.4, "upper95": 1.2, "n": 40},
            {"treatment_variant": "tier2_bank_only", "treatment_family": "measurement", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.3, "se": 0.2, "lower95": -0.7, "upper95": 0.1, "n": 40},
            {"treatment_variant": "tier3_bank_only", "treatment_family": "measurement", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 0.7, "se": 0.2, "lower95": 0.3, "upper95": 1.1, "n": 40},
            {"treatment_variant": "tier3_bank_only", "treatment_family": "measurement", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.2, "se": 0.2, "lower95": -0.6, "upper95": 0.2, "n": 40},
            {"treatment_variant": "tier3_broad_depository", "treatment_family": "measurement", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.1, "se": 0.2, "lower95": 0.7, "upper95": 1.5, "n": 40},
            {"treatment_variant": "tier3_broad_depository", "treatment_family": "measurement", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.5, "se": 0.2, "lower95": -0.9, "upper95": -0.1, "n": 40},
        ]
    )

    payload = build_tdcest_broad_treatment_sensitivity_summary(sensitivity)

    assert payload["status"] == "available"
    assert payload["classification"]["headline_direction_status"] == "unchanged_across_corrected_broad_variants"
    assert payload["classification"]["strict_framework_effect"] == "unchanged"
    h0 = payload["key_horizons"]["h0"]
    assert h0["baseline"]["total_deposits"]["beta"] == 1.0
    assert abs(h0["variants"]["tier2_bank_only"]["total_deposits_delta_vs_baseline"] - (-0.2)) < 1e-12
    assert (
        payload["recommendation"]["status"]
        == "use_as_broad_object_sensitivity_only"
    )


def test_tdcest_broad_treatment_sensitivity_summary_reports_insufficient_history() -> None:
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
        ]
    )
    shocked = pd.DataFrame(
        {
            "tdc_tier2_bank_only_qoq": [1.0, None, 2.0],
            "tdc_tier2_bank_only_residual_z": [None, None, None],
            "tdc_tier3_bank_only_qoq": [1.0, 2.0, 3.0],
            "tdc_tier3_bank_only_residual_z": [None, None, None],
            "tdc_tier3_broad_depository_qoq": [1.0, 2.0, None],
            "tdc_tier3_broad_depository_residual_z": [None, None, None],
        }
    )

    payload = build_tdcest_broad_treatment_sensitivity_summary(sensitivity, shocked=shocked)

    assert payload["status"] == "insufficient_history"
    assert payload["reason"] == "corrected_tdcest_variants_do_not_clear_current_shock_history_gate"
    assert payload["missing_variants"]["tier2_bank_only"]["target_nonnull_count"] == 2
    assert payload["missing_variants"]["tier2_bank_only"]["usable_shock_count"] == 0
    assert payload["recommendation"]["status"] == "use_tdcest_ladder_as_level_comparison_only"


def test_tdcest_broad_treatment_sensitivity_summary_adds_short_history_exploratory_read() -> None:
    sensitivity = pd.DataFrame(
        [
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"treatment_variant": "baseline", "treatment_family": "headline", "outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.2, "lower95": -0.8, "upper95": 0.0, "n": 40},
        ]
    )
    quarters = [f"202{q//4}Q{(q % 4) + 1}" for q in range(13)]
    shocked = pd.DataFrame(
        {
            "quarter": quarters,
            "tdc_bank_only_qoq": [10, 12, 9, 14, 11, 16, 15, 17, 13, 18, 16, 19, 17],
            "tdc_tier2_bank_only_qoq": [8, 9, 7, 10, 8, 11, 10, 12, 9, 13, 11, 14, 12],
            "tdc_tier3_bank_only_qoq": [8.5, 9.5, 7.5, 10.5, 8.5, 11.5, 10.5, 12.5, 9.5, 13.5, 11.5, 14.5, 12.5],
            "tdc_tier3_broad_depository_qoq": [7.5, 8.5, 6.5, 9.5, 7.5, 10.5, 9.5, 11.5, 8.5, 12.5, 10.5, 13.5, 11.5],
            "total_deposits_bank_qoq": [1, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6],
            "other_component_qoq": [-1, -2, -1, -3, -2, -4, -3, -5, -4, -6, -5, -7, -6],
        }
    )

    payload = build_tdcest_broad_treatment_sensitivity_summary(sensitivity, shocked=shocked)

    assert payload["status"] == "insufficient_history"
    exploratory = payload["exploratory_short_history"]
    assert exploratory["status"] == "available"
    assert exploratory["sample_window"]["n_quarters"] == 13
    assert exploratory["baseline"]["total_deposits"] is not None
    assert "short-history exploratory h0 check" in payload["takeaways"][-1]
