from __future__ import annotations

import pandas as pd

from tdcpass.analysis.direct_identification import (
    build_direct_identification_summary,
    build_total_minus_other_contrast,
)


def test_total_minus_other_contrast_tracks_direct_tdc_response() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 1.4, "se": 0.4, "lower95": 0.62, "upper95": 2.18, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.0, "se": 0.5, "lower95": 0.02, "upper95": 1.98, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -0.4, "se": 0.5, "lower95": -1.38, "upper95": 0.58, "n": 40},
        ]
    )

    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )

    row = contrast.iloc[0].to_dict()
    assert row["scope"] == "baseline"
    assert row["beta_implied"] == 1.4
    assert row["beta_direct"] == 1.4
    assert row["gap_implied_minus_direct"] == 0.0
    assert row["contrast_consistent"] is True


def test_direct_identification_summary_marks_weak_first_stage_as_not_ready() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 0.03, "se": 0.04, "lower95": -0.05, "upper95": 0.11, "n": 40},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.2, "se": 1.0, "lower95": -0.76, "upper95": 3.16, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 1.1, "se": 1.0, "lower95": -0.86, "upper95": 3.06, "n": 40},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 0.02, "se": 0.05, "lower95": -0.08, "upper95": 0.12, "n": 36},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 2.0, "se": 2.0, "lower95": -1.92, "upper95": 5.92, "n": 36},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 2.1, "se": 2.0, "lower95": -1.82, "upper95": 6.02, "n": 36},
        ]
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_irf,
        sensitivity=pd.DataFrame(),
        control_sensitivity=pd.DataFrame(),
        sample_sensitivity=pd.DataFrame(),
    )
    sample_sensitivity = pd.DataFrame(
        [
            {"sample_variant": "all_usable_shocks", "sample_role": "headline", "sample_filter": "all_usable_shocks", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 1.2, "se": 1.0, "lower95": -0.76, "upper95": 3.16, "n": 40},
            {"sample_variant": "drop_flagged_shocks", "sample_role": "exploratory", "sample_filter": "shock_flag==''", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 4.0, "se": 2.0, "lower95": 0.08, "upper95": 7.92, "n": 38},
        ]
    )

    payload = build_direct_identification_summary(
        lp_irf=lp_irf,
        contrast=contrast,
        sample_sensitivity=sample_sensitivity,
    )

    assert payload["status"] == "not_ready"
    assert payload["first_stage_checks"]["tdc_ci_excludes_zero_at_h0_or_h4"] is False
    assert payload["sample_fragility"]["impact_magnitude_shift_gt_100pct"] is True
    assert any("move TDC itself" in item for item in payload["reasons"])
