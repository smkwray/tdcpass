from __future__ import annotations

import pandas as pd

from tdcpass.analysis.proxy_factor_diagnostics import build_proxy_factor_diagnostics


def test_proxy_factor_diagnostics_groups_families_and_normalizes_direction() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -10.0, "se": 1.0, "lower95": -12.0, "upper95": -8.0, "n": 40},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 40},
            {"outcome": "cb_nonts_qoq", "horizon": 0, "beta": -1.0, "se": 2.0, "lower95": -4.9, "upper95": 2.9, "n": 40},
            {"outcome": "foreign_nonts_qoq", "horizon": 0, "beta": -3.0, "se": 1.0, "lower95": -5.0, "upper95": -1.0, "n": 40},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": -2.0, "se": 2.0, "lower95": -5.9, "upper95": 1.9, "n": 40},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 36},
            {"outcome": "bank_credit_private_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 36},
            {"outcome": "cb_nonts_qoq", "horizon": 4, "beta": -3.0, "se": 1.0, "lower95": -5.0, "upper95": -1.0, "n": 36},
            {"outcome": "foreign_nonts_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 36},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 4, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 36},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 4, "beta": 2.0, "se": 0.2, "lower95": 1.6, "upper95": 2.4, "n": 36},
        ]
    )

    frame, summary = build_proxy_factor_diagnostics(lp_irf=lp_irf, horizons=(0, 4))

    assert not frame.empty
    assert set(frame["family"]) == {"funding_side", "asset_side"}
    assert summary["status"] == "mixed"
    assert summary["key_horizons"]["h0"]["families"]["asset_side"]["family_label"] == "supportive"
    assert summary["key_horizons"]["h0"]["families"]["funding_side"]["family_label"] == "opposite_direction"
    assert summary["key_horizons"]["h4"]["families"]["asset_side"]["family_label"] == "opposite_direction"


def test_proxy_factor_diagnostics_handles_weak_other_component() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.9, "upper95": 4.9, "n": 30},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.1, "upper95": 3.9, "n": 30},
        ]
    )

    _, summary = build_proxy_factor_diagnostics(lp_irf=lp_irf, horizons=(0,))

    assert summary["status"] == "weak"
    assert summary["key_horizons"]["h0"]["families"]["asset_side"]["family_label"] == "other_component_not_decisive"
