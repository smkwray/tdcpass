from __future__ import annotations

import pandas as pd

from tdcpass.analysis.state_proxy_factor_diagnostics import build_state_proxy_factor_diagnostics


def test_state_proxy_factor_diagnostics_finds_supportive_and_contradictory_contexts() -> None:
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "bank_absorption_low", "outcome": "other_component_qoq", "horizon": 0, "beta": -10.0, "se": 1.0, "lower95": -12.0, "upper95": -8.0, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "cb_nonts_qoq", "horizon": 0, "beta": -1.0, "se": 2.0, "lower95": -4.9, "upper95": 2.9, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "foreign_nonts_qoq", "horizon": 0, "beta": -3.0, "se": 1.0, "lower95": -5.0, "upper95": -1.0, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": -2.0, "se": 2.0, "lower95": -5.9, "upper95": 1.9, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": -1.0, "se": 0.2, "lower95": -1.4, "upper95": -0.6, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "other_component_qoq", "horizon": 0, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.0, "upper95": 5.0, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "cb_nonts_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.9, "upper95": 4.9, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "foreign_nonts_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.0, "upper95": 4.0, "n": 20},
            {"regime": "reserve_drain_high", "outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 1.0, "se": 0.2, "lower95": 0.6, "upper95": 1.4, "n": 20},
        ]
    )

    frame, summary = build_state_proxy_factor_diagnostics(
        lp_irf_regimes=lp_irf_regimes,
        regime_diagnostics={
            "regimes": [
                {"regime": "bank_absorption", "stable_for_interpretation": True, "publication_role": "diagnostic_only"},
                {"regime": "reserve_drain", "stable_for_interpretation": True, "publication_role": "published"},
            ]
        },
        horizons=(0,),
    )

    assert not frame.empty
    assert summary["status"] == "published_mixed"
    assert any("bank_absorption_low_h0:asset_side" in item for item in summary["supportive_contexts"])
    assert any("bank_absorption_low_h0:asset_side" in item for item in summary["diagnostic_only_supportive_contexts"])
    assert any("reserve_drain_high_h0:asset_side" in item for item in summary["contradictory_contexts"])
    assert any("reserve_drain_high_h0:asset_side" in item for item in summary["published_contradictory_contexts"])


def test_state_proxy_factor_diagnostics_marks_weak_when_other_not_decisive() -> None:
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "bank_absorption_high", "outcome": "other_component_qoq", "horizon": 0, "beta": 1.0, "se": 2.0, "lower95": -2.9, "upper95": 4.9, "n": 20},
            {"regime": "bank_absorption_high", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 4.0, "se": 1.0, "lower95": 2.0, "upper95": 6.0, "n": 20},
        ]
    )

    _, summary = build_state_proxy_factor_diagnostics(lp_irf_regimes=lp_irf_regimes, horizons=(0,))

    assert summary["status"] == "weak"
    family = summary["regimes"][0]["horizons"]["h0"]["high"]["families"]["asset_side"]
    assert family["family_label"] == "other_component_not_decisive"


def test_state_proxy_factor_diagnostics_distinguishes_diagnostic_only_support() -> None:
    lp_irf_regimes = pd.DataFrame(
        [
            {"regime": "bank_absorption_low", "outcome": "other_component_qoq", "horizon": 0, "beta": -10.0, "se": 1.0, "lower95": -12.0, "upper95": -8.0, "n": 20},
            {"regime": "bank_absorption_low", "outcome": "bank_credit_private_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 20},
        ]
    )

    _, summary = build_state_proxy_factor_diagnostics(
        lp_irf_regimes=lp_irf_regimes,
        regime_diagnostics={
            "regimes": [
                {"regime": "bank_absorption", "stable_for_interpretation": True, "publication_role": "diagnostic_only"},
            ]
        },
        horizons=(0,),
    )

    assert summary["status"] == "diagnostic_only_supportive"
    assert summary["published_supportive_contexts"] == []
    assert any("bank_absorption_low_h0:asset_side" in item for item in summary["diagnostic_only_supportive_contexts"])
