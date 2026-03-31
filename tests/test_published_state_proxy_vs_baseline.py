from __future__ import annotations

import pandas as pd

from tdcpass.analysis.published_state_proxy_vs_baseline import build_published_state_proxy_vs_baseline_summary


def test_published_state_proxy_vs_baseline_compares_lead_context_to_matching_horizon() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": -2.5, "lower95": -12.5, "upper95": 7.5, "n": 261},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "lower95": -16.6, "upper95": 2.4, "n": 261},
        ]
    )
    summary = build_published_state_proxy_vs_baseline_summary(
        lp_irf=lp_irf,
        published_state_proxy_comparator={
            "primary_contexts": [
                {
                    "regime": "reserve_drain",
                    "state_label": "low",
                    "horizon": 0,
                    "supportive_families": ["funding_side", "asset_side"],
                    "contradictory_families": [],
                    "other_component": {"beta": -32.8, "ci_excludes_zero": True},
                }
            ]
        },
    )

    assert summary["status"] == "context_sharpens_mechanism_only"
    assert summary["baseline"]["horizon"] == 0
    assert summary["baseline"]["other_component"]["sign"] == "negative"
    assert summary["lead_context"]["regime"] == "reserve_drain"


def test_published_state_proxy_vs_baseline_handles_missing_context() -> None:
    summary = build_published_state_proxy_vs_baseline_summary(
        lp_irf=pd.DataFrame(columns=["outcome", "horizon", "beta", "lower95", "upper95", "n"]),
        published_state_proxy_comparator={"primary_contexts": []},
    )

    assert summary["status"] == "no_published_context"
