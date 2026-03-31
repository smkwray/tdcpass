from __future__ import annotations

from tdcpass.analysis.published_state_proxy_comparator import build_published_state_proxy_comparator


def test_published_state_proxy_comparator_focuses_on_published_stable_contexts() -> None:
    summary = build_published_state_proxy_comparator(
        state_proxy_factor_summary={
            "regimes": [
                {
                    "regime": "bank_absorption",
                    "publication_role": "diagnostic_only",
                    "stable_for_interpretation": True,
                    "horizons": {
                        "h0": {
                            "low": {
                                "other_component": {"beta": -10.0, "ci_excludes_zero": True},
                                "families": {"asset_side": {"family_label": "supportive"}},
                            }
                        }
                    },
                },
                {
                    "regime": "reserve_drain",
                    "publication_role": "published",
                    "stable_for_interpretation": True,
                    "horizons": {
                        "h0": {
                            "low": {
                                "other_component": {"beta": -8.0, "ci_excludes_zero": True},
                                "families": {
                                    "funding_side": {"family_label": "supportive"},
                                    "asset_side": {"family_label": "weak"},
                                },
                            }
                        },
                        "h4": {
                            "high": {
                                "other_component": {"beta": 5.0, "ci_excludes_zero": True},
                                "families": {"funding_side": {"family_label": "opposite_direction"}},
                            }
                        },
                    },
                },
            ]
        },
        horizons=(0, 4),
    )

    assert summary["status"] == "published_mixed_signal"
    assert summary["primary_contexts"][0]["regime"] == "reserve_drain"
    assert summary["key_horizons"]["h0"][0]["supportive_families"] == ["funding_side"]
    assert summary["key_horizons"]["h4"][0]["contradictory_families"] == ["funding_side"]


def test_published_state_proxy_comparator_reports_no_published_signal() -> None:
    summary = build_published_state_proxy_comparator(
        state_proxy_factor_summary={
            "regimes": [
                {
                    "regime": "bank_absorption",
                    "publication_role": "diagnostic_only",
                    "stable_for_interpretation": True,
                    "horizons": {
                        "h0": {
                            "low": {
                                "other_component": {"beta": -10.0, "ci_excludes_zero": True},
                                "families": {"asset_side": {"family_label": "supportive"}},
                            }
                        }
                    },
                }
            ]
        },
        horizons=(0,),
    )

    assert summary["status"] == "no_published_signal"
    assert summary["primary_contexts"] == []
