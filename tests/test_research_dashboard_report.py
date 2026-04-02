from __future__ import annotations

from tdcpass.reports.research_dashboard_report import render_research_dashboard_report


def test_render_research_dashboard_report_includes_core_sections() -> None:
    dashboard = {
        "status": "not_ready",
        "status_board": {
            "readiness": "not_ready",
            "smoothed_lp": "stable",
            "factor_controls": "core_adequate",
            "proxy_factors": "weak",
        },
        "best_core_factor_variant": "recursive_macro_factors2",
        "key_horizons": {
            "h0": {
                "baseline": {
                    "total_deposits": {"beta": -2.5, "lower95": -12.5, "upper95": 7.5, "n": 261, "sign": "negative"},
                    "other_component": {"beta": -7.0, "lower95": -16.6, "upper95": 2.4, "n": 261, "sign": "negative"},
                },
                "smoothed": {
                    "total_deposits_bank_qoq": {"raw_beta": -2.5, "smoothed_beta": -1.9, "adjustment": 0.6, "raw_sign": "negative", "smoothed_sign": "negative", "n": 261},
                    "other_component_qoq": {"raw_beta": -7.0, "smoothed_beta": -5.9, "adjustment": 1.1, "raw_sign": "negative", "smoothed_sign": "negative", "n": 261},
                },
                "best_core_factor_control": {
                    "factor_variant": "recursive_macro_factors2",
                    "total_deposits": {"beta": -2.4, "lower95": -12.4, "upper95": 7.6, "n": 261, "sign": "negative"},
                    "other_component": {"beta": -7.1, "lower95": -16.7, "upper95": 2.5, "n": 261, "sign": "negative"},
                },
                "state_dependence": {
                    "bank_absorption": {
                        "total_deposits_bank_qoq": {
                            "low": {"beta": -20.0, "sign": "negative", "ci_excludes_zero": True},
                            "high": {"beta": 10.0, "sign": "positive", "ci_excludes_zero": True},
                        }
                    }
                },
                "proxy_families": {
                    "funding_side": {"family_label": "other_component_not_decisive", "normalized_beta_sum": 7.0, "decisive_same_direction_count": 1, "decisive_opposite_direction_count": 0}
                },
                "state_proxy_contexts": [
                    {
                        "regime": "bank_absorption",
                        "publication_role": "diagnostic_only",
                        "stable_for_interpretation": True,
                        "low": {
                            "other_component": {"beta": -17.1, "ci_excludes_zero": True, "n": 130},
                            "families": {
                                "asset_side": {
                                    "family_label": "supportive",
                                    "normalized_beta_sum": 22.8,
                                    "decisive_same_direction_count": 1,
                                    "decisive_opposite_direction_count": 0,
                                }
                            },
                        },
                        "high": {
                            "other_component": {"beta": 4.0, "ci_excludes_zero": False, "n": 131},
                            "families": {
                                "asset_side": {
                                    "family_label": "same_direction_not_decisive",
                                    "normalized_beta_sum": 3.4,
                                    "decisive_same_direction_count": 0,
                                    "decisive_opposite_direction_count": 0,
                                }
                            },
                        },
                    }
                ],
            }
        },
        "takeaways": ["backend extensions are informative"],
        "next_questions": ["What changes after richer controls?"],
    }

    text = render_research_dashboard_report(dashboard)

    assert "# Research Dashboard" in text
    assert "## Status Board" in text
    assert "## H0" in text
    assert "### Baseline" in text
    assert "### Smoothed" in text
    assert "### Best Core Factor Control" in text
    assert "### State Dependence" in text
    assert "### Proxy Families" in text
    assert "### State Proxy Contexts" in text
    assert "regime=bank_absorption" in text
    assert "low asset_side: label=supportive" in text
    assert "backend extensions are informative" in text
    assert "What changes after richer controls?" in text
