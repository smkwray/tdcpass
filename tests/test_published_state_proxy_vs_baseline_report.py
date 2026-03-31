from __future__ import annotations

from tdcpass.reports.published_state_proxy_vs_baseline_report import render_published_state_proxy_vs_baseline_report


def test_render_published_state_proxy_vs_baseline_report_includes_lead_context_and_baseline() -> None:
    text = render_published_state_proxy_vs_baseline_report(
        {
            "status": "context_sharpens_mechanism_only",
            "headline_question": "How does the lead published regime-state mechanism context compare with the headline full-sample baseline?",
            "lead_context": {
                "regime": "reserve_drain",
                "state_label": "low",
                "horizon": 0,
                "supportive_families": ["funding_side"],
                "contradictory_families": [],
                "other_component": {"beta": -32.8, "ci_excludes_zero": True},
            },
            "baseline": {
                "horizon": 0,
                "total_deposits": {"beta": -2.5, "sign": "negative", "ci_excludes_zero": False},
                "other_component": {"beta": -7.0, "sign": "negative", "ci_excludes_zero": False},
            },
            "takeaways": ["The published context is informative because grouped proxy families are supportive there even though the full-sample baseline remains non-decisive at the same horizon."],
        }
    )

    assert "# Published Context vs Baseline" in text
    assert "reserve_drain_low_h0" in text
    assert "h0 other_component: beta=-7.0" in text
    assert "full-sample baseline remains non-decisive" in text
