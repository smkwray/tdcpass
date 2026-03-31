from __future__ import annotations

from tdcpass.reports.published_state_proxy_report import render_published_state_proxy_report


def test_render_published_state_proxy_report_includes_contexts() -> None:
    text = render_published_state_proxy_report(
        {
            "status": "published_signal",
            "headline_question": "Which published regime-state contexts materially sharpen the grouped mechanism read?",
            "primary_contexts": [
                {
                    "regime": "reserve_drain",
                    "state_label": "low",
                    "horizon": 0,
                    "supportive_families": ["funding_side"],
                    "contradictory_families": [],
                    "other_component": {"beta": -8.0, "ci_excludes_zero": True},
                }
            ],
            "key_horizons": {
                "h0": [
                    {
                        "regime": "reserve_drain",
                        "state_label": "low",
                        "supportive_families": ["funding_side"],
                        "contradictory_families": [],
                        "other_component": {"beta": -8.0, "ci_excludes_zero": True},
                    }
                ]
            },
            "takeaways": ["Published regime-state mechanism evidence is one-sided supportive in the current bundle."],
        }
    )

    assert "# Published State-Proxy Comparator" in text
    assert "Status: `published_signal`" in text
    assert "reserve_drain_low_h0" in text
    assert "supportive=['funding_side']" in text
    assert "Published regime-state mechanism evidence is one-sided supportive" in text
