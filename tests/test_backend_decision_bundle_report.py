from __future__ import annotations

from tdcpass.reports.backend_decision_bundle_report import render_backend_decision_bundle_report


def test_render_backend_decision_bundle_report_includes_action_and_status_board() -> None:
    text = render_backend_decision_bundle_report(
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "What does the current backend evidence stack support in the current release?",
            "status_board": {
                "readiness": "not_ready",
                "methods_stack": "mature",
                "published_contexts": "published_signal",
            },
            "published_contexts": [
                {
                    "regime": "reserve_drain",
                    "state_label": "low",
                    "horizon": 0,
                    "supportive_families": ["funding_side"],
                    "contradictory_families": [],
                    "other_component": {"beta": -32.8, "ci_excludes_zero": True},
                }
            ],
            "takeaways": ["The current backend is complete enough to package with the repo's narrow release wording."],
        }
    )

    assert "# Backend Scope Bundle" in text
    assert "Recommended action: `stop_and_package`" in text
    assert "- methods_stack: `mature`" in text
    assert "reserve_drain_low_h0" in text
    assert "narrow release wording" in text
