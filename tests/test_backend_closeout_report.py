from __future__ import annotations

from tdcpass.reports.backend_closeout_report import render_backend_closeout_report


def test_render_backend_closeout_report_includes_sections() -> None:
    text = render_backend_closeout_report(
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "What is settled, what remains unsupported, and what follow-up scope is justified by the current backend?",
            "settled_points": ["The strongest published mechanism context is reserve_drain_low_h0."],
            "unsupported_claims": ["The quarterly design is not ready for a clean pass-through versus crowd-out claim."],
            "next_lane_options": ["Package the current backend outputs with the repo's narrow release wording."],
            "takeaways": ["The current release boundary is driven by a mature methods stack plus persistent claim-level non-readiness."],
        }
    )

    assert "# Backend Scope Summary" in text
    assert "Recommended action: `stop_and_package`" in text
    assert "## Settled Points" in text
    assert "reserve_drain_low_h0" in text
    assert "## Unsupported Claims" in text
