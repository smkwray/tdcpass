from __future__ import annotations

from tdcpass.reports.backend_evidence_packet_report import render_backend_evidence_packet_report


def test_render_backend_evidence_packet_report_includes_reading_order() -> None:
    text = render_backend_evidence_packet_report(
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "Which backend artifacts should be used to make the quarterly-method stop/continue decision?",
            "packet_sections": [
                {
                    "label": "Decision Bundle",
                    "purpose": "Start here for the stop/continue call.",
                    "json_path": "/tmp/demo/output/models/backend_decision_bundle_summary.json",
                    "report_path": "/tmp/demo/output/reports/backend_decision_bundle.md",
                }
            ],
            "takeaways": ["The packet is complete enough to support ending the current quarterly backend iteration cycle."],
        }
    )

    assert "# Backend Evidence Packet" in text
    assert "Recommended action: `stop_and_package`" in text
    assert "Decision Bundle: Start here for the stop/continue call." in text
    assert "/tmp/demo/output/models/backend_decision_bundle_summary.json" in text
