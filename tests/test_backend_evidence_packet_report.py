from __future__ import annotations

from tdcpass.reports.backend_evidence_packet_report import render_backend_evidence_packet_report


def test_render_backend_evidence_packet_report_includes_reading_order() -> None:
    text = render_backend_evidence_packet_report(
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "Which backend artifacts define the current release scope?",
            "packet_sections": [
                {
                    "label": "Decision Bundle",
                    "purpose": "Start here for the release-scope summary.",
                    "json_path": "/tmp/demo/output/models/backend_decision_bundle_summary.json",
                    "report_path": "/tmp/demo/output/reports/backend_decision_bundle.md",
                }
            ],
            "takeaways": ["The packet is complete enough to support the current release boundary."],
        }
    )

    assert "# Backend Evidence Packet" in text
    assert "Recommended action: `stop_and_package`" in text
    assert "Decision Bundle: Start here for the release-scope summary." in text
    assert "/tmp/demo/output/models/backend_decision_bundle_summary.json" in text
