from __future__ import annotations

from pathlib import Path

from tdcpass.analysis.backend_evidence_packet import build_backend_evidence_packet


def test_backend_evidence_packet_builds_reading_order() -> None:
    summary = build_backend_evidence_packet(
        root=Path("/tmp/demo"),
        backend_decision_bundle={"status": "not_ready", "recommended_action": "stop_and_package"},
        research_dashboard_path=Path("/tmp/demo/output/models/research_dashboard_summary.json"),
        research_dashboard_report_path=Path("/tmp/demo/output/reports/research_dashboard.md"),
        published_state_proxy_comparator_path=Path("/tmp/demo/output/models/published_state_proxy_comparator_summary.json"),
        published_state_proxy_report_path=Path("/tmp/demo/output/reports/published_state_proxy_comparator.md"),
        published_state_proxy_vs_baseline_path=Path("/tmp/demo/output/models/published_state_proxy_vs_baseline_summary.json"),
        published_state_proxy_vs_baseline_report_path=Path("/tmp/demo/output/reports/published_state_proxy_vs_baseline.md"),
        backend_decision_bundle_path=Path("/tmp/demo/output/models/backend_decision_bundle_summary.json"),
        backend_decision_bundle_report_path=Path("/tmp/demo/output/reports/backend_decision_bundle.md"),
    )

    assert summary["recommended_action"] == "stop_and_package"
    assert summary["packet_sections"][0]["label"] == "Decision Bundle"
    assert summary["packet_sections"][1]["label"] == "Research Dashboard"
