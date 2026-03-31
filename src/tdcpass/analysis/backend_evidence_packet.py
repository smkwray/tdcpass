from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def build_backend_evidence_packet(
    *,
    root: Path,
    backend_decision_bundle: Mapping[str, Any],
    research_dashboard_path: Path,
    research_dashboard_report_path: Path,
    published_state_proxy_comparator_path: Path,
    published_state_proxy_report_path: Path,
    published_state_proxy_vs_baseline_path: Path,
    published_state_proxy_vs_baseline_report_path: Path,
    backend_decision_bundle_path: Path,
    backend_decision_bundle_report_path: Path,
) -> dict[str, Any]:
    recommended_action = str(backend_decision_bundle.get("recommended_action", "unknown"))
    packet_sections = [
        {
            "label": "Decision Bundle",
            "purpose": "Start here for the release-scope summary.",
            "json_path": str(backend_decision_bundle_path),
            "report_path": str(backend_decision_bundle_report_path),
        },
        {
            "label": "Research Dashboard",
            "purpose": "Full backend status board across readiness, controls, smoothing, and state dependence.",
            "json_path": str(research_dashboard_path),
            "report_path": str(research_dashboard_report_path),
        },
        {
            "label": "Published Comparator",
            "purpose": "Stable published regime-state proxy contexts only.",
            "json_path": str(published_state_proxy_comparator_path),
            "report_path": str(published_state_proxy_report_path),
        },
        {
            "label": "Published Context vs Baseline",
            "purpose": "Direct comparison between the lead published mechanism context and the headline full-sample baseline.",
            "json_path": str(published_state_proxy_vs_baseline_path),
            "report_path": str(published_state_proxy_vs_baseline_report_path),
        },
    ]

    takeaways = [
        "This packet packages the backend evidence into a fixed reading order.",
        f"Current recommended action: `{recommended_action}`.",
    ]
    if recommended_action == "stop_and_package":
        takeaways.append("The packet is complete enough to support the current release boundary.")

    return {
        "status": str(backend_decision_bundle.get("status", "unknown")),
        "headline_question": "Which backend artifacts define the current release scope?",
        "root": str(root),
        "recommended_action": recommended_action,
        "packet_sections": packet_sections,
        "takeaways": takeaways,
    }
