from __future__ import annotations

from typing import Any, Mapping


def build_backend_closeout_summary(
    *,
    decision_bundle: Mapping[str, Any],
    evidence_packet: Mapping[str, Any],
) -> dict[str, Any]:
    recommended_action = str(decision_bundle.get("recommended_action", "unknown"))
    status_board = dict(decision_bundle.get("status_board", {}))
    published_contexts = list(decision_bundle.get("published_contexts", []))

    settled_points = [
        "The backend methods stack is mature enough for a stopping decision.",
        "The repaired frozen treatment passes treatment-quality diagnostics.",
        "Smoothed LPs and the core factor-control path do not overturn the headline full-sample signs.",
    ]
    if published_contexts:
        lead = published_contexts[0]
        settled_points.append(
            f"The strongest published mechanism context is {lead.get('regime')}_{lead.get('state_label')}_h{lead.get('horizon')}."
        )

    unsupported_claims = [
        "The quarterly design is not ready for a clean pass-through versus crowd-out claim.",
        "Direct pass-through or crowd-out ratios remain suppressed or not interpretable.",
        "Full-sample mechanism evidence remains non-decisive even though some published state contexts are sharper.",
    ]

    next_lane_options = [
        "Package the internal backend evidence stack and stop adding quarterly backend estimators.",
        "If work continues, limit it to narrow follow-ups that could change the decision boundary rather than broad spec exploration.",
        "Treat any future work on the headline causal claim as a design-change problem rather than another quarterly backend iteration.",
    ]

    takeaways = [
        f"Recommended action: `{recommended_action}`.",
        "This closeout summary is intended to be the final backend handoff note for the current quarterly iteration cycle.",
    ]
    if str(status_board.get("readiness", "not_ready")) != "ready_for_interpretation":
        takeaways.append("The stop recommendation is driven by a mature methods stack plus persistent claim-level non-readiness.")

    return {
        "status": str(decision_bundle.get("status", "unknown")),
        "recommended_action": recommended_action,
        "headline_question": "What should be treated as settled, unsupported, and next-step options at backend closeout?",
        "evidence_packet_report_path": str(evidence_packet.get("packet_sections", [{}])[0].get("report_path", "")) if evidence_packet.get("packet_sections") else "",
        "status_board": status_board,
        "settled_points": settled_points,
        "unsupported_claims": unsupported_claims,
        "next_lane_options": next_lane_options,
        "takeaways": takeaways,
    }
