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
        "Headline pass-through or crowd-out ratios are out of scope in the current release.",
        "Full-sample mechanism evidence remains non-decisive even though some published state contexts are sharper.",
    ]

    next_lane_options = [
        "Package the current backend outputs with the repo's narrow release wording.",
        "If work continues, limit it to narrow follow-ups that could change the supported claim boundary rather than broad spec exploration.",
        "Treat any future work on the headline causal claim as a design-change problem rather than another round of backend specification churn.",
    ]

    takeaways = [
        f"Recommended action: `{recommended_action}`.",
        "This summary records the current backend claim boundary for the release.",
    ]
    if str(status_board.get("readiness", "not_ready")) != "ready_for_interpretation":
        takeaways.append("The current release boundary is driven by a mature methods stack plus persistent claim-level non-readiness.")
    if recommended_action == "targeted_followup_only":
        takeaways.append("Broad backend specification expansion is not warranted; only targeted follow-up remains justified.")

    return {
        "status": str(decision_bundle.get("status", "unknown")),
        "recommended_action": recommended_action,
        "headline_question": "What is settled, what remains unsupported, and what follow-up scope is justified by the current backend?",
        "evidence_packet_report_path": str(evidence_packet.get("packet_sections", [{}])[0].get("report_path", "")) if evidence_packet.get("packet_sections") else "",
        "status_board": status_board,
        "settled_points": settled_points,
        "unsupported_claims": unsupported_claims,
        "next_lane_options": next_lane_options,
        "takeaways": takeaways,
    }
