from __future__ import annotations

from typing import Any, Mapping


def _methods_stack_status(
    *,
    shock_diagnostics: Mapping[str, Any] | None,
    smoothed_lp_diagnostics: Mapping[str, Any] | None,
    factor_control_diagnostics: Mapping[str, Any] | None,
) -> str:
    treatment_quality = None if shock_diagnostics is None else str(shock_diagnostics.get("treatment_quality_status", "not_evaluated"))
    smoothed = None if smoothed_lp_diagnostics is None else str(smoothed_lp_diagnostics.get("status", "unknown"))
    factor_controls = None if factor_control_diagnostics is None else str(factor_control_diagnostics.get("status", "unknown"))
    if treatment_quality == "pass" and smoothed == "stable" and factor_controls == "core_adequate":
        return "mature"
    return "still_building"


def _recommended_action(
    *,
    readiness_status: str,
    methods_stack_status: str,
    published_context_status: str,
    published_vs_baseline_status: str,
) -> str:
    if readiness_status == "ready_for_interpretation":
        return "ready_for_claim_work"
    if (
        methods_stack_status == "mature"
        and published_context_status in {"published_signal", "published_mixed_signal"}
        and published_vs_baseline_status == "context_sharpens_mechanism_only"
    ):
        return "stop_and_package"
    if methods_stack_status == "mature":
        return "targeted_followup_only"
    return "continue_backend_work"


def build_backend_decision_bundle(
    *,
    readiness: Mapping[str, Any],
    direct_identification: Mapping[str, Any] | None,
    shock_diagnostics: Mapping[str, Any] | None,
    smoothed_lp_diagnostics: Mapping[str, Any] | None,
    factor_control_diagnostics: Mapping[str, Any] | None,
    proxy_factor_summary: Mapping[str, Any] | None,
    state_proxy_factor_summary: Mapping[str, Any] | None,
    published_state_proxy_comparator: Mapping[str, Any] | None,
    published_state_proxy_vs_baseline: Mapping[str, Any] | None,
) -> dict[str, Any]:
    readiness_status = str(readiness.get("status", "not_ready"))
    direct_status = None if direct_identification is None else str(direct_identification.get("status", "not_ready"))
    treatment_quality = None if shock_diagnostics is None else str(shock_diagnostics.get("treatment_quality_status", "not_evaluated"))
    smoothed_status = None if smoothed_lp_diagnostics is None else str(smoothed_lp_diagnostics.get("status", "unknown"))
    factor_status = None if factor_control_diagnostics is None else str(factor_control_diagnostics.get("status", "unknown"))
    proxy_status = None if proxy_factor_summary is None else str(proxy_factor_summary.get("status", "weak"))
    state_proxy_status = None if state_proxy_factor_summary is None else str(state_proxy_factor_summary.get("status", "weak"))
    published_context_status = (
        None if published_state_proxy_comparator is None else str(published_state_proxy_comparator.get("status", "no_published_signal"))
    )
    published_vs_baseline_status = (
        None if published_state_proxy_vs_baseline is None else str(published_state_proxy_vs_baseline.get("status", "no_comparator"))
    )

    methods_stack_status = _methods_stack_status(
        shock_diagnostics=shock_diagnostics,
        smoothed_lp_diagnostics=smoothed_lp_diagnostics,
        factor_control_diagnostics=factor_control_diagnostics,
    )
    recommended_action = _recommended_action(
        readiness_status=readiness_status,
        methods_stack_status=methods_stack_status,
        published_context_status=str(published_context_status),
        published_vs_baseline_status=str(published_vs_baseline_status),
    )

    takeaways = [
        "This bundle synthesizes the backend-only methodological stack into a single stopping-rule artifact.",
    ]
    if methods_stack_status == "mature":
        takeaways.append("The methods stack is mature: treatment quality passes, smoothing is stable, and the core factor-control path is adequate.")
    if readiness_status != "ready_for_interpretation":
        takeaways.append("The quarterly design still fails the main readiness gate for a clean pass-through versus crowd-out claim.")
    if published_context_status in {"published_signal", "published_mixed_signal"}:
        takeaways.append("There is at least one stable published regime-state mechanism context that sharpens the grouped proxy read.")
    if published_vs_baseline_status == "context_sharpens_mechanism_only":
        takeaways.append("The published mechanism context sharpens interpretation relative to the full-sample baseline, but only on the mechanism side.")
    if recommended_action == "stop_and_package":
        takeaways.append("Recommended action: stop adding backend estimators and package the current internal evidence stack.")
    elif recommended_action == "targeted_followup_only":
        takeaways.append("Recommended action: only run narrow targeted follow-ups that could materially change the decision boundary.")
    elif recommended_action == "continue_backend_work":
        takeaways.append("Recommended action: continue backend work because the core methodology stack is not yet stable enough to support a stopping decision.")

    return {
        "status": readiness_status,
        "headline_question": "What does the current backend evidence stack imply about whether to continue or stop quarterly-method work?",
        "status_board": {
            "readiness": readiness_status,
            "direct_identification": direct_status,
            "treatment_quality": treatment_quality,
            "smoothed_lp": smoothed_status,
            "factor_controls": factor_status,
            "proxy_factors": proxy_status,
            "state_proxy_factors": state_proxy_status,
            "published_contexts": published_context_status,
            "published_vs_baseline": published_vs_baseline_status,
            "methods_stack": methods_stack_status,
        },
        "recommended_action": recommended_action,
        "published_contexts": [] if published_state_proxy_comparator is None else list(published_state_proxy_comparator.get("primary_contexts", [])),
        "published_vs_baseline": {} if published_state_proxy_vs_baseline is None else dict(published_state_proxy_vs_baseline),
        "takeaways": takeaways,
    }
