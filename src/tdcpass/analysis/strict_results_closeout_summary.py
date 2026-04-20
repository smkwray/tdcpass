from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_strict_results_closeout_summary(
    *,
    strict_release_framing_summary: Mapping[str, Any] | None,
    strict_component_framework_summary: Mapping[str, Any] | None,
    strict_di_loans_nec_measurement_audit_summary: Mapping[str, Any] | None,
    strict_additional_creator_candidate_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    required = (
        strict_release_framing_summary,
        strict_component_framework_summary,
        strict_di_loans_nec_measurement_audit_summary,
        strict_additional_creator_candidate_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    release_position = dict(strict_release_framing_summary.get("release_position", {}) or {})
    framework_roles = dict(strict_component_framework_summary.get("frozen_roles", {}) or {})
    framework_h0 = dict(strict_component_framework_summary.get("h0_snapshot", {}) or {})
    di_classification = dict(strict_di_loans_nec_measurement_audit_summary.get("classification", {}) or {})
    di_recommendation = dict(strict_di_loans_nec_measurement_audit_summary.get("recommendation", {}) or {})
    creator_classification = dict(strict_additional_creator_candidate_summary.get("classification", {}) or {})
    creator_recommendation = dict(strict_additional_creator_candidate_summary.get("recommendation", {}) or {})

    headline_direct = str(release_position.get("headline_direct_benchmark", "strict_loan_core_min_qoq"))
    standard_bridge = str(release_position.get("standard_bridge_comparison", "strict_loan_core_plus_nonfinancial_corporate_qoq"))
    impact_candidate = str(release_position.get("impact_horizon_candidate", "strict_loan_mortgages_qoq"))

    support_bundle_beta = _safe_float(framework_h0.get("toc_row_support_bundle_beta"))
    core_residual_beta = _safe_float(framework_h0.get("core_residual_beta"))
    headline_direct_beta = _safe_float(framework_h0.get("headline_direct_core_beta"))
    standard_bridge_beta = _safe_float(framework_h0.get("standard_secondary_beta"))

    settled_findings = [
        "Full TDC remains the broad Treasury-attributed object; it is not currently validated as the strict deposit component.",
        "The strict object excludes both TOC and ROW under current evidence.",
        f"The multihorizon direct strict benchmark is `{headline_direct}`.",
        f"The impact-horizon candidate is `{impact_candidate}`.",
        f"The standard narrow bridge comparison is `{standard_bridge}`.",
        "Closure-style accounting is descriptive only and does not count as independent verification.",
    ]
    if creator_recommendation:
        settled_findings.append(
            "The additional creator-channel search is closed under current evidence: "
            f"`{str(creator_recommendation.get('status', 'no_additional_extension_candidate_supported'))}`."
        )
    if di_recommendation:
        settled_findings.append(
            "The final DI-loans-n.e.c. audit is also closed under current public data: "
            f"`{str(di_recommendation.get('status', 'no_promotable_same_scope_transaction_subcomponent_supported'))}`."
        )

    unresolved_questions = [
        "Whether any future scope- and timing-matched incidence evidence validates a narrow in-scope TOC share.",
        "Whether a new public same-scope transaction split ever becomes available inside `strict_loan_di_loans_nec_qoq`.",
        "Whether any genuinely new same-scope mechanical deposit-creation channel appears beyond the current direct core.",
    ]

    takeaways = [
        "This is the final closeout surface for the strict branch: it summarizes what is settled, what remains unresolved, and whether further empirical expansion is still justified.",
        "The repo now has a frozen broad-vs-strict split rather than an open-ended TDC redesign loop.",
        "The strict side is intentionally narrow: direct core first, bridge second, support bundle outside.",
    ]
    if None not in (support_bundle_beta, core_residual_beta, headline_direct_beta, standard_bridge_beta):
        takeaways.append(
            "Current h0 snapshot: "
            f"TOC/ROW support bundle ≈ {support_bundle_beta:.2f}, "
            f"core residual ≈ {core_residual_beta:.2f}, "
            f"headline direct core ≈ {headline_direct_beta:.2f}, "
            f"standard bridge comparison ≈ {standard_bridge_beta:.2f}."
        )
    if di_classification:
        takeaways.append(
            "The unresolved DI bucket still does not yield a promotable same-scope transaction split: "
            f"best cross-scope bridge = `{str(di_classification.get('h0_best_cross_scope_transaction_bridge', 'not_available'))}`, "
            f"best same-scope proxy = `{str(di_classification.get('h0_best_same_scope_proxy', 'not_available'))}`."
        )
    if creator_classification:
        takeaways.append(
            "The remaining creator-channel search is also exhausted for now: "
            f"best extension candidate = `{str(creator_classification.get('h0_best_extension_candidate', 'not_available'))}`, "
            "but no additional extension candidate is supported."
        )

    return {
        "status": "available",
        "headline_question": "What is the final closeout position on the strict deposit-component branch after the frozen framework and last measurement audits?",
        "estimation_path": {
            "summary_artifact": "strict_results_closeout_summary.json",
            "source_artifacts": [
                "strict_release_framing_summary.json",
                "strict_component_framework_summary.json",
                "strict_di_loans_nec_measurement_audit_summary.json",
                "strict_additional_creator_candidate_summary.json",
            ],
        },
        "release_position": {
            "full_tdc_role": str(release_position.get("full_tdc_release_role", "broad_treasury_attributed_object_only")),
            "strict_object_rule": str(release_position.get("strict_object_rule", "exclude_toc_and_row_under_current_evidence")),
            "headline_direct_benchmark": headline_direct,
            "impact_horizon_candidate": impact_candidate,
            "standard_bridge_comparison": standard_bridge,
            "diagnostic_envelope": str(release_position.get("diagnostic_envelope", framework_roles.get("narrowing_diagnostic", "strict_loan_core_plus_private_borrower_qoq"))),
        },
        "settled_findings": settled_findings,
        "evidence_tiers": dict(strict_release_framing_summary.get("evidence_tiers", {}) or {}),
        "unresolved_questions": unresolved_questions,
        "h0_snapshot": {
            "toc_row_support_bundle_beta": support_bundle_beta,
            "core_residual_beta": core_residual_beta,
            "headline_direct_core_beta": headline_direct_beta,
            "standard_bridge_beta": standard_bridge_beta,
        },
        "classification": {
            "branch_state": "strict_empirical_expansion_effectively_complete",
            "di_bucket_state": str(di_classification.get("same_scope_transaction_subcomponent_status", "not_available")),
            "creator_extension_state": str(creator_recommendation.get("status", "not_available")),
            "closeout_readiness": "writeup_ready_under_current_evidence",
        },
        "recommendation": {
            "status": "move_to_writeup_and_results_packaging",
            "reopen_rule": "only_reopen_on_new_same_scope_transaction_or_incidence_evidence",
            "next_branch": "writeup_results_and_release_packaging",
        },
        "takeaways": takeaways,
    }
