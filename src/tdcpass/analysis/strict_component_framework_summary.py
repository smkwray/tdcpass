from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_strict_component_framework_summary(
    *,
    big_picture_synthesis_summary: Mapping[str, Any] | None,
    split_treatment_architecture_summary: Mapping[str, Any] | None,
    core_treatment_promotion_summary: Mapping[str, Any] | None,
    strict_loan_core_redesign_summary: Mapping[str, Any] | None,
    strict_corporate_bridge_secondary_comparison_summary: Mapping[str, Any] | None,
    toc_row_incidence_audit_summary: Mapping[str, Any] | None,
    toc_row_liability_incidence_raw_summary: Mapping[str, Any] | None = None,
    toc_validated_share_candidate_summary: Mapping[str, Any] | None = None,
    strict_direct_core_horizon_stability_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    required = (
        big_picture_synthesis_summary,
        split_treatment_architecture_summary,
        core_treatment_promotion_summary,
        strict_loan_core_redesign_summary,
        strict_corporate_bridge_secondary_comparison_summary,
        toc_row_incidence_audit_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    split_h0 = dict(split_treatment_architecture_summary.get("key_horizons", {}).get("h0", {}) or {})
    support_bundle_beta = _safe_float(split_h0.get("support_bundle_beta"))
    core_residual_beta = _safe_float(
        dict(split_h0.get("core_deposit_proximate_residual_response", {}) or {}).get("beta")
    )

    core_promotion = dict(core_treatment_promotion_summary.get("promotion_recommendation", {}) or {})
    promotion_status = str(core_promotion.get("status", "not_available"))

    core_redesign_recommendation = dict(strict_loan_core_redesign_summary.get("recommendation", {}) or {})
    headline_direct_core = str(
        core_redesign_recommendation.get("release_headline_candidate", "strict_loan_core_min_qoq")
    )

    secondary_recommendation = dict(
        strict_corporate_bridge_secondary_comparison_summary.get("recommendation", {}) or {}
    )
    standard_secondary = str(
        secondary_recommendation.get(
            "standard_secondary_candidate",
            "strict_loan_core_plus_nonfinancial_corporate_qoq",
        )
    )
    narrowing_diagnostic = str(
        secondary_recommendation.get(
            "secondary_comparison_retained_for_diagnostics",
            "strict_loan_core_plus_private_borrower_qoq",
        )
    )
    private_offset_role = str(secondary_recommendation.get("private_offset_role", "diagnostic_only"))
    fit_preferred_secondary = str(
        secondary_recommendation.get("fit_preferred_secondary_candidate", "not_available")
    )

    secondary_h0 = dict(
        strict_corporate_bridge_secondary_comparison_summary.get("key_horizons", {})
        .get("h0", {})
        .get("core_deposit_proximate", {})
        or {}
    )
    headline_direct_core_beta = _safe_float(
        dict(secondary_h0.get("headline_direct_core_response", {}) or {}).get("beta")
    )
    standard_secondary_beta = _safe_float(
        dict(secondary_h0.get("core_plus_nonfinancial_corporate_response", {}) or {}).get("beta")
    )
    narrowing_diagnostic_beta = _safe_float(
        dict(secondary_h0.get("core_plus_private_bridge_response", {}) or {}).get("beta")
    )

    synthesis_interpretation = str(big_picture_synthesis_summary.get("interpretation", "not_available"))
    toc_row_incidence_classification = dict(toc_row_incidence_audit_summary.get("classification", {}) or {})
    toc_row_bundle_role = str(
        toc_row_incidence_classification.get(
            "bundle_role",
            "measured_support_bundle_with_unresolved_strict_deposit_incidence",
        )
    )
    raw_incidence_classification = dict((toc_row_liability_incidence_raw_summary or {}).get("classification", {}) or {})
    raw_incidence_recommendation = dict((toc_row_liability_incidence_raw_summary or {}).get("recommendation", {}) or {})
    raw_decision_gate = str(raw_incidence_classification.get("decision_gate", "not_available"))
    toc_candidate_classification = dict((toc_validated_share_candidate_summary or {}).get("classification", {}) or {})
    toc_candidate_recommendation = dict((toc_validated_share_candidate_summary or {}).get("recommendation", {}) or {})
    toc_candidate_decision = str(toc_candidate_classification.get("decision", "not_available"))
    direct_core_horizon_classification = dict(
        (strict_direct_core_horizon_stability_summary or {}).get("classification", {}) or {}
    )
    direct_core_horizon_recommendation = dict(
        (strict_direct_core_horizon_stability_summary or {}).get("recommendation", {}) or {}
    )
    direct_core_horizon_winners = dict(
        (strict_direct_core_horizon_stability_summary or {}).get("horizon_winners", {}) or {}
    )
    direct_core_horizon_status = str(
        direct_core_horizon_classification.get("recommendation_status", "not_available")
    )
    impact_horizon_candidate = str(
        direct_core_horizon_recommendation.get("impact_candidate", "not_available")
    )
    multihorizon_direct_core = str(
        direct_core_horizon_recommendation.get("multihorizon_candidate", headline_direct_core)
    )

    takeaways: list[str] = [
        "Closure-oriented accounting remains out of the independent-evidence tier; it can still be useful descriptively, but it is frozen as non-evidence for strict deposit-component verification.",
        "The current full TDC stays in the repo as the provisional broad Treasury-attributed object, not as a settled strict deposit component.",
        "TOC/ROW is now frozen as a real measured Treasury support bundle with unresolved strict deposit incidence, not just as vague specification risk.",
        f"The headline strict direct evidence is frozen as `{headline_direct_core}`.",
        f"The standard bridge comparison is frozen as `{standard_secondary}`, while `{narrowing_diagnostic}` becomes the wider diagnostic envelope and the private-offset block stays `{private_offset_role}`.",
    ]
    if support_bundle_beta is not None and core_residual_beta is not None:
        takeaways.append(
            "The split-treatment read stays central to the framing: "
            f"h0 TOC/ROW support bundle ≈ {support_bundle_beta:.2f}, core residual ≈ {core_residual_beta:.2f}."
        )
    if (
        headline_direct_core_beta is not None
        and standard_secondary_beta is not None
        and narrowing_diagnostic_beta is not None
    ):
        takeaways.append(
            "The strict comparison roles are frozen off the direct h0 comparison under the core-deposit-proximate shock: "
            f"headline direct core ≈ {headline_direct_core_beta:.2f}, "
            f"standard bridge comparison ≈ {standard_secondary_beta:.2f}, "
            f"wider diagnostic envelope ≈ {narrowing_diagnostic_beta:.2f}."
        )
    if fit_preferred_secondary and fit_preferred_secondary != "not_available":
        takeaways.append(
            "The role assignment is no longer based on raw h0 fit alone: "
            f"fit-preferred bridge = `{fit_preferred_secondary}`, but the frozen strict-design bridge stays `{standard_secondary}`."
        )
    if direct_core_horizon_status != "not_available":
        takeaways.append(
            "The direct-core headline is now explicitly horizon-aware: "
            f"multihorizon benchmark = `{multihorizon_direct_core}`, "
            f"impact-horizon candidate = `{impact_horizon_candidate}`."
        )
    if direct_core_horizon_winners:
        takeaways.append(
            "Current horizon winners = "
            f"{direct_core_horizon_winners}."
        )
    if promotion_status:
        takeaways.append(
            f"The deposit-proximate core shock remains `{promotion_status}`, so the split architecture is still interpretive rather than promoted as the new headline treatment."
        )
    if raw_decision_gate != "not_available":
        takeaways.append(
            "The raw-units TOC/ROW liability-incidence audit is now the main decision gate: "
            f"decision = `{raw_decision_gate}`."
        )
    if toc_candidate_decision != "not_available":
        takeaways.append(
            "The narrow-TOC candidate gate is now settled too: "
            f"decision = `{toc_candidate_decision}`."
        )

    recommendation_status = toc_candidate_recommendation.get(
        "status",
        raw_incidence_recommendation.get(
            "status",
            "use_frozen_framework_and_run_toc_row_incidence_audit",
        ),
    )
    recommendation_toc_role = toc_candidate_recommendation.get(
        "toc_rule",
        "candidate_not_yet_decided",
    )
    recommendation_next_branch = toc_candidate_recommendation.get(
        "next_branch",
        raw_incidence_recommendation.get(
            "next_branch",
            "run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units",
        ),
    )
    if toc_candidate_decision == "keep_toc_outside_strict_object_under_current_evidence":
        recommendation_status = "strict_release_framing_finalized"
        recommendation_toc_role = "keep_outside_strict_object"
        recommendation_next_branch = "only_reopen_toc_or_row_if_new_incidence_evidence_appears"

    return {
        "status": "available",
        "headline_question": "What is the frozen release-facing framework for interpreting the strict deposit component and the current TDC construction?",
        "estimation_path": {
            "summary_artifact": "strict_component_framework_summary.json",
            "source_artifacts": [
                "big_picture_synthesis_summary.json",
                "split_treatment_architecture_summary.json",
                "core_treatment_promotion_summary.json",
                "strict_loan_core_redesign_summary.json",
                "strict_corporate_bridge_secondary_comparison_summary.json",
                "toc_row_incidence_audit_summary.json",
            ],
        },
        "frozen_roles": {
            "accounting_lane_role": "non_evidence_for_independent_verification",
            "full_tdc_role": "provisional_broad_treasury_attributed_object",
            "toc_row_role": toc_row_bundle_role,
            "toc_narrow_share_role": (
                "not_reincorporated_under_current_evidence"
                if toc_candidate_decision == "keep_toc_outside_strict_object_under_current_evidence"
                else "candidate_not_yet_decided"
            ),
            "core_treatment_role": "interpretive_subobject_not_promoted",
            "headline_direct_core": headline_direct_core,
            "multihorizon_direct_core": multihorizon_direct_core,
            "impact_horizon_candidate": impact_horizon_candidate,
            "standard_secondary_comparison": standard_secondary,
            "narrowing_diagnostic": narrowing_diagnostic,
            "private_offset_role": private_offset_role,
        },
        "h0_snapshot": {
            "toc_row_support_bundle_beta": support_bundle_beta,
            "core_residual_beta": core_residual_beta,
            "headline_direct_core_beta": headline_direct_core_beta,
            "standard_secondary_beta": standard_secondary_beta,
            "narrowing_diagnostic_beta": narrowing_diagnostic_beta,
        },
        "classification": {
            "strict_deposit_component_goal": "mechanical_or_near_mechanical_component_of_deposits",
            "tdc_measurement_status": "mechanically_coherent_but_not_yet_settled_as_strict_component",
            "independent_verification_status": "partial_and_not_fully_validated",
            "project_interpretation": synthesis_interpretation,
            "framework_state": (
                "external_critique_incorporated_and_toc_candidate_gate_built"
                if toc_candidate_decision != "not_available"
                else "external_critique_incorporated_and_raw_incidence_gate_built"
                if raw_decision_gate != "not_available"
                else "external_critique_incorporated_and_frozen"
            ),
            "external_critique_readiness": "critique_incorporated",
            "raw_incidence_decision_gate": raw_decision_gate,
            "toc_narrow_share_decision": toc_candidate_decision,
            "direct_core_horizon_rule": direct_core_horizon_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "headline_tdc_release_role": "keep_full_tdc_provisional_broad_object",
            "headline_strict_release_role": headline_direct_core,
            "multihorizon_direct_core": multihorizon_direct_core,
            "impact_horizon_candidate": impact_horizon_candidate,
            "standard_secondary_release_role": standard_secondary,
            "narrowing_diagnostic_role": narrowing_diagnostic,
            "toc_release_role": recommendation_toc_role,
            "next_branch": recommendation_next_branch,
        },
        "takeaways": takeaways,
    }
