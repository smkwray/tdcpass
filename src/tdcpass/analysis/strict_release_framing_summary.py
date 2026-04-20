from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_strict_release_framing_summary(
    *,
    strict_component_framework_summary: Mapping[str, Any] | None,
    toc_row_liability_incidence_raw_summary: Mapping[str, Any] | None,
    toc_validated_share_candidate_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    required = (
        strict_component_framework_summary,
        toc_row_liability_incidence_raw_summary,
        toc_validated_share_candidate_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    framework_roles = dict(strict_component_framework_summary.get("frozen_roles", {}) or {})
    framework_h0 = dict(strict_component_framework_summary.get("h0_snapshot", {}) or {})
    raw_classification = dict(toc_row_liability_incidence_raw_summary.get("classification", {}) or {})
    raw_h0 = dict(toc_row_liability_incidence_raw_summary.get("key_horizons", {}).get("h0", {}) or {})
    toc_candidate_classification = dict(toc_validated_share_candidate_summary.get("classification", {}) or {})
    toc_candidate_h0 = dict(toc_validated_share_candidate_summary.get("key_horizons", {}).get("h0", {}) or {})
    best_candidate = dict(toc_candidate_h0.get("best_candidate", {}) or {})

    toc_leg = dict(raw_h0.get("toc_leg", {}) or {})
    row_leg = dict(raw_h0.get("row_leg", {}) or {})
    toc_shares = dict(toc_leg.get("counterpart_share_of_leg_beta", {}) or {})
    row_shares = dict(row_leg.get("counterpart_share_of_leg_beta", {}) or {})

    support_bundle_beta = _safe_float(framework_h0.get("toc_row_support_bundle_beta"))
    core_residual_beta = _safe_float(framework_h0.get("core_residual_beta"))
    headline_direct_core_beta = _safe_float(framework_h0.get("headline_direct_core_beta"))
    standard_bridge_beta = _safe_float(framework_h0.get("standard_secondary_beta"))
    toc_deposits_only_share = _safe_float(toc_shares.get("deposits_only_bank_qoq"))
    toc_reserve_share = _safe_float(toc_shares.get("reserves_qoq"))
    row_checkable_share = _safe_float(row_shares.get("checkable_rest_of_world_bank_qoq"))
    row_external_share = _safe_float(row_shares.get("foreign_nonts_qoq"))
    best_candidate_abs_gap = _safe_float(best_candidate.get("abs_gap_to_direct_core"))
    best_candidate_implied_residual = _safe_float(best_candidate.get("implied_residual_beta"))

    headline_direct_core = str(
        framework_roles.get("headline_direct_core", "strict_loan_core_min_qoq")
    )
    standard_bridge = str(
        framework_roles.get(
            "standard_secondary_comparison",
            "strict_loan_core_plus_nonfinancial_corporate_qoq",
        )
    )
    multihorizon_direct_core = str(
        framework_roles.get("multihorizon_direct_core", headline_direct_core)
    )
    impact_horizon_candidate = str(
        framework_roles.get("impact_horizon_candidate", "not_available")
    )
    diagnostic_envelope = str(
        framework_roles.get(
            "narrowing_diagnostic",
            "strict_loan_core_plus_private_borrower_qoq",
        )
    )
    raw_gate = str(raw_classification.get("decision_gate", "not_available"))
    toc_decision = str(toc_candidate_classification.get("decision", "not_available"))

    takeaways = [
        "This summary freezes the release-facing rule after the incidence gates, not just the intermediate diagnostics.",
        "Full TDC remains in the repo as the broad Treasury-attributed object, but TOC and ROW stay outside the strict object under current evidence.",
        f"The strict evidence hierarchy is now fixed around `{headline_direct_core}` as the headline direct benchmark and `{standard_bridge}` as the standard bridge comparison; `{diagnostic_envelope}` stays diagnostic.",
    ]
    if impact_horizon_candidate != "not_available":
        takeaways.append(
            "The direct-core rule is horizon-specific rather than one-size-fits-all: "
            f"multihorizon benchmark = `{multihorizon_direct_core}`, "
            f"impact-horizon candidate = `{impact_horizon_candidate}`."
        )
    if None not in (support_bundle_beta, core_residual_beta, headline_direct_core_beta, standard_bridge_beta):
        takeaways.append(
            "The h0 release snapshot is now explicit: "
            f"TOC/ROW support bundle ≈ {support_bundle_beta:.2f}, "
            f"core residual ≈ {core_residual_beta:.2f}, "
            f"headline direct core ≈ {headline_direct_core_beta:.2f}, "
            f"standard bridge comparison ≈ {standard_bridge_beta:.2f}."
        )
    if None not in (toc_deposits_only_share, toc_reserve_share, row_checkable_share, row_external_share):
        takeaways.append(
            "The raw incidence gate is what keeps the support bundle outside the strict object: "
            f"TOC deposits-only share ≈ {toc_deposits_only_share:.2f} versus reserves share ≈ {toc_reserve_share:.2f}, "
            f"ROW-checkable share ≈ {row_checkable_share:.2f} versus foreign-NONTS share ≈ {row_external_share:.2f}."
        )
    if None not in (best_candidate_implied_residual, best_candidate_abs_gap):
        takeaways.append(
            "The narrow-TOC rescue branch is closed too: "
            f"even the best candidate implies residual ≈ {best_candidate_implied_residual:.2f} with abs gap ≈ {best_candidate_abs_gap:.2f} to the headline direct core."
        )

    return {
        "status": "available",
        "headline_question": "What is the final release-facing rule for strict deposit-component interpretation after the TOC/ROW incidence gates?",
        "estimation_path": {
            "summary_artifact": "strict_release_framing_summary.json",
            "source_artifacts": [
                "strict_component_framework_summary.json",
                "toc_row_liability_incidence_raw_summary.json",
                "toc_validated_share_candidate_summary.json",
            ],
        },
        "release_position": {
            "full_tdc_release_role": "broad_treasury_attributed_object_only",
            "strict_object_rule": "exclude_toc_and_row_under_current_evidence",
            "toc_rule": "outside_strict_object",
            "row_rule": "outside_strict_object",
            "accounting_lane_role": "descriptive_only_not_independent_evidence",
            "headline_direct_benchmark": headline_direct_core,
            "multihorizon_direct_benchmark": multihorizon_direct_core,
            "impact_horizon_candidate": impact_horizon_candidate,
            "standard_bridge_comparison": standard_bridge,
            "diagnostic_envelope": diagnostic_envelope,
        },
        "evidence_tiers": {
            "independent_evidence": [
                headline_direct_core,
            ],
            "comparison_rows": [
                standard_bridge,
            ],
            "impact_horizon_candidates": [
                impact_horizon_candidate,
            ]
            if impact_horizon_candidate != "not_available"
            else [],
            "diagnostic_only": [
                diagnostic_envelope,
                "toc_row_support_bundle_qoq",
                "strict_loan_source_qoq",
            ],
            "outside_strict_object": [
                "tdc_treasury_operating_cash_qoq",
                "tdc_rest_of_world_treasury_transactions_qoq",
            ],
            "unresolved": [
                "Whether any future TOC incidence evidence justifies a validated strict share.",
                "Whether the strict lane is still missing major direct creator channels beyond the current loan core.",
            ],
        },
        "classification": {
            "release_state": "strict_release_framing_finalized",
            "raw_incidence_gate": raw_gate,
            "toc_narrow_share_decision": toc_decision,
            "strict_object_scope": "toc_and_row_excluded_under_current_evidence",
        },
        "h0_snapshot": {
            "toc_row_support_bundle_beta": support_bundle_beta,
            "core_residual_beta": core_residual_beta,
            "headline_direct_core_beta": headline_direct_core_beta,
            "standard_bridge_beta": standard_bridge_beta,
            "toc_deposits_only_share": toc_deposits_only_share,
            "toc_reserve_share": toc_reserve_share,
            "row_checkable_share": row_checkable_share,
            "row_external_share": row_external_share,
            "best_toc_candidate_implied_residual_beta": best_candidate_implied_residual,
            "best_toc_candidate_abs_gap_to_direct_core": best_candidate_abs_gap,
        },
        "recommendation": {
            "status": "strict_release_framing_finalized",
            "headline_rule": "full_tdc_is_broad_object_only",
            "strict_rule": "exclude_toc_and_row_from_strict_object",
            "reopen_rule": "reopen_only_if_new_scope_and_timing_matched_incidence_evidence_appears",
            "next_branch": "return_to_other_strict_channel_work_or_stop",
        },
        "takeaways": takeaways,
    }
