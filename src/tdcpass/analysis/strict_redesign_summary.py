from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_strict_redesign_summary(
    *,
    strict_identifiable_followup_summary: Mapping[str, Any] | None,
    strict_missing_channel_summary: Mapping[str, Any] | None,
    split_treatment_architecture_summary: Mapping[str, Any] | None,
    core_treatment_promotion_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    required = (
        strict_identifiable_followup_summary,
        strict_missing_channel_summary,
        split_treatment_architecture_summary,
        core_treatment_promotion_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    followup_h0 = dict(
        strict_identifiable_followup_summary.get("strict_component_diagnostics", {}).get("key_horizons", {}).get("h0", {})
        or {}
    )
    funding_h0 = dict(
        strict_identifiable_followup_summary.get("funding_offset_sensitivity", {}).get("key_horizons", {}).get("h0", {})
        or {}
    )
    scope_h0 = dict(
        strict_identifiable_followup_summary.get("scope_check_gap_assessment", {}).get("key_horizons", {}).get("h0", {})
        or {}
    )
    scope_us_chartered = dict(scope_h0.get("variant_gap_assessments", {}).get("us_chartered_bank_only", {}) or {})

    missing_h0 = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}) or {})
    excluded_h0 = dict(missing_h0.get("toc_row_excluded", {}) or {})

    split_h0 = dict(split_treatment_architecture_summary.get("key_horizons", {}).get("h0", {}) or {})
    split_recommendation = dict(split_treatment_architecture_summary.get("architecture_recommendation", {}) or {})

    promotion_recommendation = dict(core_treatment_promotion_summary.get("promotion_recommendation", {}) or {})
    strict_validation = dict(core_treatment_promotion_summary.get("strict_validation_check", {}) or {})

    baseline_gap_beta = _safe_float(dict(followup_h0.get("strict_identifiable_gap", {}) or {}).get("beta"))
    baseline_total_beta = _safe_float(dict(followup_h0.get("strict_identifiable_total", {}) or {}).get("beta"))
    dominant_loan_component = followup_h0.get("dominant_loan_component")
    di_loans_nec_share = _safe_float(followup_h0.get("strict_loan_di_loans_nec_share_of_loan_source_beta"))
    funding_share = _safe_float(funding_h0.get("strict_funding_offset_share_of_identifiable_total_beta"))
    remaining_scope_gap_share = _safe_float(scope_us_chartered.get("remaining_share_of_baseline_strict_gap"))
    scope_relief_share = _safe_float(scope_us_chartered.get("relief_share_of_baseline_strict_gap"))

    excluded_residual_beta = _safe_float(dict(excluded_h0.get("residual_response", {}) or {}).get("beta"))
    excluded_total_beta = _safe_float(dict(excluded_h0.get("strict_identifiable_total_response", {}) or {}).get("beta"))
    excluded_net_after_funding_beta = _safe_float(
        dict(excluded_h0.get("strict_identifiable_net_after_funding_response", {}) or {}).get("beta")
    )
    excluded_gap_share = _safe_float(excluded_h0.get("strict_gap_share_of_residual_abs"))
    excluded_gap_after_funding_share = _safe_float(excluded_h0.get("strict_gap_after_funding_share_of_residual_abs"))
    excluded_interpretation = str(missing_h0.get("interpretation", "not_available"))

    support_bundle_beta = _safe_float(split_h0.get("support_bundle_beta"))
    core_target_beta = _safe_float(dict(split_h0.get("core_deposit_proximate_target_response", {}) or {}).get("beta"))
    core_residual_beta = _safe_float(strict_validation.get("h0_core_residual_beta"))
    strict_total_beta = _safe_float(strict_validation.get("h0_strict_identifiable_total_beta"))
    gap_after_funding_beta = _safe_float(strict_validation.get("h0_gap_after_funding_beta"))
    sign_match = strict_validation.get("h0_sign_match")

    sign_mismatch = False
    if sign_match is False:
        sign_mismatch = True
    elif core_residual_beta is not None and strict_total_beta is not None:
        sign_mismatch = core_residual_beta * strict_total_beta < 0.0

    scope_status = "not_available"
    if remaining_scope_gap_share is not None:
        scope_status = (
            "not_primary"
            if remaining_scope_gap_share > 0.8
            else "partial"
            if remaining_scope_gap_share > 0.4
            else "mostly_resolved"
        )

    funding_status = "not_available"
    if funding_share is not None:
        funding_status = (
            "material"
            if abs(funding_share) >= 0.75
            else "moderate"
            if abs(funding_share) >= 0.25
            else "small"
        )

    loan_bucket_status = "not_available"
    if dominant_loan_component is not None and di_loans_nec_share is not None:
        loan_bucket_status = (
            "di_loans_nec_concentrated"
            if dominant_loan_component == "strict_loan_di_loans_nec_qoq" or abs(di_loans_nec_share) >= 0.25
            else "loan_core_not_h0_di_loans_nec_concentrated"
        )

    remaining_channels_status = "not_available"
    if excluded_gap_after_funding_share is not None or excluded_gap_share is not None:
        gap_metric = excluded_gap_after_funding_share
        if gap_metric is None:
            gap_metric = excluded_gap_share
        if gap_metric is not None:
            remaining_channels_status = (
                "large"
                if abs(float(gap_metric)) >= 1.0
                else "material"
                if abs(float(gap_metric)) >= 0.5
                else "small"
            )

    recommended_build_order: list[dict[str, Any]] = []
    recommended_build_order.append(
        {
            "step": "redesign_strict_loan_core_before_adding_more_channels",
            "why": (
                "The treatment split is now explicit but the direct-count failure remains: the h0 core residual is still "
                f"about {core_residual_beta:.2f} while the strict identifiable total is about {strict_total_beta:.2f}."
                if core_residual_beta is not None and strict_total_beta is not None
                else "The treatment split is now explicit but the direct-count failure remains."
            ),
            "deliverable": (
                "Create a release-facing core-loan subtotal that keeps consumer credit and mortgages explicit and moves "
                "broad DI-loans-n.e.c. content out of the headline core until it is subdivided."
            ),
        }
    )
    recommended_build_order.append(
        {
            "step": "split_di_loans_nec_into_core_vs_noncore_credit_buckets",
            "why": (
                "At h0 the dominant loan block is "
                f"`{dominant_loan_component}` and DI-loans-n.e.c. contributes only about {di_loans_nec_share:.2f} of signed loan-source beta, "
                "so the next problem is not simply “add more DI loans n.e.c.”; it is classifying broad system/financial/external credit separately from deposit-proximate core credit."
                if dominant_loan_component is not None and di_loans_nec_share is not None
                else "DI-loans-n.e.c. remains too broad to leave inside the headline strict core."
            ),
            "deliverable": (
                "Publish a DI-loans-n.e.c. split that separates private-credit-like content from system, financial, interbank, "
                "and external content using the existing borrower-side diagnostics as scaffolding."
            ),
        }
    )
    recommended_build_order.append(
        {
            "step": "keep_securities_and_funding_secondary_until_loan_core_stabilizes",
            "why": (
                f"Funding offsets are already about {funding_share:.2f} of the signed strict identifiable total at h0, "
                "so broadening the headline strict lane with more netting now would blur the failure rather than clarify it."
                if funding_share is not None
                else "Securities and funding are already secondary lanes and should stay that way until the loan core stabilizes."
            ),
            "deliverable": (
                "Leave securities and funding-offset blocks as sensitivities while the loan-core redesign is evaluated against the split treatment architecture."
            ),
        }
    )

    current_problem_label = "strict_lane_ready_for_redesign"
    if sign_mismatch:
        current_problem_label = "treatment_split_fixed_but_core_direct_counts_still_point_the_wrong_way"

    takeaways = [
        "The treatment-side redesign is now stable enough to stop debating the headline split and return to the strict lane itself.",
    ]
    if support_bundle_beta is not None and core_residual_beta is not None:
        takeaways.append(
            f"At h0, the split treatment leaves a large support bundle (≈ {support_bundle_beta:.2f}) but a much smaller core residual (≈ {core_residual_beta:.2f})."
        )
    if remaining_scope_gap_share is not None:
        takeaways.append(
            f"The matched-bank-leg scope check is no longer the main strict-lane issue: about {remaining_scope_gap_share:.2f} of the baseline strict gap still remains when only that scope shift is applied."
        )
    if sign_mismatch and excluded_residual_beta is not None and excluded_total_beta is not None:
        takeaways.append(
            f"Even on the TOC/ROW-excluded comparison, the direct-count strict lane still points the wrong way at h0: residual ≈ {excluded_residual_beta:.2f}, strict identifiable total ≈ {excluded_total_beta:.2f}."
        )
    if dominant_loan_component is not None and di_loans_nec_share is not None:
        takeaways.append(
            f"The h0 loan picture does not justify treating DI-loans-n.e.c. as the headline core: the dominant loan block is `{dominant_loan_component}` and DI-loans-n.e.c. contributes only about {di_loans_nec_share:.2f} of signed loan-source beta."
        )
    if funding_share is not None:
        takeaways.append(
            f"Funding offsets are already material at h0 (about {funding_share:.2f} of signed identifiable total), so the next strict branch should stabilize the loan core before revisiting more netting."
        )
    if promotion_recommendation:
        takeaways.append(
            "The separate core-treatment shock stays interpretive only for now, so strict-lane redesign should proceed under the current split architecture rather than around a newly promoted treatment."
        )

    return {
        "status": "available",
        "headline_question": "With the treatment side now split into a deposit-proximate core and a TOC/ROW support bundle, what exactly is failing in the strict lane and what should be built next?",
        "estimation_path": {
            "summary_artifact": "strict_redesign_summary.json",
            "source_artifacts": [
                "strict_identifiable_followup_summary.json",
                "strict_missing_channel_summary.json",
                "split_treatment_architecture_summary.json",
                "core_treatment_promotion_summary.json",
            ],
            "release_role": "strict_redesign_planning_surface",
        },
        "current_strict_problem_definition": {
            "label": current_problem_label,
            "promotion_status": str(promotion_recommendation.get("status", "not_available")),
            "split_next_branch": str(split_recommendation.get("recommended_next_branch", "not_available")),
            "h0_support_bundle_beta": support_bundle_beta,
            "h0_core_deposit_proximate_target_beta": core_target_beta,
            "h0_core_residual_beta": core_residual_beta,
            "h0_baseline_strict_gap_beta": baseline_gap_beta,
            "h0_baseline_strict_identifiable_total_beta": baseline_total_beta,
            "h0_toc_row_excluded_residual_beta": excluded_residual_beta,
            "h0_toc_row_excluded_strict_identifiable_total_beta": excluded_total_beta,
            "h0_toc_row_excluded_net_after_funding_beta": excluded_net_after_funding_beta,
            "h0_gap_after_funding_beta": gap_after_funding_beta,
            "h0_sign_match": sign_match,
            "toc_row_excluded_interpretation": excluded_interpretation,
        },
        "failure_modes": {
            "scope_mismatch_not_primary": {
                "status": scope_status,
                "h0_remaining_share_of_baseline_strict_gap": remaining_scope_gap_share,
                "h0_relief_share_of_baseline_strict_gap": scope_relief_share,
            },
            "sign_mismatch_under_core_residual": {
                "status": "confirmed" if sign_mismatch else "not_confirmed",
                "h0_core_residual_beta": core_residual_beta,
                "h0_strict_identifiable_total_beta": strict_total_beta,
                "h0_toc_row_excluded_residual_beta": excluded_residual_beta,
                "h0_toc_row_excluded_strict_identifiable_total_beta": excluded_total_beta,
            },
            "loan_bucket_shape": {
                "status": loan_bucket_status,
                "h0_dominant_loan_component": dominant_loan_component,
                "h0_di_loans_nec_share_of_signed_loan_source_beta": di_loans_nec_share,
            },
            "funding_offset_instability": {
                "status": funding_status,
                "h0_funding_offset_share_of_identifiable_total_beta": funding_share,
                "h0_gap_after_funding_beta": gap_after_funding_beta,
            },
            "remaining_unmeasured_channels": {
                "status": remaining_channels_status,
                "h0_toc_row_excluded_gap_share_of_residual_abs": excluded_gap_share,
                "h0_toc_row_excluded_gap_after_funding_share_of_residual_abs": excluded_gap_after_funding_share,
            },
        },
        "recommended_build_order": recommended_build_order,
        "takeaways": takeaways,
    }
