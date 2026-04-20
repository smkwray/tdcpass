from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_treatment_object_comparison_summary(
    *,
    scope_alignment_summary: Mapping[str, Any] | None,
    broad_scope_system_summary: Mapping[str, Any] | None,
    tdc_treatment_audit_summary: Mapping[str, Any] | None,
    toc_row_excluded_interpretation_summary: Mapping[str, Any] | None,
    strict_missing_channel_summary: Mapping[str, Any] | None,
    strict_sign_mismatch_audit_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    required = (
        scope_alignment_summary,
        broad_scope_system_summary,
        tdc_treatment_audit_summary,
        toc_row_excluded_interpretation_summary,
        strict_missing_channel_summary,
        strict_sign_mismatch_audit_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    scope_h0 = (
        dict(scope_alignment_summary.get("deposit_concepts", {}))
        .get("total_deposits_including_interbank", {})
        .get("key_horizons", {})
        .get("h0", {})
    )
    scope_variants = dict(scope_h0.get("variants", {}))
    baseline_residual = _safe_float(
        dict(dict(tdc_treatment_audit_summary.get("key_horizons", {}).get("h0", {})).get("baseline_residual_response", {})).get(
            "beta"
        )
    )
    us_chartered_shift = _safe_float(
        dict(dict(scope_variants.get("us_chartered_bank_only", {})).get("differences_vs_baseline_beta", {})).get(
            "residual_response"
        )
    )
    domestic_shift = _safe_float(
        dict(dict(scope_variants.get("domestic_bank_only", {})).get("differences_vs_baseline_beta", {})).get(
            "residual_response"
        )
    )
    broad_gap_share = _safe_float(
        dict(broad_scope_system_summary.get("broad_matched_system", {}).get("key_horizons", {}).get("h0", {})).get(
            "broad_strict_gap_share_of_residual"
        )
    )

    treatment_h0 = dict(tdc_treatment_audit_summary.get("key_horizons", {}).get("h0", {}))
    variant_diagnostics = dict(treatment_h0.get("variant_removal_diagnostics", {}))
    no_toc_shift = _safe_float(dict(variant_diagnostics.get("no_toc_bank_only", {})).get("residual_shift_vs_baseline_beta"))
    no_toc_no_row_shift = _safe_float(
        dict(variant_diagnostics.get("no_toc_no_row_bank_only", {})).get("residual_shift_vs_baseline_beta")
    )
    no_foreign_shift = _safe_float(
        dict(variant_diagnostics.get("no_foreign_bank_sectors", {})).get("residual_shift_vs_baseline_beta")
    )

    excluded_h0 = dict(toc_row_excluded_interpretation_summary.get("key_horizons", {}).get("h0", {}))
    excluded_residual = _safe_float(
        dict(dict(excluded_h0.get("toc_row_excluded", {})).get("residual_response", {})).get("beta")
    )
    excluded_gap_share = _safe_float(
        dict(excluded_h0.get("toc_row_excluded", {})).get("strict_gap_share_of_residual")
    )

    missing_h0 = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}))
    missing_excluded = dict(missing_h0.get("toc_row_excluded", {}))
    excluded_strict_total = _safe_float(
        dict(missing_excluded.get("strict_identifiable_total_response", {})).get("beta")
    )
    excluded_gap_after_funding_share = _safe_float(
        missing_excluded.get("strict_gap_after_funding_share_of_residual_abs")
    )

    shock_alignment = dict(strict_sign_mismatch_audit_summary.get("shock_alignment", {}))
    quarter_concentration = dict(strict_sign_mismatch_audit_summary.get("quarter_concentration", {}))
    overlap_corr = _safe_float(shock_alignment.get("shock_corr"))
    same_sign_share = _safe_float(shock_alignment.get("same_sign_share"))
    top5_share = _safe_float(quarter_concentration.get("top5_abs_gap_share"))
    dominant_period_bucket = str(quarter_concentration.get("dominant_period_bucket", ""))

    candidates: list[dict[str, Any]] = []

    candidates.append(
        {
            "candidate": "baseline_full_tdc",
            "role": "current_headline",
            "headline_eligibility": "provisional_only",
            "object_definition": "Fed + bank-sector + ROW Treasury transactions - TOC + positive Fed remittances",
            "why_it_exists": "Conceptually closest to the original full Treasury-attributed deposit object.",
            "h0_residual_beta": baseline_residual,
            "scope_alignment_note": "Broader than the U.S.-chartered headline outcome; scope mismatch is real but not dominant.",
            "treatment_object_risk": "TOC and ROW appear to be a mixed support bundle rather than a clean same-object deposit-treatment leg.",
        }
    )

    usc_residual = None if baseline_residual is None or us_chartered_shift is None else baseline_residual + us_chartered_shift
    candidates.append(
        {
            "candidate": "us_chartered_bank_leg_match",
            "role": "scope_check",
            "headline_eligibility": "comparison_only",
            "object_definition": "Bank-leg-matched TDC sensitivity on the U.S.-chartered outcome scope.",
            "why_it_exists": "Best scope-diagnostic comparison against the current headline outcome.",
            "h0_residual_beta": usc_residual,
            "h0_residual_shift_vs_baseline_beta": us_chartered_shift,
            "scope_alignment_note": "Improves scope coherence but does not solve the main residual problem.",
            "treatment_object_risk": "Too narrow to stand in for the full TDC object if ROW is conceptually part of TDC.",
        }
    )

    candidates.append(
        {
            "candidate": "toc_row_excluded_core",
            "role": "diagnostic_only",
            "headline_eligibility": "not_headline",
            "object_definition": "Baseline TDC with TOC and ROW removed together.",
            "why_it_exists": "Tests how much of the residual problem is concentrated in the TOC/ROW block.",
            "h0_residual_beta": excluded_residual,
            "h0_residual_shift_vs_baseline_beta": no_toc_no_row_shift,
            "h0_strict_identifiable_total_beta": excluded_strict_total,
            "h0_strict_gap_share_of_residual": excluded_gap_share,
            "h0_gap_after_funding_share_of_residual_abs": excluded_gap_after_funding_share,
            "shock_overlap_corr_with_baseline": overlap_corr,
            "shock_same_sign_share_with_baseline": same_sign_share,
            "top5_abs_gap_share": top5_share,
            "dominant_period_bucket": dominant_period_bucket,
            "scope_alignment_note": "Useful diagnostic, not a stable replacement treatment.",
            "treatment_object_risk": "Shock rotates materially and the independent lane still does not validate the remaining object.",
        }
    )

    candidates.append(
        {
            "candidate": "split_core_plus_support_bundle",
            "role": "recommended_redesign",
            "headline_eligibility": "recommended_next_architecture",
            "object_definition": "Publish a deposit-proximate core TDC separately from a TOC/ROW support bundle instead of forcing them into one headline object.",
            "why_it_exists": "Current evidence says TOC/ROW dominates the treatment-side problem, while a TOC/ROW-excluded object is not stable enough to become the new headline shock.",
            "h0_core_diagnostic_residual_beta": excluded_residual,
            "h0_core_diagnostic_strict_total_beta": excluded_strict_total,
            "h0_toc_shift_vs_baseline_beta": no_toc_shift,
            "h0_toc_row_shift_vs_baseline_beta": no_toc_no_row_shift,
            "h0_no_row_shift_vs_baseline_beta": domestic_shift,
            "h0_no_foreign_bank_shift_vs_baseline_beta": no_foreign_shift,
            "broad_scope_gap_share_of_residual_h0": broad_gap_share,
            "scope_alignment_note": "Preserves the broad TDC idea while separating the mixed support bundle from the deposit-proximate core.",
            "treatment_object_risk": "Requires an explicit redesign of the headline interpretation layer rather than a simple variant swap.",
        }
    )

    recommendation = {
        "recommended_next_branch": "split_core_plus_support_bundle",
        "headline_decision_now": "keep current headline provisional and do not promote the TOC_ROW_excluded object",
        "why": (
            "Baseline full TDC still best represents the broad original concept, but the residual problem is concentrated in TOC/ROW. "
            "The TOC/ROW-excluded object is useful diagnostically yet too unstable to replace the headline. "
            "So the next serious branch should redesign the treatment architecture into a deposit-proximate core plus an explicit TOC/ROW support bundle."
        ),
    }

    takeaways = [
        f"Baseline full TDC remains the broad conceptual object, but its h0 residual is about {baseline_residual:.2f} and the main treatment-side movement comes from TOC/ROW rather than foreign bank sectors."
        if baseline_residual is not None
        else "Baseline full TDC remains the broad conceptual object, but the main treatment-side movement comes from TOC/ROW rather than foreign bank sectors.",
        f"The U.S.-chartered bank-leg match is the right scope comparison, but it only shifts the h0 residual by about {us_chartered_shift:.2f}."
        if us_chartered_shift is not None
        else "The U.S.-chartered bank-leg match is the right scope comparison, but it does not solve the main residual problem.",
        f"The TOC/ROW-excluded object is diagnostic only: h0 residual ≈ {excluded_residual:.2f}, strict total ≈ {excluded_strict_total:.2f}, overlap corr with baseline ≈ {overlap_corr:.2f}."
        if excluded_residual is not None and excluded_strict_total is not None and overlap_corr is not None
        else "The TOC/ROW-excluded object is diagnostic only; it should not be promoted to the headline treatment.",
        f"The broad matched-scope system still leaves a large h0 strict gap share (≈ {broad_gap_share:.2f}), so the project does not currently have a clean validated replacement treatment object."
        if broad_gap_share is not None
        else "The broad matched-scope system still leaves a large strict gap, so the project does not currently have a clean validated replacement treatment object.",
        "The most defensible next step is a split treatment architecture: keep the broad TDC concept visible, but separate the TOC/ROW support bundle from the deposit-proximate core instead of forcing one headline object to do both jobs.",
    ]

    return {
        "status": "available",
        "headline_question": "Which TDC treatment object should be treated as the headline, which should stay diagnostic, and what redesign is most defensible given the current evidence?",
        "estimation_path": {
            "summary_artifact": "treatment_object_comparison_summary.json",
            "source_artifacts": [
                "scope_alignment_summary.json",
                "broad_scope_system_summary.json",
                "tdc_treatment_audit_summary.json",
                "toc_row_excluded_interpretation_summary.json",
                "strict_missing_channel_summary.json",
                "strict_sign_mismatch_audit_summary.json",
            ],
        },
        "candidate_objects": candidates,
        "recommendation": recommendation,
        "takeaways": takeaways,
    }
