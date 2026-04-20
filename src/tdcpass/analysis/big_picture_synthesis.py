from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def build_big_picture_synthesis_summary(
    *,
    scope_alignment_summary: Mapping[str, Any] | None,
    broad_scope_system_summary: Mapping[str, Any] | None,
    tdc_treatment_audit_summary: Mapping[str, Any] | None,
    toc_row_excluded_interpretation_summary: Mapping[str, Any] | None,
    strict_missing_channel_summary: Mapping[str, Any] | None,
    strict_sign_mismatch_audit_summary: Mapping[str, Any] | None,
    strict_top_gap_anomaly_backdrop_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if (
        scope_alignment_summary is None
        or broad_scope_system_summary is None
        or tdc_treatment_audit_summary is None
        or toc_row_excluded_interpretation_summary is None
        or strict_missing_channel_summary is None
        or strict_sign_mismatch_audit_summary is None
        or strict_top_gap_anomaly_backdrop_summary is None
    ):
        return {"status": "not_available", "reason": "missing_input_summary"}

    if any(
        str(summary.get("status", "not_available")) != "available"
        for summary in (
            scope_alignment_summary,
            broad_scope_system_summary,
            tdc_treatment_audit_summary,
            toc_row_excluded_interpretation_summary,
            strict_missing_channel_summary,
            strict_sign_mismatch_audit_summary,
            strict_top_gap_anomaly_backdrop_summary,
        )
    ):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    scope_h0 = (
        dict(scope_alignment_summary.get("deposit_concepts", {}))
        .get("total_deposits_including_interbank", {})
        .get("key_horizons", {})
        .get("h0", {})
    )
    scope_variants = dict(scope_h0.get("variants", {}))
    domestic_scope_relief = _safe_float(
        dict(dict(scope_variants.get("domestic_bank_only", {})).get("differences_vs_baseline_beta", {})).get(
            "residual_response"
        )
    )
    us_chartered_scope_relief = _safe_float(
        dict(dict(scope_variants.get("us_chartered_bank_only", {})).get("differences_vs_baseline_beta", {})).get(
            "residual_response"
        )
    )

    broad_h0 = dict(broad_scope_system_summary.get("broad_matched_system", {}).get("key_horizons", {}).get("h0", {}))
    broad_gap_share = _safe_float(broad_h0.get("broad_strict_gap_share_of_residual"))

    treatment_h0 = dict(tdc_treatment_audit_summary.get("key_horizons", {}).get("h0", {}))
    treatment_variants = dict(treatment_h0.get("variant_removal_diagnostics", {}))
    baseline_residual = _safe_float(dict(treatment_h0.get("baseline_residual_response", {})).get("beta"))
    no_toc_shift = _safe_float(
        dict(treatment_variants.get("no_toc_bank_only", {})).get("residual_shift_vs_baseline_beta")
    )
    no_toc_no_row_shift = _safe_float(
        dict(treatment_variants.get("no_toc_no_row_bank_only", {})).get("residual_shift_vs_baseline_beta")
    )
    no_row_shift = _safe_float(
        dict(treatment_variants.get("domestic_bank_only", {})).get("residual_shift_vs_baseline_beta")
    )
    no_foreign_bank_shift = _safe_float(
        dict(treatment_variants.get("no_foreign_bank_sectors", {})).get("residual_shift_vs_baseline_beta")
    )

    excluded_h0 = dict(toc_row_excluded_interpretation_summary.get("key_horizons", {}).get("h0", {}))
    excluded_residual = _safe_float(
        dict(dict(excluded_h0.get("toc_row_excluded", {})).get("residual_response", {})).get("beta")
    )
    excluded_gap_share = _safe_float(
        dict(excluded_h0.get("toc_row_excluded", {})).get("strict_gap_share_of_residual")
    )
    excluded_interpretation = str(excluded_h0.get("interpretation", ""))

    missing_h0 = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}))
    missing_excluded = dict(missing_h0.get("toc_row_excluded", {}))
    excluded_strict_total = _safe_float(
        dict(missing_excluded.get("strict_identifiable_total_response", {})).get("beta")
    )
    excluded_funding_adjusted_net = _safe_float(
        dict(missing_excluded.get("strict_identifiable_net_after_funding_response", {})).get("beta")
    )
    excluded_gap_after_funding_share = _safe_float(
        missing_excluded.get("strict_gap_after_funding_share_of_residual_abs")
    )
    missing_channel_interpretation = str(missing_h0.get("interpretation", ""))

    shock_alignment = dict(strict_sign_mismatch_audit_summary.get("shock_alignment", {}))
    quarter_concentration = dict(strict_sign_mismatch_audit_summary.get("quarter_concentration", {}))
    shock_overlap_corr = _safe_float(shock_alignment.get("shock_corr"))
    shock_same_sign_share = _safe_float(shock_alignment.get("same_sign_share"))
    top5_gap_share = _safe_float(quarter_concentration.get("top5_abs_gap_share"))
    dominant_period_bucket = str(quarter_concentration.get("dominant_period_bucket", ""))

    anomaly_interpretation = str(strict_top_gap_anomaly_backdrop_summary.get("interpretation", ""))
    anomaly_backdrop_ratio = _safe_float(
        strict_top_gap_anomaly_backdrop_summary.get("liquidity_external_abs_to_corporate_abs_ratio")
    )

    interpretation = "big_picture_unclassified"
    if (
        no_toc_no_row_shift is not None
        and no_row_shift is not None
        and no_foreign_bank_shift is not None
        and excluded_residual is not None
        and excluded_strict_total is not None
    ):
        if (
            abs(no_toc_no_row_shift) > abs(no_row_shift)
            and abs(no_row_shift) > abs(no_foreign_bank_shift)
            and abs(excluded_residual) <= 10.0
            and excluded_strict_total * excluded_residual < 0.0
        ):
            interpretation = "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated"
        elif abs(no_toc_no_row_shift) > abs(no_row_shift):
            interpretation = "treatment_side_problem_dominates_but_independent_lane_read_remains_unsettled"

    takeaways: list[str] = []
    if us_chartered_scope_relief is not None and domestic_scope_relief is not None:
        takeaways.append(
            "Scope mismatch is real but partial: at h0 the true U.S.-chartered bank-leg match shifts the residual by "
            f"about {us_chartered_scope_relief:.2f}, versus about {domestic_scope_relief:.2f} for the no-ROW sensitivity."
        )
    if broad_gap_share is not None:
        takeaways.append(
            "Even after broadening to a matched-scope bank system, the strict lane still leaves a large gap: "
            f"about {broad_gap_share:.2f} of the broad residual remains in the broad strict gap at h0."
        )
    if baseline_residual is not None and no_toc_shift is not None and no_toc_no_row_shift is not None:
        takeaways.append(
            "The residual problem is concentrated in TOC/ROW, not foreign bank sectors: baseline h0 residual ≈ "
            f"{baseline_residual:.2f}, removing TOC shifts it by about {no_toc_shift:.2f}, and removing TOC+ROW shifts it by about {no_toc_no_row_shift:.2f}."
        )
    if excluded_residual is not None and excluded_strict_total is not None and excluded_gap_share is not None:
        takeaways.append(
            "But TOC/ROW exclusion does not validate the independent lane: the h0 residual shrinks to about "
            f"{excluded_residual:.2f} while the strict identifiable total is about {excluded_strict_total:.2f}, with strict-gap share about {excluded_gap_share:.2f}."
        )
    if excluded_funding_adjusted_net is not None and excluded_gap_after_funding_share is not None:
        takeaways.append(
            "Funding adjustment does not rescue the strict lane under that comparison: funding-adjusted net is about "
            f"{excluded_funding_adjusted_net:.2f} and the remaining gap-after-funding share is about {excluded_gap_after_funding_share:.2f}."
        )
    if shock_overlap_corr is not None and shock_same_sign_share is not None and top5_gap_share is not None:
        takeaways.append(
            "The TOC/ROW-excluded shock is not a stable replacement object: overlap correlation with baseline is about "
            f"{shock_overlap_corr:.2f}, same-sign share about {shock_same_sign_share:.2f}, and the top five gap quarters explain about {top5_gap_share:.2f} of gap mass."
        )
    if dominant_period_bucket:
        takeaways.append(
            f"That instability is concentrated in `{dominant_period_bucket}`, so the excluded shock rotates around specific windows rather than delivering a clean new baseline."
        )
    if anomaly_backdrop_ratio is not None:
        takeaways.append(
            "The quarter-level anomaly work is now enough for classification and should stay secondary: in `2009Q4`, the combined liquidity/external shortfall is about "
            f"{anomaly_backdrop_ratio:.2f} times the corporate-credit shortfall ({anomaly_interpretation})."
        )

    return {
        "status": "available",
        "headline_question": "What is the big-picture read after separating scope issues, treatment construction issues, and strict-lane verification issues?",
        "estimation_path": {
            "summary_artifact": "big_picture_synthesis_summary.json",
            "source_artifacts": [
                "scope_alignment_summary.json",
                "broad_scope_system_summary.json",
                "tdc_treatment_audit_summary.json",
                "toc_row_excluded_interpretation_summary.json",
                "strict_missing_channel_summary.json",
                "strict_sign_mismatch_audit_summary.json",
                "strict_top_gap_anomaly_backdrop_summary.json",
            ],
        },
        "h0_snapshot": {
            "us_chartered_scope_relief_beta": us_chartered_scope_relief,
            "domestic_no_row_scope_relief_beta": domestic_scope_relief,
            "broad_strict_gap_share_of_residual": broad_gap_share,
            "baseline_residual_beta": baseline_residual,
            "no_row_residual_shift_vs_baseline_beta": no_row_shift,
            "no_foreign_bank_sectors_residual_shift_vs_baseline_beta": no_foreign_bank_shift,
            "no_toc_residual_shift_vs_baseline_beta": no_toc_shift,
            "no_toc_no_row_residual_shift_vs_baseline_beta": no_toc_no_row_shift,
            "toc_row_excluded_residual_beta": excluded_residual,
            "toc_row_excluded_strict_gap_share_of_residual": excluded_gap_share,
            "toc_row_excluded_strict_identifiable_total_beta": excluded_strict_total,
            "toc_row_excluded_funding_adjusted_net_beta": excluded_funding_adjusted_net,
            "toc_row_excluded_gap_after_funding_share_of_residual_abs": excluded_gap_after_funding_share,
        },
        "quarter_composition": {
            "shock_overlap_corr": shock_overlap_corr,
            "shock_same_sign_share": shock_same_sign_share,
            "top5_abs_gap_share": top5_gap_share,
            "dominant_period_bucket": dominant_period_bucket,
        },
        "supporting_case": {
            "anomaly_quarter": "2009Q4",
            "interpretation": anomaly_interpretation,
            "liquidity_external_abs_to_corporate_abs_ratio": anomaly_backdrop_ratio,
        },
        "classification": {
            "scope_issue_status": "real_but_partial",
            "treatment_issue_status": "toc_row_dominant",
            "independent_lane_status": "not_validated",
            "single_quarter_drill_priority": "secondary_only",
            "toc_row_excluded_interpretation": excluded_interpretation,
            "strict_missing_channel_interpretation": missing_channel_interpretation,
        },
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
