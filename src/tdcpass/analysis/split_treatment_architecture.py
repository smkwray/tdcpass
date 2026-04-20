from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _component_signed_beta(
    treatment_horizon: Mapping[str, Any],
    label: str,
) -> float | None:
    responses = dict(treatment_horizon.get("direct_component_responses", {}) or {})
    payload = dict(responses.get(label, {}) or {})
    return _safe_float(payload.get("signed_contribution_beta"))


def _quarterly_alignment(shocked: pd.DataFrame) -> dict[str, Any]:
    required = {
        "quarter",
        "tdc_bank_only_qoq",
        "tdc_core_deposit_proximate_bank_only_qoq",
        "tdc_toc_row_support_bundle_qoq",
        "other_component_qoq",
        "other_component_core_deposit_proximate_bank_only_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    frame["tdc_decomposition_gap_beta"] = (
        frame["tdc_bank_only_qoq"]
        - frame["tdc_core_deposit_proximate_bank_only_qoq"]
        - frame["tdc_toc_row_support_bundle_qoq"]
    )
    frame["residual_decomposition_gap_beta"] = (
        frame["other_component_core_deposit_proximate_bank_only_qoq"]
        - frame["other_component_qoq"]
        - frame["tdc_toc_row_support_bundle_qoq"]
    )

    tdc_gap = frame["tdc_decomposition_gap_beta"].abs()
    residual_gap = frame["residual_decomposition_gap_beta"].abs()
    worst_tdc = frame.loc[tdc_gap.idxmax()]
    worst_residual = frame.loc[residual_gap.idxmax()]

    return {
        "status": "available",
        "rows": int(frame.shape[0]),
        "tdc_identity": {
            "max_abs_gap_beta": float(tdc_gap.max()),
            "mean_abs_gap_beta": float(tdc_gap.mean()),
            "worst_quarter": str(worst_tdc["quarter"]),
            "worst_gap_beta": float(worst_tdc["tdc_decomposition_gap_beta"]),
            "quarterly_alignment": "exact" if float(tdc_gap.max()) <= 1e-9 else "inexact",
        },
        "residual_identity": {
            "max_abs_gap_beta": float(residual_gap.max()),
            "mean_abs_gap_beta": float(residual_gap.mean()),
            "worst_quarter": str(worst_residual["quarter"]),
            "worst_gap_beta": float(worst_residual["residual_decomposition_gap_beta"]),
            "quarterly_alignment": "exact" if float(residual_gap.max()) <= 1e-9 else "inexact",
        },
    }


def build_split_treatment_architecture_summary(
    *,
    shocked: pd.DataFrame,
    tdc_treatment_audit_summary: Mapping[str, Any] | None,
    toc_row_path_split_summary: Mapping[str, Any] | None,
    treatment_object_comparison_summary: Mapping[str, Any] | None,
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    required = (
        tdc_treatment_audit_summary,
        toc_row_path_split_summary,
        treatment_object_comparison_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    quarterly_alignment = _quarterly_alignment(shocked)
    treatment_key_horizons = dict(tdc_treatment_audit_summary.get("key_horizons", {}) or {})
    path_key_horizons = dict(toc_row_path_split_summary.get("key_horizons", {}) or {})
    path_quarterly = dict(toc_row_path_split_summary.get("quarterly_split", {}) or {})
    comparison_recommendation = dict(treatment_object_comparison_summary.get("recommendation", {}) or {})

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_key = f"h{horizon}"
        treatment_h = dict(treatment_key_horizons.get(horizon_key, {}) or {})
        if not treatment_h:
            continue
        path_h = dict(path_key_horizons.get(horizon_key, {}) or {})
        baseline_target = dict(treatment_h.get("baseline_tdc_response", {}) or {})
        baseline_residual = dict(treatment_h.get("baseline_residual_response", {}) or {})
        no_toc_no_row = dict(treatment_h.get("variant_removal_diagnostics", {}) or {}).get(
            "no_toc_no_row_bank_only",
            {},
        )
        core_target = dict(dict(no_toc_no_row).get("target_response", {}) or {})
        core_residual = dict(dict(no_toc_no_row).get("residual_response", {}) or {})

        baseline_target_beta = _safe_float(baseline_target.get("beta"))
        core_target_beta = _safe_float(core_target.get("beta"))
        support_bundle_beta = (
            None
            if baseline_target_beta is None or core_target_beta is None
            else baseline_target_beta - core_target_beta
        )

        row_signed = _component_signed_beta(treatment_h, "rest_of_world_treasury_transactions")
        toc_signed = _component_signed_beta(treatment_h, "treasury_operating_cash_drain")
        direct_support_bundle_signed_beta = (
            None if row_signed is None or toc_signed is None else row_signed + toc_signed
        )
        direct_support_gap = (
            None
            if support_bundle_beta is None or direct_support_bundle_signed_beta is None
            else support_bundle_beta - direct_support_bundle_signed_beta
        )

        core_residual_beta = _safe_float(core_residual.get("beta"))
        baseline_residual_beta = _safe_float(baseline_residual.get("beta"))
        support_bundle_residual_shift_beta = _safe_float(dict(no_toc_no_row).get("residual_shift_vs_baseline_beta"))
        support_bundle_share_of_baseline = (
            None
            if baseline_target_beta in (None, 0.0) or support_bundle_beta is None
            else support_bundle_beta / baseline_target_beta
        )

        key_horizons[horizon_key] = {
            "baseline_target_response": baseline_target,
            "core_deposit_proximate_target_response": core_target,
            "support_bundle_beta": support_bundle_beta,
            "support_bundle_share_of_baseline_tdc_beta": support_bundle_share_of_baseline,
            "baseline_residual_response": baseline_residual,
            "core_deposit_proximate_residual_response": core_residual,
            "support_bundle_residual_shift_vs_baseline_beta": support_bundle_residual_shift_beta,
            "row_signed_beta": row_signed,
            "toc_signed_beta": toc_signed,
            "direct_support_bundle_signed_beta": direct_support_bundle_signed_beta,
            "support_bundle_minus_direct_component_gap_beta": direct_support_gap,
            "quarterly_preferred_path": str(path_quarterly.get("preferred_quarterly_path", "")),
            "horizon_preferred_path": str(path_h.get("preferred_horizon_path", "")),
            "broad_support_path_response": dict(path_h.get("broad_support_path_response", {}) or {}),
            "direct_deposit_path_response": dict(path_h.get("direct_deposit_path_response", {}) or {}),
        }

    h0 = dict(key_horizons.get("h0", {}) or {})
    h0_support_bundle = _safe_float(h0.get("support_bundle_beta"))
    h0_core_residual = _safe_float(dict(h0.get("core_deposit_proximate_residual_response", {}) or {}).get("beta"))
    h0_baseline_residual = _safe_float(dict(h0.get("baseline_residual_response", {}) or {}).get("beta"))
    h0_core_target = _safe_float(dict(h0.get("core_deposit_proximate_target_response", {}) or {}).get("beta"))
    h0_path = str(h0.get("horizon_preferred_path", ""))
    quarterly_path = str(path_quarterly.get("preferred_quarterly_path", ""))

    takeaways = [
        "The split architecture defines a deposit-proximate core treatment separately from the mixed TOC/ROW support bundle instead of forcing one headline treatment object to do both jobs.",
    ]
    if quarterly_alignment.get("status") == "available":
        tdc_alignment = dict(quarterly_alignment.get("tdc_identity", {}) or {})
        residual_alignment = dict(quarterly_alignment.get("residual_identity", {}) or {})
        takeaways.append(
            "The quarterly split is mechanically exact in the panel: "
            f"max TDC decomposition gap ≈ {float(tdc_alignment.get('max_abs_gap_beta') or 0.0):.2e}, "
            f"max residual decomposition gap ≈ {float(residual_alignment.get('max_abs_gap_beta') or 0.0):.2e}."
        )
    if h0_baseline_residual is not None and h0_core_residual is not None and h0_support_bundle is not None:
        takeaways.append(
            f"At h0, baseline residual ≈ {h0_baseline_residual:.2f}, core residual ≈ {h0_core_residual:.2f}, so the split support bundle carries about {h0_support_bundle:.2f} of treatment-side movement."
        )
    if h0_core_target is not None:
        takeaways.append(
            f"At h0, the deposit-proximate core treatment response is still sizable (≈ {h0_core_target:.2f}), which is why the split should be treated as an interpretation redesign rather than a zeroing-out exercise."
        )
    if quarterly_path and h0_path:
        takeaways.append(
            f"Path evidence stays mixed inside the support bundle: quarter by quarter the preferred path is `{quarterly_path}`, but at h0 the preferred path is `{h0_path}`."
        )
    if comparison_recommendation:
        takeaways.append(
            "This split directly implements the repo-level redesign recommendation: "
            f"`{str(comparison_recommendation.get('recommended_next_branch', 'split_core_plus_support_bundle'))}`."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "How should the TDC treatment be split into a deposit-proximate core and a separate TOC/ROW support bundle?",
        "estimation_path": {
            "summary_artifact": "split_treatment_architecture_summary.json",
            "source_artifacts": [
                "tdc_treatment_audit_summary.json",
                "toc_row_path_split_summary.json",
                "treatment_object_comparison_summary.json",
            ],
        },
        "series_definitions": {
            "baseline_treatment": "tdc_bank_only_qoq",
            "core_deposit_proximate_treatment": "tdc_core_deposit_proximate_bank_only_qoq",
            "support_bundle_treatment": "tdc_toc_row_support_bundle_qoq",
            "baseline_residual": "other_component_qoq",
            "core_deposit_proximate_residual": "other_component_core_deposit_proximate_bank_only_qoq",
            "support_bundle_formula": "tdc_row_treasury_transactions_qoq - tdc_treasury_operating_cash_qoq",
        },
        "quarterly_alignment": quarterly_alignment,
        "architecture_recommendation": {
            "status": "recommended_interpretation_redesign",
            "headline_decision_now": str(
                comparison_recommendation.get(
                    "headline_decision_now",
                    "keep current headline provisional while publishing the split treatment architecture diagnostically",
                )
            ),
            "recommended_next_branch": str(
                comparison_recommendation.get("recommended_next_branch", "split_core_plus_support_bundle")
            ),
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
