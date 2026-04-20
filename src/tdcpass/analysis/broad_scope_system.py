from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.identity_baseline import build_identity_baseline_irf
from tdcpass.analysis.local_projections import run_local_projections


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def build_broad_scope_system_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    scope_alignment_summary: dict[str, Any] | None = None,
    strict_identifiable_followup_summary: dict[str, Any] | None = None,
    tdc_treatment_audit_summary: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 1, 4, 8),
    bootstrap_reps: int = 40,
    bootstrap_block_length: int = 4,
) -> dict[str, Any]:
    broad_identity = build_identity_baseline_irf(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        tdc_outcome_col=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
        total_outcome_col="broad_bank_deposits_qoq",
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="broad_scope_identity_baseline",
        bootstrap_reps=bootstrap_reps,
        bootstrap_block_length=bootstrap_block_length,
        nested_shock_spec=dict(baseline_shock_spec),
    )
    if not broad_identity.empty:
        broad_identity = broad_identity.copy()
        broad_identity.loc[broad_identity["outcome"] == "other_component_qoq", "outcome"] = "broad_bank_other_component_qoq"

    broad_strict_lp = run_local_projections(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[
            "broad_strict_loan_foreign_offices_qoq",
            "broad_strict_loan_affiliated_areas_qoq",
            "broad_strict_loan_source_qoq",
            "broad_strict_gap_qoq",
        ],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=bool(baseline_lp_spec.get("include_lagged_outcome", False)),
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="broad_scope_strict_baseline",
    )

    broad_system_key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        broad_total = _snapshot(_lp_row(broad_identity, outcome="broad_bank_deposits_qoq", horizon=horizon))
        broad_residual = _snapshot(_lp_row(broad_identity, outcome="broad_bank_other_component_qoq", horizon=horizon))
        broad_tdc = _snapshot(_lp_row(broad_identity, outcome=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")), horizon=horizon))
        broad_strict_foreign = _snapshot(_lp_row(broad_strict_lp, outcome="broad_strict_loan_foreign_offices_qoq", horizon=horizon))
        broad_strict_aff = _snapshot(_lp_row(broad_strict_lp, outcome="broad_strict_loan_affiliated_areas_qoq", horizon=horizon))
        broad_strict_total = _snapshot(_lp_row(broad_strict_lp, outcome="broad_strict_loan_source_qoq", horizon=horizon))
        broad_strict_gap = _snapshot(_lp_row(broad_strict_lp, outcome="broad_strict_gap_qoq", horizon=horizon))
        gap_share = (
            None
            if broad_residual is None or broad_strict_gap is None or float(broad_residual["beta"]) == 0.0
            else abs(float(broad_strict_gap["beta"])) / abs(float(broad_residual["beta"]))
        )
        if gap_share is None:
            interpretation = "missing_alignment_inputs"
        elif gap_share > 0.8:
            interpretation = "large_broad_scope_strict_gap"
        elif gap_share > 0.4:
            interpretation = "partial_broad_scope_strict_coverage"
        else:
            interpretation = "material_broad_scope_strict_coverage"
        if any(item is not None for item in (broad_total, broad_residual, broad_strict_total, broad_strict_gap)):
            broad_system_key_horizons[f"h{horizon}"] = {
                "broad_tdc": broad_tdc,
                "broad_deposits": broad_total,
                "broad_other_component": broad_residual,
                "broad_strict_loan_foreign_offices": broad_strict_foreign,
                "broad_strict_loan_affiliated_areas": broad_strict_aff,
                "broad_strict_loan_source": broad_strict_total,
                "broad_strict_gap": broad_strict_gap,
                "broad_strict_gap_share_of_residual": gap_share,
                "interpretation": interpretation,
            }

    usc_context: dict[str, Any] = {"key_horizons": {}}
    if scope_alignment_summary is not None:
        total_concept = dict(scope_alignment_summary.get("deposit_concepts", {})).get("total_deposits_including_interbank", {})
        for horizon in horizons:
            h = dict(total_concept.get("key_horizons", {}).get(f"h{horizon}", {}))
            usc_variant = dict(h.get("variants", {}).get("us_chartered_bank_only", {}))
            usc_context["key_horizons"][f"h{horizon}"] = {
                "us_chartered_total_response": usc_variant.get("total_response"),
                "us_chartered_residual_response": usc_variant.get("residual_response"),
                "us_chartered_residual_shift_vs_baseline_beta": (
                    usc_variant.get("differences_vs_baseline_beta", {}).get("residual_response")
                ),
            }
    if strict_identifiable_followup_summary is not None:
        scope_gap_block = dict(strict_identifiable_followup_summary.get("scope_check_gap_assessment", {}))
        for horizon in horizons:
            horizon_key = f"h{horizon}"
            current = dict(usc_context["key_horizons"].get(horizon_key, {}))
            usc_scope_gap = (
                dict(scope_gap_block.get("key_horizons", {}).get(horizon_key, {}))
                .get("variant_gap_assessments", {})
                .get("us_chartered_bank_only", {})
            )
            current["us_chartered_remaining_share_of_baseline_strict_gap"] = usc_scope_gap.get(
                "remaining_share_of_baseline_strict_gap"
            )
            usc_context["key_horizons"][horizon_key] = current

    tdc_component_audit = (
        {"status": "not_available", "key_horizons": {}}
        if tdc_treatment_audit_summary is None
        else {
            "status": str(tdc_treatment_audit_summary.get("status", "not_available")),
            "artifact": "tdc_treatment_audit_summary.json",
            "key_horizons": dict(tdc_treatment_audit_summary.get("key_horizons", {})),
            "takeaways": list(tdc_treatment_audit_summary.get("takeaways", [])),
        }
    )

    takeaways = [
        "This artifact compares two matched-scope system reads rather than adding another narrow sensitivity: the current U.S.-chartered matched-bank-leg comparison and a broad-bank matched system built on broad-bank deposits plus a broad loan-led strict core.",
        "The broad matched system keeps the broad headline TDC object and matches it to a broad-bank deposit outcome plus a broad loans-only strict core, rather than trying to force the current U.S.-chartered outcome to absorb the broader treatment.",
    ]
    broad_h0 = broad_system_key_horizons.get("h0", {})
    broad_h0_gap_share = broad_h0.get("broad_strict_gap_share_of_residual")
    usc_h0_remaining = usc_context.get("key_horizons", {}).get("h0", {}).get("us_chartered_remaining_share_of_baseline_strict_gap")
    if broad_h0_gap_share is not None:
        takeaways.append(
            f"At h0, the broad matched-scope system leaves about {float(broad_h0_gap_share):.2f} of the broad non-TDC residual in the broad strict gap."
        )
    if broad_h0_gap_share is not None and usc_h0_remaining is not None:
        comparison = "smaller" if float(broad_h0_gap_share) < float(usc_h0_remaining) else "not smaller"
        takeaways.append(
            f"Compared with the U.S.-chartered matched-bank-leg read, the broad matched-scope strict gap is {comparison} at h0 ({float(broad_h0_gap_share):.2f} versus {float(usc_h0_remaining):.2f})."
        )
    audit_h0 = tdc_component_audit.get("key_horizons", {}).get("h0", {})
    largest_variant = audit_h0.get("largest_residual_shift_variant")
    largest_shift = audit_h0.get("largest_abs_residual_shift_beta")
    if largest_variant is not None and largest_shift is not None:
        takeaways.append(
            f"At h0, the largest treatment-side residual shift in the component audit comes from `{largest_variant}`, with an absolute change of about {float(largest_shift):.2f}."
        )
    no_row_shift = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("domestic_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    no_foreign_bank_shift = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("no_foreign_bank_sectors", {})
        .get("residual_shift_vs_baseline_beta")
    )
    if no_row_shift is not None and no_foreign_bank_shift is not None:
        takeaways.append(
            f"At h0, removing only ROW changes the headline residual by about {float(no_row_shift):.2f}, while removing only foreign bank-sector Treasury legs changes it by about {float(no_foreign_bank_shift):.2f}."
        )

    return {
        "status": "available" if broad_system_key_horizons else "not_available",
        "headline_question": "Does a broad matched-scope system materially reduce the strict gap, and which TDC components move the residual most when removed?",
        "estimation_path": {
            "broad_identity_mode": "exact_identity_baseline",
            "broad_identity_spec_name": "broad_scope_identity_baseline",
            "broad_strict_spec_name": "broad_scope_strict_baseline",
            "summary_artifact": "broad_scope_system_summary.json",
        },
        "usc_matched_context": usc_context,
        "broad_matched_system": {
            "total_outcome": "broad_bank_deposits_qoq",
            "residual_outcome": "broad_bank_other_component_qoq",
            "strict_core_outcome": "broad_strict_loan_source_qoq",
            "key_horizons": broad_system_key_horizons,
        },
        "tdc_component_audit": tdc_component_audit,
        "takeaways": takeaways,
    }
