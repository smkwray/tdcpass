from __future__ import annotations

from typing import Any

import pandas as pd


def _mechanism_scope(
    *,
    readiness_status: str,
    structural_proxy_evidence: dict[str, Any] | None,
    proxy_coverage_summary: dict[str, Any] | None,
) -> str:
    structural_status = str((structural_proxy_evidence or {}).get("status", "weak"))
    coverage_status = str((proxy_coverage_summary or {}).get("status", "weak"))
    if readiness_status == "ready_for_interpretation" and structural_status == "supportive" and coverage_status == "supportive":
        return "deposit_response_and_mechanism"
    if readiness_status in {"ready_for_interpretation", "provisional"}:
        return "deposit_response_with_partial_mechanism_cross_checks"
    return "deposit_response_only"


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _ci_excludes_zero(row: dict[str, Any] | None) -> bool:
    if row is None:
        return False
    return float(row["lower95"]) > 0.0 or float(row["upper95"]) < 0.0


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "ci_excludes_zero": _ci_excludes_zero(row),
    }


def _horizon_assessment(total_row: dict[str, Any] | None, other_row: dict[str, Any] | None) -> dict[str, Any]:
    total = _snapshot(total_row)
    other = _snapshot(other_row)
    gap = None
    if total_row is not None and other_row is not None:
        gap = float(total_row["beta"]) - float(other_row["beta"])

    label = "not_separated"
    if total and other:
        if total["ci_excludes_zero"] and total["beta"] > 0 and other["ci_excludes_zero"] and other["beta"] < 0:
            label = "crowd_out_signal"
        elif total["ci_excludes_zero"] and total["beta"] > 0 and other["ci_excludes_zero"] and other["beta"] > 0:
            label = "total_up_other_up"
        elif total["ci_excludes_zero"] and total["beta"] < 0 and other["ci_excludes_zero"] and other["beta"] < 0:
            label = "total_down_other_down"
        elif total["ci_excludes_zero"] and total["beta"] > 0:
            label = "total_up_other_unclear"
        elif other["ci_excludes_zero"] and other["beta"] < 0:
            label = "other_down_total_unclear"

    return {
        "total_deposits": total,
        "other_component": other,
        "beta_gap_total_minus_other": gap,
        "same_sign": (
            total is not None
            and other is not None
            and ((total["beta"] >= 0 and other["beta"] >= 0) or (total["beta"] <= 0 and other["beta"] <= 0))
        ),
        "assessment": label,
    }


def _contrast_row(contrast: pd.DataFrame, *, scope: str, variant: str, horizon: int) -> dict[str, Any] | None:
    if contrast.empty:
        return None
    sample = contrast[
        (contrast["scope"] == scope) & (contrast["variant"] == variant) & (contrast["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _variant_rows(
    df: pd.DataFrame,
    *,
    variant_column: str,
    role_column: str,
    allowed_roles: set[str],
    horizons: tuple[int, ...],
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    rows: list[dict[str, Any]] = []
    sample = df[df[role_column].isin(allowed_roles)].copy()
    for variant in sample[variant_column].drop_duplicates().tolist():
        variant_df = sample[sample[variant_column] == variant]
        role = str(variant_df.iloc[0][role_column])
        horizon_rows: dict[str, Any] = {}
        for horizon in horizons:
            total_row = _lp_row(variant_df, outcome="total_deposits_bank_qoq", horizon=horizon)
            other_row = _lp_row(variant_df, outcome="other_component_qoq", horizon=horizon)
            horizon_rows[f"h{horizon}"] = _horizon_assessment(total_row, other_row)
        rows.append({"variant": str(variant), "role": role, "horizons": horizon_rows})
    return rows


def _sample_variant_rows(sample_sensitivity: pd.DataFrame, *, horizons: tuple[int, ...]) -> list[dict[str, Any]]:
    if sample_sensitivity.empty:
        return []
    rows: list[dict[str, Any]] = []
    for sample_variant in sample_sensitivity["sample_variant"].drop_duplicates().tolist():
        variant_df = sample_sensitivity[sample_sensitivity["sample_variant"] == sample_variant]
        role = str(variant_df.iloc[0]["sample_role"])
        sample_filter = str(variant_df.iloc[0].get("sample_filter", ""))
        horizon_rows: dict[str, Any] = {}
        for horizon in horizons:
            total_row = _lp_row(variant_df, outcome="total_deposits_bank_qoq", horizon=horizon)
            other_row = _lp_row(variant_df, outcome="other_component_qoq", horizon=horizon)
            tdc_row = _lp_row(variant_df, outcome="tdc_bank_only_qoq", horizon=horizon)
            horizon_rows[f"h{horizon}"] = _horizon_assessment(total_row, other_row)
            horizon_rows[f"h{horizon}"]["direct_tdc_response"] = _snapshot(tdc_row)
        rows.append(
            {
                "sample_variant": str(sample_variant),
                "sample_role": role,
                "sample_filter": sample_filter,
                "horizons": horizon_rows,
            }
        )
    return rows


def _flagged_window_robustness(
    sample_variants: list[dict[str, Any]],
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    headline = next((row for row in sample_variants if row.get("sample_variant") == "all_usable_shocks"), None)
    drop_flagged = next((row for row in sample_variants if row.get("sample_variant") == "drop_flagged_shocks"), None)
    drop_severe = next((row for row in sample_variants if row.get("sample_variant") == "drop_severe_scale_tail"), None)
    compared = [row for row in [drop_flagged, drop_severe] if row is not None]
    if headline is None or not compared:
        return {
            "status": "not_available",
            "headline_sign_pattern_stable": None,
            "note": "No flagged-window robustness comparison is available.",
        }

    stable = True
    details: dict[str, Any] = {}
    for horizon in horizons:
        key = f"h{horizon}"
        baseline_assessment = str(headline["horizons"].get(key, {}).get("assessment", "missing"))
        details[key] = {
            "headline_assessment": baseline_assessment,
            "comparisons": [],
        }
        for row in compared:
            variant_assessment = str(row["horizons"].get(key, {}).get("assessment", "missing"))
            same_assessment = variant_assessment == baseline_assessment
            stable = stable and same_assessment
            details[key]["comparisons"].append(
                {
                    "sample_variant": str(row.get("sample_variant", "")),
                    "assessment": variant_assessment,
                    "matches_headline_assessment": same_assessment,
                }
            )

    if stable:
        status = "stable"
        note = (
            "Dropping flagged windows or only the severe realized-scale tail does not overturn the headline h0/h4 sign pattern."
        )
    else:
        status = "changed"
        note = "Flagged-window trims materially change the headline h0/h4 sign pattern."

    return {
        "status": status,
        "headline_sign_pattern_stable": stable,
        "note": note,
        "details": details,
    }


def _regime_rows(lp_irf_regimes: pd.DataFrame, horizons: tuple[int, ...]) -> list[dict[str, Any]]:
    if lp_irf_regimes.empty:
        return []
    regimes: list[dict[str, Any]] = []
    base_names = sorted({name.rsplit("_", 1)[0] for name in lp_irf_regimes["regime"].drop_duplicates().tolist()})
    for base_name in base_names:
        horizon_rows: dict[str, Any] = {}
        for horizon in horizons:
            high_row = _lp_row(
                lp_irf_regimes[lp_irf_regimes["regime"] == f"{base_name}_high"],
                outcome="total_deposits_bank_qoq",
                horizon=horizon,
            )
            low_row = _lp_row(
                lp_irf_regimes[lp_irf_regimes["regime"] == f"{base_name}_low"],
                outcome="total_deposits_bank_qoq",
                horizon=horizon,
            )
            horizon_rows[f"h{horizon}"] = {"high": _snapshot(high_row), "low": _snapshot(low_row)}
        regimes.append({"regime": base_name, "horizons": horizon_rows})
    return regimes


def _counterpart_channel_context(
    counterpart_channel_scorecard: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if counterpart_channel_scorecard is None:
        return {}
    key_horizons = dict(counterpart_channel_scorecard.get("key_horizons", {}))
    return {
        "status": str(counterpart_channel_scorecard.get("status", "not_available")),
        "artifact": "counterpart_channel_scorecard.json",
        "legacy_private_credit_proxy_role": str(
            counterpart_channel_scorecard.get("legacy_private_credit_proxy_role", "coarse_legacy_creator_proxy")
        ),
        "creator_channel_outcomes_present": list(counterpart_channel_scorecard.get("creator_channel_outcomes_present", [])),
        "key_horizons": {
            f"h{horizon}": {
                "other_component": dict((key_horizons.get(f"h{horizon}") or {}).get("other_component") or {})
                if (key_horizons.get(f"h{horizon}") or {}).get("other_component") is not None
                else None,
                "legacy_private_credit_proxy": dict((key_horizons.get(f"h{horizon}") or {}).get("legacy_private_credit_proxy") or {}),
                "decisive_positive_creator_channels": list(
                    (key_horizons.get(f"h{horizon}") or {}).get("decisive_positive_creator_channels", [])
                ),
                "decisive_negative_creator_channels": list(
                    (key_horizons.get(f"h{horizon}") or {}).get("decisive_negative_creator_channels", [])
                ),
                "decisive_positive_asset_purchase_channels": list(
                    (key_horizons.get(f"h{horizon}") or {}).get("decisive_positive_asset_purchase_channels", [])
                ),
                "decisive_positive_retention_support_channels": list(
                    (key_horizons.get(f"h{horizon}") or {}).get("decisive_positive_retention_support_channels", [])
                ),
                "decisive_negative_retention_support_channels": list(
                    (key_horizons.get(f"h{horizon}") or {}).get("decisive_negative_retention_support_channels", [])
                ),
                "escape_support_context": dict(
                    (key_horizons.get(f"h{horizon}") or {}).get("escape_support_context") or {}
                ),
                "asset_purchase_plumbing_context": dict(
                    (key_horizons.get(f"h{horizon}") or {}).get("asset_purchase_plumbing_context") or {}
                ),
                "proxy_coverage_label": (key_horizons.get(f"h{horizon}") or {}).get("proxy_coverage_label"),
            }
            for horizon in horizons
        },
        "takeaways": list(counterpart_channel_scorecard.get("takeaways", [])),
    }


def _scope_alignment_context(
    scope_alignment_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if scope_alignment_summary is None:
        return {}
    if str(scope_alignment_summary.get("status", "not_available")) != "available":
        return {
            "status": str(scope_alignment_summary.get("status", "not_available")),
            "artifact": "scope_alignment_summary.json",
            "headline_read": "Scope-alignment diagnostics are not available in the current run.",
            "takeaways": list(scope_alignment_summary.get("takeaways", [])),
        }
    deposit_concepts = dict(scope_alignment_summary.get("deposit_concepts", {}))
    total_concept = dict(deposit_concepts.get("total_deposits_including_interbank", {}))
    key_horizons = dict(total_concept.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    h0_variants = dict(h0.get("variants", {}))
    domestic_delta = (
        h0_variants.get("domestic_bank_only", {})
        .get("differences_vs_baseline_beta", {})
        .get("residual_response")
    )
    us_chartered_delta = (
        h0_variants.get("us_chartered_bank_only", {})
        .get("differences_vs_baseline_beta", {})
        .get("residual_response")
    )
    headline_read = "Scope-alignment diagnostics are available but do not contain an h0 comparison for the headline total-deposits concept."
    if domestic_delta is not None and us_chartered_delta is not None:
        headline_read = (
            "Scope mismatch is real but partial: at h0, removing only the rest-of-world term makes the headline residual "
            f"about {float(domestic_delta):.2f} less negative, while the true U.S.-chartered bank-leg match makes it about "
            f"{float(us_chartered_delta):.2f} less negative."
        )
    elif us_chartered_delta is not None:
        headline_read = (
            "Scope mismatch is real but partial: at h0, the true U.S.-chartered bank-leg match makes the headline residual "
            f"about {float(us_chartered_delta):.2f} less negative."
        )

    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "baseline_residual_response": dict(horizon_payload.get("baseline", {}).get("residual_response") or {})
            if horizon_payload.get("baseline", {}).get("residual_response") is not None
            else None,
            "domestic_bank_only_residual_delta": (
                horizon_payload.get("variants", {})
                .get("domestic_bank_only", {})
                .get("differences_vs_baseline_beta", {})
                .get("residual_response")
            ),
            "us_chartered_bank_only_residual_delta": (
                horizon_payload.get("variants", {})
                .get("us_chartered_bank_only", {})
                .get("differences_vs_baseline_beta", {})
                .get("residual_response")
            ),
        }

    return {
        "status": "available",
        "artifact": "scope_alignment_summary.json",
        "headline_read": headline_read,
        "headline_total_deposit_concept": "total_deposits_including_interbank",
        "recommended_release_comparison": dict(scope_alignment_summary.get("recommended_policy") or {}),
        "variant_definitions": {
            "baseline": dict(scope_alignment_summary.get("variant_definitions", {}).get("baseline") or {}),
            "domestic_bank_only": dict(
                scope_alignment_summary.get("variant_definitions", {}).get("domestic_bank_only") or {}
            ),
            "us_chartered_bank_only": dict(
                scope_alignment_summary.get("variant_definitions", {}).get("us_chartered_bank_only") or {}
            ),
        },
        "key_horizons": compact_horizons,
        "takeaways": list(scope_alignment_summary.get("takeaways", [])),
    }


def _strict_gap_scope_check_context(
    strict_identifiable_followup_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if strict_identifiable_followup_summary is None:
        return {}
    if str(strict_identifiable_followup_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_identifiable_followup_summary.get("status", "not_available")),
            "artifact": "strict_identifiable_followup_summary.json",
            "headline_read": "Strict-gap scope-check diagnostics are not available in the current run.",
        }
    scope_block = dict(strict_identifiable_followup_summary.get("scope_check_gap_assessment", {}))
    key_horizons = dict(scope_block.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    preferred = dict(h0.get("variant_gap_assessments", {}).get("us_chartered_bank_only", {}))
    remaining_share = preferred.get("remaining_share_of_baseline_strict_gap")
    headline_read = (
        "Strict-gap scope-check diagnostics are available but do not contain an h0 matched-bank-leg comparison."
    )
    if remaining_share is not None:
        headline_read = (
            "The matched-bank-leg scope check only relieves a small share of the current strict gap: at h0, about "
            f"{float(remaining_share):.2f} of the baseline strict gap remains when the residual-side scope shift is "
            "applied while holding the direct-count strict total fixed."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        preferred_h = dict(horizon_payload.get("variant_gap_assessments", {}).get("us_chartered_bank_only", {}))
        secondary_h = dict(horizon_payload.get("variant_gap_assessments", {}).get("domestic_bank_only", {}))
        compact_horizons[f"h{horizon}"] = {
            "baseline_strict_gap_beta": horizon_payload.get("baseline_strict_gap_beta"),
            "us_chartered_remaining_share_of_baseline_strict_gap": preferred_h.get(
                "remaining_share_of_baseline_strict_gap"
            ),
            "us_chartered_relief_share_of_baseline_strict_gap": preferred_h.get(
                "relief_share_of_baseline_strict_gap"
            ),
            "domestic_remaining_share_of_baseline_strict_gap": secondary_h.get(
                "remaining_share_of_baseline_strict_gap"
            ),
        }
    return {
        "status": "available",
        "artifact": "strict_identifiable_followup_summary.json",
        "assumption": str(scope_block.get("assumption", "")),
        "headline_read": headline_read,
        "preferred_variant": "us_chartered_bank_only",
        "secondary_variant": "domestic_bank_only",
        "key_horizons": compact_horizons,
    }


def _broad_scope_system_context(
    broad_scope_system_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if broad_scope_system_summary is None:
        return {}
    if str(broad_scope_system_summary.get("status", "not_available")) != "available":
        return {
            "status": str(broad_scope_system_summary.get("status", "not_available")),
            "artifact": "broad_scope_system_summary.json",
            "headline_read": "Broad matched-scope diagnostics are not available in the current run.",
        }
    broad_horizons = dict(broad_scope_system_summary.get("broad_matched_system", {}).get("key_horizons", {}))
    h0 = dict(broad_horizons.get("h0", {}))
    gap_share = h0.get("broad_strict_gap_share_of_residual")
    headline_read = "Broad matched-scope diagnostics are available but do not contain an h0 broad strict-gap comparison."
    if gap_share is not None:
        headline_read = (
            "The broad matched-scope system still leaves a large direct-count gap: at h0, about "
            f"{float(gap_share):.2f} of the broad non-TDC residual remains in the broad strict gap."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(broad_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "broad_other_component": horizon_payload.get("broad_other_component"),
            "broad_strict_loan_source": horizon_payload.get("broad_strict_loan_source"),
            "broad_strict_gap": horizon_payload.get("broad_strict_gap"),
            "broad_strict_gap_share_of_residual": horizon_payload.get("broad_strict_gap_share_of_residual"),
            "interpretation": horizon_payload.get("interpretation"),
        }
    audit_h0 = dict(broad_scope_system_summary.get("tdc_component_audit", {}).get("key_horizons", {}).get("h0", {}))
    return {
        "status": "available",
        "artifact": "broad_scope_system_summary.json",
        "headline_read": headline_read,
        "headline_system": "broad_matched_scope",
        "largest_h0_treatment_shift_variant": audit_h0.get("largest_residual_shift_variant"),
        "key_horizons": compact_horizons,
        "takeaways": list(broad_scope_system_summary.get("takeaways", [])),
    }


def _tdc_treatment_audit_context(
    tdc_treatment_audit_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if tdc_treatment_audit_summary is None:
        return {}
    if str(tdc_treatment_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(tdc_treatment_audit_summary.get("status", "not_available")),
            "artifact": "tdc_treatment_audit_summary.json",
            "headline_read": "Direct TDC treatment-audit diagnostics are not available in the current run.",
        }
    key_horizons = dict(tdc_treatment_audit_summary.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    dominant_component = h0.get("dominant_signed_component")
    no_row_shift = (
        h0.get("variant_removal_diagnostics", {})
        .get("domestic_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    no_toc_shift = (
        h0.get("variant_removal_diagnostics", {})
        .get("no_toc_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    no_foreign_shift = (
        h0.get("variant_removal_diagnostics", {})
        .get("no_foreign_bank_sectors", {})
        .get("residual_shift_vs_baseline_beta")
    )
    headline_read = "Direct TDC treatment-audit diagnostics are available but do not contain an h0 component ranking."
    if dominant_component is not None:
        headline_read = (
            "The direct TDC treatment audit now identifies the leading signed building block at h0: "
            f"`{dominant_component}`."
        )
    if dominant_component is not None and no_row_shift is not None and no_foreign_shift is not None:
        headline_read = (
            f"The direct TDC treatment audit identifies `{dominant_component}` as the leading signed h0 component, "
            f"while the removal tests show ROW is the larger residual mover ({float(no_row_shift):.2f} versus {float(no_foreign_shift):.2f} for foreign bank sectors)."
        )
    if dominant_component is not None and no_toc_shift is not None and no_row_shift is not None:
        headline_read = (
            f"The direct TDC treatment audit identifies `{dominant_component}` as the leading signed h0 component; the removal tests now compare Treasury operating cash and ROW directly ({float(no_toc_shift):.2f} versus {float(no_row_shift):.2f})."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "baseline_tdc_response": horizon_payload.get("baseline_tdc_response"),
            "dominant_signed_component": horizon_payload.get("dominant_signed_component"),
            "largest_residual_shift_variant": horizon_payload.get("largest_residual_shift_variant"),
            "signed_component_sum_beta": horizon_payload.get("signed_component_sum_beta"),
            "signed_component_sum_minus_direct_tdc_beta": horizon_payload.get("signed_component_sum_minus_direct_tdc_beta"),
            "no_toc_residual_shift_vs_baseline_beta": (
                horizon_payload.get("variant_removal_diagnostics", {})
                .get("no_toc_bank_only", {})
                .get("residual_shift_vs_baseline_beta")
            ),
            "rest_of_world_residual_shift_vs_baseline_beta": (
                horizon_payload.get("variant_removal_diagnostics", {})
                .get("domestic_bank_only", {})
                .get("residual_shift_vs_baseline_beta")
            ),
            "foreign_bank_sector_residual_shift_vs_baseline_beta": (
                horizon_payload.get("variant_removal_diagnostics", {})
                .get("no_foreign_bank_sectors", {})
                .get("residual_shift_vs_baseline_beta")
            ),
        }
    return {
        "status": "available",
        "artifact": "tdc_treatment_audit_summary.json",
        "headline_read": headline_read,
        "key_horizons": compact_horizons,
        "takeaways": list(tdc_treatment_audit_summary.get("takeaways", [])),
    }


def _treasury_operating_cash_audit_context(
    treasury_operating_cash_audit_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if treasury_operating_cash_audit_summary is None:
        return {}
    if str(treasury_operating_cash_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(treasury_operating_cash_audit_summary.get("status", "not_available")),
            "artifact": "treasury_operating_cash_audit_summary.json",
            "headline_read": "Treasury-operating-cash audit diagnostics are not available in the current run.",
        }
    quarterly_alignment = dict(treasury_operating_cash_audit_summary.get("quarterly_alignment", {}))
    key_horizons = dict(treasury_operating_cash_audit_summary.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    toc_response = dict(h0.get("treasury_operating_cash_response", {}) or {})
    tga_response = dict(h0.get("tga_response", {}) or {})
    reserves_response = dict(h0.get("reserves_response", {}) or {})
    interpretation = h0.get("interpretation")
    corr = quarterly_alignment.get("contemporaneous_corr_tga_vs_toc")
    slope = dict(quarterly_alignment.get("ols_tga_on_toc", {})).get("slope")
    headline_read = "Treasury-operating-cash audit diagnostics are available but do not contain an h0 plumbing snapshot."
    if toc_response and tga_response and reserves_response:
        headline_read = (
            "The Treasury-operating-cash audit points to a genuine cash-plumbing pattern rather than a simple sign bug: "
            f"at h0, TOC ≈ {float(toc_response['beta']):.2f}, TGA ≈ {float(tga_response['beta']):.2f}, "
            f"reserves ≈ {float(reserves_response['beta']):.2f}."
        )
    if interpretation is not None and corr is not None and slope is not None:
        headline_read += (
            f" Quarter-level TOC/TGA alignment is also tight (corr ≈ {float(corr):.2f}, slope ≈ {float(slope):.2f}; "
            f"interpretation = `{interpretation}`)."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "treasury_operating_cash_response": horizon_payload.get("treasury_operating_cash_response"),
            "treasury_operating_cash_signed_contribution_beta": horizon_payload.get(
                "treasury_operating_cash_signed_contribution_beta"
            ),
            "tga_response": horizon_payload.get("tga_response"),
            "reserves_response": horizon_payload.get("reserves_response"),
            "cb_nonts_response": horizon_payload.get("cb_nonts_response"),
            "toc_minus_tga_beta_gap": horizon_payload.get("toc_minus_tga_beta_gap"),
            "interpretation": horizon_payload.get("interpretation"),
        }
    return {
        "status": "available",
        "artifact": "treasury_operating_cash_audit_summary.json",
        "headline_read": headline_read,
        "quarterly_alignment": {
            "contemporaneous_corr_tga_vs_toc": quarterly_alignment.get("contemporaneous_corr_tga_vs_toc"),
            "ols_tga_on_toc": quarterly_alignment.get("ols_tga_on_toc"),
            "sign_match_share_tga_vs_toc": quarterly_alignment.get("sign_match_share_tga_vs_toc"),
        },
        "key_horizons": compact_horizons,
        "takeaways": list(treasury_operating_cash_audit_summary.get("takeaways", [])),
    }


def _rest_of_world_treasury_audit_context(
    rest_of_world_treasury_audit_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if rest_of_world_treasury_audit_summary is None:
        return {}
    if str(rest_of_world_treasury_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(rest_of_world_treasury_audit_summary.get("status", "not_available")),
            "artifact": "rest_of_world_treasury_audit_summary.json",
            "headline_read": "ROW Treasury-leg audit diagnostics are not available in the current run.",
        }
    quarterly_alignment = dict(rest_of_world_treasury_audit_summary.get("quarterly_alignment", {}))
    key_horizons = dict(rest_of_world_treasury_audit_summary.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    row_response = dict(h0.get("rest_of_world_treasury_response", {}) or {})
    foreign_nonts = dict(h0.get("foreign_nonts_response", {}) or {})
    row_deposits = dict(h0.get("checkable_rest_of_world_bank_response", {}) or {})
    foreign_bank_assets = dict(h0.get("interbank_transactions_foreign_banks_asset_response", {}) or {})
    interpretation = h0.get("interpretation")
    counterparts = dict(quarterly_alignment.get("counterparts", {}))
    foreign_nonts_corr = dict(counterparts.get("foreign_nonts_qoq", {})).get("contemporaneous_corr")
    row_deposits_corr = dict(counterparts.get("checkable_rest_of_world_bank_qoq", {})).get("contemporaneous_corr")
    headline_read = "ROW Treasury-leg audit diagnostics are available but do not contain an h0 external-counterpart snapshot."
    if row_response and foreign_nonts and foreign_bank_assets:
        headline_read = (
            "The ROW Treasury-leg audit points away from a simple same-quarter deposit-liability story: "
            f"at h0, ROW ≈ {float(row_response['beta']):.2f}, foreign NONTS ≈ {float(foreign_nonts['beta']):.2f}, "
            f"foreign-bank interbank assets ≈ {float(foreign_bank_assets['beta']):.2f}."
        )
    if foreign_nonts_corr is not None and row_deposits_corr is not None and interpretation is not None:
        headline_read += (
            f" Quarter-level same-quarter alignment is weak (foreign NONTS corr ≈ {float(foreign_nonts_corr):.2f}, "
            f"ROW bank-deposit corr ≈ {float(row_deposits_corr):.2f}; interpretation = `{interpretation}`)."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "rest_of_world_treasury_response": horizon_payload.get("rest_of_world_treasury_response"),
            "foreign_nonts_response": horizon_payload.get("foreign_nonts_response"),
            "checkable_rest_of_world_bank_response": horizon_payload.get("checkable_rest_of_world_bank_response"),
            "interbank_transactions_foreign_banks_liability_response": horizon_payload.get(
                "interbank_transactions_foreign_banks_liability_response"
            ),
            "interbank_transactions_foreign_banks_asset_response": horizon_payload.get(
                "interbank_transactions_foreign_banks_asset_response"
            ),
            "deposits_at_foreign_banks_asset_response": horizon_payload.get("deposits_at_foreign_banks_asset_response"),
            "interpretation": horizon_payload.get("interpretation"),
        }
    return {
        "status": "available",
        "artifact": "rest_of_world_treasury_audit_summary.json",
        "headline_read": headline_read,
        "quarterly_alignment": {
            "foreign_nonts_contemporaneous_corr": foreign_nonts_corr,
            "checkable_rest_of_world_bank_contemporaneous_corr": row_deposits_corr,
            "foreign_nonts_lead_lag_correlations": dict(counterparts.get("foreign_nonts_qoq", {})).get(
                "lead_lag_correlations"
            ),
            "checkable_rest_of_world_bank_lead_lag_correlations": dict(
                counterparts.get("checkable_rest_of_world_bank_qoq", {})
            ).get("lead_lag_correlations"),
        },
        "key_horizons": compact_horizons,
        "takeaways": list(rest_of_world_treasury_audit_summary.get("takeaways", [])),
    }


def _toc_row_path_split_context(
    toc_row_path_split_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if toc_row_path_split_summary is None:
        return {}
    if str(toc_row_path_split_summary.get("status", "not_available")) != "available":
        return {
            "status": str(toc_row_path_split_summary.get("status", "not_available")),
            "artifact": "toc_row_path_split_summary.json",
            "headline_read": "TOC/ROW path-split diagnostics are not available in the current run.",
        }
    quarterly_split = dict(toc_row_path_split_summary.get("quarterly_split", {}))
    key_horizons = dict(toc_row_path_split_summary.get("key_horizons", {}))
    quarterly_preferred = quarterly_split.get("preferred_quarterly_path")
    quarterly_corrs = dict(quarterly_split.get("bundle_contemporaneous_corr", {}))
    broad_corr = quarterly_corrs.get("broad_support_path")
    direct_corr = quarterly_corrs.get("direct_deposit_path")
    h0 = dict(key_horizons.get("h0", {}))
    h0_preferred = h0.get("preferred_horizon_path")
    h0_broad = dict(h0.get("broad_support_path_response", {}) or {}).get("beta")
    h0_direct = dict(h0.get("direct_deposit_path_response", {}) or {}).get("beta")
    headline_read = "TOC/ROW path-split diagnostics are available but do not contain a compact quarterly-versus-horizon comparison."
    if quarterly_preferred is not None and h0_preferred is not None and broad_corr is not None and direct_corr is not None:
        headline_read = (
            "The TOC/ROW split now separates quarter-level fit from shock-response dominance: "
            f"quarter by quarter the preferred path is `{quarterly_preferred}` "
            f"(direct corr ≈ {float(direct_corr):.2f}, broad corr ≈ {float(broad_corr):.2f}), "
            f"but at h0 the preferred path is `{h0_preferred}`."
        )
    if h0_preferred is not None and h0_broad is not None and h0_direct is not None:
        headline_read = (
            "The TOC/ROW split now separates quarter-level fit from shock-response dominance: "
            f"quarter by quarter the preferred path is `{quarterly_preferred}` "
            f"(direct corr ≈ {float(direct_corr):.2f}, broad corr ≈ {float(broad_corr):.2f}), "
            f"but at h0 the preferred path is `{h0_preferred}` "
            f"(broad ≈ {float(h0_broad):.2f}, direct ≈ {float(h0_direct):.2f})."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "preferred_horizon_path": horizon_payload.get("preferred_horizon_path"),
            "direct_minus_broad_beta_gap": horizon_payload.get("direct_minus_broad_beta_gap"),
            "coverage_share_of_bundle_beta": horizon_payload.get("coverage_share_of_bundle_beta"),
        }
    return {
        "status": "available",
        "artifact": "toc_row_path_split_summary.json",
        "headline_read": headline_read,
        "quarterly_split": quarterly_split,
        "key_horizons": compact_horizons,
        "takeaways": list(toc_row_path_split_summary.get("takeaways", [])),
    }


def _toc_row_excluded_interpretation_context(
    toc_row_excluded_interpretation_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if toc_row_excluded_interpretation_summary is None:
        return {}
    if str(toc_row_excluded_interpretation_summary.get("status", "not_available")) != "available":
        return {
            "status": str(toc_row_excluded_interpretation_summary.get("status", "not_available")),
            "artifact": "toc_row_excluded_interpretation_summary.json",
            "headline_read": "TOC/ROW-excluded interpretation diagnostics are not available in the current run.",
        }
    key_horizons = dict(toc_row_excluded_interpretation_summary.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    baseline_h0 = dict(h0.get("baseline", {}))
    excluded_h0 = dict(h0.get("toc_row_excluded", {}))
    baseline_residual = dict(baseline_h0.get("residual_response", {}) or {})
    excluded_residual = dict(excluded_h0.get("residual_response", {}) or {})
    baseline_gap_share = baseline_h0.get("strict_gap_share_of_residual")
    excluded_gap_share = excluded_h0.get("strict_gap_share_of_residual")
    headline_read = "TOC/ROW-excluded interpretation diagnostics are available but do not contain a compact h0 comparison."
    if baseline_residual and excluded_residual:
        headline_read = (
            "As a secondary comparison only, excluding TOC/ROW materially changes the h0 residual read: "
            f"baseline residual ≈ {float(baseline_residual['beta']):.2f}, "
            f"TOC/ROW-excluded residual ≈ {float(excluded_residual['beta']):.2f}."
        )
    if baseline_gap_share is not None and excluded_gap_share is not None:
        headline_read += (
            " The strict direct-count gap share moves from about "
            f"{float(baseline_gap_share):.2f} to about {float(excluded_gap_share):.2f} under that comparison."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        compact_horizons[f"h{horizon}"] = {
            "baseline_residual_response": dict(dict(horizon_payload.get("baseline", {})).get("residual_response", {}) or {}),
            "toc_row_excluded_residual_response": dict(
                dict(horizon_payload.get("toc_row_excluded", {})).get("residual_response", {}) or {}
            ),
            "baseline_strict_gap_share_of_residual": dict(horizon_payload.get("baseline", {})).get(
                "strict_gap_share_of_residual"
            ),
            "toc_row_excluded_strict_gap_share_of_residual": dict(horizon_payload.get("toc_row_excluded", {})).get(
                "strict_gap_share_of_residual"
            ),
            "excluded_minus_baseline_beta": horizon_payload.get("excluded_minus_baseline_beta"),
            "interpretation": horizon_payload.get("interpretation"),
        }
    return {
        "status": "available",
        "artifact": "toc_row_excluded_interpretation_summary.json",
        "headline_read": headline_read,
        "key_horizons": compact_horizons,
        "takeaways": list(toc_row_excluded_interpretation_summary.get("takeaways", [])),
    }


def _strict_missing_channel_context(
    strict_missing_channel_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if strict_missing_channel_summary is None:
        return {}
    if str(strict_missing_channel_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_missing_channel_summary.get("status", "not_available")),
            "artifact": "strict_missing_channel_summary.json",
            "headline_read": "Strict missing-channel diagnostics are not available in the current run.",
        }
    key_horizons = dict(strict_missing_channel_summary.get("key_horizons", {}))
    h0 = dict(key_horizons.get("h0", {}))
    excluded_h0 = dict(h0.get("toc_row_excluded", {}))
    residual = dict(excluded_h0.get("residual_response", {}) or {})
    direct_core = dict(excluded_h0.get("strict_headline_direct_core_response", {}) or {})
    loan = dict(excluded_h0.get("strict_loan_source_response", {}) or {})
    private_aug = dict(excluded_h0.get("strict_loan_core_plus_private_borrower_response", {}) or {})
    noncore = dict(excluded_h0.get("strict_loan_noncore_system_response", {}) or {})
    securities = dict(excluded_h0.get("strict_non_treasury_securities_response", {}) or {})
    net_after_funding = dict(excluded_h0.get("strict_identifiable_net_after_funding_response", {}) or {})
    gap_after_funding_share = excluded_h0.get("strict_gap_after_funding_share_of_residual_abs")
    headline_read = (
        "Strict missing-channel diagnostics are available but do not contain a compact h0 TOC/ROW-excluded comparison."
    )
    if residual and direct_core and loan and private_aug and noncore and securities and net_after_funding:
        headline_read = (
            "After excluding TOC/ROW as a diagnostic comparison, the strict lane still looks incomplete at h0: "
            f"residual ≈ {float(residual['beta']):.2f}, headline direct core ≈ {float(direct_core['beta']):.2f}, "
            f"current broad loan source ≈ {float(loan['beta']):.2f}, private-borrower-augmented core ≈ {float(private_aug['beta']):.2f}, "
            f"noncore/system diagnostic ≈ {float(noncore['beta']):.2f}, securities ≈ {float(securities['beta']):.2f}, "
            f"funding-adjusted net ≈ {float(net_after_funding['beta']):.2f}."
        )
    if gap_after_funding_share is not None:
        headline_read += (
            " The remaining gap-after-funding share of the residual is about "
            f"{float(gap_after_funding_share):.2f}."
        )
    compact_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_payload = dict(key_horizons.get(f"h{horizon}", {}))
        if not horizon_payload:
            continue
        baseline_payload = dict(horizon_payload.get("baseline", {}))
        excluded_payload = dict(horizon_payload.get("toc_row_excluded", {}))
        compact_horizons[f"h{horizon}"] = {
            "baseline_residual_response": dict(baseline_payload.get("residual_response", {}) or {}),
            "toc_row_excluded_residual_response": dict(excluded_payload.get("residual_response", {}) or {}),
            "toc_row_excluded_strict_headline_direct_core_response": dict(
                excluded_payload.get("strict_headline_direct_core_response", {}) or {}
            ),
            "toc_row_excluded_strict_loan_source_response": dict(
                excluded_payload.get("strict_loan_source_response", {}) or {}
            ),
            "toc_row_excluded_strict_non_treasury_securities_response": dict(
                excluded_payload.get("strict_non_treasury_securities_response", {}) or {}
            ),
            "toc_row_excluded_strict_identifiable_net_after_funding_response": dict(
                excluded_payload.get("strict_identifiable_net_after_funding_response", {}) or {}
            ),
            "toc_row_excluded_strict_gap_after_funding_share_of_residual_abs": excluded_payload.get(
                "strict_gap_after_funding_share_of_residual_abs"
            ),
            "interpretation": horizon_payload.get("interpretation"),
        }
    return {
        "status": "available",
        "artifact": "strict_missing_channel_summary.json",
        "headline_read": headline_read,
        "key_horizons": compact_horizons,
        "takeaways": list(strict_missing_channel_summary.get("takeaways", [])),
    }


def _strict_sign_mismatch_audit_context(
    strict_sign_mismatch_audit_summary: dict[str, Any] | None,
    *,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    if strict_sign_mismatch_audit_summary is None:
        return {}
    if str(strict_sign_mismatch_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_sign_mismatch_audit_summary.get("status", "not_available")),
            "artifact": "strict_sign_mismatch_audit_summary.json",
            "headline_read": "Strict sign-mismatch diagnostics are not available in the current run.",
        }
    shock_alignment = dict(strict_sign_mismatch_audit_summary.get("shock_alignment", {}))
    quarter_concentration = dict(strict_sign_mismatch_audit_summary.get("quarter_concentration", {}))
    gap_driver_alignment = dict(strict_sign_mismatch_audit_summary.get("gap_driver_alignment", {}))
    component_alignment = dict(strict_sign_mismatch_audit_summary.get("component_alignment", {}))
    shock_corr = shock_alignment.get("shock_corr")
    same_sign_share = shock_alignment.get("same_sign_share")
    top5_share = quarter_concentration.get("top5_abs_gap_share")
    dominant_period = quarter_concentration.get("dominant_period_bucket")
    driver_corr = dict(gap_driver_alignment.get("shock_gap_driver_correlations", {})).get(
        "baseline_minus_excluded_target_qoq"
    )
    direct_core = dict(component_alignment.get("strict_loan_core_min_qoq", {}))
    total = dict(component_alignment.get("strict_identifiable_total_qoq", {}))
    headline_read = "Strict sign-mismatch diagnostics are available but do not contain the compact overlap summary."
    if (
        shock_corr is not None
        and same_sign_share is not None
        and direct_core.get("baseline_shock_corr") is not None
        and direct_core.get("toc_row_excluded_shock_corr") is not None
        and total.get("baseline_shock_corr") is not None
        and total.get("toc_row_excluded_shock_corr") is not None
    ):
        headline_read = (
            "The TOC/ROW-excluded shock rotates materially away from the baseline shock: "
            f"overlap corr ≈ {float(shock_corr):.2f}, same-sign share ≈ {float(same_sign_share):.2f}, "
            f"headline direct-core corr ≈ {float(direct_core['baseline_shock_corr']):.2f} to {float(direct_core['toc_row_excluded_shock_corr']):.2f}, "
            f"strict identifiable total corr ≈ {float(total['baseline_shock_corr']):.2f} to {float(total['toc_row_excluded_shock_corr']):.2f}."
        )
        if top5_share is not None and dominant_period is not None and driver_corr is not None:
            headline_read += (
                f" Top five gap quarters explain ≈ {float(top5_share):.2f} of absolute gap mass, the largest share is in "
                f"`{str(dominant_period)}`, and shock-gap corr with the baseline-minus-excluded target bundle is ≈ {float(driver_corr):.2f}."
            )
    compact_horizons: dict[str, Any] = {}
    h0_context = dict(strict_sign_mismatch_audit_summary.get("h0_strict_context", {}))
    for horizon in horizons:
        horizon_payload = dict(h0_context if horizon == 0 else {})
        compact_horizons[f"h{horizon}"] = {
            "interpretation": horizon_payload.get("interpretation"),
            "strict_context": horizon_payload if horizon_payload else None,
        }
    return {
        "status": "available",
        "artifact": "strict_sign_mismatch_audit_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_sign_mismatch_audit_summary.get("interpretation"),
        "quarter_concentration": quarter_concentration,
        "gap_driver_alignment": gap_driver_alignment,
        "shock_alignment": shock_alignment,
        "component_alignment": {
            "strict_loan_core_min_qoq": direct_core,
            "strict_identifiable_total_qoq": total,
        },
        "key_horizons": compact_horizons,
        "takeaways": list(strict_sign_mismatch_audit_summary.get("takeaways", [])),
    }


def _strict_shock_composition_context(
    strict_shock_composition_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_shock_composition_summary is None:
        return {}
    if str(strict_shock_composition_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_shock_composition_summary.get("status", "not_available")),
            "artifact": "strict_shock_composition_summary.json",
            "headline_read": "Strict shock-composition diagnostics are not available in the current run.",
        }
    top_gap_quarters = list(strict_shock_composition_summary.get("top_gap_quarters", []))
    period_bucket_profiles = list(strict_shock_composition_summary.get("period_bucket_profiles", []))
    trim_diagnostics = dict(strict_shock_composition_summary.get("trim_diagnostics", {}))
    dominant_bucket = period_bucket_profiles[0]["period_bucket"] if period_bucket_profiles else None
    top5 = dict(trim_diagnostics.get("drop_top5_gap_quarters", {}))
    drop_covid = dict(trim_diagnostics.get("drop_covid_post", {}))
    headline_read = "Strict shock-composition diagnostics are available but do not contain the compact trim summary."
    if (
        dominant_bucket is not None
        and top5
        and drop_covid
        and top5.get("shock_corr") is not None
        and top5.get("same_sign_share") is not None
        and drop_covid.get("shock_corr") is not None
        and drop_covid.get("same_sign_share") is not None
    ):
        headline_read = (
            "Shock composition diagnostics show the rotation is led by "
            f"`{str(dominant_bucket)}`, but not by only a handful of quarters: dropping the top five gaps gives "
            f"corr ≈ {float(top5.get('shock_corr')):.2f}, same-sign share ≈ {float(top5.get('same_sign_share')):.2f}, "
            f"while dropping `covid_post` gives corr ≈ {float(drop_covid.get('shock_corr')):.2f}, "
            f"same-sign share ≈ {float(drop_covid.get('same_sign_share')):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_shock_composition_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_shock_composition_summary.get("interpretation"),
        "top_gap_quarters": top_gap_quarters,
        "period_bucket_profiles": period_bucket_profiles,
        "trim_diagnostics": trim_diagnostics,
        "takeaways": list(strict_shock_composition_summary.get("takeaways", [])),
    }


def _strict_top_gap_quarter_audit_context(
    strict_top_gap_quarter_audit_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_quarter_audit_summary is None:
        return {}
    if str(strict_top_gap_quarter_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_quarter_audit_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_quarter_audit_summary.json",
            "headline_read": "Strict top-gap quarter diagnostics are not available in the current run.",
        }
    dominant_leg_summary = list(strict_top_gap_quarter_audit_summary.get("dominant_leg_summary", []))
    contribution_pattern_summary = list(strict_top_gap_quarter_audit_summary.get("contribution_pattern_summary", []))
    top_gap_quarters = list(strict_top_gap_quarter_audit_summary.get("top_gap_quarters", []))
    headline_read = "Strict top-gap quarter diagnostics are available but do not contain the compact TOC/ROW composition summary."
    if dominant_leg_summary and contribution_pattern_summary:
        dominant = dominant_leg_summary[0]
        contribution = contribution_pattern_summary[0]
        headline_read = (
            "Top-gap quarter diagnostics show the largest gap windows are "
            f"`{str(dominant.get('dominant_leg'))}` with abs-gap share ≈ {float(dominant.get('abs_gap_share') or 0.0):.2f}, "
            f"and the leading contribution pattern is `{str(contribution.get('contribution_pattern'))}` with "
            f"abs-gap share ≈ {float(contribution.get('abs_gap_share') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_quarter_audit_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_quarter_audit_summary.get("interpretation"),
        "top_gap_quarters": top_gap_quarters,
        "dominant_leg_summary": dominant_leg_summary,
        "contribution_pattern_summary": contribution_pattern_summary,
        "takeaways": list(strict_top_gap_quarter_audit_summary.get("takeaways", [])),
    }


def _strict_top_gap_quarter_direction_context(
    strict_top_gap_quarter_direction_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_quarter_direction_summary is None:
        return {}
    if str(strict_top_gap_quarter_direction_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_quarter_direction_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_quarter_direction_summary.json",
            "headline_read": "Strict top-gap quarter direction diagnostics are not available in the current run.",
        }
    gap_bundle_alignment_summary = list(strict_top_gap_quarter_direction_summary.get("gap_bundle_alignment_summary", []))
    directional_driver_summary = list(strict_top_gap_quarter_direction_summary.get("directional_driver_summary", []))
    top_gap_quarters = list(strict_top_gap_quarter_direction_summary.get("top_gap_quarters", []))
    headline_read = "Strict top-gap quarter direction diagnostics are available but do not contain the compact direction summary."
    if gap_bundle_alignment_summary and directional_driver_summary:
        lead_alignment = gap_bundle_alignment_summary[0]
        lead_driver = directional_driver_summary[0]
        headline_read = (
            "Top-gap quarter direction diagnostics show the leading gap-versus-bundle alignment is "
            f"`{str(lead_alignment.get('gap_alignment_to_bundle'))}` with abs-gap share ≈ {float(lead_alignment.get('abs_gap_share') or 0.0):.2f}, "
            f"and the leading directional-driver bucket is `{str(lead_driver.get('directional_driver'))}` with "
            f"abs-gap share ≈ {float(lead_driver.get('abs_gap_share') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_quarter_direction_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_quarter_direction_summary.get("interpretation"),
        "top_gap_quarters": top_gap_quarters,
        "gap_bundle_alignment_summary": gap_bundle_alignment_summary,
        "directional_driver_summary": directional_driver_summary,
        "takeaways": list(strict_top_gap_quarter_direction_summary.get("takeaways", [])),
    }


def _strict_top_gap_inversion_context(
    strict_top_gap_inversion_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_inversion_summary is None:
        return {}
    if str(strict_top_gap_inversion_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_inversion_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_inversion_summary.json",
            "headline_read": "Strict top-gap inversion diagnostics are not available in the current run.",
        }
    directional_driver_context_summary = list(
        strict_top_gap_inversion_summary.get("directional_driver_context_summary", [])
    )
    top_gap_quarters = list(strict_top_gap_inversion_summary.get("top_gap_quarters", []))
    headline_read = "Strict top-gap inversion diagnostics are available but do not contain the compact inversion-context summary."
    if directional_driver_context_summary:
        lead = directional_driver_context_summary[0]
        headline_read = (
            "Top-gap inversion diagnostics show the leading driver bucket is "
            f"`{str(lead.get('directional_driver'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}; "
            f"its weighted excluded residual is ≈ {float(lead.get('weighted_mean_excluded_other_component_qoq') or 0.0):.2f} "
            f"versus weighted strict total ≈ {float(lead.get('weighted_mean_strict_identifiable_total_qoq') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_inversion_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_inversion_summary.get("interpretation"),
        "top_gap_quarters": top_gap_quarters,
        "directional_driver_context_summary": directional_driver_context_summary,
        "residual_strict_pattern_summary": list(
            strict_top_gap_inversion_summary.get("residual_strict_pattern_summary", [])
        ),
        "takeaways": list(strict_top_gap_inversion_summary.get("takeaways", [])),
    }


def _strict_top_gap_anomaly_context(
    strict_top_gap_anomaly_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_anomaly_summary is None:
        return {}
    if str(strict_top_gap_anomaly_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_anomaly_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_anomaly_summary.json",
            "headline_read": "Strict top-gap anomaly diagnostics are not available in the current run.",
        }
    anomaly_quarter = dict(strict_top_gap_anomaly_summary.get("anomaly_quarter", {}) or {})
    anomaly_vs_peer_deltas = dict(strict_top_gap_anomaly_summary.get("anomaly_vs_peer_deltas", {}) or {})
    ranked_deltas = list(strict_top_gap_anomaly_summary.get("ranked_anomaly_component_deltas", []))
    headline_read = "Strict top-gap anomaly diagnostics are available but do not contain the compact anomaly-versus-peer comparison."
    if anomaly_quarter and anomaly_vs_peer_deltas:
        headline_read = (
            "The main within-bucket anomaly is "
            f"`{str(anomaly_quarter.get('quarter'))}`: excluded residual ≈ {float(anomaly_quarter.get('excluded_other_component_qoq') or 0.0):.2f}, "
            f"strict total ≈ {float(anomaly_quarter.get('strict_identifiable_total_qoq') or 0.0):.2f}, "
            f"anomaly-minus-peer loan delta ≈ {float(anomaly_vs_peer_deltas.get('strict_loan_source_qoq') or 0.0):.2f}, "
            f"and anomaly-minus-peer strict-total delta ≈ {float(anomaly_vs_peer_deltas.get('strict_identifiable_total_qoq') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_anomaly_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_anomaly_summary.get("interpretation"),
        "anomaly_quarter": anomaly_quarter,
        "peer_quarters": list(strict_top_gap_anomaly_summary.get("peer_quarters", [])),
        "peer_pattern_summary": list(strict_top_gap_anomaly_summary.get("peer_pattern_summary", [])),
        "anomaly_vs_peer_deltas": anomaly_vs_peer_deltas,
        "ranked_anomaly_component_deltas": ranked_deltas,
        "takeaways": list(strict_top_gap_anomaly_summary.get("takeaways", [])),
    }


def _strict_top_gap_anomaly_component_split_context(
    strict_top_gap_anomaly_component_split_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_anomaly_component_split_summary is None:
        return {}
    if str(strict_top_gap_anomaly_component_split_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_anomaly_component_split_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_anomaly_component_split_summary.json",
            "headline_read": "Strict top-gap anomaly component-split diagnostics are not available in the current run.",
        }
    loan_rows = list(strict_top_gap_anomaly_component_split_summary.get("loan_subcomponent_deltas", []))
    liquidity_rows = list(strict_top_gap_anomaly_component_split_summary.get("liquidity_external_deltas", []))
    headline_read = "Strict top-gap anomaly component-split diagnostics are available but do not contain the compact component comparison."
    if loan_rows and liquidity_rows:
        headline_read = (
            "The `2009Q4` anomaly is mainly a "
            f"`{str(loan_rows[0].get('label'))}` shortfall at ≈ {float(loan_rows[0].get('anomaly_minus_peer_delta') or 0.0):.2f}, "
            f"with `{str(liquidity_rows[0].get('label'))}` also weak at ≈ {float(liquidity_rows[0].get('anomaly_minus_peer_delta') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_anomaly_component_split_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_anomaly_component_split_summary.get("interpretation"),
        "anomaly_quarter": dict(strict_top_gap_anomaly_component_split_summary.get("anomaly_quarter", {}) or {}),
        "loan_subcomponent_deltas": loan_rows,
        "securities_subcomponent_deltas": list(
            strict_top_gap_anomaly_component_split_summary.get("securities_subcomponent_deltas", [])
        ),
        "funding_subcomponent_deltas": list(
            strict_top_gap_anomaly_component_split_summary.get("funding_subcomponent_deltas", [])
        ),
        "liquidity_external_deltas": liquidity_rows,
        "ranked_component_deltas": list(
            strict_top_gap_anomaly_component_split_summary.get("ranked_component_deltas", [])
        ),
        "takeaways": list(strict_top_gap_anomaly_component_split_summary.get("takeaways", [])),
    }


def _strict_top_gap_anomaly_di_loans_split_context(
    strict_top_gap_anomaly_di_loans_split_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_anomaly_di_loans_split_summary is None:
        return {}
    if str(strict_top_gap_anomaly_di_loans_split_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_anomaly_di_loans_split_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_anomaly_di_loans_split_summary.json",
            "headline_read": "Strict top-gap anomaly DI-loans split diagnostics are not available in the current run.",
        }
    dominant = dict(strict_top_gap_anomaly_di_loans_split_summary.get("dominant_borrower_component") or {})
    borrower_gap = dict(strict_top_gap_anomaly_di_loans_split_summary.get("borrower_gap_row") or {})
    headline_read = "Strict top-gap anomaly DI-loans split diagnostics are available but do not contain the compact borrower-side comparison."
    if dominant:
        headline_read = (
            "Inside the DI-loans-n.e.c. split, the main borrower-side delta is "
            f"`{str(dominant.get('label'))}` at ≈ {float(dominant.get('anomaly_minus_peer_delta') or 0.0):.2f}; "
            f"borrower-gap delta ≈ {float(borrower_gap.get('anomaly_minus_peer_delta') or 0.0):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_anomaly_di_loans_split_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_anomaly_di_loans_split_summary.get("interpretation"),
        "anomaly_quarter": dict(strict_top_gap_anomaly_di_loans_split_summary.get("anomaly_quarter", {}) or {}),
        "di_loans_nec_component_deltas": list(
            strict_top_gap_anomaly_di_loans_split_summary.get("di_loans_nec_component_deltas", [])
        ),
        "dominant_borrower_component": dominant,
        "borrower_gap_row": borrower_gap,
        "takeaways": list(strict_top_gap_anomaly_di_loans_split_summary.get("takeaways", [])),
    }


def _strict_top_gap_anomaly_backdrop_context(
    strict_top_gap_anomaly_backdrop_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_top_gap_anomaly_backdrop_summary is None:
        return {}
    if str(strict_top_gap_anomaly_backdrop_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_top_gap_anomaly_backdrop_summary.get("status", "not_available")),
            "artifact": "strict_top_gap_anomaly_backdrop_summary.json",
            "headline_read": "Strict top-gap anomaly backdrop diagnostics are not available in the current run.",
        }
    corporate = dict(strict_top_gap_anomaly_backdrop_summary.get("corporate_credit_row") or {})
    reserves = dict(strict_top_gap_anomaly_backdrop_summary.get("reserves_row") or {})
    foreign_nonts = dict(strict_top_gap_anomaly_backdrop_summary.get("foreign_nonts_row") or {})
    headline_read = "Strict top-gap anomaly backdrop diagnostics are available but do not contain the compact backdrop comparison."
    if corporate and reserves and foreign_nonts:
        headline_read = (
            "The `2009Q4` anomaly combines nonfinancial-corporate DI-loans weakness "
            f"(≈ {float(corporate.get('anomaly_minus_peer_delta') or 0.0):.2f}) "
            f"with weaker reserves (≈ {float(reserves.get('anomaly_minus_peer_delta') or 0.0):.2f}) "
            f"and weaker foreign NONTS (≈ {float(foreign_nonts.get('anomaly_minus_peer_delta') or 0.0):.2f})."
        )
    return {
        "status": "available",
        "artifact": "strict_top_gap_anomaly_backdrop_summary.json",
        "headline_read": headline_read,
        "interpretation": strict_top_gap_anomaly_backdrop_summary.get("interpretation"),
        "anomaly_quarter": dict(strict_top_gap_anomaly_backdrop_summary.get("anomaly_quarter", {}) or {}),
        "corporate_credit_row": corporate,
        "loan_source_row": dict(strict_top_gap_anomaly_backdrop_summary.get("loan_source_row") or {}),
        "reserves_row": reserves,
        "foreign_nonts_row": foreign_nonts,
        "tga_row": dict(strict_top_gap_anomaly_backdrop_summary.get("tga_row") or {}),
        "residual_row": dict(strict_top_gap_anomaly_backdrop_summary.get("residual_row") or {}),
        "liquidity_external_abs_to_corporate_abs_ratio": strict_top_gap_anomaly_backdrop_summary.get(
            "liquidity_external_abs_to_corporate_abs_ratio"
        ),
        "takeaways": list(strict_top_gap_anomaly_backdrop_summary.get("takeaways", [])),
    }


def _big_picture_synthesis_context(
    big_picture_synthesis_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if big_picture_synthesis_summary is None:
        return {}
    if str(big_picture_synthesis_summary.get("status", "not_available")) != "available":
        return {
            "status": str(big_picture_synthesis_summary.get("status", "not_available")),
            "artifact": "big_picture_synthesis_summary.json",
            "headline_read": "Big-picture synthesis diagnostics are not available in the current run.",
        }
    snapshot = dict(big_picture_synthesis_summary.get("h0_snapshot", {}))
    headline_read = (
        "The big-picture synthesis says the main residual problem is mostly treatment-side, especially TOC/ROW, "
        "while the independent strict lane still does not verify the remaining non-TDC object."
    )
    excluded_residual = snapshot.get("toc_row_excluded_residual_beta")
    excluded_total = snapshot.get("toc_row_excluded_strict_identifiable_total_beta")
    if excluded_residual is not None and excluded_total is not None:
        headline_read = (
            "The big-picture synthesis says the residual problem is mostly treatment-side, especially TOC/ROW, "
            f"but the independent strict lane still does not validate the remaining object: after excluding TOC/ROW at h0, residual ≈ {float(excluded_residual):.2f} "
            f"while strict identifiable total ≈ {float(excluded_total):.2f}."
        )
    return {
        "status": "available",
        "artifact": "big_picture_synthesis_summary.json",
        "headline_read": headline_read,
        "classification": dict(big_picture_synthesis_summary.get("classification", {})),
        "h0_snapshot": snapshot,
        "quarter_composition": dict(big_picture_synthesis_summary.get("quarter_composition", {})),
        "supporting_case": dict(big_picture_synthesis_summary.get("supporting_case", {})),
        "interpretation": str(big_picture_synthesis_summary.get("interpretation", "")),
        "takeaways": list(big_picture_synthesis_summary.get("takeaways", [])),
    }


def _treatment_object_comparison_context(
    treatment_object_comparison_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if treatment_object_comparison_summary is None:
        return {}
    if str(treatment_object_comparison_summary.get("status", "not_available")) != "available":
        return {
            "status": str(treatment_object_comparison_summary.get("status", "not_available")),
            "artifact": "treatment_object_comparison_summary.json",
            "headline_read": "Treatment-object comparison diagnostics are not available in the current run.",
        }
    recommendation = dict(treatment_object_comparison_summary.get("recommendation", {}))
    candidates = list(treatment_object_comparison_summary.get("candidate_objects", []))
    headline_read = (
        "Treatment-object comparison says the next branch should redesign the TDC treatment architecture rather than simply swap in a new headline variant."
    )
    if recommendation:
        headline_read = (
            "Treatment-object comparison recommends "
            f"`{str(recommendation.get('recommended_next_branch', 'unknown'))}` and says to "
            f"{str(recommendation.get('headline_decision_now', 'keep the current headline provisional'))}."
        )
    return {
        "status": "available",
        "artifact": "treatment_object_comparison_summary.json",
        "headline_read": headline_read,
        "recommendation": recommendation,
        "candidate_objects": candidates,
        "takeaways": list(treatment_object_comparison_summary.get("takeaways", [])),
    }


def _split_treatment_architecture_context(
    split_treatment_architecture_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if split_treatment_architecture_summary is None:
        return {}
    if str(split_treatment_architecture_summary.get("status", "not_available")) != "available":
        return {
            "status": str(split_treatment_architecture_summary.get("status", "not_available")),
            "artifact": "split_treatment_architecture_summary.json",
            "headline_read": "Split treatment architecture diagnostics are not available in the current run.",
        }
    recommendation = dict(split_treatment_architecture_summary.get("architecture_recommendation", {}) or {})
    key_horizons = dict(split_treatment_architecture_summary.get("key_horizons", {}) or {})
    h0 = dict(key_horizons.get("h0", {}) or {})
    support_bundle_beta = h0.get("support_bundle_beta")
    core_residual = dict(h0.get("core_deposit_proximate_residual_response", {}) or {}).get("beta")
    headline_read = (
        "Split treatment architecture is available and separates a deposit-proximate core from the TOC/ROW support bundle."
    )
    if support_bundle_beta is not None and core_residual is not None:
        headline_read = (
            "Split treatment architecture now makes the redesign explicit: "
            f"h0 support bundle ≈ {float(support_bundle_beta):.2f}, core residual ≈ {float(core_residual):.2f}, "
            f"and the repo recommendation is `{str(recommendation.get('recommended_next_branch', 'split_core_plus_support_bundle'))}`."
        )
    return {
        "status": "available",
        "artifact": "split_treatment_architecture_summary.json",
        "headline_read": headline_read,
        "architecture_recommendation": recommendation,
        "series_definitions": dict(split_treatment_architecture_summary.get("series_definitions", {}) or {}),
        "quarterly_alignment": dict(split_treatment_architecture_summary.get("quarterly_alignment", {}) or {}),
        "key_horizons": key_horizons,
        "takeaways": list(split_treatment_architecture_summary.get("takeaways", [])),
    }


def _core_treatment_promotion_context(
    core_treatment_promotion_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if core_treatment_promotion_summary is None:
        return {}
    if str(core_treatment_promotion_summary.get("status", "not_available")) != "available":
        return {
            "status": str(core_treatment_promotion_summary.get("status", "not_available")),
            "artifact": "core_treatment_promotion_summary.json",
            "headline_read": "Core-treatment promotion diagnostics are not available in the current run.",
        }
    recommendation = dict(core_treatment_promotion_summary.get("promotion_recommendation", {}) or {})
    overlap = dict(core_treatment_promotion_summary.get("shock_quality", {}).get("baseline_vs_core_overlap", {}) or {})
    strict_validation = dict(core_treatment_promotion_summary.get("strict_validation_check", {}) or {})
    overlap_corr = overlap.get("shock_corr")
    same_sign_share = overlap.get("same_sign_share")
    core_residual = strict_validation.get("h0_core_residual_beta")
    strict_total = strict_validation.get("h0_strict_identifiable_total_beta")
    headline_read = (
        "Core-treatment promotion diagnostics are available and currently keep the split architecture interpretive."
    )
    if overlap_corr is not None and same_sign_share is not None and core_residual is not None and strict_total is not None:
        headline_read = (
            "Core-treatment promotion diagnostics keep the split interpretive for now: "
            f"baseline/core shock corr ≈ {float(overlap_corr):.2f}, same-sign share ≈ {float(same_sign_share):.2f}, "
            f"h0 core residual ≈ {float(core_residual):.2f}, strict total ≈ {float(strict_total):.2f}."
        )
    return {
        "status": "available",
        "artifact": "core_treatment_promotion_summary.json",
        "headline_read": headline_read,
        "promotion_recommendation": recommendation,
        "shock_quality": dict(core_treatment_promotion_summary.get("shock_quality", {}) or {}),
        "strict_validation_check": strict_validation,
        "key_horizons": dict(core_treatment_promotion_summary.get("key_horizons", {}) or {}),
        "takeaways": list(core_treatment_promotion_summary.get("takeaways", [])),
    }


def _strict_redesign_context(
    strict_redesign_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_redesign_summary is None:
        return {}
    if str(strict_redesign_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_redesign_summary.get("status", "not_available")),
            "artifact": "strict_redesign_summary.json",
            "headline_read": "Strict redesign diagnostics are not available in the current run.",
        }
    current_problem = dict(strict_redesign_summary.get("current_strict_problem_definition", {}) or {})
    failure_modes = dict(strict_redesign_summary.get("failure_modes", {}) or {})
    loan_shape = dict(failure_modes.get("loan_bucket_shape", {}) or {})
    funding = dict(failure_modes.get("funding_offset_instability", {}) or {})
    scope = dict(failure_modes.get("scope_mismatch_not_primary", {}) or {})
    remaining_share = scope.get("h0_remaining_share_of_baseline_strict_gap")
    core_residual = current_problem.get("h0_core_residual_beta")
    strict_total = current_problem.get("h0_toc_row_excluded_strict_identifiable_total_beta")
    dominant_loan = loan_shape.get("h0_dominant_loan_component")
    funding_share = funding.get("h0_funding_offset_share_of_identifiable_total_beta")
    headline_read = "Strict redesign diagnostics are available and point back to the strict lane itself rather than to another treatment tweak."
    if (
        remaining_share is not None
        and core_residual is not None
        and strict_total is not None
        and dominant_loan is not None
        and funding_share is not None
    ):
        headline_read = (
            "Strict redesign should now focus on loan composition, not more treatment relabeling: matched-bank-leg scope "
            f"still leaves about {float(remaining_share):.2f} of the baseline strict gap, h0 core residual is about "
            f"{float(core_residual):.2f} while the direct-count total is about {float(strict_total):.2f}, the dominant "
            f"h0 loan block is `{str(dominant_loan)}`, and funding offsets are about {float(funding_share):.2f} of identifiable total."
        )
    return {
        "status": "available",
        "artifact": "strict_redesign_summary.json",
        "headline_read": headline_read,
        "current_strict_problem_definition": current_problem,
        "failure_modes": failure_modes,
        "recommended_build_order": list(strict_redesign_summary.get("recommended_build_order", [])),
        "takeaways": list(strict_redesign_summary.get("takeaways", [])),
    }


def _strict_loan_core_redesign_context(
    strict_loan_core_redesign_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_loan_core_redesign_summary is None:
        return {}
    if str(strict_loan_core_redesign_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_loan_core_redesign_summary.get("status", "not_available")),
            "artifact": "strict_loan_core_redesign_summary.json",
            "headline_read": "Strict loan-core redesign diagnostics are not available in the current run.",
        }
    recommendation = dict(strict_loan_core_redesign_summary.get("recommendation", {}) or {})
    published_roles = dict(strict_loan_core_redesign_summary.get("published_roles", {}) or {})
    h0_core = dict(strict_loan_core_redesign_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {})
    residual = dict(h0_core.get("core_residual_response", {}) or {}).get("beta")
    broad = dict(h0_core.get("current_broad_loan_source_response", {}) or {}).get("beta")
    direct = dict(h0_core.get("redesigned_direct_min_core_response", {}) or {}).get("beta")
    private_aug = dict(h0_core.get("private_borrower_augmented_core_response", {}) or {}).get("beta")
    noncore = dict(h0_core.get("noncore_system_diagnostic_response", {}) or {}).get("beta")
    headline_read = "Strict loan-core redesign diagnostics are available and compare the current broad loan source against a redesigned direct-only core."
    if None not in (residual, broad, direct, private_aug, noncore):
        headline_read = (
            "The strict loan-core redesign is now concrete under the core-deposit-proximate shock: "
            f"core residual ≈ {float(residual):.2f}, current broad loan source ≈ {float(broad):.2f}, "
            f"direct minimum core ≈ {float(direct):.2f}, private-borrower-augmented core ≈ {float(private_aug):.2f}, "
            f"noncore/system diagnostic ≈ {float(noncore):.2f}."
        )
    if published_roles:
        headline_read += (
            " Published role design: headline direct core = `strict_loan_core_min_qoq`, "
            "standard secondary comparison = `strict_loan_core_plus_private_borrower_qoq`, "
            "and the old broad loan subtotal plus DI-loans-n.e.c. remain diagnostic only."
        )
    return {
        "status": "available",
        "artifact": "strict_loan_core_redesign_summary.json",
        "headline_read": headline_read,
        "candidate_definitions": dict(strict_loan_core_redesign_summary.get("candidate_definitions", {}) or {}),
        "published_roles": published_roles,
        "recommendation": recommendation,
        "key_horizons": dict(strict_loan_core_redesign_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_loan_core_redesign_summary.get("takeaways", [])),
    }


def _strict_di_bucket_role_context(
    strict_di_bucket_role_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_di_bucket_role_summary is None:
        return {}
    if str(strict_di_bucket_role_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_di_bucket_role_summary.get("status", "not_available")),
            "artifact": "strict_di_bucket_role_summary.json",
            "headline_read": "Strict DI-bucket role diagnostics are not available in the current run.",
        }
    h0 = dict(strict_di_bucket_role_summary.get("key_horizons", {}).get("h0", {}) or {})
    residual = dict(h0.get("core_residual_response", {}) or {}).get("beta")
    headline = dict(h0.get("headline_direct_core_response", {}) or {}).get("beta")
    secondary = dict(h0.get("standard_secondary_comparison_response", {}) or {}).get("beta")
    broad = dict(h0.get("broad_loan_subtotal_response", {}) or {}).get("beta")
    dominant = h0.get("dominant_borrower_component")
    headline_read = (
        "Strict DI-bucket role diagnostics are available and keep the broad DI-loans-n.e.c. bucket diagnostic only."
    )
    if None not in (residual, headline, secondary, broad):
        headline_read = (
            "The DI-bucket role bridge is now explicit at h0: "
            f"core residual ≈ {float(residual):.2f}, headline direct core ≈ {float(headline):.2f}, "
            f"standard secondary comparison ≈ {float(secondary):.2f}, broad loan subtotal ≈ {float(broad):.2f}."
        )
        if dominant is not None:
            headline_read += (
                f" The broad DI bucket stays diagnostic only, with dominant borrower counterpart `{str(dominant)}`."
            )
    return {
        "status": "available",
        "artifact": "strict_di_bucket_role_summary.json",
        "headline_read": headline_read,
        "release_taxonomy": dict(strict_di_bucket_role_summary.get("release_taxonomy", {}) or {}),
        "recommendation": dict(strict_di_bucket_role_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_di_bucket_role_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_di_bucket_role_summary.get("takeaways", [])),
    }


def _strict_di_bucket_bridge_context(
    strict_di_bucket_bridge_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_di_bucket_bridge_summary is None:
        return {}
    if str(strict_di_bucket_bridge_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_di_bucket_bridge_summary.get("status", "not_available")),
            "artifact": "strict_di_bucket_bridge_summary.json",
            "headline_read": "Strict DI-bucket bridge diagnostics are not available in the current run.",
        }
    h0 = dict(strict_di_bucket_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {})
    di_asset = dict(h0.get("di_asset_response", {}) or {}).get("beta")
    private_bridge = dict(h0.get("private_borrower_bridge_response", {}) or {}).get("beta")
    noncore_bridge = dict(h0.get("noncore_system_bridge_response", {}) or {}).get("beta")
    bridge_residual = h0.get("bridge_residual_beta")
    interpretation = h0.get("interpretation")
    headline_read = "Strict DI-bucket bridge diagnostics are available and treat the broad DI bucket as a bridge problem rather than a release component."
    if None not in (di_asset, private_bridge, noncore_bridge, bridge_residual):
        headline_read = (
            "The DI-bucket bridge is now explicit at h0 under the core-deposit-proximate shock: "
            f"DI asset ≈ {float(di_asset):.2f}, private-borrower bridge ≈ {float(private_bridge):.2f}, "
            f"noncore/system bridge ≈ {float(noncore_bridge):.2f}, bridge residual ≈ {float(bridge_residual):.2f}."
        )
        if interpretation is not None:
            headline_read += f" Current bridge read = `{str(interpretation)}`."
    return {
        "status": "available",
        "artifact": "strict_di_bucket_bridge_summary.json",
        "headline_read": headline_read,
        "bridge_definitions": dict(strict_di_bucket_bridge_summary.get("bridge_definitions", {}) or {}),
        "recommendation": dict(strict_di_bucket_bridge_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_di_bucket_bridge_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_di_bucket_bridge_summary.get("takeaways", [])),
    }


def _strict_private_borrower_bridge_context(
    strict_private_borrower_bridge_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_private_borrower_bridge_summary is None:
        return {}
    if str(strict_private_borrower_bridge_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_private_borrower_bridge_summary.get("status", "not_available")),
            "artifact": "strict_private_borrower_bridge_summary.json",
            "headline_read": "Strict private-borrower bridge diagnostics are not available in the current run.",
        }
    h0 = dict(
        strict_private_borrower_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
    )
    private_total = dict(h0.get("private_bridge_response", {}) or {}).get("beta")
    households = dict(h0.get("households_nonprofits_response", {}) or {}).get("beta")
    corporate = dict(h0.get("nonfinancial_corporate_response", {}) or {}).get("beta")
    noncorporate = dict(h0.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    dominant = h0.get("dominant_private_component")
    headline_read = "Strict private-borrower bridge diagnostics are available and split the private bridge into households, nonfinancial-corporate, and nonfinancial-noncorporate pieces."
    if None not in (private_total, households, corporate, noncorporate):
        headline_read = (
            "The private-borrower bridge is now explicit at h0 under the core-deposit-proximate shock: "
            f"private total ≈ {float(private_total):.2f}, households/nonprofits ≈ {float(households):.2f}, "
            f"nonfinancial corporate ≈ {float(corporate):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
        )
        if dominant is not None:
            headline_read += f" Dominant private block = `{str(dominant)}`."
    return {
        "status": "available",
        "artifact": "strict_private_borrower_bridge_summary.json",
        "headline_read": headline_read,
        "bridge_definitions": dict(strict_private_borrower_bridge_summary.get("bridge_definitions", {}) or {}),
        "recommendation": dict(strict_private_borrower_bridge_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_private_borrower_bridge_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_private_borrower_bridge_summary.get("takeaways", [])),
    }


def _strict_nonfinancial_corporate_bridge_context(
    strict_nonfinancial_corporate_bridge_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_nonfinancial_corporate_bridge_summary is None:
        return {}
    if str(strict_nonfinancial_corporate_bridge_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_nonfinancial_corporate_bridge_summary.get("status", "not_available")),
            "artifact": "strict_nonfinancial_corporate_bridge_summary.json",
            "headline_read": "Strict nonfinancial-corporate bridge diagnostics are not available in the current run.",
        }
    h0 = dict(
        strict_nonfinancial_corporate_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
    )
    corporate = dict(h0.get("nonfinancial_corporate_response", {}) or {}).get("beta")
    private_total = dict(h0.get("private_bridge_response", {}) or {}).get("beta")
    households = dict(h0.get("households_nonprofits_response", {}) or {}).get("beta")
    noncorporate = dict(h0.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    headline_read = "Strict nonfinancial-corporate bridge diagnostics are available and isolate the corporate bridge from the smaller private offsets."
    if None not in (corporate, private_total, households, noncorporate):
        headline_read = (
            "The nonfinancial-corporate bridge is now explicit at h0 under the core-deposit-proximate shock: "
            f"nonfinancial corporate ≈ {float(corporate):.2f}, private total ≈ {float(private_total):.2f}, "
            f"households/nonprofits ≈ {float(households):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_nonfinancial_corporate_bridge_summary.json",
        "headline_read": headline_read,
        "bridge_definitions": dict(strict_nonfinancial_corporate_bridge_summary.get("bridge_definitions", {}) or {}),
        "recommendation": dict(strict_nonfinancial_corporate_bridge_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_nonfinancial_corporate_bridge_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_nonfinancial_corporate_bridge_summary.get("takeaways", [])),
    }


def _strict_private_offset_residual_context(
    strict_private_offset_residual_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_private_offset_residual_summary is None:
        return {}
    if str(strict_private_offset_residual_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_private_offset_residual_summary.get("status", "not_available")),
            "artifact": "strict_private_offset_residual_summary.json",
            "headline_read": "Strict private-offset diagnostics are not available in the current run.",
        }
    h0 = dict(
        strict_private_offset_residual_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
    )
    offset_total = dict(h0.get("private_offset_total_response", {}) or {}).get("beta")
    private_total = dict(h0.get("private_bridge_response", {}) or {}).get("beta")
    households = dict(h0.get("households_nonprofits_response", {}) or {}).get("beta")
    noncorporate = dict(h0.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    headline_read = "Strict private-offset diagnostics are available and isolate the remaining private detail after the corporate bridge."
    if None not in (offset_total, private_total, households, noncorporate):
        headline_read = (
            "The private offset block is now explicit at h0 under the core-deposit-proximate shock: "
            f"offset total ≈ {float(offset_total):.2f}, private total ≈ {float(private_total):.2f}, "
            f"households/nonprofits ≈ {float(households):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_private_offset_residual_summary.json",
        "headline_read": headline_read,
        "bridge_definitions": dict(strict_private_offset_residual_summary.get("bridge_definitions", {}) or {}),
        "recommendation": dict(strict_private_offset_residual_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_private_offset_residual_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_private_offset_residual_summary.get("takeaways", [])),
    }


def _strict_corporate_bridge_secondary_comparison_context(
    strict_corporate_bridge_secondary_comparison_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_corporate_bridge_secondary_comparison_summary is None:
        return {}
    if str(strict_corporate_bridge_secondary_comparison_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_corporate_bridge_secondary_comparison_summary.get("status", "not_available")),
            "artifact": "strict_corporate_bridge_secondary_comparison_summary.json",
            "headline_read": "Strict corporate-bridge secondary-comparison diagnostics are not available in the current run.",
        }
    h0 = dict(
        strict_corporate_bridge_secondary_comparison_summary.get("key_horizons", {})
        .get("h0", {})
        .get("core_deposit_proximate", {})
        or {}
    )
    residual = dict(h0.get("core_residual_response", {}) or {}).get("beta")
    direct = dict(h0.get("headline_direct_core_response", {}) or {}).get("beta")
    private_bridge = dict(h0.get("core_plus_private_bridge_response", {}) or {}).get("beta")
    corporate_bridge = dict(h0.get("core_plus_nonfinancial_corporate_response", {}) or {}).get("beta")
    headline_read = "Strict secondary-comparison diagnostics are available and compare the broad private bridge directly against the narrower corporate bridge."
    if None not in (residual, direct, private_bridge, corporate_bridge):
        headline_read = (
            "The secondary strict comparison is now estimated directly at h0 under the core-deposit-proximate shock: "
            f"core residual ≈ {float(residual):.2f}, headline direct core ≈ {float(direct):.2f}, "
            f"core + private bridge ≈ {float(private_bridge):.2f}, core + nonfinancial corporate ≈ {float(corporate_bridge):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_corporate_bridge_secondary_comparison_summary.json",
        "headline_read": headline_read,
        "candidate_definitions": dict(
            strict_corporate_bridge_secondary_comparison_summary.get("candidate_definitions", {}) or {}
        ),
        "recommendation": dict(
            strict_corporate_bridge_secondary_comparison_summary.get("recommendation", {}) or {}
        ),
        "key_horizons": dict(strict_corporate_bridge_secondary_comparison_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_corporate_bridge_secondary_comparison_summary.get("takeaways", [])),
    }


def _strict_component_framework_context(
    strict_component_framework_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_component_framework_summary is None:
        return {}
    if str(strict_component_framework_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_component_framework_summary.get("status", "not_available")),
            "artifact": "strict_component_framework_summary.json",
            "headline_read": "The strict deposit-component framework is not available in the current run.",
        }
    h0 = dict(strict_component_framework_summary.get("h0_snapshot", {}) or {})
    support_bundle = h0.get("toc_row_support_bundle_beta")
    core_residual = h0.get("core_residual_beta")
    headline_direct_core = h0.get("headline_direct_core_beta")
    standard_secondary = h0.get("standard_secondary_beta")
    narrowing = h0.get("narrowing_diagnostic_beta")
    headline_read = (
        "The strict deposit-component framework is frozen for release framing: "
        "accounting closure is non-evidence, full TDC stays provisional as a broad object, "
        "and the strict lane roles are now fixed."
    )
    if None not in (support_bundle, core_residual, headline_direct_core, standard_secondary, narrowing):
        headline_read = (
            "The strict deposit-component framework is now frozen in one place: "
            f"h0 TOC/ROW support bundle ≈ {float(support_bundle):.2f}, core residual ≈ {float(core_residual):.2f}, "
            f"headline direct core ≈ {float(headline_direct_core):.2f}, "
            f"standard bridge comparison ≈ {float(standard_secondary):.2f}, "
            f"wider diagnostic envelope ≈ {float(narrowing):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_component_framework_summary.json",
        "headline_read": headline_read,
        "frozen_roles": dict(strict_component_framework_summary.get("frozen_roles", {}) or {}),
        "classification": dict(strict_component_framework_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_component_framework_summary.get("recommendation", {}) or {}),
        "h0_snapshot": h0,
        "takeaways": list(strict_component_framework_summary.get("takeaways", [])),
    }


def _toc_row_incidence_audit_context(
    toc_row_incidence_audit_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if toc_row_incidence_audit_summary is None:
        return {}
    if str(toc_row_incidence_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(toc_row_incidence_audit_summary.get("status", "not_available")),
            "artifact": "toc_row_incidence_audit_summary.json",
            "headline_read": "TOC/ROW incidence-audit diagnostics are not available in the current run.",
        }
    h0 = dict(toc_row_incidence_audit_summary.get("key_horizons", {}).get("h0", {}) or {})
    toc_leg = dict(h0.get("toc_leg", {}) or {})
    row_leg = dict(h0.get("row_leg", {}) or {})
    toc_share = toc_leg.get("in_scope_deposit_proxy_share_of_toc_beta")
    toc_reserve = toc_leg.get("reserve_capture_share_of_toc_beta")
    row_share = row_leg.get("in_scope_deposit_proxy_share_of_row_beta")
    row_external = row_leg.get("external_support_share_of_row_beta")
    headline_read = (
        "TOC/ROW incidence-audit diagnostics are available and test whether each leg lands in the in-scope bank-deposit aggregate."
    )
    if None not in (toc_share, toc_reserve, row_share, row_external):
        headline_read = (
            "The TOC/ROW incidence audit now makes the strict-incidence problem concrete at h0: "
            f"TOC deposit-proxy share ≈ {float(toc_share):.2f} versus reserve share ≈ {float(toc_reserve):.2f}, "
            f"ROW deposit-proxy share ≈ {float(row_share):.2f} versus external-support share ≈ {float(row_external):.2f}."
        )
    return {
        "status": "available",
        "artifact": "toc_row_incidence_audit_summary.json",
        "headline_read": headline_read,
        "classification": dict(toc_row_incidence_audit_summary.get("classification", {}) or {}),
        "recommendation": dict(toc_row_incidence_audit_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(toc_row_incidence_audit_summary.get("key_horizons", {}) or {}),
        "takeaways": list(toc_row_incidence_audit_summary.get("takeaways", [])),
    }


def _toc_row_liability_incidence_raw_context(
    toc_row_liability_incidence_raw_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if toc_row_liability_incidence_raw_summary is None:
        return {}
    if str(toc_row_liability_incidence_raw_summary.get("status", "not_available")) != "available":
        return {
            "status": str(toc_row_liability_incidence_raw_summary.get("status", "not_available")),
            "artifact": "toc_row_liability_incidence_raw_summary.json",
            "headline_read": "Raw-units TOC/ROW liability-incidence diagnostics are not available in the current run.",
        }
    h0 = dict(toc_row_liability_incidence_raw_summary.get("key_horizons", {}).get("h0", {}) or {})
    toc_leg = dict(h0.get("toc_leg", {}) or {})
    row_leg = dict(h0.get("row_leg", {}) or {})
    toc_shares = dict(toc_leg.get("counterpart_share_of_leg_beta", {}) or {})
    row_shares = dict(row_leg.get("counterpart_share_of_leg_beta", {}) or {})
    toc_dep_only = toc_shares.get("deposits_only_bank_qoq")
    toc_reserves = toc_shares.get("reserves_qoq")
    row_row_checkable = row_shares.get("checkable_rest_of_world_bank_qoq")
    row_external = row_shares.get("foreign_nonts_qoq")
    headline_read = (
        "The raw-units TOC/ROW liability-incidence audit is available and acts as the binary gate for whether either leg belongs in the strict deposit component."
    )
    if None not in (toc_dep_only, toc_reserves, row_row_checkable, row_external):
        headline_read = (
            "The raw-units TOC/ROW liability-incidence audit sharpens the binary gate at h0: "
            f"TOC deposits-only share ≈ {float(toc_dep_only):.2f} versus reserves share ≈ {float(toc_reserves):.2f}, "
            f"ROW-checkable share ≈ {float(row_row_checkable):.2f} versus foreign-NONTS share ≈ {float(row_external):.2f}."
        )
    return {
        "status": "available",
        "artifact": "toc_row_liability_incidence_raw_summary.json",
        "headline_read": headline_read,
        "classification": dict(toc_row_liability_incidence_raw_summary.get("classification", {}) or {}),
        "recommendation": dict(toc_row_liability_incidence_raw_summary.get("recommendation", {}) or {}),
        "quarterly_alignment": dict(toc_row_liability_incidence_raw_summary.get("quarterly_alignment", {}) or {}),
        "key_horizons": dict(toc_row_liability_incidence_raw_summary.get("key_horizons", {}) or {}),
        "takeaways": list(toc_row_liability_incidence_raw_summary.get("takeaways", [])),
    }


def _toc_validated_share_candidate_context(
    toc_validated_share_candidate_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if toc_validated_share_candidate_summary is None:
        return {}
    if str(toc_validated_share_candidate_summary.get("status", "not_available")) != "available":
        return {
            "status": str(toc_validated_share_candidate_summary.get("status", "not_available")),
            "artifact": "toc_validated_share_candidate_summary.json",
            "headline_read": "Narrow-TOC candidate diagnostics are not available in the current run.",
        }
    h0 = dict(toc_validated_share_candidate_summary.get("key_horizons", {}).get("h0", {}) or {})
    best_candidate = dict(h0.get("best_candidate", {}) or {})
    core_residual = h0.get("core_residual_beta")
    direct_core = h0.get("headline_direct_core_beta")
    implied_residual = best_candidate.get("implied_residual_beta")
    abs_gap = best_candidate.get("abs_gap_to_direct_core")
    headline_read = (
        "The narrow-TOC candidate gate is available and asks whether any validated TOC share should be added back to the strict object."
    )
    if None not in (core_residual, direct_core, implied_residual, abs_gap):
        headline_read = (
            "The narrow-TOC candidate gate now closes that branch directly: "
            f"core residual ≈ {float(core_residual):.2f}, direct core ≈ {float(direct_core):.2f}, "
            f"best TOC-share candidate implied residual ≈ {float(implied_residual):.2f} with abs gap ≈ {float(abs_gap):.2f}."
        )
    return {
        "status": "available",
        "artifact": "toc_validated_share_candidate_summary.json",
        "headline_read": headline_read,
        "classification": dict(toc_validated_share_candidate_summary.get("classification", {}) or {}),
        "recommendation": dict(toc_validated_share_candidate_summary.get("recommendation", {}) or {}),
        "quarterly_gate": dict(toc_validated_share_candidate_summary.get("quarterly_gate", {}) or {}),
        "key_horizons": dict(toc_validated_share_candidate_summary.get("key_horizons", {}) or {}),
        "takeaways": list(toc_validated_share_candidate_summary.get("takeaways", [])),
    }


def _strict_release_framing_context(
    strict_release_framing_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_release_framing_summary is None:
        return {}
    if str(strict_release_framing_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_release_framing_summary.get("status", "not_available")),
            "artifact": "strict_release_framing_summary.json",
            "headline_read": "Strict release-framing diagnostics are not available in the current run.",
        }
    h0 = dict(strict_release_framing_summary.get("h0_snapshot", {}) or {})
    support_bundle = h0.get("toc_row_support_bundle_beta")
    core_residual = h0.get("core_residual_beta")
    direct_core = h0.get("headline_direct_core_beta")
    toc_share = h0.get("toc_deposits_only_share")
    row_share = h0.get("row_checkable_share")
    headline_read = (
        "The strict release framing is available and states the settled rule for the broad TDC object versus the strict object."
    )
    if None not in (support_bundle, core_residual, direct_core, toc_share, row_share):
        headline_read = (
            "The strict release framing is now frozen explicitly: "
            f"TOC/ROW support bundle ≈ {float(support_bundle):.2f}, core residual ≈ {float(core_residual):.2f}, "
            f"headline direct core ≈ {float(direct_core):.2f}, "
            f"TOC deposits-only share ≈ {float(toc_share):.2f}, ROW-checkable share ≈ {float(row_share):.2f}; "
            "TOC and ROW stay outside the strict object under current evidence."
        )
    return {
        "status": "available",
        "artifact": "strict_release_framing_summary.json",
        "headline_read": headline_read,
        "release_position": dict(strict_release_framing_summary.get("release_position", {}) or {}),
        "evidence_tiers": dict(strict_release_framing_summary.get("evidence_tiers", {}) or {}),
        "classification": dict(strict_release_framing_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_release_framing_summary.get("recommendation", {}) or {}),
        "h0_snapshot": h0,
        "takeaways": list(strict_release_framing_summary.get("takeaways", [])),
    }


def _strict_direct_core_component_context(
    strict_direct_core_component_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_direct_core_component_summary is None:
        return {}
    if str(strict_direct_core_component_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_direct_core_component_summary.get("status", "not_available")),
            "artifact": "strict_direct_core_component_summary.json",
            "headline_read": "Strict direct-core component diagnostics are not available in the current run.",
        }
    h0 = dict(strict_direct_core_component_summary.get("key_horizons", {}).get("h0", {}) or {})
    core = dict(h0.get("core_deposit_proximate", {}) or {})
    residual = dict(core.get("residual_response", {}) or {}).get("beta")
    mortgages = dict(core.get("mortgages_response", {}) or {}).get("beta")
    consumer = dict(core.get("consumer_credit_response", {}) or {}).get("beta")
    direct = dict(core.get("direct_core_response", {}) or {}).get("beta")
    headline_read = (
        "Strict direct-core component diagnostics are available and split the current headline direct core into mortgages and consumer credit."
    )
    if None not in (residual, mortgages, consumer, direct):
        headline_read = (
            "The strict direct-core component split is now explicit at h0 under the core-deposit-proximate shock: "
            f"residual ≈ {float(residual):.2f}, mortgages ≈ {float(mortgages):.2f}, "
            f"consumer credit ≈ {float(consumer):.2f}, bundled direct core ≈ {float(direct):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_direct_core_component_summary.json",
        "headline_read": headline_read,
        "candidate_definitions": dict(strict_direct_core_component_summary.get("candidate_definitions", {}) or {}),
        "classification": dict(strict_direct_core_component_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_direct_core_component_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_direct_core_component_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_direct_core_component_summary.get("takeaways", [])),
    }


def _strict_direct_core_horizon_stability_context(
    strict_direct_core_horizon_stability_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_direct_core_horizon_stability_summary is None:
        return {}
    if str(strict_direct_core_horizon_stability_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_direct_core_horizon_stability_summary.get("status", "not_available")),
            "artifact": "strict_direct_core_horizon_stability_summary.json",
            "headline_read": "Strict direct-core horizon-stability diagnostics are not available in the current run.",
        }
    winners = dict(strict_direct_core_horizon_stability_summary.get("horizon_winners", {}) or {})
    headline_read = (
        "Strict direct-core horizon-stability diagnostics are available and compare the best direct-core candidate across h0, h4, and h8."
    )
    if winners:
        headline_read = (
            "The direct-core winner is now horizon-specific rather than universal: "
            f"h0 = `{winners.get('h0', 'not_available')}`, "
            f"h4 = `{winners.get('h4', 'not_available')}`, "
            f"h8 = `{winners.get('h8', 'not_available')}`."
        )
    return {
        "status": "available",
        "artifact": "strict_direct_core_horizon_stability_summary.json",
        "headline_read": headline_read,
        "classification": dict(strict_direct_core_horizon_stability_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_direct_core_horizon_stability_summary.get("recommendation", {}) or {}),
        "horizon_winners": winners,
        "takeaways": list(strict_direct_core_horizon_stability_summary.get("takeaways", [])),
    }


def _strict_additional_creator_candidate_context(
    strict_additional_creator_candidate_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_additional_creator_candidate_summary is None:
        return {}
    if str(strict_additional_creator_candidate_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_additional_creator_candidate_summary.get("status", "not_available")),
            "artifact": "strict_additional_creator_candidate_summary.json",
            "headline_read": "Strict additional creator-candidate diagnostics are not available in the current run.",
        }
    h0 = dict(strict_additional_creator_candidate_summary.get("key_horizons", {}).get("h0", {}) or {})
    core = dict(h0.get("core_deposit_proximate", {}) or {})
    best_validation = dict((core.get("validation_proxies") or {}).get("best_candidate") or {})
    best_extension = dict((core.get("extension_candidates") or {}).get("best_candidate") or {})
    headline_read = (
        "Strict additional creator-candidate diagnostics are available and separate broad validation proxies from true extension candidates beyond the current direct core."
    )
    if best_validation or best_extension:
        validation_name = str(best_validation.get("outcome", "not_available"))
        validation_beta = dict(best_validation.get("response", {}) or {}).get("beta")
        extension_name = str(best_extension.get("outcome", "not_available"))
        extension_beta = dict(best_extension.get("response", {}) or {}).get("beta")
        headline_read = (
            "The additional-creator candidate search is now explicit at h0 under the core-deposit-proximate shock: "
            f"best broad validation proxy = `{validation_name}`"
            + (f" (beta ≈ {float(validation_beta):.2f})" if validation_beta is not None else "")
            + ", "
            f"best extension candidate = `{extension_name}`"
            + (f" (beta ≈ {float(extension_beta):.2f})" if extension_beta is not None else "")
            + "."
        )
    return {
        "status": "available",
        "artifact": "strict_additional_creator_candidate_summary.json",
        "headline_read": headline_read,
        "candidate_groups": dict(strict_additional_creator_candidate_summary.get("candidate_groups", {}) or {}),
        "classification": dict(strict_additional_creator_candidate_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_additional_creator_candidate_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_additional_creator_candidate_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_additional_creator_candidate_summary.get("takeaways", [])),
    }


def _strict_di_loans_nec_measurement_audit_context(
    strict_di_loans_nec_measurement_audit_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_di_loans_nec_measurement_audit_summary is None:
        return {}
    if str(strict_di_loans_nec_measurement_audit_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_di_loans_nec_measurement_audit_summary.get("status", "not_available")),
            "artifact": "strict_di_loans_nec_measurement_audit_summary.json",
            "headline_read": "Strict DI-loans-n.e.c. measurement-audit diagnostics are not available in the current run.",
        }
    h0 = dict(strict_di_loans_nec_measurement_audit_summary.get("key_horizons", {}).get("h0", {}) or {})
    core = dict(h0.get("core_deposit_proximate", {}) or {})
    target = dict((core.get("cross_scope_transaction_bridges") or {}).get("target_response") or {}).get("beta")
    best_cross_scope = dict((core.get("cross_scope_transaction_bridges") or {}).get("best_candidate") or {})
    best_proxy = dict((core.get("same_scope_proxies") or {}).get("best_candidate") or {})
    headline_read = (
        "Strict DI-loans-n.e.c. measurement-audit diagnostics are available and ask whether any clean same-scope transaction split exists inside the remaining DI bucket."
    )
    if target is not None and best_cross_scope and best_proxy:
        best_cross_scope_response = dict(best_cross_scope.get("response", {}) or {})
        best_proxy_response = dict(best_proxy.get("response", {}) or {})
        headline_read = (
            "The DI-loans-n.e.c. measurement audit now says the public data still do not isolate a promotable same-scope transaction split: "
            f"DI aggregate ≈ {float(target):.2f}, best cross-scope bridge = `{str(best_cross_scope.get('outcome', 'not_available'))}` "
            f"at ≈ {float(best_cross_scope_response.get('beta', 0.0)):.2f}, best same-scope proxy = `{str(best_proxy.get('outcome', 'not_available'))}` "
            f"at ≈ {float(best_proxy_response.get('beta', 0.0)):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_di_loans_nec_measurement_audit_summary.json",
        "headline_read": headline_read,
        "candidate_groups": dict(strict_di_loans_nec_measurement_audit_summary.get("candidate_groups", {}) or {}),
        "classification": dict(strict_di_loans_nec_measurement_audit_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_di_loans_nec_measurement_audit_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(strict_di_loans_nec_measurement_audit_summary.get("key_horizons", {}) or {}),
        "takeaways": list(strict_di_loans_nec_measurement_audit_summary.get("takeaways", [])),
    }


def _strict_results_closeout_context(
    strict_results_closeout_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_results_closeout_summary is None:
        return {}
    if str(strict_results_closeout_summary.get("status", "not_available")) != "available":
        return {
            "status": str(strict_results_closeout_summary.get("status", "not_available")),
            "artifact": "strict_results_closeout_summary.json",
            "headline_read": "Strict-results closeout diagnostics are not available in the current run.",
        }
    h0 = dict(strict_results_closeout_summary.get("h0_snapshot", {}) or {})
    headline_read = (
        "The strict closeout summary is available and says the empirical expansion branch is effectively complete under current evidence."
    )
    if h0:
        headline_read = (
            "The strict closeout summary now freezes the end-state directly: "
            f"core residual ≈ {float(h0.get('core_residual_beta', 0.0)):.2f}, "
            f"headline direct core ≈ {float(h0.get('headline_direct_core_beta', 0.0)):.2f}, "
            f"standard bridge comparison ≈ {float(h0.get('standard_bridge_beta', 0.0)):.2f}."
        )
    return {
        "status": "available",
        "artifact": "strict_results_closeout_summary.json",
        "headline_read": headline_read,
        "release_position": dict(strict_results_closeout_summary.get("release_position", {}) or {}),
        "settled_findings": list(strict_results_closeout_summary.get("settled_findings", [])),
        "evidence_tiers": dict(strict_results_closeout_summary.get("evidence_tiers", {}) or {}),
        "unresolved_questions": list(strict_results_closeout_summary.get("unresolved_questions", [])),
        "classification": dict(strict_results_closeout_summary.get("classification", {}) or {}),
        "recommendation": dict(strict_results_closeout_summary.get("recommendation", {}) or {}),
        "takeaways": list(strict_results_closeout_summary.get("takeaways", [])),
    }


def _tdcest_ladder_integration_context(
    tdcest_ladder_integration_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if tdcest_ladder_integration_summary is None:
        return {}
    if str(tdcest_ladder_integration_summary.get("status", "not_available")) != "available":
        return {
            "status": str(tdcest_ladder_integration_summary.get("status", "not_available")),
            "artifact": "tdcest_ladder_integration_summary.json",
            "headline_read": "Selective tdcest ladder-integration diagnostics are not available in the current run.",
        }
    roles = list(tdcest_ladder_integration_summary.get("series_roles", []) or [])
    tier2 = next((row for row in roles if str(row.get("series_key")) == "tdc_tier2_bank_only_qoq"), {})
    tier3 = next((row for row in roles if str(row.get("series_key")) == "tdc_tier3_bank_only_qoq"), {})
    hist = next((row for row in roles if str(row.get("series_key")) == "tdc_bank_receipt_historical_overlay_qoq"), {})
    mrv = next((row for row in roles if str(row.get("series_key")) == "tdc_row_mrv_nondefault_pilot_qoq"), {})
    headline_read = (
        "Selective tdcest ladder integration is available: corrected broad-object comparisons, a historical-only bank-receipt overlay, "
        "and a bounded nondefault ROW pilot are now imported without changing the frozen strict framework."
    )
    if tier2.get("latest_value") is not None and tier3.get("latest_value") is not None:
        headline_read = (
            "The tdcest ladder is now partially integrated into tdcpass without a full pivot: "
            f"latest Tier 2 bank-only comparison ≈ {float(tier2['latest_value']):.2f}, "
            f"latest Tier 3 bank-only comparison ≈ {float(tier3['latest_value']):.2f}, "
            f"historical overlay latest nonzero quarter = {str(hist.get('latest_nonzero_quarter'))}, "
            f"ROW MRV pilot latest nonzero quarter = {str(mrv.get('latest_nonzero_quarter'))}."
        )
    return {
        "status": "available",
        "artifact": "tdcest_ladder_integration_summary.json",
        "headline_read": headline_read,
        "classification": dict(tdcest_ladder_integration_summary.get("classification", {}) or {}),
        "recommendation": dict(tdcest_ladder_integration_summary.get("recommendation", {}) or {}),
        "series_roles": roles,
        "takeaways": list(tdcest_ladder_integration_summary.get("takeaways", [])),
    }


def _tdcest_broad_object_comparison_context(
    tdcest_broad_object_comparison_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if tdcest_broad_object_comparison_summary is None:
        return {}
    if str(tdcest_broad_object_comparison_summary.get("status", "not_available")) != "available":
        return {
            "status": str(tdcest_broad_object_comparison_summary.get("status", "not_available")),
            "artifact": "tdcest_broad_object_comparison_summary.json",
            "headline_read": "The tdcest broad-object comparison layer is not available in the current run.",
        }
    latest = dict(tdcest_broad_object_comparison_summary.get("latest_common_broad_comparison", {}) or {})
    overlay = dict(
        dict(tdcest_broad_object_comparison_summary.get("supplemental_surfaces", {}) or {}).get(
            "historical_bank_receipt_overlay", {}
        )
        or {}
    )
    mrv = dict(
        dict(tdcest_broad_object_comparison_summary.get("supplemental_surfaces", {}) or {}).get(
            "row_mrv_nondefault_pilot", {}
        )
        or {}
    )
    headline_read = (
        "The tdcest broad-object comparison layer is available and keeps the corrected ladder on the broad-object side only."
    )
    if latest:
        headline_read = (
            "Latest common broad-object comparison "
            f"{str(latest.get('quarter'))}: headline ≈ {float(latest.get('headline_bank_only_beta')):.2f}, "
            f"Tier 2 ≈ {float(latest.get('tier2_bank_only_beta')):.2f}, "
            f"Tier 3 ≈ {float(latest.get('tier3_bank_only_beta')):.2f}, "
            f"Tier 3 broad-depository ≈ {float(latest.get('tier3_broad_depository_beta')):.2f}; "
            f"historical overlay latest nonzero quarter = {str(overlay.get('latest_nonzero_quarter'))}, "
            f"ROW MRV pilot latest nonzero quarter = {str(mrv.get('latest_nonzero_quarter'))}."
        )
    return {
        "status": "available",
        "artifact": "tdcest_broad_object_comparison_summary.json",
        "headline_read": headline_read,
        "classification": dict(tdcest_broad_object_comparison_summary.get("classification", {}) or {}),
        "recommendation": dict(tdcest_broad_object_comparison_summary.get("recommendation", {}) or {}),
        "latest_common_broad_comparison": latest,
        "supplemental_surfaces": dict(tdcest_broad_object_comparison_summary.get("supplemental_surfaces", {}) or {}),
        "takeaways": list(tdcest_broad_object_comparison_summary.get("takeaways", [])),
    }


def _tdcest_broad_treatment_sensitivity_context(
    tdcest_broad_treatment_sensitivity_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if tdcest_broad_treatment_sensitivity_summary is None:
        return {}
    status = str(tdcest_broad_treatment_sensitivity_summary.get("status", "not_available"))
    if status != "available":
        reason = str(tdcest_broad_treatment_sensitivity_summary.get("reason", "")).strip()
        takeaways = list(tdcest_broad_treatment_sensitivity_summary.get("takeaways", []))
        headline_read = "The corrected tdcest broad-treatment sensitivity layer is not available in the current run."
        if status == "insufficient_history":
            headline_read = (
                "The corrected tdcest broad-treatment LP sensitivity layer is currently blocked by short history under the frozen shock-design gate, so the tdcest ladder remains a broad-object comparison only."
            )
            short_history = dict(tdcest_broad_treatment_sensitivity_summary.get("exploratory_short_history", {}) or {})
            baseline = dict(short_history.get("baseline", {}) or {})
            variants = dict(short_history.get("variants", {}) or {})
            baseline_total = dict(baseline.get("total_deposits") or {})
            baseline_other = dict(baseline.get("other_component") or {})
            tier2 = dict(variants.get("tier2_bank_only", {}) or {})
            tier2_total = dict(tier2.get("total_deposits") or {})
            tier2_other = dict(tier2.get("other_component") or {})
            if (
                baseline_total.get("beta") is not None
                and baseline_other.get("beta") is not None
                and tier2_total.get("beta") is not None
                and tier2_other.get("beta") is not None
            ):
                headline_read = (
                    "The corrected tdcest broad-treatment LP sensitivity layer is blocked by short history under the frozen shock-design gate. "
                    f"On a separate short-history exploratory h0 check, the common-sample baseline is total ≈ {float(baseline_total['beta']):.2f}, residual ≈ {float(baseline_other['beta']):.2f}, "
                    f"while Tier 2 is total ≈ {float(tier2_total['beta']):.2f}, residual ≈ {float(tier2_other['beta']):.2f}."
                )
        elif takeaways:
            headline_read = str(takeaways[0])
        elif reason:
            headline_read = reason.replace("_", " ")
        return {
            "status": status,
            "artifact": "tdcest_broad_treatment_sensitivity_summary.json",
            "headline_read": headline_read,
            "recommendation": dict(tdcest_broad_treatment_sensitivity_summary.get("recommendation", {}) or {}),
            "takeaways": takeaways,
            "exploratory_short_history": dict(tdcest_broad_treatment_sensitivity_summary.get("exploratory_short_history", {}) or {}),
        }
    h0 = dict(tdcest_broad_treatment_sensitivity_summary.get("key_horizons", {}).get("h0", {}) or {})
    baseline = dict(h0.get("baseline", {}) or {})
    variants = dict(h0.get("variants", {}) or {})
    headline_read = "The corrected tdcest broad-treatment variants are available as broad-object sensitivity checks."
    if baseline and variants:
        baseline_total = dict(baseline.get("total_deposits") or {})
        baseline_other = dict(baseline.get("other_component") or {})
        tier2 = dict(variants.get("tier2_bank_only", {}) or {})
        tier3 = dict(variants.get("tier3_bank_only", {}) or {})
        tier2_total = None if dict(tier2.get("total_deposits") or {}).get("beta") is None else float(dict(tier2.get("total_deposits") or {}).get("beta"))
        tier2_other = None if dict(tier2.get("other_component") or {}).get("beta") is None else float(dict(tier2.get("other_component") or {}).get("beta"))
        tier3_total = None if dict(tier3.get("total_deposits") or {}).get("beta") is None else float(dict(tier3.get("total_deposits") or {}).get("beta"))
        tier3_other = None if dict(tier3.get("other_component") or {}).get("beta") is None else float(dict(tier3.get("other_component") or {}).get("beta"))
        if (
            baseline_total.get("beta") is not None
            and baseline_other.get("beta") is not None
            and tier2_total is not None
            and tier2_other is not None
            and tier3_total is not None
            and tier3_other is not None
        ):
            headline_read = (
                "Corrected tdcest broad-treatment sensitivity now quantifies how much the broad headline moves: "
                f"baseline h0 total ≈ {float(baseline_total['beta']):.2f}, residual ≈ {float(baseline_other['beta']):.2f}; "
                f"Tier 2 h0 total ≈ {tier2_total:.2f}, residual ≈ {tier2_other:.2f}; "
                f"Tier 3 h0 total ≈ {tier3_total:.2f}, residual ≈ {tier3_other:.2f}."
            )
    return {
        "status": "available",
        "artifact": "tdcest_broad_treatment_sensitivity_summary.json",
        "headline_read": headline_read,
        "classification": dict(tdcest_broad_treatment_sensitivity_summary.get("classification", {}) or {}),
        "recommendation": dict(tdcest_broad_treatment_sensitivity_summary.get("recommendation", {}) or {}),
        "key_horizons": dict(tdcest_broad_treatment_sensitivity_summary.get("key_horizons", {}) or {}),
        "takeaways": list(tdcest_broad_treatment_sensitivity_summary.get("takeaways", [])),
    }


def build_pass_through_summary(
    *,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    identity_measurement_ladder: pd.DataFrame | None = None,
    sensitivity: pd.DataFrame,
    identity_sensitivity: pd.DataFrame | None = None,
    control_sensitivity: pd.DataFrame,
    identity_control_sensitivity: pd.DataFrame | None = None,
    sample_sensitivity: pd.DataFrame,
    identity_sample_sensitivity: pd.DataFrame | None = None,
    contrast: pd.DataFrame,
    lp_irf_regimes: pd.DataFrame,
    readiness: dict[str, Any],
    regime_diagnostics: dict[str, Any] | None = None,
    regime_specs: dict[str, Any] | None = None,
    structural_proxy_evidence: dict[str, Any] | None = None,
    proxy_coverage_summary: dict[str, Any] | None = None,
    counterpart_channel_scorecard: dict[str, Any] | None = None,
    scope_alignment_summary: dict[str, Any] | None = None,
    strict_identifiable_followup_summary: dict[str, Any] | None = None,
    broad_scope_system_summary: dict[str, Any] | None = None,
    tdc_treatment_audit_summary: dict[str, Any] | None = None,
    treasury_operating_cash_audit_summary: dict[str, Any] | None = None,
    rest_of_world_treasury_audit_summary: dict[str, Any] | None = None,
    toc_row_path_split_summary: dict[str, Any] | None = None,
    toc_row_excluded_interpretation_summary: dict[str, Any] | None = None,
    strict_missing_channel_summary: dict[str, Any] | None = None,
    strict_sign_mismatch_audit_summary: dict[str, Any] | None = None,
    strict_shock_composition_summary: dict[str, Any] | None = None,
    strict_top_gap_quarter_audit_summary: dict[str, Any] | None = None,
    strict_top_gap_quarter_direction_summary: dict[str, Any] | None = None,
    strict_top_gap_inversion_summary: dict[str, Any] | None = None,
    strict_top_gap_anomaly_summary: dict[str, Any] | None = None,
    strict_top_gap_anomaly_component_split_summary: dict[str, Any] | None = None,
    strict_top_gap_anomaly_di_loans_split_summary: dict[str, Any] | None = None,
    strict_top_gap_anomaly_backdrop_summary: dict[str, Any] | None = None,
    big_picture_synthesis_summary: dict[str, Any] | None = None,
    treatment_object_comparison_summary: dict[str, Any] | None = None,
    split_treatment_architecture_summary: dict[str, Any] | None = None,
    core_treatment_promotion_summary: dict[str, Any] | None = None,
    strict_redesign_summary: dict[str, Any] | None = None,
    strict_loan_core_redesign_summary: dict[str, Any] | None = None,
    strict_di_bucket_role_summary: dict[str, Any] | None = None,
    strict_di_bucket_bridge_summary: dict[str, Any] | None = None,
    strict_private_borrower_bridge_summary: dict[str, Any] | None = None,
    strict_nonfinancial_corporate_bridge_summary: dict[str, Any] | None = None,
    strict_private_offset_residual_summary: dict[str, Any] | None = None,
    strict_corporate_bridge_secondary_comparison_summary: dict[str, Any] | None = None,
    strict_component_framework_summary: dict[str, Any] | None = None,
    toc_row_incidence_audit_summary: dict[str, Any] | None = None,
    toc_row_liability_incidence_raw_summary: dict[str, Any] | None = None,
    toc_validated_share_candidate_summary: dict[str, Any] | None = None,
    strict_release_framing_summary: dict[str, Any] | None = None,
    strict_direct_core_component_summary: dict[str, Any] | None = None,
    strict_direct_core_horizon_stability_summary: dict[str, Any] | None = None,
    strict_additional_creator_candidate_summary: dict[str, Any] | None = None,
    strict_di_loans_nec_measurement_audit_summary: dict[str, Any] | None = None,
    strict_results_closeout_summary: dict[str, Any] | None = None,
    tdcest_ladder_integration_summary: dict[str, Any] | None = None,
    tdcest_broad_object_comparison_summary: dict[str, Any] | None = None,
    tdcest_broad_treatment_sensitivity_summary: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    primary_lp_irf = identity_lp_irf if identity_lp_irf is not None and not identity_lp_irf.empty else lp_irf
    primary_decomposition_mode = (
        "exact_identity_baseline"
        if identity_lp_irf is not None and not identity_lp_irf.empty
        else "approximate_dynamic_decomposition"
    )
    primary_sensitivity = identity_sensitivity if identity_sensitivity is not None and not identity_sensitivity.empty else sensitivity
    primary_control_sensitivity = (
        identity_control_sensitivity
        if identity_control_sensitivity is not None and not identity_control_sensitivity.empty
        else control_sensitivity
    )
    primary_sample_sensitivity = (
        identity_sample_sensitivity
        if identity_sample_sensitivity is not None and not identity_sample_sensitivity.empty
        else sample_sensitivity
    )
    baseline_contrast = (
        contrast[(contrast["scope"] == "baseline") & (contrast["variant"] == "baseline")].copy()
        if not contrast.empty
        else pd.DataFrame()
    )
    approximate_dynamic_robustness = {
        "status": "not_available",
        "artifact": None,
        "max_abs_gap_h0_h4": None,
        "key_horizon_consistent": None,
        "note": "No approximate dynamic decomposition robustness check is available.",
    }
    if not baseline_contrast.empty:
        key_contrast = baseline_contrast[baseline_contrast["horizon"].isin(horizons)].copy()
        max_abs_gap = None
        if not key_contrast.empty and "abs_gap" in key_contrast.columns and key_contrast["abs_gap"].notna().any():
            max_abs_gap = float(key_contrast["abs_gap"].dropna().max())
        approximate_dynamic_robustness = {
            "status": (
                "divergent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline" and key_contrast["contrast_consistent"].eq(False).any()
                else "consistent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline"
                else "primary_check"
            ),
            "artifact": "total_minus_other_contrast.csv",
            "max_abs_gap_h0_h4": max_abs_gap,
            "key_horizon_consistent": None if key_contrast.empty else bool(key_contrast["contrast_consistent"].fillna(False).all()),
            "note": (
                "Primary decomposition uses the exact identity-preserving baseline; the approximate dynamic path is retained only as a secondary robustness check."
                if primary_decomposition_mode == "exact_identity_baseline"
                else "The total-minus-other contrast remains part of the active decomposition check for this specification."
            ),
        }
    baseline = {}
    for horizon in horizons:
        total_row = _lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)
        other_row = _lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon)
        tdc_row = _lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)
        baseline[f"h{horizon}"] = _horizon_assessment(total_row, other_row)
        baseline[f"h{horizon}"]["direct_tdc_response"] = _snapshot(tdc_row)
        contrast_row = _contrast_row(contrast, scope="baseline", variant="baseline", horizon=horizon)
        baseline[f"h{horizon}"]["approximate_dynamic_tdc_gap"] = (
            float(contrast_row["gap_implied_minus_direct"]) if contrast_row is not None and contrast_row["gap_implied_minus_direct"] is not None else None
        )
        baseline[f"h{horizon}"]["approximate_dynamic_contrast_consistent"] = (
            bool(contrast_row["contrast_consistent"]) if contrast_row is not None else False
        )
        baseline[f"h{horizon}"]["primary_decomposition_mode"] = primary_decomposition_mode

    readiness_status = str(readiness.get("status", "not_ready"))
    treatment_freeze_status = str(readiness.get("treatment_freeze_status", "frozen"))
    treatment_quality_status = str(readiness.get("treatment_quality_status", "not_evaluated"))
    treatment_candidates = list(readiness.get("treatment_candidates", []))
    treatment_quality_gate = readiness.get("treatment_quality_gate")
    ratio_reporting_gate = readiness.get("ratio_reporting_gate")
    mechanism_scope = _mechanism_scope(
        readiness_status=readiness_status,
        structural_proxy_evidence=structural_proxy_evidence,
        proxy_coverage_summary=proxy_coverage_summary,
    )

    if treatment_freeze_status != "frozen":
        headline = (
            "Current run remains a reproducibility and deposit-response preview only because the baseline unexpected-TDC shock "
            "is still under review and not yet a credibly frozen treatment object."
        )
    elif treatment_quality_status == "fail":
        headline = (
            "Current run remains a deposit-response preview because the frozen baseline unexpected-TDC shock still fails "
            "its publishable shock-quality gate."
        )
    elif readiness_status == "not_ready":
        headline = (
            "Current run is informative as a deposit-response readout, but it does not yet support a clean "
            "pass-through-versus-crowd-out conclusion or broad mechanism attribution."
        )
    elif readiness_status == "provisional":
        headline = (
            "Current run shows an impact-stage sign pattern suggestive of crowd-out in the exact identity baseline, "
            "while persistence and mechanism attribution remain unsettled and ratio-based headline interpretation stays out of scope in the current release."
        )
    else:
        headline = (
            "Current run is strong enough to support a first deposit-response interpretation, with structural "
            "cross-checks consistent enough for cautious mechanism discussion."
        )

    regime_diagnostic_rows = {}
    if regime_diagnostics is not None:
        for row in regime_diagnostics.get("regimes", []):
            if isinstance(row, dict) and "regime" in row:
                regime_diagnostic_rows[str(row["regime"])] = row
    published_regimes = _regime_rows(lp_irf_regimes, horizons=horizons)
    publication_roles: dict[str, str] = {}
    if regime_specs is not None:
        for regime_name, regime_def in regime_specs.get("regimes", {}).items():
            if isinstance(regime_def, dict):
                publication_roles[str(regime_name)] = str(regime_def.get("publication_role", "published"))
    for row in published_regimes:
        diag = regime_diagnostic_rows.get(str(row["regime"]), {})
        row["stable_for_interpretation"] = bool(diag.get("stable_for_interpretation", False))
        row["stability_warnings"] = list(diag.get("stability_warnings", []))
        row["publication_role"] = publication_roles.get(str(row["regime"]), "published")
    published_regimes = [
        row
        for row in published_regimes
        if row.get("publication_role") != "diagnostic_only" and row.get("stable_for_interpretation", False)
    ]
    sample_variant_rows = _sample_variant_rows(primary_sample_sensitivity, horizons=horizons)
    flagged_window_robustness = _flagged_window_robustness(sample_variant_rows, horizons=horizons)
    measurement_variant_source = (
        identity_measurement_ladder
        if identity_measurement_ladder is not None and not identity_measurement_ladder.empty
        else primary_sensitivity[primary_sensitivity.get("treatment_family", "").eq("measurement")]
        if "treatment_family" in primary_sensitivity.columns
        else pd.DataFrame()
    )

    return {
        "status": readiness_status,
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": "lp_irf_identity_baseline.csv"
            if primary_decomposition_mode == "exact_identity_baseline"
            else "lp_irf.csv",
            "approximate_robustness_artifact": "total_minus_other_contrast.csv",
            "measurement_variant_artifact": "identity_measurement_ladder.csv"
            if identity_measurement_ladder is not None and not identity_measurement_ladder.empty
            else "tdc_sensitivity_ladder.csv",
            "treatment_variant_artifact": "identity_treatment_sensitivity.csv"
            if identity_sensitivity is not None and not identity_sensitivity.empty
            else "tdc_sensitivity_ladder.csv",
            "control_variant_artifact": "identity_control_sensitivity.csv"
            if identity_control_sensitivity is not None and not identity_control_sensitivity.empty
            else "control_set_sensitivity.csv",
            "sample_variant_artifact": "identity_sample_sensitivity.csv"
            if identity_sample_sensitivity is not None and not identity_sample_sensitivity.empty
            else "shock_sample_sensitivity.csv",
            "approximate_dynamic_robustness": approximate_dynamic_robustness,
        },
        "treatment_freeze_status": treatment_freeze_status,
        "treatment_candidates": treatment_candidates,
        "treatment_quality_status": treatment_quality_status,
        "treatment_quality_gate": treatment_quality_gate,
        "ratio_reporting_gate": ratio_reporting_gate,
        "interpretation_scope": mechanism_scope,
        "headline_question": (
            "When unexpected bank-only TDC rises, how do total deposits and the non-TDC deposit component respond?"
        ),
        "headline_answer": headline,
        "mechanism_caveat": (
            "Structural proxies remain cross-checks on the residual, not standalone proof of mechanism; creator-lane evidence is surfaced separately in counterpart_channel_scorecard.json, and bank_credit_private_qoq remains only a coarse legacy creator proxy."
        ),
        "sample_policy": {
            "headline_sample_variant": "all_usable_shocks",
            "flagged_window_variant": "drop_flagged_shocks",
            "severe_tail_variant": "drop_severe_scale_tail",
            "headline_rule": "Keep the frozen headline sample and publish flagged-window trimming as a robustness check, not as a replacement estimand.",
        },
        "flagged_window_robustness": flagged_window_robustness,
        "baseline_horizons": baseline,
        "core_treatment_variants": _variant_rows(
            primary_sensitivity,
            variant_column="treatment_variant",
            role_column="treatment_role",
            allowed_roles={"core"},
            horizons=horizons,
        ),
        "measurement_treatment_variants": _variant_rows(
            measurement_variant_source,
            variant_column="treatment_variant",
            role_column="treatment_role",
            allowed_roles={"exploratory"},
            horizons=horizons,
        ),
        "shock_design_treatment_variants": _variant_rows(
            primary_sensitivity[primary_sensitivity.get("treatment_family", "").eq("shock_design")]
            if "treatment_family" in primary_sensitivity.columns
            else pd.DataFrame(),
            variant_column="treatment_variant",
            role_column="treatment_role",
            allowed_roles={"exploratory"},
            horizons=horizons,
        ),
        "core_control_variants": _variant_rows(
            primary_control_sensitivity,
            variant_column="control_variant",
            role_column="control_role",
            allowed_roles={"headline", "core"},
            horizons=horizons,
        ),
        "shock_sample_variants": sample_variant_rows,
        "structural_proxy_context": {}
        if structural_proxy_evidence is None
        else dict(structural_proxy_evidence.get("key_horizons", {})),
        "proxy_coverage_context": {}
        if proxy_coverage_summary is None
        else {
            "status": str(proxy_coverage_summary.get("status", "weak")),
            "key_horizons": dict(proxy_coverage_summary.get("key_horizons", {})),
            "published_regime_contexts": list(proxy_coverage_summary.get("published_regime_contexts", [])),
            "release_caveat": str(proxy_coverage_summary.get("release_caveat", "")),
        },
        "counterpart_channel_context": _counterpart_channel_context(
            counterpart_channel_scorecard,
            horizons=horizons,
        ),
        "scope_alignment_context": _scope_alignment_context(
            scope_alignment_summary,
            horizons=horizons,
        ),
        "strict_gap_scope_check_context": _strict_gap_scope_check_context(
            strict_identifiable_followup_summary,
            horizons=horizons,
        ),
        "broad_scope_system_context": _broad_scope_system_context(
            broad_scope_system_summary,
            horizons=horizons,
        ),
        "tdc_treatment_audit_context": _tdc_treatment_audit_context(
            tdc_treatment_audit_summary,
            horizons=horizons,
        ),
        "treasury_operating_cash_audit_context": _treasury_operating_cash_audit_context(
            treasury_operating_cash_audit_summary,
            horizons=horizons,
        ),
        "rest_of_world_treasury_audit_context": _rest_of_world_treasury_audit_context(
            rest_of_world_treasury_audit_summary,
            horizons=horizons,
        ),
        "toc_row_path_split_context": _toc_row_path_split_context(
            toc_row_path_split_summary,
            horizons=horizons,
        ),
        "toc_row_excluded_interpretation_context": _toc_row_excluded_interpretation_context(
            toc_row_excluded_interpretation_summary,
            horizons=horizons,
        ),
        "strict_missing_channel_context": _strict_missing_channel_context(
            strict_missing_channel_summary,
            horizons=horizons,
        ),
        "strict_sign_mismatch_audit_context": _strict_sign_mismatch_audit_context(
            strict_sign_mismatch_audit_summary,
            horizons=horizons,
        ),
        "strict_shock_composition_context": _strict_shock_composition_context(
            strict_shock_composition_summary,
        ),
        "strict_top_gap_quarter_audit_context": _strict_top_gap_quarter_audit_context(
            strict_top_gap_quarter_audit_summary,
        ),
        "strict_top_gap_quarter_direction_context": _strict_top_gap_quarter_direction_context(
            strict_top_gap_quarter_direction_summary,
        ),
        "strict_top_gap_inversion_context": _strict_top_gap_inversion_context(
            strict_top_gap_inversion_summary,
        ),
        "strict_top_gap_anomaly_context": _strict_top_gap_anomaly_context(
            strict_top_gap_anomaly_summary,
        ),
        "strict_top_gap_anomaly_component_split_context": _strict_top_gap_anomaly_component_split_context(
            strict_top_gap_anomaly_component_split_summary,
        ),
        "strict_top_gap_anomaly_di_loans_split_context": _strict_top_gap_anomaly_di_loans_split_context(
            strict_top_gap_anomaly_di_loans_split_summary,
        ),
        "strict_top_gap_anomaly_backdrop_context": _strict_top_gap_anomaly_backdrop_context(
            strict_top_gap_anomaly_backdrop_summary,
        ),
        "big_picture_synthesis_context": _big_picture_synthesis_context(
            big_picture_synthesis_summary,
        ),
        "treatment_object_comparison_context": _treatment_object_comparison_context(
            treatment_object_comparison_summary,
        ),
        "split_treatment_architecture_context": _split_treatment_architecture_context(
            split_treatment_architecture_summary,
        ),
        "core_treatment_promotion_context": _core_treatment_promotion_context(
            core_treatment_promotion_summary,
        ),
        "strict_redesign_context": _strict_redesign_context(
            strict_redesign_summary,
        ),
        "strict_loan_core_redesign_context": _strict_loan_core_redesign_context(
            strict_loan_core_redesign_summary,
        ),
        "strict_di_bucket_role_context": _strict_di_bucket_role_context(
            strict_di_bucket_role_summary,
        ),
        "strict_di_bucket_bridge_context": _strict_di_bucket_bridge_context(
            strict_di_bucket_bridge_summary,
        ),
        "strict_private_borrower_bridge_context": _strict_private_borrower_bridge_context(
            strict_private_borrower_bridge_summary,
        ),
        "strict_nonfinancial_corporate_bridge_context": _strict_nonfinancial_corporate_bridge_context(
            strict_nonfinancial_corporate_bridge_summary,
        ),
        "strict_private_offset_residual_context": _strict_private_offset_residual_context(
            strict_private_offset_residual_summary,
        ),
        "strict_corporate_bridge_secondary_comparison_context": _strict_corporate_bridge_secondary_comparison_context(
            strict_corporate_bridge_secondary_comparison_summary,
        ),
        "strict_component_framework_context": _strict_component_framework_context(
            strict_component_framework_summary,
        ),
        "toc_row_incidence_audit_context": _toc_row_incidence_audit_context(
            toc_row_incidence_audit_summary,
        ),
        "toc_row_liability_incidence_raw_context": _toc_row_liability_incidence_raw_context(
            toc_row_liability_incidence_raw_summary,
        ),
        "toc_validated_share_candidate_context": _toc_validated_share_candidate_context(
            toc_validated_share_candidate_summary,
        ),
        "strict_release_framing_context": _strict_release_framing_context(
            strict_release_framing_summary,
        ),
        "strict_direct_core_component_context": _strict_direct_core_component_context(
            strict_direct_core_component_summary,
        ),
        "strict_direct_core_horizon_stability_context": _strict_direct_core_horizon_stability_context(
            strict_direct_core_horizon_stability_summary,
        ),
        "strict_additional_creator_candidate_context": _strict_additional_creator_candidate_context(
            strict_additional_creator_candidate_summary,
        ),
        "strict_di_loans_nec_measurement_audit_context": _strict_di_loans_nec_measurement_audit_context(
            strict_di_loans_nec_measurement_audit_summary,
        ),
        "strict_results_closeout_context": _strict_results_closeout_context(
            strict_results_closeout_summary,
        ),
        "tdcest_ladder_integration_context": _tdcest_ladder_integration_context(
            tdcest_ladder_integration_summary,
        ),
        "tdcest_broad_object_comparison_context": _tdcest_broad_object_comparison_context(
            tdcest_broad_object_comparison_summary,
        ),
        "tdcest_broad_treatment_sensitivity_context": _tdcest_broad_treatment_sensitivity_context(
            tdcest_broad_treatment_sensitivity_summary,
        ),
        "published_regime_contexts": published_regimes,
        "readiness_reasons": list(readiness.get("reasons", [])),
        "readiness_warnings": list(readiness.get("warnings", [])),
    }
