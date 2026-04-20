from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _share(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _toc_leg_horizon(*, toc_horizon: Mapping[str, Any]) -> dict[str, Any]:
    toc_response = dict(toc_horizon.get("treasury_operating_cash_response", {}) or {})
    toc_signed = _safe_float(toc_horizon.get("treasury_operating_cash_signed_contribution_beta"))
    tga_response = dict(toc_horizon.get("tga_response", {}) or {})
    reserves_response = dict(toc_horizon.get("reserves_response", {}) or {})
    cb_nonts_response = dict(toc_horizon.get("cb_nonts_response", {}) or {})
    reserve_beta = _safe_float(reserves_response.get("beta"))
    deposit_beta = _safe_float(cb_nonts_response.get("beta"))
    interpretation = "not_classified"
    if toc_signed is not None and reserve_beta is not None and deposit_beta is not None:
        if abs(deposit_beta) < abs(reserve_beta) and abs(deposit_beta) < abs(toc_signed):
            interpretation = "reserve_plumbing_real_but_strict_deposit_incidence_partial"
        else:
            interpretation = "deposit_incidence_not_separable_from_reserve_release"
    return {
        "toc_signed_contribution_beta": toc_signed,
        "treasury_operating_cash_response": toc_response or None,
        "tga_response": tga_response or None,
        "reserves_response": reserves_response or None,
        "in_scope_deposit_proxy_response": cb_nonts_response or None,
        "reserve_capture_share_of_toc_beta": _share(reserve_beta, toc_signed),
        "in_scope_deposit_proxy_share_of_toc_beta": _share(deposit_beta, toc_signed),
        "interpretation": interpretation,
    }


def _row_leg_horizon(*, row_horizon: Mapping[str, Any]) -> dict[str, Any]:
    row_response = dict(row_horizon.get("rest_of_world_treasury_response", {}) or {})
    row_beta = _safe_float(row_response.get("beta"))
    deposit_proxy = dict(row_horizon.get("checkable_rest_of_world_bank_response", {}) or {})
    foreign_nonts = dict(row_horizon.get("foreign_nonts_response", {}) or {})
    foreign_asset = dict(row_horizon.get("interbank_transactions_foreign_banks_asset_response", {}) or {})
    deposit_beta = _safe_float(deposit_proxy.get("beta"))
    external_beta = _safe_float(foreign_nonts.get("beta"))
    foreign_asset_beta = _safe_float(foreign_asset.get("beta"))
    interpretation = "not_classified"
    if row_beta is not None and deposit_beta is not None and external_beta is not None:
        if abs(deposit_beta) < abs(row_beta) * 0.25 and abs(external_beta) > abs(row_beta):
            interpretation = "weak_in_scope_deposit_incidence_external_support_dominant"
        else:
            interpretation = "deposit_incidence_not_cleanly_separable"
    return {
        "row_treasury_response": row_response or None,
        "in_scope_deposit_proxy_response": deposit_proxy or None,
        "external_support_proxy_response": foreign_nonts or None,
        "foreign_bank_asset_proxy_response": foreign_asset or None,
        "in_scope_deposit_proxy_share_of_row_beta": _share(deposit_beta, row_beta),
        "external_support_share_of_row_beta": _share(external_beta, row_beta),
        "foreign_bank_asset_share_of_row_beta": _share(foreign_asset_beta, row_beta),
        "interpretation": interpretation,
    }


def build_toc_row_incidence_audit_summary(
    *,
    treasury_operating_cash_audit_summary: Mapping[str, Any] | None,
    rest_of_world_treasury_audit_summary: Mapping[str, Any] | None,
    toc_row_path_split_summary: Mapping[str, Any] | None,
    split_treatment_architecture_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    required = (
        treasury_operating_cash_audit_summary,
        rest_of_world_treasury_audit_summary,
        toc_row_path_split_summary,
        split_treatment_architecture_summary,
    )
    if any(summary is None for summary in required):
        return {"status": "not_available", "reason": "missing_input_summary"}
    if any(str(summary.get("status", "not_available")) != "available" for summary in required):
        return {"status": "not_available", "reason": "input_summary_not_available"}

    toc_quarterly = dict(treasury_operating_cash_audit_summary.get("quarterly_alignment", {}) or {})
    row_quarterly = dict(rest_of_world_treasury_audit_summary.get("quarterly_alignment", {}) or {})
    path_quarterly = dict(toc_row_path_split_summary.get("quarterly_split", {}) or {})
    split_h0 = dict(split_treatment_architecture_summary.get("key_horizons", {}).get("h0", {}) or {})

    toc_counterpart_corr = _safe_float(toc_quarterly.get("contemporaneous_corr_tga_vs_toc"))
    row_counterparts = dict(row_quarterly.get("counterparts", {}) or {})
    row_checkable_corr = _safe_float(
        dict(row_counterparts.get("checkable_rest_of_world_bank_qoq", {}) or {}).get("contemporaneous_corr")
    )
    row_external_corr = _safe_float(
        dict(row_counterparts.get("foreign_nonts_qoq", {}) or {}).get("contemporaneous_corr")
    )
    path_corrs = dict(path_quarterly.get("bundle_contemporaneous_corr", {}) or {})
    quarterly_preferred = str(path_quarterly.get("preferred_quarterly_path", "not_available"))

    key_horizons: dict[str, Any] = {}
    for horizon_key in ("h0", "h1"):
        toc_horizon = dict(treasury_operating_cash_audit_summary.get("key_horizons", {}).get(horizon_key, {}) or {})
        row_horizon = dict(rest_of_world_treasury_audit_summary.get("key_horizons", {}).get(horizon_key, {}) or {})
        split_horizon = dict(toc_row_path_split_summary.get("key_horizons", {}).get(horizon_key, {}) or {})
        key_horizons[horizon_key] = {
            "toc_leg": _toc_leg_horizon(toc_horizon=toc_horizon),
            "row_leg": _row_leg_horizon(row_horizon=row_horizon),
            "bundle": {
                "support_bundle_beta": _safe_float(split_h0.get("support_bundle_beta")) if horizon_key == "h0" else None,
                "broad_support_path_response": dict(split_horizon.get("broad_support_path_response", {}) or {}) or None,
                "direct_deposit_path_response": dict(split_horizon.get("direct_deposit_path_response", {}) or {}) or None,
                "preferred_horizon_path": str(split_horizon.get("preferred_horizon_path", "not_available")),
            },
        }

    toc_h0 = dict(key_horizons.get("h0", {}).get("toc_leg", {}) or {})
    row_h0 = dict(key_horizons.get("h0", {}).get("row_leg", {}) or {})
    toc_interpretation = str(toc_h0.get("interpretation", "not_classified"))
    row_interpretation = str(row_h0.get("interpretation", "not_classified"))
    framework_verdict = "measured_support_bundle_with_unresolved_strict_deposit_incidence"
    if toc_interpretation == "reserve_plumbing_real_but_strict_deposit_incidence_partial" and row_interpretation == "weak_in_scope_deposit_incidence_external_support_dominant":
        framework_verdict = "toc_partial_deposit_incidence_row_weak_deposit_incidence"

    support_bundle_beta = _safe_float(split_h0.get("support_bundle_beta"))
    core_residual_beta = _safe_float(dict(split_h0.get("core_deposit_proximate_residual_response", {}) or {}).get("beta"))
    toc_signed_beta = _safe_float(split_h0.get("toc_signed_beta"))
    row_signed_beta = _safe_float(split_h0.get("row_signed_beta"))

    takeaways = [
        "This is the first-pass liability-incidence audit for the TOC/ROW block. It treats the bundle as economically real and asks whether each leg behaves like an in-scope bank-deposit component rather than only like broad support.",
        "The goal here is not to relitigate arithmetic. It is to test strict deposit incidence by leg using the already-built TOC, ROW, and path-split evidence.",
    ]
    if toc_counterpart_corr is not None:
        takeaways.append(
            f"TOC continues to look like real Treasury cash plumbing in raw quarterly data: contemporaneous TGA-vs-TOC corr ≈ {float(toc_counterpart_corr):.2f}."
        )
    toc_deposit_share = toc_h0.get("in_scope_deposit_proxy_share_of_toc_beta")
    toc_reserve_share = toc_h0.get("reserve_capture_share_of_toc_beta")
    if toc_deposit_share is not None and toc_reserve_share is not None:
        takeaways.append(
            "But TOC does not yet earn strict-component status: "
            f"at h0 the in-scope deposit proxy captures only about {float(toc_deposit_share):.2f} of TOC while reserves capture about {float(toc_reserve_share):.2f}."
        )
    if row_checkable_corr is not None and row_external_corr is not None:
        takeaways.append(
            "ROW looks weaker as an in-scope deposit leg and stronger as broader external support: "
            f"checkable-ROW-bank corr ≈ {float(row_checkable_corr):.2f}, foreign-NONTS corr ≈ {float(row_external_corr):.2f}."
        )
    row_deposit_share = row_h0.get("in_scope_deposit_proxy_share_of_row_beta")
    row_external_share = row_h0.get("external_support_share_of_row_beta")
    if row_deposit_share is not None and row_external_share is not None:
        takeaways.append(
            "The h0 ROW split points the same way: "
            f"in-scope deposit proxy share ≈ {float(row_deposit_share):.2f}, external-support share ≈ {float(row_external_share):.2f}."
        )
    if None not in (support_bundle_beta, toc_signed_beta, row_signed_beta, core_residual_beta):
        takeaways.append(
            "The combined support bundle remains the dominant treatment-side issue: "
            f"h0 TOC/ROW support bundle ≈ {float(support_bundle_beta):.2f}, with TOC signed contribution ≈ {float(toc_signed_beta):.2f}, "
            f"ROW signed contribution ≈ {float(row_signed_beta):.2f}, while the remaining core residual is only ≈ {float(core_residual_beta):.2f}."
        )
    if quarterly_preferred:
        takeaways.append(
            "The existing path split still matters for interpretation: "
            f"quarterly preferred bundle path = `{quarterly_preferred}`, but the bundle is not yet validated as a strict bank-deposit component."
        )

    return {
        "status": "available",
        "headline_question": "Does the TOC/ROW bundle have validated strict deposit incidence, or does it remain a measured Treasury support bundle with unresolved in-scope deposit incidence?",
        "estimation_path": {
            "summary_artifact": "toc_row_incidence_audit_summary.json",
            "source_artifacts": [
                "treasury_operating_cash_audit_summary.json",
                "rest_of_world_treasury_audit_summary.json",
                "toc_row_path_split_summary.json",
                "split_treatment_architecture_summary.json",
            ],
            "raw_quarterly_scope": "uses raw quarterly alignment diagnostics from existing TOC and ROW audit surfaces plus LP h0/h1 responses",
        },
        "leg_definitions": {
            "toc_leg": "tdc_treasury_operating_cash_qoq",
            "row_leg": "tdc_row_treasury_transactions_qoq",
            "support_bundle": "tdc_toc_row_support_bundle_qoq",
            "toc_in_scope_deposit_proxy": "cb_nonts_qoq",
            "toc_reserve_proxy": "reserves_qoq",
            "row_in_scope_deposit_proxy": "checkable_rest_of_world_bank_qoq",
            "row_external_support_proxy": "foreign_nonts_qoq",
        },
        "quarterly_alignment": {
            "toc_tga_corr": toc_counterpart_corr,
            "row_checkable_bank_corr": row_checkable_corr,
            "row_external_support_corr": row_external_corr,
            "bundle_quarterly_preferred_path": quarterly_preferred,
            "bundle_direct_deposit_corr": _safe_float(path_corrs.get("direct_deposit_path")),
            "bundle_broad_support_corr": _safe_float(path_corrs.get("broad_support_path")),
        },
        "key_horizons": key_horizons,
        "classification": {
            "bundle_role": "measured_support_bundle_with_unresolved_strict_deposit_incidence",
            "toc_leg_status": toc_interpretation,
            "row_leg_status": row_interpretation,
            "framework_verdict": framework_verdict,
        },
        "recommendation": {
            "status": "use_as_next_binary_decision_gate",
            "strict_component_rule": "reincorporate_only_validated_in_scope_deposit_incidence_share",
            "if_no_validated_share": "keep_toc_row_outside_strict_object_and_leave_full_tdc_broad",
            "next_branch": "run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units",
        },
        "takeaways": takeaways,
    }
