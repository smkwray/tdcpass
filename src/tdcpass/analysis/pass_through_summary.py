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


def build_pass_through_summary(
    *,
    lp_irf: pd.DataFrame,
    sensitivity: pd.DataFrame,
    control_sensitivity: pd.DataFrame,
    sample_sensitivity: pd.DataFrame,
    contrast: pd.DataFrame,
    lp_irf_regimes: pd.DataFrame,
    readiness: dict[str, Any],
    regime_diagnostics: dict[str, Any] | None = None,
    regime_specs: dict[str, Any] | None = None,
    structural_proxy_evidence: dict[str, Any] | None = None,
    proxy_coverage_summary: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    baseline = {}
    for horizon in horizons:
        total_row = _lp_row(lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)
        other_row = _lp_row(lp_irf, outcome="other_component_qoq", horizon=horizon)
        tdc_row = _lp_row(lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)
        baseline[f"h{horizon}"] = _horizon_assessment(total_row, other_row)
        baseline[f"h{horizon}"]["direct_tdc_response"] = _snapshot(tdc_row)
        contrast_row = _contrast_row(contrast, scope="baseline", variant="baseline", horizon=horizon)
        baseline[f"h{horizon}"]["direct_vs_identity_tdc_gap"] = (
            float(contrast_row["gap_implied_minus_direct"]) if contrast_row is not None and contrast_row["gap_implied_minus_direct"] is not None else None
        )
        baseline[f"h{horizon}"]["contrast_consistent"] = bool(contrast_row["contrast_consistent"]) if contrast_row is not None else False

    readiness_status = str(readiness.get("status", "not_ready"))
    mechanism_scope = _mechanism_scope(
        readiness_status=readiness_status,
        structural_proxy_evidence=structural_proxy_evidence,
        proxy_coverage_summary=proxy_coverage_summary,
    )

    if readiness_status == "not_ready":
        headline = (
            "Current run is informative as a deposit-response readout, but it does not yet support a clean "
            "pass-through-versus-crowd-out conclusion or broad mechanism attribution."
        )
    elif readiness_status == "provisional":
        headline = (
            "Current run supports a provisional deposit-response interpretation with partial mechanism "
            "cross-checks, but the pass-through-versus-crowd-out read remains fragile."
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

    return {
        "status": readiness_status,
        "interpretation_scope": mechanism_scope,
        "headline_question": (
            "When unexpected bank-only TDC rises, how do total deposits and the non-TDC deposit component respond?"
        ),
        "headline_answer": headline,
        "mechanism_caveat": (
            "Structural proxies remain cross-checks on the residual, not standalone proof of mechanism."
        ),
        "sample_policy": {
            "headline_sample_variant": "all_usable_shocks",
            "flagged_window_variant": "drop_flagged_shocks",
            "headline_rule": "Keep the frozen headline sample and publish flagged-window trimming as a robustness check, not as a replacement estimand.",
        },
        "baseline_horizons": baseline,
        "core_treatment_variants": _variant_rows(
            sensitivity,
            variant_column="treatment_variant",
            role_column="treatment_role",
            allowed_roles={"core"},
            horizons=horizons,
        ),
        "core_control_variants": _variant_rows(
            control_sensitivity,
            variant_column="control_variant",
            role_column="control_role",
            allowed_roles={"headline", "core"},
            horizons=horizons,
        ),
        "shock_sample_variants": _sample_variant_rows(sample_sensitivity, horizons=horizons),
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
        "published_regime_contexts": published_regimes,
        "readiness_reasons": list(readiness.get("reasons", [])),
        "readiness_warnings": list(readiness.get("warnings", [])),
    }
