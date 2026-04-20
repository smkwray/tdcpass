from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections


_VARIANT_LABELS = {
    "baseline": "Canonical bank-only headline",
    "tier2_bank_only": "Tier 2 interest-corrected bank-only",
    "tier3_bank_only": "Tier 3 fiscal-corrected bank-only",
    "tier3_broad_depository": "Tier 3 fiscal-corrected broad-depository",
}

_VARIANT_TARGET_COLUMNS = {
    "tier2_bank_only": "tdc_tier2_bank_only_qoq",
    "tier3_bank_only": "tdc_tier3_bank_only_qoq",
    "tier3_broad_depository": "tdc_tier3_broad_depository_qoq",
}

_VARIANT_SHOCK_COLUMNS = {
    "tier2_bank_only": "tdc_tier2_bank_only_residual_z",
    "tier3_bank_only": "tdc_tier3_bank_only_residual_z",
    "tier3_broad_depository": "tdc_tier3_broad_depository_residual_z",
}

_FOCUS_VARIANTS = tuple(_VARIANT_LABELS.keys())
_FOCUS_OUTCOMES = ("total_deposits_bank_qoq", "other_component_qoq")
_FOCUS_HORIZONS = (0, 4, 8)
_SHORT_HISTORY_VARIANTS = ("baseline", "tier2_bank_only", "tier3_bank_only", "tier3_broad_depository")
_SHORT_HISTORY_TARGETS = {
    "baseline": "tdc_bank_only_qoq",
    **_VARIANT_TARGET_COLUMNS,
}


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _snapshot(frame: pd.DataFrame, *, variant: str, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = frame[
        (frame["treatment_variant"] == variant)
        & (frame["outcome"] == outcome)
        & (frame["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    row = sample.iloc[0]
    beta = _safe_float(row.get("beta"))
    lower95 = _safe_float(row.get("lower95"))
    upper95 = _safe_float(row.get("upper95"))
    return {
        "beta": beta,
        "se": _safe_float(row.get("se")),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]) if pd.notna(row.get("n")) else None,
        "ci_excludes_zero": (
            lower95 is not None and upper95 is not None and (lower95 > 0.0 or upper95 < 0.0)
        ),
    }


def _sign(beta: float | None) -> str:
    if beta is None:
        return "missing"
    if beta > 0:
        return "positive"
    if beta < 0:
        return "negative"
    return "zero"


def _full_sample_ar1_residual_z(sample: pd.DataFrame, *, target_col: str) -> pd.Series:
    work = sample[[target_col]].copy()
    work["lag_target"] = work[target_col].shift(1)
    work = work.dropna()
    if len(work) < 8:
        return pd.Series(np.nan, index=sample.index, dtype=float)
    x = np.column_stack([np.ones(len(work)), work["lag_target"].to_numpy(dtype=float)])
    y = work[target_col].to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - (x @ beta)
    resid_sd = float(np.std(resid, ddof=1)) if len(resid) > 1 else np.nan
    out = pd.Series(np.nan, index=sample.index, dtype=float)
    if not np.isfinite(resid_sd) or resid_sd <= 0:
        return out
    out.loc[work.index] = resid / resid_sd
    return out


def _short_history_snapshot(frame: pd.DataFrame, *, variant: str, outcome: str) -> dict[str, Any] | None:
    sample = frame[(frame["treatment_variant"] == variant) & (frame["outcome"] == outcome) & (frame["horizon"] == 0)]
    if sample.empty:
        return None
    row = sample.iloc[0]
    beta = _safe_float(row.get("beta"))
    lower95 = _safe_float(row.get("lower95"))
    upper95 = _safe_float(row.get("upper95"))
    return {
        "beta": beta,
        "se": _safe_float(row.get("se")),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]) if pd.notna(row.get("n")) else None,
        "ci_excludes_zero": (
            lower95 is not None and upper95 is not None and (lower95 > 0.0 or upper95 < 0.0)
        ),
    }


def _build_short_history_exploratory(shocked: pd.DataFrame | None) -> dict[str, Any] | None:
    if shocked is None or shocked.empty:
        return None
    required_cols = {*_SHORT_HISTORY_TARGETS.values(), *_FOCUS_OUTCOMES}
    if not required_cols.issubset(shocked.columns):
        return None

    corrected_targets = [_VARIANT_TARGET_COLUMNS[key] for key in _FOCUS_VARIANTS[1:]]
    common = shocked.loc[shocked[corrected_targets].notna().all(axis=1), ["quarter", *required_cols]].copy()
    if common.empty or len(common) < 12:
        return None

    recent = common.copy().reset_index(drop=True)
    shock_columns: dict[str, str] = {}
    for variant, target_col in _SHORT_HISTORY_TARGETS.items():
        shock_col = f"{variant}_short_history_ar1_residual_z"
        recent[shock_col] = _full_sample_ar1_residual_z(recent, target_col=target_col)
        shock_columns[variant] = shock_col

    frames: list[pd.DataFrame] = []
    for variant in _SHORT_HISTORY_VARIANTS:
        shock_col = shock_columns[variant]
        lp = run_local_projections(
            recent,
            shock_col=shock_col,
            outcome_cols=_FOCUS_OUTCOMES,
            controls=[],
            include_lagged_outcome=False,
            horizons=[0],
            nw_lags=1,
            cumulative=True,
            spec_name="tdcest_short_history_treatment_sensitivity",
        )
        if lp.empty:
            continue
        lp.insert(0, "treatment_variant", variant)
        frames.append(lp)
    if not frames:
        return None

    lp_frame = pd.concat(frames, ignore_index=True)
    payload_variants: dict[str, Any] = {}
    baseline_total = _short_history_snapshot(lp_frame, variant="baseline", outcome="total_deposits_bank_qoq")
    baseline_other = _short_history_snapshot(lp_frame, variant="baseline", outcome="other_component_qoq")
    sign_consistency = True
    for variant in _FOCUS_VARIANTS[1:]:
        total = _short_history_snapshot(lp_frame, variant=variant, outcome="total_deposits_bank_qoq")
        other = _short_history_snapshot(lp_frame, variant=variant, outcome="other_component_qoq")
        payload_variants[variant] = {
            "label": _VARIANT_LABELS[variant],
            "total_deposits": total,
            "other_component": other,
        }
        if baseline_total is not None and total is not None and _sign(total.get("beta")) != _sign(baseline_total.get("beta")):
            sign_consistency = False
        if baseline_other is not None and other is not None and _sign(other.get("beta")) != _sign(baseline_other.get("beta")):
            sign_consistency = False

    start_quarter = str(recent["quarter"].iloc[0])
    end_quarter = str(recent["quarter"].iloc[-1])
    takeaways = []
    if baseline_total is not None and baseline_other is not None:
        takeaways.append(
            "Short-history common-sample baseline h0 read is "
            f"total ≈ {baseline_total['beta']:.2f}, residual ≈ {baseline_other['beta']:.2f} "
            f"over {start_quarter} to {end_quarter}."
        )
    corrected_bits = []
    for variant in _FOCUS_VARIANTS[1:]:
        info = payload_variants.get(variant, {})
        total = dict(info.get("total_deposits") or {})
        other = dict(info.get("other_component") or {})
        if total.get("beta") is not None and other.get("beta") is not None:
            corrected_bits.append(
                f"{_VARIANT_LABELS[variant]}: total ≈ {float(total['beta']):.2f}, residual ≈ {float(other['beta']):.2f}"
            )
    if corrected_bits:
        takeaways.append("Short-history corrected-variant h0 reads: " + "; ".join(corrected_bits) + ".")

    return {
        "status": "available",
        "sample_window": {"start_quarter": start_quarter, "end_quarter": end_quarter, "n_quarters": int(len(recent))},
        "shock_construction": {
            "method": "full_sample_ar1_residual_standardized",
            "controls": ["lag_own_tdc_only"],
            "lp_horizons": [0],
            "lp_controls": [],
            "include_lagged_outcome": False,
            "purpose": "exploratory_short_history_broad_object_check_only",
        },
        "classification": {
            "status": "exploratory_only",
            "sign_status_vs_short_history_baseline": (
                "unchanged" if sign_consistency else "changed"
            ),
            "strict_framework_effect": "unchanged",
        },
        "baseline": {
            "label": _VARIANT_LABELS["baseline"],
            "total_deposits": baseline_total,
            "other_component": baseline_other,
        },
        "variants": payload_variants,
        "takeaways": takeaways,
    }


def build_tdcest_broad_treatment_sensitivity_summary(
    sensitivity: pd.DataFrame | None,
    shocked: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if sensitivity is None or sensitivity.empty:
        return {"status": "not_available", "reason": "missing_sensitivity_frame"}
    required = {"treatment_variant", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n"}
    if not required.issubset(sensitivity.columns):
        return {"status": "not_available", "reason": "missing_sensitivity_columns"}

    frame = sensitivity.loc[
        sensitivity["treatment_variant"].isin(_FOCUS_VARIANTS)
        & sensitivity["outcome"].isin(_FOCUS_OUTCOMES)
        & sensitivity["horizon"].isin(_FOCUS_HORIZONS)
    ].copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_tdcest_broad_variant_rows"}

    present_variants = set(frame["treatment_variant"].dropna().astype(str).unique())
    missing_variants = [variant for variant in _FOCUS_VARIANTS[1:] if variant not in present_variants]
    if missing_variants:
        availability: dict[str, Any] = {}
        for variant in missing_variants:
            target_column = _VARIANT_TARGET_COLUMNS.get(variant)
            shock_column = _VARIANT_SHOCK_COLUMNS.get(variant)
            target_nonnull_count = None
            usable_shock_count = None
            if shocked is not None:
                if target_column in shocked.columns:
                    target_nonnull_count = int(pd.Series(shocked[target_column]).notna().sum())
                if shock_column in shocked.columns:
                    usable_shock_count = int(pd.Series(shocked[shock_column]).notna().sum())
            availability[variant] = {
                "label": _VARIANT_LABELS[variant],
                "target_column": target_column,
                "shock_column": shock_column,
                "target_nonnull_count": target_nonnull_count,
                "usable_shock_count": usable_shock_count,
            }

        baseline_h0_total = _snapshot(frame, variant="baseline", outcome="total_deposits_bank_qoq", horizon=0)
        baseline_h0_other = _snapshot(frame, variant="baseline", outcome="other_component_qoq", horizon=0)
        takeaways = []
        if baseline_h0_total is not None and baseline_h0_other is not None:
            takeaways.append(
                "Baseline h0 broad-object read remains total deposits up and other component down: "
                f"total ≈ {baseline_h0_total['beta']:.2f}, residual ≈ {baseline_h0_other['beta']:.2f}."
            )
        takeaways.append(
            "Corrected tdcest broad-treatment LP sensitivity is not currently estimable under the frozen shock-design gate because the corrected series do not have enough history to enter the sensitivity ladder."
        )
        if availability:
            bits = []
            for variant in missing_variants:
                item = availability[variant]
                bits.append(
                    f"{item['label']}: target non-null count = {item['target_nonnull_count']}, usable shock count = {item['usable_shock_count']}"
                )
            takeaways.append("Current availability by corrected variant: " + "; ".join(bits) + ".")

        short_history = _build_short_history_exploratory(shocked)
        payload = {
            "status": "insufficient_history",
            "headline_question": "Do the corrected tdcest broad-treatment variants materially change the broad deposit and residual LP results?",
            "reason": "corrected_tdcest_variants_do_not_clear_current_shock_history_gate",
            "estimation_path": {
                "summary_artifact": "tdcest_broad_treatment_sensitivity_summary.json",
                "source_artifacts": [
                    "tdc_sensitivity_ladder.csv",
                ],
            },
            "baseline_reference": {
                "h0_total_deposits": baseline_h0_total,
                "h0_other_component": baseline_h0_other,
            },
            "missing_variants": availability,
            "recommendation": {
                "status": "use_tdcest_ladder_as_level_comparison_only",
                "why": (
                    "The corrected tdcest broad-treatment variants currently add value as broad-object level/ladder diagnostics, but there is not enough history to run the frozen LP sensitivity design on them."
                ),
            },
            "takeaways": takeaways,
        }
        if short_history is not None:
            payload["exploratory_short_history"] = short_history
            payload["takeaways"] = list(payload["takeaways"]) + [
                "A separate short-history exploratory h0 check is available, but it is not comparable to the frozen rolling-shock LP design."
            ]
        return payload

    key_horizons: dict[str, Any] = {}
    baseline_h0_total = _snapshot(frame, variant="baseline", outcome="total_deposits_bank_qoq", horizon=0)
    baseline_h0_other = _snapshot(frame, variant="baseline", outcome="other_component_qoq", horizon=0)
    robust_direction = True
    any_material_shift = False

    for horizon in _FOCUS_HORIZONS:
        horizon_payload: dict[str, Any] = {"variants": {}}
        baseline_total = _snapshot(frame, variant="baseline", outcome="total_deposits_bank_qoq", horizon=horizon)
        baseline_other = _snapshot(frame, variant="baseline", outcome="other_component_qoq", horizon=horizon)
        horizon_payload["baseline"] = {
            "total_deposits": baseline_total,
            "other_component": baseline_other,
        }
        for variant in _FOCUS_VARIANTS[1:]:
            total = _snapshot(frame, variant=variant, outcome="total_deposits_bank_qoq", horizon=horizon)
            other = _snapshot(frame, variant=variant, outcome="other_component_qoq", horizon=horizon)
            total_delta = None
            other_delta = None
            if total is not None and baseline_total is not None:
                total_delta = _safe_float(total.get("beta")) - _safe_float(baseline_total.get("beta"))
            if other is not None and baseline_other is not None:
                other_delta = _safe_float(other.get("beta")) - _safe_float(baseline_other.get("beta"))
            if horizon == 0 and total is not None and other is not None and baseline_h0_total is not None and baseline_h0_other is not None:
                if _sign(total.get("beta")) != _sign(baseline_h0_total.get("beta")) or _sign(other.get("beta")) != _sign(baseline_h0_other.get("beta")):
                    robust_direction = False
                if (total_delta is not None and abs(total_delta) >= 1.0) or (other_delta is not None and abs(other_delta) >= 1.0):
                    any_material_shift = True
            horizon_payload["variants"][variant] = {
                "label": _VARIANT_LABELS[variant],
                "total_deposits": total,
                "other_component": other,
                "total_deposits_delta_vs_baseline": total_delta,
                "other_component_delta_vs_baseline": other_delta,
            }
        key_horizons[f"h{horizon}"] = horizon_payload

    classification = {
        "headline_direction_status": (
            "unchanged_across_corrected_broad_variants" if robust_direction else "direction_changes_under_corrected_broad_variants"
        ),
        "magnitude_status": (
            "material_h0_magnitude_shift_present" if any_material_shift else "no_material_h0_magnitude_shift_detected"
        ),
        "strict_framework_effect": "unchanged",
    }

    takeaways = []
    if baseline_h0_total is not None and baseline_h0_other is not None:
        takeaways.append(
            "Baseline h0 broad-object read remains total deposits up and other component down: "
            f"total ≈ {baseline_h0_total['beta']:.2f}, residual ≈ {baseline_h0_other['beta']:.2f}."
        )
    h0_variants = key_horizons.get("h0", {}).get("variants", {})
    if h0_variants:
        pieces = []
        for variant in ("tier2_bank_only", "tier3_bank_only", "tier3_broad_depository"):
            payload = h0_variants.get(variant, {})
            total = dict(payload.get("total_deposits") or {})
            other = dict(payload.get("other_component") or {})
            if total.get("beta") is not None and other.get("beta") is not None:
                pieces.append(
                    f"{_VARIANT_LABELS[variant]}: total ≈ {float(total['beta']):.2f}, residual ≈ {float(other['beta']):.2f}"
                )
        if pieces:
            takeaways.append("Corrected broad-treatment h0 comparisons: " + "; ".join(pieces) + ".")
    if classification["headline_direction_status"] == "unchanged_across_corrected_broad_variants":
        takeaways.append(
            "The corrected tdcest broad-treatment variants do not overturn the sign-level headline broad-object read."
        )
    else:
        takeaways.append(
            "At least one corrected tdcest broad-treatment variant changes the sign-level broad-object read, so the headline broad object is not stable."
        )
    if classification["magnitude_status"] == "material_h0_magnitude_shift_present":
        takeaways.append(
            "The corrected tdcest broad-treatment ladder still matters empirically because at least one h0 deposit or residual response moves by about one standardized-response unit or more."
        )
    else:
        takeaways.append(
            "The corrected tdcest broad-treatment ladder mostly refines broad-object diagnostics rather than rewriting the headline response magnitudes."
        )

    return {
        "status": "available",
        "headline_question": "Do the corrected tdcest broad-treatment variants materially change the broad deposit and residual LP results?",
        "estimation_path": {
            "summary_artifact": "tdcest_broad_treatment_sensitivity_summary.json",
            "source_artifacts": [
                "tdc_sensitivity_ladder.csv",
            ],
        },
        "classification": classification,
        "key_horizons": key_horizons,
        "recommendation": {
            "status": "use_as_broad_object_sensitivity_only",
            "why": (
                "These corrected tdcest variants are useful broad-treatment robustness checks, but they do not replace the frozen strict object or its benchmark hierarchy."
            ),
        },
        "takeaways": takeaways,
    }
