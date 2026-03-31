from __future__ import annotations

from typing import Any

import pandas as pd


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _beta_sign(row: dict[str, Any] | None) -> str:
    if row is None:
        return "missing"
    beta = float(row["beta"])
    if beta > 0.0:
        return "positive"
    if beta < 0.0:
        return "negative"
    return "zero"


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "beta": float(row["beta"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "sign": _beta_sign(row),
    }


def _comparison_payload(
    baseline_row: dict[str, Any] | None,
    factor_row: dict[str, Any] | None,
) -> dict[str, Any]:
    baseline = _snapshot(baseline_row)
    factor = _snapshot(factor_row)
    baseline_n = None if baseline is None else int(baseline["n"])
    factor_n = None if factor is None else int(factor["n"])
    n_ratio = None
    n_drop = None
    sign_flip = False
    if baseline_n not in {None, 0} and factor_n is not None:
        n_ratio = float(factor_n / baseline_n)
        n_drop = int(baseline_n - factor_n)
    if baseline is not None and factor is not None:
        sign_flip = baseline["sign"] in {"positive", "negative"} and factor["sign"] in {"positive", "negative"} and baseline["sign"] != factor["sign"]
    return {
        "baseline": baseline,
        "factor_augmented": factor,
        "n_ratio_vs_baseline": n_ratio,
        "n_drop_vs_baseline": n_drop,
        "sign_flip_vs_baseline": sign_flip,
    }


def build_factor_control_diagnostics_summary(
    *,
    control_sensitivity: pd.DataFrame,
    factor_control_sensitivity: pd.DataFrame,
    baseline_variant: str = "headline_lagged_macro",
    key_outcomes: tuple[str, ...] = ("total_deposits_bank_qoq", "other_component_qoq"),
    key_horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if factor_control_sensitivity.empty:
        return {
            "status": "no_factor_rows",
            "headline_question": "Do factor-augmented controls preserve enough quarterly support to be informative?",
            "baseline_control_variant": baseline_variant,
            "factor_variants": [],
            "takeaways": ["No factor-augmented LP rows were produced, so the recursive factor path is currently unevaluable."],
        }

    baseline_df = control_sensitivity[control_sensitivity["control_variant"] == baseline_variant].copy()
    factor_variants: list[dict[str, Any]] = []
    severe_history_loss = False
    sign_flip_count = 0
    core_variants_present = False
    core_severe_history_loss = False
    core_sign_flip_count = 0
    core_adequate_variant_count = 0

    for factor_variant in factor_control_sensitivity["factor_variant"].drop_duplicates().tolist():
        variant_df = factor_control_sensitivity[factor_control_sensitivity["factor_variant"] == factor_variant].copy()
        factor_role = str(variant_df.iloc[0].get("factor_role", "exploratory"))
        if factor_role == "core":
            core_variants_present = True
        factor_columns = str(variant_df.iloc[0].get("factor_columns", ""))
        source_columns = str(variant_df.iloc[0].get("source_columns", ""))
        factor_count = int(variant_df.iloc[0].get("factor_count", 0))
        min_train_obs = int(variant_df.iloc[0].get("min_train_obs", 0))

        horizon_payloads: dict[str, Any] = {}
        min_ratio = None
        for horizon in key_horizons:
            outcome_payloads: dict[str, Any] = {}
            for outcome in key_outcomes:
                baseline_row = _lp_row(baseline_df, outcome=outcome, horizon=horizon)
                factor_row = _lp_row(variant_df, outcome=outcome, horizon=horizon)
                comparison = _comparison_payload(baseline_row, factor_row)
                ratio = comparison["n_ratio_vs_baseline"]
                if ratio is not None:
                    min_ratio = ratio if min_ratio is None else min(min_ratio, ratio)
                    if ratio < 0.5:
                        severe_history_loss = True
                        if factor_role == "core":
                            core_severe_history_loss = True
                if comparison["sign_flip_vs_baseline"]:
                    sign_flip_count += 1
                    if factor_role == "core":
                        core_sign_flip_count += 1
                outcome_payloads[outcome] = comparison
            horizon_payloads[f"h{horizon}"] = outcome_payloads

        coverage_label = "adequate"
        if min_ratio is None:
            coverage_label = "missing_baseline_comparison"
        elif min_ratio < 0.35:
            coverage_label = "severe_history_loss"
        elif min_ratio < 0.6:
            coverage_label = "material_history_loss"
        elif min_ratio < 0.8:
            coverage_label = "moderate_history_loss"
        else:
            if factor_role == "core":
                core_adequate_variant_count += 1

        factor_variants.append(
            {
                "factor_variant": str(factor_variant),
                "factor_role": factor_role,
                "factor_columns": factor_columns.split("|") if factor_columns else [],
                "source_columns": source_columns.split("|") if source_columns else [],
                "factor_count": factor_count,
                "min_train_obs": min_train_obs,
                "coverage_label": coverage_label,
                "min_key_horizon_n_ratio": min_ratio,
                "key_horizons": horizon_payloads,
            }
        )

    status = "adequate"
    if core_variants_present:
        if core_severe_history_loss:
            status = "short_history"
        elif core_sign_flip_count > 0:
            status = "mixed"
        elif core_adequate_variant_count > 0:
            status = "core_adequate"
    elif severe_history_loss:
        status = "short_history"
    elif sign_flip_count > 0:
        status = "mixed"

    takeaways = [
        "Factor-control diagnostics compare the recursive factor LP against the headline lagged-macro control variant at key horizons.",
    ]
    if status == "short_history":
        takeaways.append("The current core factor-control specification loses a large share of the usable quarterly LP sample at key horizons.")
    elif status == "mixed":
        takeaways.append("The current core factor-control specification changes the sign of at least one key outcome relative to the headline control LP.")
    elif status == "core_adequate":
        takeaways.append("At least one core factor-control specification preserves the headline LP sample at key horizons without flipping signs.")
    else:
        takeaways.append("The current factor-control specification preserves enough support to compare coefficient movement against the headline control LP.")
    if severe_history_loss and status != "short_history":
        takeaways.append("Some exploratory factor-control variants still lose a large share of the usable quarterly LP sample.")

    return {
        "status": status,
        "headline_question": "Do factor-augmented controls preserve enough quarterly support to be informative?",
        "baseline_control_variant": baseline_variant,
        "factor_variants": factor_variants,
        "takeaways": takeaways,
    }
