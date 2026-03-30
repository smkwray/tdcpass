from __future__ import annotations

from typing import Any

import pandas as pd


def _variant_row(
    sensitivity: pd.DataFrame,
    *,
    treatment_variant: str,
    outcome: str,
    horizon: int,
) -> dict[str, Any] | None:
    sample = sensitivity[
        (sensitivity["treatment_variant"] == treatment_variant)
        & (sensitivity["outcome"] == outcome)
        & (sensitivity["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _row_snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "treatment_role": str(row.get("treatment_role", "")),
        "shock_column": str(row.get("shock_column", "")),
        "shock_scale": str(row.get("shock_scale", "")),
        "response_type": str(row.get("response_type", "")),
    }


def _variant_shock_target_map(shock_specs: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if shock_specs is None:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for spec in shock_specs.values():
        if not isinstance(spec, dict):
            continue
        shock_column = spec.get("standardized_column")
        if not shock_column:
            continue
        mapping[str(shock_column)] = {
            "target": str(spec.get("target", "")),
            "model_name": str(spec.get("model_name", "")),
            "predictors": [str(item) for item in spec.get("predictors", [])],
            "min_train_obs": int(spec.get("min_train_obs", 0)),
        }
    return mapping


def _comparison_payload(
    *,
    shocks: pd.DataFrame,
    treatment_variant: str,
    treatment_role: str,
    variant_shock_column: str,
    variant_target_column: str | None,
    baseline_shock_column: str,
    baseline_target_column: str,
    outcome_column: str,
    impact_row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    required = [baseline_shock_column, variant_shock_column, baseline_target_column, outcome_column]
    if variant_target_column:
        required.append(variant_target_column)
    missing = [column for column in required if column not in shocks.columns]
    if missing:
        return None
    usable = shocks.dropna(subset=required).copy()
    overlap = int(len(usable))
    if overlap == 0:
        return None
    sign_disagreement = usable[baseline_shock_column].mul(usable[variant_shock_column]).lt(0.0)
    target_corr = None
    if variant_target_column:
        target_corr = float(usable[baseline_target_column].corr(usable[variant_target_column]))
    return {
        "treatment_variant": treatment_variant,
        "treatment_role": treatment_role,
        "shock_column": variant_shock_column,
        "target_column": variant_target_column,
        "overlap_observations": overlap,
        "sample_start_quarter": str(usable["quarter"].iloc[0]),
        "sample_end_quarter": str(usable["quarter"].iloc[-1]),
        "shock_correlation": float(usable[baseline_shock_column].corr(usable[variant_shock_column])),
        "target_correlation": target_corr,
        "shock_sign_disagreement_quarters": int(sign_disagreement.sum()),
        "shock_sign_disagreement_share": float(sign_disagreement.mean()),
        "baseline_shock_outcome_correlation": float(usable[baseline_shock_column].corr(usable[outcome_column])),
        "variant_shock_outcome_correlation": float(usable[variant_shock_column].corr(usable[outcome_column])),
        "impact_total_deposits_h0": _row_snapshot(impact_row),
    }


def build_shock_diagnostics_summary(
    *,
    shocks: pd.DataFrame,
    sensitivity: pd.DataFrame,
    baseline_shock_spec: dict[str, Any] | None = None,
    shock_specs: dict[str, Any] | None = None,
    baseline_shock_column: str = "tdc_residual_z",
    alternate_shock_column: str = "tdc_broad_depository_residual_z",
    baseline_target_column: str = "tdc_bank_only_qoq",
    alternate_target_column: str = "tdc_broad_depository_qoq",
    outcome_column: str = "total_deposits_bank_qoq",
) -> dict[str, Any]:
    usable = shocks.dropna(
        subset=[
            baseline_shock_column,
            alternate_shock_column,
            baseline_target_column,
            alternate_target_column,
            outcome_column,
        ]
    ).copy()

    overlap = int(len(usable))
    if overlap:
        shock_corr = float(usable[baseline_shock_column].corr(usable[alternate_shock_column]))
        target_corr = float(usable[baseline_target_column].corr(usable[alternate_target_column]))
        outcome_corr_baseline = float(usable[baseline_shock_column].corr(usable[outcome_column]))
        outcome_corr_alternate = float(usable[alternate_shock_column].corr(usable[outcome_column]))
        sign_disagreement = (
            usable[baseline_shock_column].mul(usable[alternate_shock_column]).lt(0.0)
        )
        quarters = usable.assign(
            shock_diff=usable[baseline_shock_column] - usable[alternate_shock_column]
        ).reindex(
            usable.assign(
                shock_diff=usable[baseline_shock_column] - usable[alternate_shock_column]
            )["shock_diff"].abs().sort_values(ascending=False).index
        )
        largest_disagreement_quarters = [
            {
                "quarter": str(row["quarter"]),
                "baseline_shock": float(row[baseline_shock_column]),
                "alternate_shock": float(row[alternate_shock_column]),
                "baseline_target_qoq": float(row[baseline_target_column]),
                "alternate_target_qoq": float(row[alternate_target_column]),
                "total_deposits_bank_qoq": float(row[outcome_column]),
            }
            for _, row in quarters.head(8).iterrows()
        ]
    else:
        shock_corr = None
        target_corr = None
        outcome_corr_baseline = None
        outcome_corr_alternate = None
        largest_disagreement_quarters = []

    baseline_h0 = _variant_row(
        sensitivity,
        treatment_variant="baseline",
        outcome=outcome_column,
        horizon=0,
    )
    alternate_h0 = _variant_row(
        sensitivity,
        treatment_variant="broad_depository",
        outcome=outcome_column,
        horizon=0,
    )

    shock_target_map = _variant_shock_target_map(shock_specs)
    treatment_variant_comparisons: list[dict[str, Any]] = []
    seen_variants: set[str] = set()
    for _, row in sensitivity[
        (sensitivity["outcome"] == outcome_column) & (sensitivity["horizon"] == 0)
    ].iterrows():
        treatment_variant = str(row.get("treatment_variant", ""))
        if treatment_variant in {"", "baseline"} or treatment_variant in seen_variants:
            continue
        seen_variants.add(treatment_variant)
        variant_shock_column = str(row.get("shock_column", ""))
        metadata = shock_target_map.get(variant_shock_column, {})
        comparison = _comparison_payload(
            shocks=shocks,
            treatment_variant=treatment_variant,
            treatment_role=str(row.get("treatment_role", "")),
            variant_shock_column=variant_shock_column,
            variant_target_column=str(metadata.get("target", "")) or None,
            baseline_shock_column=baseline_shock_column,
            baseline_target_column=baseline_target_column,
            outcome_column=outcome_column,
            impact_row=row.to_dict(),
        )
        if comparison is None:
            continue
        comparison["model_name"] = str(metadata.get("model_name", ""))
        comparison["predictors"] = list(metadata.get("predictors", []))
        comparison["min_train_obs"] = metadata.get("min_train_obs")
        treatment_variant_comparisons.append(comparison)

    takeaways: list[str] = [
        "LP betas are in outcome units per one rolling out-of-sample shock standard deviation when the shock column is standardized.",
    ]
    if shock_corr is None:
        takeaways.append("No overlap is available to compare baseline and alternate shocks.")
    else:
        if abs(shock_corr) < 0.25:
            takeaways.append("Baseline and broad-depository shocks are only weakly correlated in the usable sample.")
        if abs(target_corr) < 0.25:
            takeaways.append("Bank-only and broad-depository TDC quarterly changes are only weakly correlated in the usable sample.")
        if baseline_h0 is not None and alternate_h0 is not None:
            baseline_beta = float(baseline_h0["beta"])
            alternate_beta = float(alternate_h0["beta"])
            if baseline_beta == 0.0 or alternate_beta == 0.0 or baseline_beta * alternate_beta < 0.0:
                takeaways.append(
                    "The sensitivity sign disagreement is driven by materially different treatment objects, not by a simple coefficient-rescaling mismatch."
                )
        if alternate_h0 is not None and str(alternate_h0.get("treatment_role", "")) == "exploratory":
            takeaways.append(
                "The broad-depository variant is classified as exploratory and should not be treated as a near-baseline robustness rung."
            )
    for comparison in treatment_variant_comparisons:
        if comparison["treatment_role"] == "core" and (
            abs(float(comparison["shock_correlation"])) < 0.5
            or float(comparison["shock_sign_disagreement_share"]) > 0.25
        ):
            takeaways.append(
                f"Core variant {comparison['treatment_variant']} is not behaving like a near-baseline shock object and should be interpreted cautiously."
            )
        if comparison["treatment_role"] == "exploratory" and (
            abs(float(comparison["shock_correlation"])) < 0.5
            or float(comparison["shock_sign_disagreement_share"]) > 0.25
        ):
            takeaways.append(
                f"Exploratory variant {comparison['treatment_variant']} is materially different from the headline shock object and should stay out of core robustness claims."
            )
    baseline_flags = usable.get("shock_flag")
    flagged_count = 0
    flagged_share = None
    max_scale_ratio = None
    max_condition_number = None
    if baseline_flags is not None:
        flag_text = baseline_flags.fillna("").astype(str)
        flagged_count = int(flag_text.ne("").sum())
        flagged_share = float(flag_text.ne("").mean()) if len(flag_text) else None
        if flagged_count > 0:
            takeaways.append("Some baseline shock windows are flagged as numerically unstable or badly scaled.")
    if "fitted_to_target_scale_ratio" in shocks.columns:
        max_scale_ratio = float(shocks["fitted_to_target_scale_ratio"].dropna().max()) if shocks["fitted_to_target_scale_ratio"].notna().any() else None
    if "train_condition_number" in shocks.columns:
        max_condition_number = float(shocks["train_condition_number"].dropna().max()) if shocks["train_condition_number"].notna().any() else None

    return {
        "estimand_interpretation": {
            "baseline_shock_column": baseline_shock_column,
            "baseline_target_column": baseline_target_column,
            "alternate_shock_column": alternate_shock_column,
            "alternate_target_column": alternate_target_column,
            "response_type": "cumulative_sum_of_quarterly_changes_from_h0_to_h",
            "outcome_units": f"same units as {outcome_column} in quarterly_panel.csv",
            "shock_scale": "per_one_rolling_out_of_sample_standard_deviation",
            "baseline_model_name": None if baseline_shock_spec is None else str(baseline_shock_spec.get("model_name", "")),
            "baseline_predictors": [] if baseline_shock_spec is None else [str(item) for item in baseline_shock_spec.get("predictors", [])],
            "baseline_min_train_obs": None if baseline_shock_spec is None else int(baseline_shock_spec.get("min_train_obs", 0)),
        },
        "sample_comparison": {
            "overlap_observations": overlap,
            "sample_start_quarter": str(usable["quarter"].iloc[0]) if overlap else None,
            "sample_end_quarter": str(usable["quarter"].iloc[-1]) if overlap else None,
            "shock_correlation": shock_corr,
            "target_correlation": target_corr,
            "shock_sign_disagreement_quarters": int(sign_disagreement.sum()) if overlap else 0,
            "shock_sign_disagreement_share": float(sign_disagreement.mean()) if overlap else None,
            "baseline_shock_mean": float(usable[baseline_shock_column].mean()) if overlap else None,
            "baseline_shock_std": float(usable[baseline_shock_column].std()) if overlap else None,
            "alternate_shock_mean": float(usable[alternate_shock_column].mean()) if overlap else None,
            "alternate_shock_std": float(usable[alternate_shock_column].std()) if overlap else None,
            "baseline_target_mean": float(usable[baseline_target_column].mean()) if overlap else None,
            "baseline_target_std": float(usable[baseline_target_column].std()) if overlap else None,
            "alternate_target_mean": float(usable[alternate_target_column].mean()) if overlap else None,
            "alternate_target_std": float(usable[alternate_target_column].std()) if overlap else None,
            "baseline_shock_outcome_correlation": outcome_corr_baseline,
            "alternate_shock_outcome_correlation": outcome_corr_alternate,
        },
        "impact_response_comparison": {
            "baseline_total_deposits_h0": _row_snapshot(baseline_h0),
            "broad_depository_total_deposits_h0": _row_snapshot(alternate_h0),
        },
        "treatment_variant_comparisons": treatment_variant_comparisons,
        "shock_quality": {
            "flagged_observations": flagged_count,
            "flagged_share": flagged_share,
            "max_fitted_to_target_scale_ratio": max_scale_ratio,
            "max_train_condition_number": max_condition_number,
        },
        "largest_disagreement_quarters": largest_disagreement_quarters,
        "takeaways": takeaways,
    }
