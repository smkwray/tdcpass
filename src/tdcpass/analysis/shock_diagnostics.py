from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


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


def _treatment_freeze_status(baseline_shock_spec: dict[str, Any] | None) -> str:
    if baseline_shock_spec is None:
        return "frozen"
    return str(baseline_shock_spec.get("freeze_status", "frozen"))


def _treatment_candidates(shock_specs: dict[str, Any] | None) -> list[dict[str, Any]]:
    if shock_specs is None:
        return []
    candidates: list[dict[str, Any]] = []
    for name, spec in shock_specs.items():
        if not isinstance(spec, dict):
            continue
        if str(spec.get("candidate_role", "")) != "repair_candidate":
            continue
        candidates.append(
            {
                "name": str(name),
                "model_name": str(spec.get("model_name", "")),
                "shock_column": str(spec.get("standardized_column", "")),
                "raw_shock_column": str(spec.get("residual_column", "")),
                "target": str(spec.get("target", "")),
                "method": str(spec.get("method", "expanding_window_ols")),
                "min_train_obs": int(spec.get("min_train_obs", 0)),
                "max_train_obs": None if spec.get("max_train_obs") is None else int(spec.get("max_train_obs")),
                "predictors": [str(item) for item in spec.get("predictors", [])],
            }
        )
    return candidates


def _ratio_quantiles(series: pd.Series) -> dict[str, float]:
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    quantiles = cleaned.quantile([0.5, 0.9, 0.95, 0.99])
    return {f"p{int(index * 100):02d}": float(value) for index, value in quantiles.items()}


def _quality_gate_rules(baseline_shock_spec: dict[str, Any] | None) -> dict[str, Any]:
    if baseline_shock_spec is None:
        return {}
    gate = baseline_shock_spec.get("quality_gate", {})
    return dict(gate) if isinstance(gate, dict) else {}


def _baseline_quality_summary(
    *,
    shocks: pd.DataFrame,
    baseline_shock_spec: dict[str, Any] | None,
    baseline_shock_column: str,
    baseline_target_column: str,
) -> dict[str, Any]:
    spec = {} if baseline_shock_spec is None else dict(baseline_shock_spec)
    flag_column = str(spec.get("flag_column", "shock_flag"))
    scale_ratio_column = str(spec.get("scale_ratio_column", "fitted_to_target_scale_ratio"))
    condition_number_column = str(spec.get("condition_number_column", "train_condition_number"))
    target_sd_column = str(spec.get("target_sd_column", "train_target_sd"))
    usable = shocks.dropna(subset=[baseline_shock_column, baseline_target_column]).copy()
    usable_rows = int(len(usable))
    usable_target_sd = None if usable.empty else float(usable[baseline_target_column].std(ddof=1))
    initial_train_target_sd = None
    if target_sd_column in usable.columns and usable[target_sd_column].notna().any():
        initial_train_target_sd = float(usable[target_sd_column].dropna().iloc[0])
    train_to_usable_target_volatility_ratio = None
    if (
        initial_train_target_sd is not None
        and usable_target_sd is not None
        and pd.notna(usable_target_sd)
        and usable_target_sd > 0.0
    ):
        train_to_usable_target_volatility_ratio = float(initial_train_target_sd / usable_target_sd)
    shock_target_correlation_usable = None
    if usable_rows >= 2:
        shock_target_correlation_usable = float(usable[baseline_shock_column].corr(usable[baseline_target_column]))
    flagged_observations = 0
    flagged_share = None
    if flag_column in usable.columns:
        flag_text = usable[flag_column].fillna("").astype(str)
        flagged_observations = int(flag_text.ne("").sum())
        flagged_share = float(flag_text.ne("").mean()) if len(flag_text) else None
    realized_scale_fit_ratio_quantiles = (
        _ratio_quantiles(usable[scale_ratio_column]) if scale_ratio_column in usable.columns else {}
    )
    max_train_condition_number = None
    if condition_number_column in usable.columns and usable[condition_number_column].notna().any():
        max_train_condition_number = float(usable[condition_number_column].dropna().max())

    rules = _quality_gate_rules(spec)
    checks: dict[str, dict[str, Any]] = {}

    def _check(name: str, observed: Any, threshold: Any, passed: bool) -> None:
        checks[name] = {"observed": observed, "threshold": threshold, "passed": passed}

    if "min_usable_observations" in rules:
        threshold = int(rules["min_usable_observations"])
        _check("min_usable_observations", usable_rows, threshold, usable_rows >= threshold)
    if "min_shock_target_correlation" in rules:
        threshold = float(rules["min_shock_target_correlation"])
        observed = shock_target_correlation_usable
        _check(
            "min_shock_target_correlation",
            observed,
            threshold,
            observed is not None and pd.notna(observed) and float(observed) >= threshold,
        )
    if "max_flagged_share" in rules:
        threshold = float(rules["max_flagged_share"])
        observed = flagged_share
        _check(
            "max_flagged_share",
            observed,
            threshold,
            observed is not None and pd.notna(observed) and float(observed) <= threshold,
        )
    for quantile_name in ("p95", "p99"):
        rule_key = f"max_realized_scale_ratio_{quantile_name}"
        if rule_key not in rules:
            continue
        threshold = float(rules[rule_key])
        observed = realized_scale_fit_ratio_quantiles.get(quantile_name)
        _check(
            rule_key,
            observed,
            threshold,
            observed is not None and pd.notna(observed) and float(observed) <= threshold,
        )
    if "max_initial_train_to_usable_volatility_ratio" in rules:
        threshold = float(rules["max_initial_train_to_usable_volatility_ratio"])
        observed = train_to_usable_target_volatility_ratio
        _check(
            "max_initial_train_to_usable_volatility_ratio",
            observed,
            threshold,
            observed is not None and pd.notna(observed) and float(observed) <= threshold,
        )

    failed_checks = [name for name, payload in checks.items() if not payload["passed"]]
    treatment_quality_status = "pass" if checks and not failed_checks else "fail" if checks else "not_evaluated"

    return {
        "baseline_usable_sample": {
            "rows": usable_rows,
            "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
            "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
        },
        "shock_target_correlation_usable": shock_target_correlation_usable,
        "usable_target_sd": usable_target_sd,
        "initial_train_target_sd": initial_train_target_sd,
        "train_to_usable_target_volatility_ratio": train_to_usable_target_volatility_ratio,
        "realized_scale_fit_ratio_quantiles": realized_scale_fit_ratio_quantiles,
        "flagged_observations": flagged_observations,
        "flagged_share": flagged_share,
        "max_train_condition_number": max_train_condition_number,
        "treatment_quality_status": treatment_quality_status,
        "treatment_quality_gate": {
            "status": treatment_quality_status,
            "checks": checks,
            "failed_checks": failed_checks,
        },
    }


def _severe_tail_audit(
    *,
    shocks: pd.DataFrame,
    baseline_shock_spec: dict[str, Any] | None,
    baseline_shock_column: str,
    baseline_target_column: str,
    top_n: int = 12,
) -> dict[str, Any]:
    spec = {} if baseline_shock_spec is None else dict(baseline_shock_spec)
    scale_ratio_column = str(spec.get("scale_ratio_column", "fitted_to_target_scale_ratio"))
    fitted_column = str(spec.get("fitted_column", "tdc_fitted"))
    residual_column = str(spec.get("residual_column", "tdc_residual"))
    flag_column = str(spec.get("flag_column", "shock_flag"))
    condition_number_column = str(spec.get("condition_number_column", "train_condition_number"))
    target_sd_column = str(spec.get("target_sd_column", "train_target_sd"))
    threshold = _quality_gate_rules(spec).get("max_realized_scale_ratio_p95")
    usable = shocks.dropna(subset=[baseline_shock_column, baseline_target_column]).copy()
    if usable.empty or scale_ratio_column not in usable.columns:
        return {
            "threshold": threshold,
            "tail_rows": 0,
            "tail_share": None,
            "quarters": [],
        }
    tail = usable
    if threshold is not None:
        tail = tail[tail[scale_ratio_column] > float(threshold)]
    tail = tail.sort_values(scale_ratio_column, ascending=False)
    rows = [
        {
            "quarter": str(row["quarter"]),
            "realized_target_qoq": float(row[baseline_target_column]),
            "fitted_value": None if pd.isna(row.get(fitted_column)) else float(row[fitted_column]),
            "raw_shock_residual": None if pd.isna(row.get(residual_column)) else float(row[residual_column]),
            "standardized_shock": None if pd.isna(row.get(baseline_shock_column)) else float(row[baseline_shock_column]),
            "realized_scale_fit_ratio": None if pd.isna(row.get(scale_ratio_column)) else float(row[scale_ratio_column]),
            "train_target_sd": None if pd.isna(row.get(target_sd_column)) else float(row[target_sd_column]),
            "train_condition_number": None
            if pd.isna(row.get(condition_number_column))
            else float(row[condition_number_column]),
            "shock_flag": str(row.get(flag_column, "")),
        }
        for _, row in tail.head(top_n).iterrows()
    ]
    return {
        "threshold": None if threshold is None else float(threshold),
        "tail_rows": int(len(tail)),
        "tail_share": float(len(tail) / len(usable)) if len(usable) else None,
        "quarters": rows,
    }


def _candidate_diagnostics(
    *,
    shocks: pd.DataFrame,
    candidate: dict[str, Any],
    lp_controls: list[str],
    include_lagged_outcome: bool,
) -> dict[str, Any]:
    shock_column = str(candidate["shock_column"])
    raw_shock_column = str(candidate["raw_shock_column"])
    target_column = str(candidate["target"])
    usable = shocks.dropna(subset=[shock_column, target_column]).copy()
    usable_target_sd = None if usable.empty else float(usable[target_column].std(ddof=1))
    realized_ratio_column = None
    train_ratio_column = None
    if str(candidate["name"]) == "unexpected_tdc_default":
        realized_ratio_column = "fitted_to_target_scale_ratio"
        train_ratio_column = "fitted_to_train_target_sd_ratio"
        target_sd_column = "train_target_sd"
    else:
        model_stub = raw_shock_column.removesuffix("_residual")
        realized_ratio_column = f"{model_stub}_fitted_to_target_scale_ratio"
        train_ratio_column = f"{model_stub}_fitted_to_train_target_sd_ratio"
        target_sd_column = f"{model_stub}_train_target_sd"
    realized_ratio_quantiles = (
        _ratio_quantiles(usable[realized_ratio_column]) if realized_ratio_column in usable.columns else {}
    )
    raw_unit_tdc_horizons: dict[str, Any] = {}
    if raw_shock_column in shocks.columns:
        raw_lp = run_local_projections(
            shocks,
            shock_col=raw_shock_column,
            outcome_cols=[target_column],
            controls=lp_controls,
            include_lagged_outcome=include_lagged_outcome,
            horizons=[0, 4],
            nw_lags=4,
            cumulative=True,
            spec_name=f"{candidate['name']}_raw_unit_tdc",
        )
        for horizon in (0, 4):
            raw_unit_tdc_horizons[f"h{horizon}"] = _row_snapshot(
                _lp_row(raw_lp, outcome=target_column, horizon=horizon)
            )
    initial_train_target_sd = None
    if not usable.empty and target_sd_column in usable.columns and usable[target_sd_column].notna().any():
        initial_train_target_sd = float(usable[target_sd_column].dropna().iloc[0])
    initial_train_to_usable_volatility_ratio = None
    if (
        initial_train_target_sd is not None
        and usable_target_sd is not None
        and pd.notna(usable_target_sd)
        and usable_target_sd > 0.0
    ):
        initial_train_to_usable_volatility_ratio = float(initial_train_target_sd / usable_target_sd)
    shock_target_corr = None
    if len(usable) >= 2:
        shock_target_corr = float(usable[shock_column].corr(usable[target_column]))
    return {
        **candidate,
        "usable_sample": {
            "rows": int(len(usable)),
            "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
            "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
        },
        "shock_vs_realized_target_correlation": shock_target_corr,
        "usable_target_sd": usable_target_sd,
        "initial_train_target_sd": initial_train_target_sd,
        "initial_train_to_usable_volatility_ratio": initial_train_to_usable_volatility_ratio,
        "realized_scale_ratio_column": realized_ratio_column,
        "realized_scale_ratio_quantiles": realized_ratio_quantiles,
        "train_target_scale_ratio_column": train_ratio_column,
        "raw_unit_tdc_lp": raw_unit_tdc_horizons,
    }


def _comparison_payload(
    *,
    shocks: pd.DataFrame,
    treatment_variant: str,
    treatment_role: str,
    treatment_family: str,
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
        "treatment_family": treatment_family,
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
    lp_controls: list[str] | None = None,
    include_lagged_outcome: bool = False,
    baseline_shock_column: str = "tdc_residual_z",
    alternate_shock_column: str = "tdc_broad_depository_residual_z",
    baseline_target_column: str = "tdc_bank_only_qoq",
    alternate_target_column: str = "tdc_broad_depository_qoq",
    outcome_column: str = "total_deposits_bank_qoq",
) -> dict[str, Any]:
    treatment_freeze_status = _treatment_freeze_status(baseline_shock_spec)
    treatment_candidates = _treatment_candidates(shock_specs)
    baseline_quality = _baseline_quality_summary(
        shocks=shocks,
        baseline_shock_spec=baseline_shock_spec,
        baseline_shock_column=baseline_shock_column,
        baseline_target_column=baseline_target_column,
    )
    severe_tail_audit = _severe_tail_audit(
        shocks=shocks,
        baseline_shock_spec=baseline_shock_spec,
        baseline_shock_column=baseline_shock_column,
        baseline_target_column=baseline_target_column,
    )
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

    baseline_raw_unit_tdc_lp: dict[str, Any] = {}
    if baseline_shock_spec is not None:
        raw_shock_column = str(baseline_shock_spec.get("residual_column", "tdc_residual"))
        if raw_shock_column in shocks.columns:
            raw_lp = run_local_projections(
                shocks,
                shock_col=raw_shock_column,
                outcome_cols=[baseline_target_column],
                controls=list(lp_controls or []),
                include_lagged_outcome=include_lagged_outcome,
                horizons=[0, 4],
                nw_lags=4,
                cumulative=True,
                spec_name="baseline_raw_unit_tdc",
            )
            for horizon in (0, 4):
                baseline_raw_unit_tdc_lp[f"h{horizon}"] = _row_snapshot(
                    _lp_row(raw_lp, outcome=baseline_target_column, horizon=horizon)
                )

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
            treatment_family=str(row.get("treatment_family", "shock_design")),
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
    if treatment_freeze_status != "frozen":
        takeaways.append("The baseline unexpected-TDC shock is still under review and should not be treated as a frozen treatment object.")
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
        if comparison["treatment_family"] == "measurement":
            takeaways.append(
                f"Variant {comparison['treatment_variant']} is a treatment-measurement check and should be read separately from shock-design robustness."
            )
    max_scale_ratio = None
    max_condition_number = None
    flagged_rows = int(baseline_quality["flagged_observations"])
    severe_tail_rows = int(severe_tail_audit["tail_rows"])
    mild_flagged_rows = max(flagged_rows - severe_tail_rows, 0)
    if flagged_rows > 0:
        if severe_tail_rows > 0 and mild_flagged_rows > 0:
            takeaways.append(
                f"The baseline shock has {flagged_rows} scale-ratio flagged windows: {severe_tail_rows} severe realized-scale tail quarter(s) and {mild_flagged_rows} milder threshold breaches."
            )
        elif severe_tail_rows > 0:
            takeaways.append(
                f"The baseline shock has {flagged_rows} scale-ratio flagged windows, all concentrated in the severe realized-scale tail audit."
            )
        else:
            takeaways.append(
                f"The baseline shock has {flagged_rows} scale-ratio flagged windows, but none exceed the severe-tail audit threshold."
            )
    if "fitted_to_target_scale_ratio" in shocks.columns:
        max_scale_ratio = float(shocks["fitted_to_target_scale_ratio"].dropna().max()) if shocks["fitted_to_target_scale_ratio"].notna().any() else None
    if "train_condition_number" in shocks.columns:
        max_condition_number = float(shocks["train_condition_number"].dropna().max()) if shocks["train_condition_number"].notna().any() else None
    if baseline_quality["treatment_quality_status"] == "fail":
        takeaways.append(
            "The frozen baseline shock still fails its publishable quality gate, with the remaining weakness concentrated in realized-scale fit-ratio tails."
        )
    if severe_tail_audit["tail_rows"] > 0:
        takeaways.append(
            "A small set of severe realized-scale tail quarters is now surfaced explicitly for audit and sample-sensitivity checks."
        )
    candidate_diagnostics = [
        _candidate_diagnostics(
            shocks=shocks,
            candidate=candidate,
            lp_controls=list(lp_controls or []),
            include_lagged_outcome=include_lagged_outcome,
        )
        for candidate in treatment_candidates
    ]

    return {
        "treatment_freeze_status": treatment_freeze_status,
        "treatment_quality_status": baseline_quality["treatment_quality_status"],
        "treatment_quality_gate": baseline_quality["treatment_quality_gate"],
        "baseline_usable_sample": baseline_quality["baseline_usable_sample"],
        "baseline_shock_name": None if baseline_shock_spec is None else str(baseline_shock_spec.get("model_name", "")),
        "shock_target_correlation_usable": baseline_quality["shock_target_correlation_usable"],
        "raw_unit_tdc_lp_response": baseline_raw_unit_tdc_lp,
        "realized_scale_fit_ratio_quantiles": baseline_quality["realized_scale_fit_ratio_quantiles"],
        "train_to_usable_target_volatility_ratio": baseline_quality["train_to_usable_target_volatility_ratio"],
        "severe_realized_scale_tail_audit": severe_tail_audit,
        "treatment_candidates": candidate_diagnostics,
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
            "flagged_observations": baseline_quality["flagged_observations"],
            "flagged_share": baseline_quality["flagged_share"],
            "max_fitted_to_target_scale_ratio": max_scale_ratio,
            "realized_scale_ratio_quantiles": baseline_quality["realized_scale_fit_ratio_quantiles"],
            "max_train_condition_number": (
                baseline_quality["max_train_condition_number"]
                if baseline_quality["max_train_condition_number"] is not None
                else max_condition_number
            ),
        },
        "largest_disagreement_quarters": largest_disagreement_quarters,
        "takeaways": takeaways,
    }
