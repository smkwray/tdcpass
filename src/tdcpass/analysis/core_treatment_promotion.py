from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _ratio_quantiles(series: pd.Series) -> dict[str, float]:
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    quantiles = cleaned.quantile([0.5, 0.9, 0.95, 0.99])
    return {f"p{int(index * 100):02d}": float(value) for index, value in quantiles.items()}


def _shock_quality_snapshot(shocked: pd.DataFrame, spec: Mapping[str, Any] | None) -> dict[str, Any]:
    if spec is None:
        return {"status": "not_available", "reason": "missing_spec"}

    shock_col = str(spec.get("standardized_column", ""))
    target_col = str(spec.get("target", ""))
    if not shock_col or not target_col or shock_col not in shocked.columns or target_col not in shocked.columns:
        return {
            "status": "not_available",
            "reason": "missing_required_columns",
            "shock_column": shock_col,
            "target": target_col,
        }

    usable = shocked.dropna(subset=[shock_col, target_col]).copy()
    if usable.empty:
        return {
            "status": "not_available",
            "reason": "no_usable_rows",
            "shock_column": shock_col,
            "target": target_col,
        }

    flag_column = str(spec.get("flag_column", "shock_flag"))
    scale_ratio_column = str(spec.get("scale_ratio_column", "fitted_to_target_scale_ratio"))
    condition_number_column = str(spec.get("condition_number_column", "train_condition_number"))
    target_sd_column = str(spec.get("target_sd_column", "train_target_sd"))

    usable_target_sd = None
    if usable[target_col].notna().sum() >= 2:
        usable_target_sd = float(usable[target_col].std(ddof=1))
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
    if len(usable) >= 2:
        shock_target_correlation_usable = float(usable[shock_col].corr(usable[target_col]))

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

    return {
        "status": "available",
        "model_name": str(spec.get("model_name", "")),
        "target": target_col,
        "shock_column": shock_col,
        "predictors": [str(item) for item in spec.get("predictors", [])],
        "usable_sample": {
            "rows": int(len(usable)),
            "start_quarter": str(usable["quarter"].iloc[0]) if "quarter" in usable.columns else None,
            "end_quarter": str(usable["quarter"].iloc[-1]) if "quarter" in usable.columns else None,
        },
        "shock_target_correlation_usable": shock_target_correlation_usable,
        "usable_target_sd": usable_target_sd,
        "initial_train_target_sd": initial_train_target_sd,
        "train_to_usable_target_volatility_ratio": train_to_usable_target_volatility_ratio,
        "flagged_observations": flagged_observations,
        "flagged_share": flagged_share,
        "realized_scale_fit_ratio_quantiles": realized_scale_fit_ratio_quantiles,
        "max_train_condition_number": max_train_condition_number,
    }


def _shock_overlap(shocked: pd.DataFrame, *, left: str, right: str) -> dict[str, Any]:
    required = {"quarter", left, right}
    if not required.issubset(shocked.columns):
        return {
            "status": "not_available",
            "reason": "missing_required_columns",
            "left_shock_column": left,
            "right_shock_column": right,
        }
    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {
            "status": "not_available",
            "reason": "no_overlap_rows",
            "left_shock_column": left,
            "right_shock_column": right,
        }
    same_sign = frame[left] * frame[right] > 0
    opposite_sign = frame[left] * frame[right] < 0
    left_sd = frame[left].std(ddof=1)
    right_sd = frame[right].std(ddof=1)
    scale_ratio = None
    if pd.notna(left_sd) and pd.notna(right_sd) and float(left_sd) > 0.0:
        scale_ratio = float(right_sd / left_sd)
    return {
        "status": "available",
        "rows": int(len(frame)),
        "start_quarter": str(frame["quarter"].iloc[0]),
        "end_quarter": str(frame["quarter"].iloc[-1]),
        "shock_corr": float(frame[left].corr(frame[right])) if len(frame) >= 2 else None,
        "same_sign_share": float(same_sign.mean()),
        "opposite_sign_share": float(opposite_sign.mean()),
        "core_to_baseline_sd_ratio": scale_ratio,
    }


def _identity_snapshot(
    ladder: pd.DataFrame,
    *,
    treatment_variant: str,
    horizon: int,
    outcome: str,
) -> dict[str, Any] | None:
    if ladder.empty:
        return None
    sample = ladder[
        (ladder["treatment_variant"] == treatment_variant)
        & (ladder["horizon"] == horizon)
        & (ladder["outcome"] == outcome)
    ]
    if sample.empty:
        return None
    row = sample.iloc[0]
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "target": str(row.get("target", "")),
        "shock_column": str(row.get("shock_column", "")),
        "outcome_construction": str(row.get("outcome_construction", "")),
    }


def build_core_treatment_promotion_summary(
    *,
    shocked: pd.DataFrame,
    identity_treatment_sensitivity: pd.DataFrame,
    shock_specs: Mapping[str, Any],
    split_treatment_architecture_summary: Mapping[str, Any] | None,
    strict_missing_channel_summary: Mapping[str, Any] | None,
    baseline_shock_name: str = "unexpected_tdc_default",
    core_shock_name: str = "unexpected_tdc_core_deposit_proximate_bank_only",
    legacy_core_shock_name: str = "unexpected_tdc_no_toc_no_row_bank_only",
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    baseline_spec = dict(shock_specs.get(baseline_shock_name, {}) or {})
    core_spec = dict(shock_specs.get(core_shock_name, {}) or {})
    legacy_core_spec = dict(shock_specs.get(legacy_core_shock_name, {}) or {})
    if not baseline_spec or not core_spec:
        return {"status": "not_available", "reason": "missing_required_shock_specs"}

    baseline_quality = _shock_quality_snapshot(shocked, baseline_spec)
    core_quality = _shock_quality_snapshot(shocked, core_spec)
    shock_overlap = _shock_overlap(
        shocked,
        left=str(baseline_spec.get("standardized_column", "tdc_residual_z")),
        right=str(core_spec.get("standardized_column", "")),
    )

    alias_check = {"status": "not_available", "reason": "missing_required_alias_columns"}
    required_alias_cols = {
        "tdc_core_deposit_proximate_bank_only_qoq",
        "tdc_no_toc_no_row_bank_only_qoq",
    }
    if required_alias_cols.issubset(shocked.columns):
        alias_gap = (
            shocked["tdc_core_deposit_proximate_bank_only_qoq"] - shocked["tdc_no_toc_no_row_bank_only_qoq"]
        ).abs()
        alias_check = {
            "status": "available",
            "current_core_series": "tdc_core_deposit_proximate_bank_only_qoq",
            "legacy_equivalent_series": "tdc_no_toc_no_row_bank_only_qoq",
            "legacy_equivalent_shock_name": legacy_core_shock_name,
            "max_abs_gap_beta": float(alias_gap.max()),
            "mean_abs_gap_beta": float(alias_gap.mean()),
        }

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        horizon_key = f"h{horizon}"
        baseline_target = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="baseline",
            horizon=horizon,
            outcome=str(baseline_spec.get("target", "tdc_bank_only_qoq")),
        )
        baseline_total = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="baseline",
            horizon=horizon,
            outcome="total_deposits_bank_qoq",
        )
        baseline_other = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="baseline",
            horizon=horizon,
            outcome="other_component_qoq",
        )
        core_target = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="core_deposit_proximate",
            horizon=horizon,
            outcome=str(core_spec.get("target", "tdc_core_deposit_proximate_bank_only_qoq")),
        )
        core_total = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="core_deposit_proximate",
            horizon=horizon,
            outcome="total_deposits_bank_qoq",
        )
        core_other = _identity_snapshot(
            identity_treatment_sensitivity,
            treatment_variant="core_deposit_proximate",
            horizon=horizon,
            outcome="other_component_qoq",
        )
        if not any((baseline_target, baseline_total, baseline_other, core_target, core_total, core_other)):
            continue
        residual_shift = None
        if baseline_other is not None and core_other is not None:
            residual_shift = float(core_other["beta"] - baseline_other["beta"])
        key_horizons[horizon_key] = {
            "baseline_target_response": baseline_target,
            "baseline_total_response": baseline_total,
            "baseline_residual_response": baseline_other,
            "core_target_response": core_target,
            "core_total_response": core_total,
            "core_residual_response": core_other,
            "core_minus_baseline_residual_beta": residual_shift,
        }

    strict_validation = {"status": "not_available", "reason": "missing_input_summary"}
    if (
        split_treatment_architecture_summary is not None
        and strict_missing_channel_summary is not None
        and str(split_treatment_architecture_summary.get("status", "not_available")) == "available"
        and str(strict_missing_channel_summary.get("status", "not_available")) == "available"
    ):
        split_h0 = dict(split_treatment_architecture_summary.get("key_horizons", {}).get("h0", {}) or {})
        missing_h0 = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}) or {})
        excluded = dict(missing_h0.get("toc_row_excluded", {}) or {})
        core_residual_beta = _safe_float(
            dict(split_h0.get("core_deposit_proximate_residual_response", {}) or {}).get("beta")
        )
        strict_total_beta = _safe_float(
            dict(excluded.get("strict_identifiable_total_response", {}) or {}).get("beta")
        )
        sign_match = None
        if core_residual_beta is not None and strict_total_beta is not None:
            if core_residual_beta == 0.0 or strict_total_beta == 0.0:
                sign_match = core_residual_beta == strict_total_beta
            else:
                sign_match = core_residual_beta * strict_total_beta > 0
        strict_validation = {
            "status": "available",
            "comparison_target": str(excluded.get("comparison_target", "tdc_no_toc_no_row_bank_only_qoq")),
            "comparison_residual_outcome": str(
                excluded.get("comparison_residual_outcome", "other_component_no_toc_no_row_bank_only_qoq")
            ),
            "h0_core_residual_beta": core_residual_beta,
            "h0_strict_identifiable_total_beta": strict_total_beta,
            "h0_sign_match": sign_match,
            "h0_gap_after_funding_beta": _safe_float(
                dict(excluded.get("strict_gap_after_funding_response", {}) or {}).get("beta")
            ),
        }

    overlap_corr = _safe_float(shock_overlap.get("shock_corr"))
    same_sign_share = _safe_float(shock_overlap.get("same_sign_share"))
    h0_core_residual = _safe_float(dict(key_horizons.get("h0", {}).get("core_residual_response", {}) or {}).get("beta"))
    h0_core_target = _safe_float(dict(key_horizons.get("h0", {}).get("core_target_response", {}) or {}).get("beta"))
    h0_strict_total = _safe_float(strict_validation.get("h0_strict_identifiable_total_beta"))
    h0_sign_match = strict_validation.get("h0_sign_match")

    recommendation_status = "keep_interpretive_only"
    recommendation_why = (
        "The core deposit-proximate series is currently a semantic alias of the no-TOC/no-ROW treatment, "
        "and the separately estimated core shock should stay interpretive until it delivers a clearer validated object than the existing diagnostic branch."
    )
    if (
        overlap_corr is not None
        and same_sign_share is not None
        and h0_sign_match is True
        and h0_core_residual is not None
        and h0_strict_total is not None
        and abs(h0_core_residual) >= abs(h0_strict_total)
        and overlap_corr >= 0.8
        and same_sign_share >= 0.8
    ):
        recommendation_status = "promote_as_secondary_estimated_comparison"
        recommendation_why = (
            "The core shock stays close enough to the baseline design, survives a separate first-stage estimation pass, "
            "and no longer fails the h0 direct-count sign check, so it can be published as a secondary comparison object."
        )

    takeaways = []
    if alias_check.get("status") == "available":
        takeaways.append(
            "The current core deposit-proximate target is mechanically the same series as the legacy no-TOC/no-ROW diagnostic, so promoting it is a semantic and reporting decision, not a new target-data discovery."
        )
    if overlap_corr is not None and same_sign_share is not None:
        takeaways.append(
            f"Baseline and core shocks overlap only moderately (corr ≈ {overlap_corr:.2f}, same-sign share ≈ {same_sign_share:.2f}), so a promoted core shock would not just be a trivial relabeling of the frozen baseline shock."
        )
    if h0_core_target is not None and h0_core_residual is not None:
        takeaways.append(
            f"Under separate estimation, the core treatment still has a live h0 target response (≈ {h0_core_target:.2f}) and a small remaining residual (≈ {h0_core_residual:.2f})."
        )
    if h0_core_residual is not None and h0_strict_total is not None and h0_sign_match is not None:
        takeaways.append(
            f"But the direct-count validation is still not there at h0: core residual ≈ {h0_core_residual:.2f}, strict identifiable total ≈ {h0_strict_total:.2f}, sign_match = {str(h0_sign_match).lower()}."
        )
    takeaways.append(recommendation_why)

    return {
        "status": "available",
        "headline_question": "Should the split architecture remain interpretive only, or should the core deposit-proximate treatment be estimated and reported as a separate shock?",
        "estimation_path": {
            "summary_artifact": "core_treatment_promotion_summary.json",
            "source_artifacts": [
                "unexpected_tdc.csv",
                "identity_treatment_sensitivity.csv",
                "split_treatment_architecture_summary.json",
                "strict_missing_channel_summary.json",
            ],
        },
        "series_alias_check": alias_check,
        "shock_quality": {
            "baseline": baseline_quality,
            "core_deposit_proximate": core_quality,
            "baseline_vs_core_overlap": shock_overlap,
        },
        "key_horizons": key_horizons,
        "strict_validation_check": strict_validation,
        "promotion_recommendation": {
            "status": recommendation_status,
            "why": recommendation_why,
            "current_release_role": (
                "estimate core shock as a secondary comparison"
                if recommendation_status == "promote_as_secondary_estimated_comparison"
                else "keep split architecture interpretive and diagnostic"
            ),
        },
        "takeaways": takeaways,
    }
