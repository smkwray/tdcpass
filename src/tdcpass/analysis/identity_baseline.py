from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm

from tdcpass.analysis.local_projections import cumulative_forward_sum
from tdcpass.analysis.shocks import expanding_window_residual


def _forward_transform(series: pd.Series, horizon: int, *, cumulative: bool) -> pd.Series:
    if cumulative:
        return cumulative_forward_sum(series, horizon)
    return series.shift(-horizon)


def _bootstrap_indices(nobs: int, *, block_length: int, rng: np.random.Generator) -> np.ndarray:
    if nobs <= 0:
        raise ValueError("nobs must be positive for bootstrap sampling.")
    block_length = max(1, min(block_length, nobs))
    blocks_needed = int(math.ceil(nobs / block_length))
    offsets = np.arange(block_length, dtype=int)
    draws: list[np.ndarray] = []
    for _ in range(blocks_needed):
        start = int(rng.integers(0, nobs))
        draws.append((start + offsets) % nobs)
    return np.concatenate(draws)[:nobs]


def _fit_beta(sample: pd.DataFrame, *, shock_col: str, response_col: str, controls: Sequence[str]) -> float:
    x = sm.add_constant(sample[[shock_col, *controls]], has_constant="add")
    fit = sm.OLS(sample[response_col], x).fit()
    return float(fit.params[shock_col])


def _reestimate_bootstrap_shock(sample: pd.DataFrame, *, shock_spec: dict[str, Any]) -> pd.DataFrame:
    return expanding_window_residual(
        sample,
        target=str(shock_spec["target"]),
        predictors=[str(item) for item in shock_spec["predictors"]],
        min_train_obs=int(shock_spec["min_train_obs"]),
        max_train_obs=None if shock_spec.get("max_train_obs") is None else int(shock_spec["max_train_obs"]),
        standardize=bool(shock_spec.get("standardize_residual", True)),
        model_name=str(shock_spec.get("model_name", "unexpected_tdc_default")),
        fitted_column=str(shock_spec.get("fitted_column", "tdc_fitted")),
        residual_column=str(shock_spec.get("residual_column", "tdc_residual")),
        standardized_column=str(shock_spec.get("standardized_column", "tdc_residual_z")),
        train_start_obs_column=str(shock_spec.get("train_start_obs_column", "train_start_obs")),
        model_name_column=str(shock_spec.get("model_name_column", "model_name")),
        condition_number_column=str(shock_spec.get("condition_number_column", "train_condition_number")),
        target_sd_column=str(shock_spec.get("target_sd_column", "train_target_sd")),
        residual_sd_column=str(shock_spec.get("residual_sd_column", "train_resid_sd")),
        scale_ratio_column=str(shock_spec.get("scale_ratio_column", "fitted_to_target_scale_ratio")),
        train_target_scale_ratio_column=str(
            shock_spec.get("train_target_scale_ratio_column", "fitted_to_train_target_sd_ratio")
        ),
        flag_column=str(shock_spec.get("flag_column", "shock_flag")),
        max_condition_number=float(shock_spec["max_condition_number"])
        if shock_spec.get("max_condition_number") is not None
        else None,
        max_scale_ratio=float(shock_spec["max_scale_ratio"]) if shock_spec.get("max_scale_ratio") is not None else None,
        ridge_alpha=float(shock_spec["ridge_alpha"]) if shock_spec.get("ridge_alpha") is not None else None,
    )


def build_identity_baseline_irf(
    df: pd.DataFrame,
    *,
    shock_col: str,
    tdc_outcome_col: str = "tdc_bank_only_qoq",
    total_outcome_col: str = "total_deposits_bank_qoq",
    controls: Sequence[str] = (),
    horizons: Sequence[int] = tuple(range(0, 9)),
    cumulative: bool = True,
    spec_name: str = "identity_baseline",
    bootstrap_reps: int = 250,
    bootstrap_block_length: int = 4,
    bootstrap_seed: int = 0,
    nested_shock_spec: dict[str, Any] | None = None,
) -> pd.DataFrame:
    required = [tdc_outcome_col, total_outcome_col, *controls]
    if nested_shock_spec is None:
        required.insert(0, shock_col)
    else:
        required.extend(
            [
                str(nested_shock_spec["target"]),
                *[str(item) for item in nested_shock_spec["predictors"]],
            ]
        )
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns for identity baseline: {missing}")

    rows: list[dict[str, object]] = []
    shock_scale = "rolling_oos_standard_deviation" if shock_col.endswith("_z") else "raw_unit"
    response_type = "cumulative_sum_h0_to_h" if cumulative else "lead_h"
    rng = np.random.default_rng(bootstrap_seed)

    for horizon in horizons:
        dep_tdc = _forward_transform(df[tdc_outcome_col], int(horizon), cumulative=cumulative)
        dep_total = _forward_transform(df[total_outcome_col], int(horizon), cumulative=cumulative)
        sample = pd.DataFrame({"dep_tdc": dep_tdc, "dep_total": dep_total})
        sample_columns: list[str] = list(controls)
        if shock_col in df.columns:
            sample_columns.append(shock_col)
        if nested_shock_spec is not None:
            sample_columns.extend(
                [
                    str(nested_shock_spec["target"]),
                    *[str(item) for item in nested_shock_spec["predictors"]],
                ]
            )
        seen: set[str] = set()
        for column in sample_columns:
            if column in seen:
                continue
            seen.add(column)
            sample[column] = df[column]
        sample = sample.dropna()
        if len(sample) < len(controls) + 12:
            continue

        beta_tdc = _fit_beta(sample, shock_col=shock_col, response_col="dep_tdc", controls=controls)
        beta_total = _fit_beta(sample, shock_col=shock_col, response_col="dep_total", controls=controls)
        beta_other = beta_total - beta_tdc

        bootstrap_tdc: list[float] = []
        bootstrap_total: list[float] = []
        bootstrap_other: list[float] = []
        for _ in range(max(bootstrap_reps, 0)):
            sampled = sample.iloc[_bootstrap_indices(len(sample), block_length=bootstrap_block_length, rng=rng)].reset_index(
                drop=True
            )
            sampled_shock_col = shock_col
            if nested_shock_spec is not None:
                sampled = _reestimate_bootstrap_shock(sampled, shock_spec=nested_shock_spec)
                sampled_shock_col = str(nested_shock_spec.get("standardized_column", shock_col))
                sampled = sampled.dropna(subset=["dep_tdc", "dep_total", sampled_shock_col, *controls]).reset_index(drop=True)
                if len(sampled) < len(controls) + 12:
                    continue
            try:
                draw_tdc = _fit_beta(sampled, shock_col=sampled_shock_col, response_col="dep_tdc", controls=controls)
                draw_total = _fit_beta(sampled, shock_col=sampled_shock_col, response_col="dep_total", controls=controls)
            except (np.linalg.LinAlgError, ValueError):
                continue
            bootstrap_tdc.append(draw_tdc)
            bootstrap_total.append(draw_total)
            bootstrap_other.append(draw_total - draw_tdc)

        def _interval(draws: list[float]) -> tuple[float, float, float]:
            if len(draws) >= 2:
                series = pd.Series(draws, dtype=float)
                se = float(series.std(ddof=1))
                lower95 = float(series.quantile(0.025))
                upper95 = float(series.quantile(0.975))
                return se, lower95, upper95
            return float("nan"), float("nan"), float("nan")

        tdc_se, tdc_lower95, tdc_upper95 = _interval(bootstrap_tdc)
        total_se, total_lower95, total_upper95 = _interval(bootstrap_total)
        other_se, other_lower95, other_upper95 = _interval(bootstrap_other)

        common = {
            "horizon": int(horizon),
            "n": int(len(sample)),
            "spec_name": spec_name,
            "shock_column": shock_col,
            "shock_scale": shock_scale,
            "response_type": response_type,
            "decomposition_mode": "exact_identity_baseline",
            "inference_method": (
                f"nested_circular_block_bootstrap_{bootstrap_block_length}q_{bootstrap_reps}reps"
                if nested_shock_spec is not None
                else f"circular_block_bootstrap_{bootstrap_block_length}q_{bootstrap_reps}reps"
            ),
        }
        rows.extend(
            [
                {
                    **common,
                    "outcome": tdc_outcome_col,
                    "beta": beta_tdc,
                    "se": tdc_se,
                    "lower95": tdc_lower95,
                    "upper95": tdc_upper95,
                    "outcome_construction": "estimated_common_design",
                },
                {
                    **common,
                    "outcome": total_outcome_col,
                    "beta": beta_total,
                    "se": total_se,
                    "lower95": total_lower95,
                    "upper95": total_upper95,
                    "outcome_construction": "estimated_common_design",
                },
                {
                    **common,
                    "outcome": "other_component_qoq",
                    "beta": beta_other,
                    "se": other_se,
                    "lower95": other_lower95,
                    "upper95": other_upper95,
                    "outcome_construction": "derived_total_minus_tdc",
                },
            ]
        )

    columns = [
        "outcome",
        "horizon",
        "beta",
        "se",
        "lower95",
        "upper95",
        "n",
        "spec_name",
        "shock_column",
        "shock_scale",
        "response_type",
        "decomposition_mode",
        "outcome_construction",
        "inference_method",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def build_identity_variant_ladder(
    df: pd.DataFrame,
    *,
    variants: Sequence[dict[str, Any]],
    total_outcome_col: str = "total_deposits_bank_qoq",
    horizons: Sequence[int] = tuple(range(0, 9)),
    cumulative: bool = True,
    spec_name: str = "identity_variant_ladder",
    bootstrap_reps: int = 250,
    bootstrap_block_length: int = 4,
    bootstrap_seed: int = 0,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    columns = [
        "treatment_variant",
        "treatment_role",
        "treatment_family",
        "target",
        "outcome",
        "horizon",
        "beta",
        "se",
        "lower95",
        "upper95",
        "n",
        "spec_name",
        "shock_column",
        "decomposition_mode",
        "outcome_construction",
        "inference_method",
    ]
    for offset, variant in enumerate(variants):
        shock_col = str(variant["shock_column"])
        tdc_outcome_col = str(variant["target"])
        controls = [str(item) for item in variant.get("controls", [])]
        required = [shock_col, tdc_outcome_col, total_outcome_col, *controls]
        if any(column not in df.columns for column in required):
            continue
        frame = build_identity_baseline_irf(
            df,
            shock_col=shock_col,
            tdc_outcome_col=tdc_outcome_col,
            total_outcome_col=total_outcome_col,
            controls=controls,
            horizons=horizons,
            cumulative=cumulative,
            spec_name=spec_name,
            bootstrap_reps=bootstrap_reps,
            bootstrap_block_length=bootstrap_block_length,
            bootstrap_seed=bootstrap_seed + offset,
        )
        if frame.empty:
            continue
        frame.insert(0, "target", tdc_outcome_col)
        frame.insert(0, "treatment_family", str(variant.get("treatment_family", "")))
        frame.insert(0, "treatment_role", str(variant.get("treatment_role", "")))
        frame.insert(0, "treatment_variant", str(variant["treatment_variant"]))
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)[columns]
