from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd


def _ols_fit(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    return beta


def _ols_residual_std(
    x_train: np.ndarray,
    y_train: np.ndarray,
    beta: np.ndarray,
) -> float:
    fitted_train = x_train @ beta
    resid_train = y_train - fitted_train
    if len(resid_train) <= 1:
        return np.nan
    sd = float(np.std(resid_train, ddof=1))
    return sd if np.isfinite(sd) and sd > 0 else np.nan


def _sample_std(values: np.ndarray) -> float:
    if len(values) <= 1:
        return np.nan
    sd = float(np.std(values, ddof=1))
    return sd if np.isfinite(sd) and sd > 0 else np.nan


def _condition_number(x: np.ndarray) -> float:
    if x.size == 0:
        return np.nan
    cond = float(np.linalg.cond(x))
    return cond if np.isfinite(cond) else np.nan


def expanding_window_residual(
    df: pd.DataFrame,
    *,
    target: str,
    predictors: List[str],
    min_train_obs: int = 24,
    standardize: bool = True,
    model_name: str = "unexpected_tdc_default",
    fitted_column: str = "tdc_fitted",
    residual_column: str = "tdc_residual",
    standardized_column: str = "tdc_residual_z",
    train_start_obs_column: str = "train_start_obs",
    model_name_column: str = "model_name",
    condition_number_column: str = "train_condition_number",
    target_sd_column: str = "train_target_sd",
    residual_sd_column: str = "train_resid_sd",
    scale_ratio_column: str = "fitted_to_target_scale_ratio",
    flag_column: str = "shock_flag",
    max_condition_number: float | None = None,
    max_scale_ratio: float | None = None,
) -> pd.DataFrame:
    if target not in df.columns:
        raise KeyError(f"Missing target column: {target}")
    for predictor in predictors:
        if predictor not in df.columns:
            raise KeyError(f"Missing predictor column: {predictor}")

    out = df.copy()
    fitted = np.full(len(out), np.nan, dtype=float)
    resid = np.full(len(out), np.nan, dtype=float)
    resid_z = np.full(len(out), np.nan, dtype=float)
    train_start_obs = np.full(len(out), np.nan, dtype=float)
    model_names = np.full(len(out), None, dtype=object)
    condition_numbers = np.full(len(out), np.nan, dtype=float)
    target_sds = np.full(len(out), np.nan, dtype=float)
    residual_sds = np.full(len(out), np.nan, dtype=float)
    scale_ratios = np.full(len(out), np.nan, dtype=float)
    flags = np.full(len(out), "", dtype=object)

    for i in range(min_train_obs, len(out)):
        train = out.iloc[:i][[target] + predictors].dropna()
        if len(train) < max(min_train_obs, len(predictors) + 3):
            continue

        x_train = train[predictors].to_numpy(dtype=float)
        y_train = train[target].to_numpy(dtype=float)
        x_train = np.column_stack([np.ones(len(x_train)), x_train])

        row = out.iloc[i][predictors]
        if row.isna().any():
            continue

        x_now = np.r_[1.0, row.to_numpy(dtype=float)]
        beta = _ols_fit(x_train, y_train)
        target_sd = _sample_std(y_train)
        train_sd = _ols_residual_std(x_train, y_train, beta)
        condition_no = _condition_number(x_train)
        fitted[i] = float(x_now @ beta)
        resid[i] = float(out.iloc[i][target] - fitted[i])
        target_sds[i] = target_sd
        residual_sds[i] = train_sd
        condition_numbers[i] = condition_no
        scale_denominator = max(abs(float(out.iloc[i][target])), target_sd if np.isfinite(target_sd) else 0.0, 1e-12)
        scale_ratios[i] = abs(fitted[i]) / scale_denominator
        if standardize:
            resid_z[i] = resid[i] / train_sd if np.isfinite(train_sd) else np.nan
        else:
            resid_z[i] = resid[i]
        train_start_obs[i] = float(len(train))
        model_names[i] = model_name
        row_flags: list[str] = []
        if max_condition_number is not None and np.isfinite(condition_no) and condition_no > max_condition_number:
            row_flags.append("condition_number")
        if max_scale_ratio is not None and np.isfinite(scale_ratios[i]) and scale_ratios[i] > max_scale_ratio:
            row_flags.append("scale_ratio")
        flags[i] = "|".join(row_flags)

    # Canonical contract columns.
    out[fitted_column] = fitted
    out[residual_column] = resid
    out[standardized_column] = resid_z
    out[train_start_obs_column] = train_start_obs
    out[model_name_column] = model_names
    out[condition_number_column] = condition_numbers
    out[target_sd_column] = target_sds
    out[residual_sd_column] = residual_sds
    out[scale_ratio_column] = scale_ratios
    out[flag_column] = flags

    # Compatibility aliases for legacy/demo paths.
    out[f"{target}_fitted"] = fitted
    out[f"{target}_residual"] = resid
    out[f"{target}_residual_z"] = resid_z
    out[f"{target}_train_start_obs"] = train_start_obs
    return out
