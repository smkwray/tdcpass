from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _scaled_horizons(horizons: np.ndarray) -> np.ndarray:
    if len(horizons) <= 1:
        return np.zeros(len(horizons), dtype=float)
    midpoint = 0.5 * (float(horizons.min()) + float(horizons.max()))
    scale = 0.5 * max(float(horizons.max()) - float(horizons.min()), 1.0)
    return (horizons.astype(float) - midpoint) / scale


def _polynomial_design(x: np.ndarray, degree: int) -> np.ndarray:
    return np.column_stack([x**power for power in range(degree + 1)])


def _fit_polynomial_ridge_path(
    *,
    horizons: np.ndarray,
    betas: np.ndarray,
    ses: np.ndarray,
    degree: int,
    ridge_alpha: float,
) -> tuple[np.ndarray, np.ndarray]:
    x = _scaled_horizons(horizons)
    design = _polynomial_design(x, degree)
    sigma2 = np.maximum(ses.astype(float) ** 2, 1e-12)
    weights = 1.0 / sigma2
    weighted_design = design * np.sqrt(weights)[:, None]
    weighted_beta = betas.astype(float) * np.sqrt(weights)

    penalty = np.eye(design.shape[1], dtype=float) * float(ridge_alpha)
    penalty[0, 0] = 0.0
    normal_matrix = weighted_design.T @ weighted_design + penalty
    rhs = weighted_design.T @ weighted_beta
    theta = np.linalg.solve(normal_matrix, rhs)

    smoothed_beta = design @ theta
    smoother = design @ np.linalg.solve(normal_matrix, design.T * weights)
    smoother_var = (smoother**2) @ sigma2
    smoothed_se = np.sqrt(np.maximum(smoother_var, 0.0))
    return smoothed_beta, smoothed_se


def _fit_gaussian_kernel_path(
    *,
    horizons: np.ndarray,
    betas: np.ndarray,
    ses: np.ndarray,
    bandwidth: float,
) -> tuple[np.ndarray, np.ndarray]:
    if bandwidth <= 0.0:
        raise ValueError(f"bandwidth must be positive, got {bandwidth}")
    distances = horizons.astype(float)[:, None] - horizons.astype(float)[None, :]
    kernel = np.exp(-0.5 * (distances / float(bandwidth)) ** 2)
    sigma2 = np.maximum(ses.astype(float) ** 2, 1e-12)
    base_weights = 1.0 / sigma2
    weighted_kernel = kernel * base_weights[None, :]
    normalizer = np.maximum(weighted_kernel.sum(axis=1, keepdims=True), 1e-12)
    smoother = weighted_kernel / normalizer
    smoothed_beta = smoother @ betas.astype(float)
    smoothed_se = np.sqrt(np.maximum((smoother**2) @ sigma2, 0.0))
    return smoothed_beta, smoothed_se


def build_smoothed_lp_irf(
    lp_irf: pd.DataFrame,
    *,
    method: str = "gaussian_kernel",
    bandwidth: float = 1.0,
    degree: int = 3,
    ridge_alpha: float = 1.0,
    min_horizons: int = 4,
    spec_name: str = "smoothed_lp",
) -> pd.DataFrame:
    output_columns = [
        "outcome",
        "horizon",
        "beta",
        "se",
        "lower95",
        "upper95",
        "raw_beta",
        "raw_se",
        "raw_lower95",
        "raw_upper95",
        "adjustment",
        "n",
        "smooth_method",
        "bandwidth",
        "degree",
        "ridge_alpha",
        "spec_name",
        "shock_column",
        "shock_scale",
        "response_type",
    ]
    if lp_irf.empty:
        return pd.DataFrame(columns=output_columns)

    rows: list[dict[str, Any]] = []
    group_columns = ["outcome", "shock_column", "shock_scale", "response_type"]
    for _, group in lp_irf.groupby(group_columns, dropna=False):
        sample = group.sort_values("horizon").copy()
        if len(sample) < min_horizons:
            continue
        horizons = sample["horizon"].to_numpy(dtype=int)
        betas = sample["beta"].to_numpy(dtype=float)
        ses = sample["se"].to_numpy(dtype=float)
        if method == "gaussian_kernel":
            smoothed_beta, smoothed_se = _fit_gaussian_kernel_path(
                horizons=horizons,
                betas=betas,
                ses=ses,
                bandwidth=bandwidth,
            )
        elif method == "polynomial_ridge":
            smoothed_beta, smoothed_se = _fit_polynomial_ridge_path(
                horizons=horizons,
                betas=betas,
                ses=ses,
                degree=degree,
                ridge_alpha=ridge_alpha,
            )
        else:
            raise ValueError(f"Unsupported smoothing method: {method}")
        for idx, row in sample.reset_index(drop=True).iterrows():
            beta = float(smoothed_beta[idx])
            se = float(smoothed_se[idx])
            rows.append(
                {
                    "outcome": str(row["outcome"]),
                    "horizon": int(row["horizon"]),
                    "beta": beta,
                    "se": se,
                    "lower95": beta - 1.96 * se,
                    "upper95": beta + 1.96 * se,
                    "raw_beta": float(row["beta"]),
                    "raw_se": float(row["se"]),
                    "raw_lower95": float(row["lower95"]),
                    "raw_upper95": float(row["upper95"]),
                    "adjustment": beta - float(row["beta"]),
                    "n": int(row["n"]),
                    "smooth_method": method,
                    "bandwidth": float(bandwidth),
                    "degree": int(degree) if method == "polynomial_ridge" else None,
                    "ridge_alpha": float(ridge_alpha) if method == "polynomial_ridge" else None,
                    "spec_name": spec_name,
                    "shock_column": str(row["shock_column"]),
                    "shock_scale": str(row["shock_scale"]),
                    "response_type": str(row["response_type"]),
                }
            )
    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns)


def build_smoothed_lp_diagnostics_summary(
    smoothed_lp_irf: pd.DataFrame,
    *,
    key_outcomes: tuple[str, ...] = ("total_deposits_bank_qoq", "other_component_qoq"),
    key_horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if smoothed_lp_irf.empty:
        return {
            "status": "no_smoothed_rows",
            "headline_question": "Does horizon smoothing materially change the fixed-shock LP readout?",
            "key_horizons": {},
            "outcomes": [],
            "takeaways": ["No smoothed LP rows were produced."],
        }

    outcome_payloads: list[dict[str, Any]] = []
    sign_flip_count = 0
    for outcome in smoothed_lp_irf["outcome"].drop_duplicates().tolist():
        sample = smoothed_lp_irf[smoothed_lp_irf["outcome"] == outcome].sort_values("horizon").copy()
        raw_beta = sample["raw_beta"].to_numpy(dtype=float)
        smooth_beta = sample["beta"].to_numpy(dtype=float)
        raw_roughness = float(np.abs(np.diff(raw_beta)).sum()) if len(sample) >= 2 else 0.0
        smooth_roughness = float(np.abs(np.diff(smooth_beta)).sum()) if len(sample) >= 2 else 0.0
        max_abs_adjustment = float(np.abs(sample["adjustment"]).max()) if not sample.empty else 0.0
        key_rows: dict[str, Any] = {}
        for horizon in key_horizons:
            row = sample[sample["horizon"] == horizon]
            if row.empty:
                continue
            entry = row.iloc[0]
            raw_sign = "positive" if float(entry["raw_beta"]) > 0 else "negative" if float(entry["raw_beta"]) < 0 else "zero"
            smooth_sign = "positive" if float(entry["beta"]) > 0 else "negative" if float(entry["beta"]) < 0 else "zero"
            if (
                outcome in key_outcomes
                and raw_sign in {"positive", "negative"}
                and smooth_sign in {"positive", "negative"}
                and raw_sign != smooth_sign
            ):
                sign_flip_count += 1
            key_rows[f"h{horizon}"] = {
                "raw_beta": float(entry["raw_beta"]),
                "smoothed_beta": float(entry["beta"]),
                "adjustment": float(entry["adjustment"]),
                "raw_sign": raw_sign,
                "smoothed_sign": smooth_sign,
                "raw_ci": [float(entry["raw_lower95"]), float(entry["raw_upper95"])],
                "smoothed_ci": [float(entry["lower95"]), float(entry["upper95"])],
                "n": int(entry["n"]),
            }
        outcome_payloads.append(
            {
                "outcome": str(outcome),
                "raw_roughness": raw_roughness,
                "smoothed_roughness": smooth_roughness,
                "roughness_reduction": raw_roughness - smooth_roughness,
                "max_abs_adjustment": max_abs_adjustment,
                "key_horizons": key_rows,
            }
        )

    status = "stable"
    if sign_flip_count > 0:
        status = "sign_changes"

    key_horizon_payload: dict[str, Any] = {}
    for horizon in key_horizons:
        horizon_key = f"h{horizon}"
        horizon_payload: dict[str, Any] = {}
        for outcome in key_outcomes:
            match = next((row for row in outcome_payloads if row["outcome"] == outcome), None)
            if match is not None and horizon_key in match["key_horizons"]:
                horizon_payload[outcome] = match["key_horizons"][horizon_key]
        key_horizon_payload[horizon_key] = horizon_payload

    takeaways = [
        "Smoothed LP diagnostics compare the raw baseline horizon-by-horizon coefficients with a local Gaussian-kernel fit across horizons.",
    ]
    if status == "sign_changes":
        takeaways.append("Smoothing changes the sign of at least one key-horizon response and should be treated as a sensitivity overlay, not a stable summary.")
    else:
        takeaways.append("Smoothing reduces horizon-to-horizon wobble without flipping the key-horizon signs in the current baseline LP.")

    return {
        "status": status,
        "headline_question": "Does horizon smoothing materially change the fixed-shock LP readout?",
        "key_horizons": key_horizon_payload,
        "outcomes": outcome_payloads,
        "takeaways": takeaways,
    }
