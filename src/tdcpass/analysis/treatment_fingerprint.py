from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def _git_commit(cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def build_headline_treatment_fingerprint(
    *,
    shock_spec: Mapping[str, Any],
    shocked: pd.DataFrame,
    repo_root: Path,
) -> dict[str, Any]:
    shock_column = str(shock_spec.get("standardized_column", "tdc_residual_z"))
    usable = shocked.dropna(subset=[shock_column]).copy() if shock_column in shocked.columns else shocked.iloc[0:0].copy()
    return {
        "treatment_freeze_status": str(shock_spec.get("freeze_status", "frozen")),
        "model_name": str(shock_spec.get("model_name", "")),
        "target": str(shock_spec.get("target", "")),
        "method": str(shock_spec.get("method", "")),
        "predictors": [str(item) for item in shock_spec.get("predictors", [])],
        "ridge_alpha": None if shock_spec.get("ridge_alpha") is None else float(shock_spec.get("ridge_alpha")),
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
        "max_train_obs": None if shock_spec.get("max_train_obs") is None else int(shock_spec.get("max_train_obs")),
        "standardized_column": shock_column,
        "residual_column": str(shock_spec.get("residual_column", "tdc_residual")),
        "fitted_column": str(shock_spec.get("fitted_column", "tdc_fitted")),
        "train_start_obs_column": str(shock_spec.get("train_start_obs_column", "train_start_obs")),
        "usable_sample": {
            "rows": int(len(usable)),
            "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
            "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
        },
        "git_commit": _git_commit(repo_root),
    }


def validate_headline_treatment_fingerprint(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
) -> list[str]:
    failures: list[str] = []
    expected_pairs = {
        "treatment_freeze_status": str(shock_spec.get("freeze_status", "frozen")),
        "model_name": str(shock_spec.get("model_name", "")),
        "target": str(shock_spec.get("target", "")),
        "method": str(shock_spec.get("method", "")),
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
        "max_train_obs": None if shock_spec.get("max_train_obs") is None else int(shock_spec.get("max_train_obs")),
        "standardized_column": str(shock_spec.get("standardized_column", "tdc_residual_z")),
        "residual_column": str(shock_spec.get("residual_column", "tdc_residual")),
        "fitted_column": str(shock_spec.get("fitted_column", "tdc_fitted")),
        "train_start_obs_column": str(shock_spec.get("train_start_obs_column", "train_start_obs")),
        "ridge_alpha": None if shock_spec.get("ridge_alpha") is None else float(shock_spec.get("ridge_alpha")),
    }
    for key, expected in expected_pairs.items():
        observed = fingerprint.get(key)
        if observed != expected:
            failures.append(f"Fingerprint mismatch for {key}: expected {expected!r}, observed {observed!r}.")
    expected_predictors = [str(item) for item in shock_spec.get("predictors", [])]
    observed_predictors = [str(item) for item in fingerprint.get("predictors", [])]
    if observed_predictors != expected_predictors:
        failures.append("Fingerprint mismatch for predictors.")
    usable_sample = fingerprint.get("usable_sample", {})
    if not isinstance(usable_sample, Mapping) or "rows" not in usable_sample:
        failures.append("Fingerprint is missing usable_sample metadata.")
    return failures
