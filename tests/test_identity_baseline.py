from __future__ import annotations

from pathlib import Path

import pandas as pd

from tdcpass.analysis.identity_baseline import build_identity_baseline_irf, build_identity_variant_ladder
from tdcpass.analysis.treatment_fingerprint import (
    build_headline_treatment_fingerprint,
    validate_headline_treatment_fingerprint,
)


def _panel(rows: int = 48) -> pd.DataFrame:
    data = []
    for i in range(rows):
        tdc = 1.0 + 0.2 * i + (i % 3) * 0.1
        total = tdc + 2.0 + 0.15 * i
        data.append(
            {
                "quarter": f"20{i // 4:02d}Q{(i % 4) + 1}",
                "tdc_residual_z": (-1) ** i * 0.5 + 0.02 * i,
                "tdc_residual": (-1) ** i * 0.2 + 0.01 * i,
                "tdc_bank_only_qoq": tdc,
                "total_deposits_bank_qoq": total,
                "lag_tdc_bank_only_qoq": None if i == 0 else data[-1]["tdc_bank_only_qoq"],
                "lag_fedfunds": 2.0 + 0.01 * i,
                "lag_unemployment": 5.0 - 0.01 * i,
                "lag_inflation": 2.0 + 0.005 * i,
            }
        )
    return pd.DataFrame(data)


def test_identity_baseline_preserves_accounting_identity() -> None:
    frame = build_identity_baseline_irf(
        _panel(),
        shock_col="tdc_residual_z",
        controls=["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        horizons=[0, 1, 4],
        bootstrap_reps=40,
        bootstrap_seed=7,
    )

    for horizon in [0, 1, 4]:
        tdc = frame[(frame["outcome"] == "tdc_bank_only_qoq") & (frame["horizon"] == horizon)].iloc[0]
        total = frame[(frame["outcome"] == "total_deposits_bank_qoq") & (frame["horizon"] == horizon)].iloc[0]
        other = frame[(frame["outcome"] == "other_component_qoq") & (frame["horizon"] == horizon)].iloc[0]
        assert abs(float(total["beta"]) - float(tdc["beta"]) - float(other["beta"])) < 1e-10
        assert int(total["n"]) == int(tdc["n"]) == int(other["n"])
        assert other["outcome_construction"] == "derived_total_minus_tdc"
        assert other["decomposition_mode"] == "exact_identity_baseline"


def test_identity_baseline_bootstrap_is_reproducible() -> None:
    kwargs = {
        "shock_col": "tdc_residual_z",
        "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        "horizons": [0, 4],
        "bootstrap_reps": 35,
        "bootstrap_seed": 11,
    }
    left = build_identity_baseline_irf(_panel(), **kwargs)
    right = build_identity_baseline_irf(_panel(), **kwargs)
    pd.testing.assert_frame_equal(left, right)


def test_identity_baseline_supports_nested_shock_reestimation() -> None:
    spec = {
        "target": "tdc_bank_only_qoq",
        "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        "min_train_obs": 24,
        "max_train_obs": 40,
        "method": "rolling_window_ridge",
        "model_name": "unexpected_tdc_default",
        "ridge_alpha": 125.0,
        "standardize_residual": True,
        "standardized_column": "tdc_residual_z",
        "residual_column": "tdc_residual",
        "fitted_column": "tdc_fitted",
        "train_start_obs_column": "train_start_obs",
    }
    frame = build_identity_baseline_irf(
        _panel(),
        shock_col="tdc_residual_z",
        controls=["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        horizons=[0],
        bootstrap_reps=20,
        bootstrap_seed=5,
        nested_shock_spec=spec,
    )

    assert not frame.empty
    assert frame["inference_method"].drop_duplicates().tolist() == ["nested_circular_block_bootstrap_4q_20reps"]


def test_identity_variant_ladder_materializes_measurement_rows() -> None:
    panel = _panel().rename(columns={"tdc_bank_only_qoq": "tdc_domestic_bank_only_qoq"}).copy()
    panel["lag_tdc_domestic_bank_only_qoq"] = panel["tdc_domestic_bank_only_qoq"].shift(1)
    panel["tdc_domestic_bank_only_residual_z"] = panel["tdc_residual_z"]
    frame = build_identity_variant_ladder(
        panel,
        variants=[
            {
                "treatment_variant": "domestic_bank_only",
                "treatment_role": "exploratory",
                "treatment_family": "measurement",
                "shock_column": "tdc_domestic_bank_only_residual_z",
                "target": "tdc_domestic_bank_only_qoq",
                "controls": ["lag_tdc_domestic_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
            }
        ],
        horizons=[0],
        spec_name="identity_measurement_ladder",
        bootstrap_reps=10,
    )

    assert set(frame["outcome"]) == {"tdc_domestic_bank_only_qoq", "total_deposits_bank_qoq", "other_component_qoq"}
    assert frame["treatment_variant"].drop_duplicates().tolist() == ["domestic_bank_only"]
    assert frame["treatment_family"].drop_duplicates().tolist() == ["measurement"]
    assert frame["spec_name"].drop_duplicates().tolist() == ["identity_measurement_ladder"]


def test_treatment_fingerprint_matches_default_spec() -> None:
    shocked = _panel()
    repo_root = Path(__file__).resolve().parents[1]
    spec = {
        "freeze_status": "frozen",
        "model_name": "unexpected_tdc_default",
        "target": "tdc_bank_only_qoq",
        "method": "rolling_window_ridge",
        "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        "ridge_alpha": 125.0,
        "min_train_obs": 24,
        "max_train_obs": 40,
        "standardized_column": "tdc_residual_z",
        "residual_column": "tdc_residual",
        "fitted_column": "tdc_fitted",
        "train_start_obs_column": "train_start_obs",
    }
    fingerprint = build_headline_treatment_fingerprint(
        shock_spec=spec,
        shocked=shocked,
        repo_root=repo_root,
    )

    assert validate_headline_treatment_fingerprint(fingerprint, shock_spec=spec, repo_root=repo_root) == []
    assert "config_hashes" in fingerprint
    assert "upstream_input" in fingerprint


def test_treatment_fingerprint_detects_git_commit_mismatch() -> None:
    shocked = _panel()
    repo_root = Path(__file__).resolve().parents[1]
    spec = {
        "freeze_status": "frozen",
        "model_name": "unexpected_tdc_default",
        "target": "tdc_bank_only_qoq",
        "method": "rolling_window_ridge",
        "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        "ridge_alpha": 125.0,
        "min_train_obs": 24,
        "max_train_obs": 40,
        "standardized_column": "tdc_residual_z",
        "residual_column": "tdc_residual",
        "fitted_column": "tdc_fitted",
        "train_start_obs_column": "train_start_obs",
    }
    fingerprint = build_headline_treatment_fingerprint(
        shock_spec=spec,
        shocked=shocked,
        repo_root=repo_root,
    )
    fingerprint["git_commit"] = "definitely-not-current-head"

    failures = validate_headline_treatment_fingerprint(fingerprint, shock_spec=spec, repo_root=repo_root)
    assert any("git_commit mismatch" in failure for failure in failures)
