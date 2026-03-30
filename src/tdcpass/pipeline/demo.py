from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from tdcpass.analysis.accounting import compute_other_component, build_accounting_summary, summary_to_frame
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.analysis.local_projections import run_local_projections
from tdcpass.core.manifest import write_manifest
from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.reports.site_export import export_frame, write_overview_json


def generate_synthetic_panel(n_periods: int = 96, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    quarters = pd.period_range("2000Q1", periods=n_periods, freq="Q")

    fedfunds = np.clip(np.cumsum(rng.normal(0.02, 0.10, n_periods)) + 2.0, 0, None)
    unemployment = np.clip(5 + np.cumsum(rng.normal(0.0, 0.08, n_periods)), 3.0, 10.0)
    inflation = np.clip(2 + rng.normal(0.0, 0.5, n_periods), -1.0, 8.0)

    bill_share = np.clip(0.45 + rng.normal(0.0, 0.10, n_periods), 0.15, 0.80)
    bank_absorption_share = np.clip(0.42 + rng.normal(0.0, 0.12, n_periods), 0.10, 0.85)
    slr_tight = (bank_absorption_share > 0.55).astype(float)

    tga_qoq = rng.normal(0.0, 25.0, n_periods)
    reserves_qoq = -0.4 * tga_qoq + rng.normal(0.0, 20.0, n_periods)

    predictable = (
        10
        + 18 * bill_share
        + 10 * bank_absorption_share
        - 2.0 * fedfunds
        + 0.8 * inflation
        - 0.6 * unemployment
        + 0.15 * tga_qoq
    )
    unexpected = rng.normal(0.0, 12.0, n_periods)
    tdc_qoq = predictable + unexpected

    bank_credit_private_qoq = 12 + 0.5 * inflation - 0.7 * fedfunds - 0.35 * unexpected + rng.normal(0.0, 6.0, n_periods)
    cb_nonts_qoq = -0.10 * unexpected + rng.normal(0.0, 3.0, n_periods)
    foreign_nonts_qoq = 4 + 0.15 * unexpected + rng.normal(0.0, 4.0, n_periods)

    other_component_qoq = 0.65 * bank_credit_private_qoq + 0.6 * cb_nonts_qoq + 0.6 * foreign_nonts_qoq - 0.40 * unexpected
    total_deposits_qoq = tdc_qoq + other_component_qoq

    df = pd.DataFrame(
        {
            "quarter": quarters.astype(str),
            "tdc_qoq": tdc_qoq,
            "total_deposits_qoq": total_deposits_qoq,
            "other_component_qoq": other_component_qoq,
            "bank_credit_private_qoq": bank_credit_private_qoq,
            "cb_nonts_qoq": cb_nonts_qoq,
            "foreign_nonts_qoq": foreign_nonts_qoq,
            "bill_share": bill_share,
            "bank_absorption_share": bank_absorption_share,
            "slr_tight": slr_tight,
            "tga_qoq": tga_qoq,
            "reserves_qoq": reserves_qoq,
            "fedfunds": fedfunds,
            "unemployment": unemployment,
            "inflation": inflation,
        }
    )

    lag_cols = [
        "total_deposits_qoq",
        "bank_credit_private_qoq",
        "tga_qoq",
        "reserves_qoq",
        "bill_share",
        "fedfunds",
        "unemployment",
        "inflation",
    ]
    for col in lag_cols:
        df[f"lag_{col}"] = df[col].shift(1)

    return df


def run_demo_pipeline(base_dir: Path | None = None) -> dict:
    root = base_dir or repo_root()
    dirs = ensure_repo_dirs(root)

    df = generate_synthetic_panel()
    df = compute_other_component(df)

    example_path = root / "data" / "examples" / "synthetic_quarterly_panel.csv"
    example_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(example_path, index=False)

    accounting = build_accounting_summary(df)
    accounting_df = summary_to_frame(accounting)
    accounting_path = root / "output" / "demo" / "accounting_summary.csv"
    export_frame(accounting_df, accounting_path)

    shocked = expanding_window_residual(
        df,
        target="tdc_qoq",
        predictors=[
            "lag_total_deposits_qoq",
            "lag_bank_credit_private_qoq",
            "lag_tga_qoq",
            "lag_reserves_qoq",
            "lag_bill_share",
            "lag_fedfunds",
            "lag_unemployment",
            "lag_inflation",
        ],
        min_train_obs=24,
        standardize=True,
    )
    shocks_path = root / "output" / "demo" / "unexpected_tdc.csv"
    export_frame(
        shocked[
            [
                "quarter",
                "tdc_qoq",
                "tdc_qoq_fitted",
                "tdc_qoq_residual",
                "tdc_qoq_residual_z",
                "tdc_qoq_train_start_obs",
            ]
        ],
        shocks_path,
    )

    irf = run_local_projections(
        shocked,
        shock_col="tdc_qoq_residual_z",
        outcome_cols=["total_deposits_qoq", "other_component_qoq", "bank_credit_private_qoq"],
        controls=["lag_fedfunds", "lag_unemployment", "lag_inflation"],
        horizons=list(range(0, 9)),
        nw_lags=4,
    )
    irf_path = root / "output" / "demo" / "lp_irf.csv"
    export_frame(irf, irf_path)

    overview_path = root / "site" / "data" / "overview.json"
    write_overview_json(
        overview_path,
        headline_metrics={
            "share_other_negative": float(accounting.share_other_negative),
            "mean_tdc": float(accounting.mean_tdc),
            "mean_total_deposits": float(accounting.mean_total_deposits),
        },
        sample={"frequency": "quarterly", "rows": int(len(df)), "demo": True},
        main_findings=[
            "Synthetic data are designed so TDC raises total deposits but partially crowds out the non-TDC component.",
            "The demo is not substantive evidence; it only illustrates the contract and pipeline.",
        ],
        caveats=[
            "Synthetic demo only.",
            "Real quarterly Z.1 and matched deposit construction still need implementation.",
        ],
        evidence_tiers={
            "direct_data": [],
            "transparent_transformations": ["other_component_qoq"],
            "model_based_estimates": ["tdc_qoq_fitted", "tdc_qoq_residual", "tdc_qoq_residual_z"],
            "inferred_counterfactuals": ["lp_irf"],
        },
        artifacts=[
            "data/examples/synthetic_quarterly_panel.csv",
            "output/demo/accounting_summary.csv",
            "output/demo/unexpected_tdc.csv",
            "output/demo/lp_irf.csv",
        ],
    )

    manifest_path = write_manifest(
        root / "output" / "demo" / "run_manifest.json",
        command="demo",
        outputs=[example_path, accounting_path, shocks_path, irf_path, overview_path],
        extra={"rows": int(len(df))},
    )

    return {
        "example_path": str(example_path),
        "accounting_path": str(accounting_path),
        "shocks_path": str(shocks_path),
        "irf_path": str(irf_path),
        "overview_path": str(overview_path),
        "manifest_path": str(manifest_path),
    }
