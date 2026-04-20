from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections
from tdcpass.analysis.toc_row_bundle_audit import (
    _BROAD_SUPPORT_COLUMN,
    _BUNDLE_COLUMN,
    _DIRECT_DEPOSIT_COLUMN,
    _LIQUIDITY_EXTERNAL_COLUMN,
    _augment_bundle_columns,
)


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _path_label_from_gap(gap: float | None, *, tolerance: float = 0.05) -> str:
    if gap is None:
        return "not_available"
    if gap > tolerance:
        return "direct_deposit_path_dominant"
    if gap < -tolerance:
        return "broad_support_path_dominant"
    return "mixed_path_signal"


def _quarterly_path_split(shocked: pd.DataFrame) -> dict[str, Any]:
    required = {
        "quarter",
        "tdc_row_treasury_transactions_qoq",
        "tdc_treasury_operating_cash_qoq",
        "foreign_nonts_qoq",
        "tga_qoq",
        "reserves_qoq",
        "checkable_rest_of_world_bank_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = _augment_bundle_columns(shocked)[
        ["quarter", _BUNDLE_COLUMN, _BROAD_SUPPORT_COLUMN, _DIRECT_DEPOSIT_COLUMN, _LIQUIDITY_EXTERNAL_COLUMN]
    ].dropna()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    bundle = frame[_BUNDLE_COLUMN].astype(float)
    broad = frame[_BROAD_SUPPORT_COLUMN].astype(float)
    direct = frame[_DIRECT_DEPOSIT_COLUMN].astype(float)
    liquidity = frame[_LIQUIDITY_EXTERNAL_COLUMN].astype(float)

    broad_corr = None if pd.isna(bundle.corr(broad)) else float(bundle.corr(broad))
    direct_corr = None if pd.isna(bundle.corr(direct)) else float(bundle.corr(direct))
    liquidity_corr = None if pd.isna(bundle.corr(liquidity)) else float(bundle.corr(liquidity))
    corr_gap = None if broad_corr is None or direct_corr is None else direct_corr - broad_corr

    return {
        "status": "available",
        "rows": int(frame.shape[0]),
        "bundle_contemporaneous_corr": {
            "broad_support_path": broad_corr,
            "direct_deposit_path": direct_corr,
            "liquidity_external_path": liquidity_corr,
            "direct_minus_broad_gap": corr_gap,
        },
        "bundle_sign_match_share": {
            "broad_support_path": float(((bundle > 0.0) == (broad > 0.0)).mean()),
            "direct_deposit_path": float(((bundle > 0.0) == (direct > 0.0)).mean()),
            "liquidity_external_path": float(((bundle > 0.0) == (liquidity > 0.0)).mean()),
        },
        "preferred_quarterly_path": _path_label_from_gap(corr_gap),
    }


def build_toc_row_path_split_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    quarterly_split = _quarterly_path_split(shocked)
    augmented = _augment_bundle_columns(shocked)
    lp = run_local_projections(
        augmented,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[
            _BUNDLE_COLUMN,
            _BROAD_SUPPORT_COLUMN,
            _DIRECT_DEPOSIT_COLUMN,
            _LIQUIDITY_EXTERNAL_COLUMN,
        ],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="toc_row_path_split",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        bundle = _snapshot(_lp_row(lp, outcome=_BUNDLE_COLUMN, horizon=horizon))
        broad = _snapshot(_lp_row(lp, outcome=_BROAD_SUPPORT_COLUMN, horizon=horizon))
        direct = _snapshot(_lp_row(lp, outcome=_DIRECT_DEPOSIT_COLUMN, horizon=horizon))
        liquidity = _snapshot(_lp_row(lp, outcome=_LIQUIDITY_EXTERNAL_COLUMN, horizon=horizon))
        if all(item is None for item in (bundle, broad, direct, liquidity)):
            continue

        bundle_beta = None if bundle is None else float(bundle["beta"])
        broad_beta = None if broad is None else float(broad["beta"])
        direct_beta = None if direct is None else float(direct["beta"])
        liquidity_beta = None if liquidity is None else float(liquidity["beta"])
        gap = None if direct_beta is None or broad_beta is None else direct_beta - broad_beta
        direct_share = None if bundle_beta in (None, 0.0) or direct_beta is None else direct_beta / bundle_beta
        broad_share = None if bundle_beta in (None, 0.0) or broad_beta is None else broad_beta / bundle_beta
        liquidity_share = None if bundle_beta in (None, 0.0) or liquidity_beta is None else liquidity_beta / bundle_beta

        key_horizons[f"h{horizon}"] = {
            "bundle_response": bundle,
            "broad_support_path_response": broad,
            "direct_deposit_path_response": direct,
            "liquidity_external_path_response": liquidity,
            "direct_minus_broad_beta_gap": gap,
            "coverage_share_of_bundle_beta": {
                "broad_support_path": broad_share,
                "direct_deposit_path": direct_share,
                "liquidity_external_path": liquidity_share,
            },
            "preferred_horizon_path": _path_label_from_gap(gap),
        }

    takeaways = [
        "This summary forces a formal split between the TGA-anchored direct-deposit path and the broader support path inside the combined TOC/ROW bundle.",
    ]
    if quarterly_split.get("status") == "available":
        corr_payload = dict(quarterly_split.get("bundle_contemporaneous_corr", {}))
        broad_corr = corr_payload.get("broad_support_path")
        direct_corr = corr_payload.get("direct_deposit_path")
        preferred = quarterly_split.get("preferred_quarterly_path")
        if broad_corr is not None and direct_corr is not None and preferred is not None:
            takeaways.append(
                f"Quarter by quarter, the preferred path is `{preferred}`: broad-support corr ≈ {float(broad_corr):.2f}, direct-deposit corr ≈ {float(direct_corr):.2f}."
            )
    h0 = key_horizons.get("h0", {})
    preferred_h0 = h0.get("preferred_horizon_path")
    broad_h0 = dict(h0.get("broad_support_path_response") or {}).get("beta")
    direct_h0 = dict(h0.get("direct_deposit_path_response") or {}).get("beta")
    liquidity_h0 = dict(h0.get("liquidity_external_path_response") or {}).get("beta")
    if preferred_h0 is not None and broad_h0 is not None and direct_h0 is not None:
        takeaways.append(
            f"At h0, the preferred path is `{preferred_h0}`: broad-support ≈ {float(broad_h0):.2f}, direct-deposit ≈ {float(direct_h0):.2f}."
        )
    if liquidity_h0 is not None:
        takeaways.append(
            f"The broader liquidity-plus-external path remains a larger upper-envelope counterpart at h0, about {float(liquidity_h0):.2f}."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "Within the combined TOC/ROW bundle, does the TGA-anchored direct-deposit path or the broader support path dominate?",
        "estimation_path": {
            "lp_spec_name": "toc_row_path_split",
            "summary_artifact": "toc_row_path_split_summary.json",
        },
        "path_definitions": {
            "bundle": _BUNDLE_COLUMN,
            "broad_support_path": _BROAD_SUPPORT_COLUMN,
            "direct_deposit_path": _DIRECT_DEPOSIT_COLUMN,
            "liquidity_external_path": _LIQUIDITY_EXTERNAL_COLUMN,
        },
        "quarterly_split": quarterly_split,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
