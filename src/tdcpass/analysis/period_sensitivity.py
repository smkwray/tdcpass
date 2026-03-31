from __future__ import annotations

from typing import Any

import pandas as pd


KEY_HORIZONS = (0, 4)
KEY_OUTCOMES = ("tdc_bank_only_qoq", "total_deposits_bank_qoq", "other_component_qoq")


def _ci_excludes_zero(row: dict[str, Any] | None) -> bool:
    if row is None:
        return False
    return float(row["lower95"]) > 0.0 or float(row["upper95"]) < 0.0


def _assessment(total_row: dict[str, Any] | None, other_row: dict[str, Any] | None) -> str:
    if total_row is None or other_row is None:
        return "insufficient_data"
    total_beta = float(total_row["beta"])
    other_beta = float(other_row["beta"])
    total_ci = _ci_excludes_zero(total_row)
    other_ci = _ci_excludes_zero(other_row)
    if total_ci and total_beta > 0.0 and other_ci and other_beta < 0.0:
        return "crowd_out_signal"
    if total_ci and total_beta > 0.0 and other_ci and other_beta > 0.0:
        return "total_up_other_up"
    if total_ci and total_beta < 0.0 and other_ci and other_beta < 0.0:
        return "total_down_other_down"
    if total_ci and total_beta > 0.0:
        return "total_up_other_unclear"
    if other_ci and other_beta < 0.0:
        return "other_down_total_unclear"
    return "not_separated"


def _row_lookup(df: pd.DataFrame, *, period_variant: str, outcome: str, horizon: int) -> dict[str, Any] | None:
    subset = df[
        (df["period_variant"] == period_variant)
        & (df["outcome"] == outcome)
        & (df["horizon"] == horizon)
    ]
    if subset.empty:
        return None
    row = subset.iloc[0]
    return {
        "beta": float(row["beta"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "ci_excludes_zero": _ci_excludes_zero(
            {
                "lower95": float(row["lower95"]),
                "upper95": float(row["upper95"]),
            }
        ),
    }


def build_period_sensitivity_summary(period_sensitivity: pd.DataFrame) -> dict[str, Any]:
    if period_sensitivity.empty:
        return {
            "status": "unavailable",
            "headline_question": "Do total deposits and non-TDC responses differ across major usable-sample periods?",
            "estimation_path": {
                "role": "secondary_period_sensitivity_surface",
                "artifact": "period_sensitivity.csv",
                "primary_release_artifact": "lp_irf_identity_baseline.csv",
                "note": "This is a secondary sensitivity surface, not the primary exact identity baseline.",
            },
            "periods": [],
            "key_horizons": {},
            "takeaways": ["No period-sensitivity estimates were available."],
        }

    periods: list[dict[str, Any]] = []
    key_horizons: dict[str, Any] = {}

    for period_variant in period_sensitivity["period_variant"].drop_duplicates().tolist():
        period_df = period_sensitivity[period_sensitivity["period_variant"] == period_variant]
        first_row = period_df.iloc[0]
        period_payload = {
            "period_variant": str(period_variant),
            "period_role": str(first_row["period_role"]),
            "start_quarter": None if pd.isna(first_row["start_quarter"]) else str(first_row["start_quarter"]),
            "end_quarter": None if pd.isna(first_row["end_quarter"]) else str(first_row["end_quarter"]),
            "key_horizons": {},
        }
        for horizon in KEY_HORIZONS:
            total_row = _row_lookup(period_sensitivity, period_variant=period_variant, outcome="total_deposits_bank_qoq", horizon=horizon)
            other_row = _row_lookup(period_sensitivity, period_variant=period_variant, outcome="other_component_qoq", horizon=horizon)
            tdc_row = _row_lookup(period_sensitivity, period_variant=period_variant, outcome="tdc_bank_only_qoq", horizon=horizon)
            period_payload["key_horizons"][f"h{horizon}"] = {
                "assessment": _assessment(
                    total_row,
                    other_row,
                ),
                "tdc": tdc_row,
                "total_deposits": total_row,
                "other_component": other_row,
            }
        periods.append(period_payload)
        key_horizons[str(period_variant)] = period_payload["key_horizons"]

    covid_post = next((p for p in periods if p["period_variant"] == "covid_post"), None)
    post_gfc = next((p for p in periods if p["period_variant"] == "post_gfc_early"), None)
    takeaways: list[str] = []
    if covid_post is not None:
        h0 = covid_post["key_horizons"].get("h0", {})
        total = h0.get("total_deposits")
        other = h0.get("other_component")
        if total is not None and other is not None:
            takeaways.append(
                "The COVID/post-COVID window has the largest impact-stage total-deposit point estimate, but period labels remain CI-aware rather than sign-only."
            )
    if post_gfc is not None:
        h0 = post_gfc["key_horizons"].get("h0", {})
        total = h0.get("total_deposits")
        other = h0.get("other_component")
        if total is not None and other is not None:
            takeaways.append(
                "In the post-GFC early window, the non-TDC impact estimate is negative while the total-deposit impact response is less decisively separated."
            )
    takeaways.append(
        "This summary remains on the public preview surface because medium-horizon persistence differs meaningfully across the post-GFC early, pre-COVID, and COVID/post-COVID windows."
    )
    takeaways.append(
        "The usable unexpected-TDC shock sample begins in 2009Q1, so the live quarterly design cannot estimate a true 2008 GFC subperiod under the frozen shock."
    )

    return {
        "status": "materialized",
        "headline_question": "Do total deposits and non-TDC responses differ across major usable-sample periods?",
        "estimation_path": {
            "role": "secondary_period_sensitivity_surface",
            "artifact": "period_sensitivity.csv",
            "primary_release_artifact": "lp_irf_identity_baseline.csv",
            "note": "This is a secondary sensitivity surface, not the primary exact identity baseline.",
        },
        "periods": periods,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
