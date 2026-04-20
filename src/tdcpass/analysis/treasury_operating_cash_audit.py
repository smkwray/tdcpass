from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections


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


def _quarterly_alignment_summary(shocked: pd.DataFrame) -> dict[str, Any]:
    required = {"quarter", "tdc_treasury_operating_cash_qoq", "tga_qoq", "reserves_qoq", "cb_nonts_qoq"}
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    toc = frame["tdc_treasury_operating_cash_qoq"].astype(float)
    tga = frame["tga_qoq"].astype(float)
    reserves = frame["reserves_qoq"].astype(float)
    cb_nonts = frame["cb_nonts_qoq"].astype(float)
    valid_ratio = toc != 0.0
    ratio = (tga[valid_ratio] / toc[valid_ratio]).astype(float)

    slope, intercept = np.polyfit(toc.to_numpy(dtype=float), tga.to_numpy(dtype=float), deg=1)
    contemporaneous_corr = float(toc.corr(tga))
    lead_lag = {}
    for shift in (-2, -1, 0, 1, 2):
        corr = toc.corr(tga.shift(shift))
        lead_lag[f"tga_shift_{shift:+d}q"] = None if pd.isna(corr) else float(corr)

    frame["toc_tga_diff_qoq"] = toc - tga
    frame["toc_tga_sum_qoq"] = toc + tga
    worst = (
        frame.assign(abs_diff=lambda df: df["toc_tga_diff_qoq"].abs())
        .sort_values("abs_diff", ascending=False)
        .head(8)
    )

    return {
        "status": "available",
        "rows": int(frame.shape[0]),
        "contemporaneous_corr_tga_vs_toc": contemporaneous_corr,
        "ols_tga_on_toc": {
            "slope": float(slope),
            "intercept": float(intercept),
            "r2": float(contemporaneous_corr**2),
        },
        "lead_lag_correlations": lead_lag,
        "sign_match_share_tga_vs_toc": float(((tga > 0.0) == (toc > 0.0)).mean()),
        "sign_match_share_cb_nonts_vs_toc": float(((cb_nonts > 0.0) == (toc > 0.0)).mean()),
        "ratio_tga_over_toc": {
            "mean": float(ratio.mean()) if not ratio.empty else None,
            "median": float(ratio.median()) if not ratio.empty else None,
            "p10": float(ratio.quantile(0.1)) if not ratio.empty else None,
            "p90": float(ratio.quantile(0.9)) if not ratio.empty else None,
        },
        "worst_quarters_by_abs_diff": [
            {
                "quarter": str(row["quarter"]),
                "tdc_treasury_operating_cash_qoq": float(row["tdc_treasury_operating_cash_qoq"]),
                "tga_qoq": float(row["tga_qoq"]),
                "reserves_qoq": float(row["reserves_qoq"]),
                "cb_nonts_qoq": float(row["cb_nonts_qoq"]),
                "toc_tga_diff_qoq": float(row["toc_tga_diff_qoq"]),
                "toc_tga_sum_qoq": float(row["toc_tga_sum_qoq"]),
            }
            for _, row in worst.iterrows()
        ],
    }


def build_treasury_operating_cash_audit_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    quarterly_alignment = _quarterly_alignment_summary(shocked)
    lp = run_local_projections(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[
            "tdc_treasury_operating_cash_qoq",
            "tga_qoq",
            "reserves_qoq",
            "cb_nonts_qoq",
        ],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="treasury_operating_cash_audit",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        toc_response = _snapshot(_lp_row(lp, outcome="tdc_treasury_operating_cash_qoq", horizon=horizon))
        tga_response = _snapshot(_lp_row(lp, outcome="tga_qoq", horizon=horizon))
        reserves_response = _snapshot(_lp_row(lp, outcome="reserves_qoq", horizon=horizon))
        cb_nonts_response = _snapshot(_lp_row(lp, outcome="cb_nonts_qoq", horizon=horizon))
        if all(item is None for item in (toc_response, tga_response, reserves_response, cb_nonts_response)):
            continue

        interpretation = "mixed_plumbing_pattern"
        toc_beta = None if toc_response is None else float(toc_response["beta"])
        tga_beta = None if tga_response is None else float(tga_response["beta"])
        reserves_beta = None if reserves_response is None else float(reserves_response["beta"])
        if toc_beta is not None and tga_beta is not None and reserves_beta is not None:
            if toc_beta < 0.0 and tga_beta < 0.0 and reserves_beta > 0.0:
                interpretation = "treasury_cash_release_pattern"
            elif toc_beta > 0.0 and tga_beta > 0.0 and reserves_beta < 0.0:
                interpretation = "treasury_cash_drain_pattern"

        key_horizons[f"h{horizon}"] = {
            "treasury_operating_cash_response": toc_response,
            "treasury_operating_cash_signed_contribution_beta": None if toc_beta is None else -toc_beta,
            "tga_response": tga_response,
            "reserves_response": reserves_response,
            "cb_nonts_response": cb_nonts_response,
            "toc_minus_tga_beta_gap": None if toc_beta is None or tga_beta is None else toc_beta - tga_beta,
            "interpretation": interpretation,
        }

    takeaways = [
        "This audit checks whether the Treasury-operating-cash leg behaves like genuine TGA plumbing or like a sign/timing/object-definition bug.",
    ]
    if quarterly_alignment.get("status") == "available":
        corr = quarterly_alignment.get("contemporaneous_corr_tga_vs_toc")
        slope = quarterly_alignment.get("ols_tga_on_toc", {}).get("slope")
        if corr is not None and slope is not None:
            takeaways.append(
                f"Quarter by quarter, the Z.1 Treasury-operating-cash flow and `tga_qoq` move together tightly: correlation ≈ {float(corr):.2f}, TGA-on-TOC slope ≈ {float(slope):.2f}."
            )
    h0 = key_horizons.get("h0", {})
    toc_response = h0.get("treasury_operating_cash_response")
    tga_response = h0.get("tga_response")
    reserves_response = h0.get("reserves_response")
    toc_signed = h0.get("treasury_operating_cash_signed_contribution_beta")
    if toc_response is not None and tga_response is not None and reserves_response is not None:
        takeaways.append(
            "At h0, the baseline TDC shock lines up with a Treasury-cash-release pattern: "
            f"TOC ≈ {float(toc_response['beta']):.2f}, TGA ≈ {float(tga_response['beta']):.2f}, reserves ≈ {float(reserves_response['beta']):.2f}."
        )
    if toc_signed is not None:
        takeaways.append(
            f"Because the TDC identity subtracts the Treasury-operating-cash leg, the h0 signed TDC contribution from that leg is about {float(toc_signed):.2f}."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "Does the Treasury-operating-cash leg behave like genuine TGA plumbing, or is it likely a sign/timing/object-definition problem?",
        "estimation_path": {
            "lp_spec_name": "treasury_operating_cash_audit",
            "summary_artifact": "treasury_operating_cash_audit_summary.json",
        },
        "quarterly_alignment": quarterly_alignment,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
