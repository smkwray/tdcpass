from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

_BUNDLE_COLUMN = "tdc_toc_row_bundle_qoq"
_BROAD_SUPPORT_COLUMN = "toc_row_broad_support_counterpart_qoq"
_LIQUIDITY_EXTERNAL_COLUMN = "toc_row_liquidity_external_counterpart_qoq"
_DIRECT_DEPOSIT_COLUMN = "toc_row_direct_deposit_counterpart_qoq"
_COUNTERPART_COLUMNS: tuple[str, ...] = (
    _BROAD_SUPPORT_COLUMN,
    _LIQUIDITY_EXTERNAL_COLUMN,
    _DIRECT_DEPOSIT_COLUMN,
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


def _augment_bundle_columns(shocked: pd.DataFrame) -> pd.DataFrame:
    frame = shocked.copy()
    frame[_BUNDLE_COLUMN] = frame["tdc_row_treasury_transactions_qoq"] - frame["tdc_treasury_operating_cash_qoq"]
    frame[_BROAD_SUPPORT_COLUMN] = frame["foreign_nonts_qoq"] - frame["tga_qoq"]
    frame[_LIQUIDITY_EXTERNAL_COLUMN] = frame["foreign_nonts_qoq"] + frame["reserves_qoq"]
    frame[_DIRECT_DEPOSIT_COLUMN] = frame["checkable_rest_of_world_bank_qoq"] - frame["tga_qoq"]
    return frame


def _quarterly_alignment_summary(shocked: pd.DataFrame) -> dict[str, Any]:
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

    frame = _augment_bundle_columns(shocked)[["quarter", _BUNDLE_COLUMN, *_COUNTERPART_COLUMNS]].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    bundle = frame[_BUNDLE_COLUMN].astype(float)
    alignment: dict[str, Any] = {"status": "available", "rows": int(frame.shape[0]), "counterparts": {}}
    for column in _COUNTERPART_COLUMNS:
        series = frame[column].astype(float)
        counterpart_summary: dict[str, Any] = {
            "contemporaneous_corr": None if pd.isna(bundle.corr(series)) else float(bundle.corr(series)),
            "lead_lag_correlations": {},
            "sign_match_share": float(((bundle > 0.0) == (series > 0.0)).mean()),
        }
        for shift in (-2, -1, 0, 1, 2):
            corr = bundle.corr(series.shift(shift))
            counterpart_summary["lead_lag_correlations"][f"shift_{shift:+d}q"] = None if pd.isna(corr) else float(corr)
        alignment["counterparts"][column] = counterpart_summary
    return alignment


def build_toc_row_bundle_audit_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    quarterly_alignment = _quarterly_alignment_summary(shocked)
    augmented = _augment_bundle_columns(shocked)
    lp = run_local_projections(
        augmented,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[
            _BUNDLE_COLUMN,
            "tdc_row_treasury_transactions_qoq",
            "tdc_treasury_operating_cash_qoq",
            _BROAD_SUPPORT_COLUMN,
            _LIQUIDITY_EXTERNAL_COLUMN,
            _DIRECT_DEPOSIT_COLUMN,
            "foreign_nonts_qoq",
            "tga_qoq",
            "reserves_qoq",
            "checkable_rest_of_world_bank_qoq",
        ],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="toc_row_bundle_audit",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        bundle_response = _snapshot(_lp_row(lp, outcome=_BUNDLE_COLUMN, horizon=horizon))
        row_response = _snapshot(_lp_row(lp, outcome="tdc_row_treasury_transactions_qoq", horizon=horizon))
        toc_response = _snapshot(_lp_row(lp, outcome="tdc_treasury_operating_cash_qoq", horizon=horizon))
        broad_support = _snapshot(_lp_row(lp, outcome=_BROAD_SUPPORT_COLUMN, horizon=horizon))
        liquidity_external = _snapshot(_lp_row(lp, outcome=_LIQUIDITY_EXTERNAL_COLUMN, horizon=horizon))
        direct_deposit = _snapshot(_lp_row(lp, outcome=_DIRECT_DEPOSIT_COLUMN, horizon=horizon))
        if all(
            item is None
            for item in (
                bundle_response,
                row_response,
                toc_response,
                broad_support,
                liquidity_external,
                direct_deposit,
            )
        ):
            continue

        interpretation = "mixed_bundle_pattern"
        bundle_beta = None if bundle_response is None else float(bundle_response["beta"])
        broad_support_beta = None if broad_support is None else float(broad_support["beta"])
        liquidity_external_beta = None if liquidity_external is None else float(liquidity_external["beta"])
        direct_deposit_beta = None if direct_deposit is None else float(direct_deposit["beta"])
        if (
            bundle_beta is not None
            and broad_support_beta is not None
            and liquidity_external_beta is not None
            and bundle_beta > 0.0
            and broad_support_beta > 0.0
            and liquidity_external_beta > 0.0
        ):
            interpretation = "broad_support_bundle_pattern"
        if (
            bundle_beta is not None
            and direct_deposit_beta is not None
            and bundle_beta > 0.0
            and direct_deposit_beta > 0.0
            and (liquidity_external_beta is None or liquidity_external_beta <= 0.0)
        ):
            interpretation = "direct_deposit_bundle_pattern"

        key_horizons[f"h{horizon}"] = {
            "toc_row_bundle_response": bundle_response,
            "rest_of_world_treasury_response": row_response,
            "treasury_operating_cash_response": toc_response,
            "broad_support_counterpart_response": broad_support,
            "liquidity_external_counterpart_response": liquidity_external,
            "direct_deposit_counterpart_response": direct_deposit,
            "interpretation": interpretation,
        }

    takeaways = [
        "This audit treats the combined TOC-plus-ROW bundle as the main suspect treatment block and checks whether it behaves more like a broad support bundle or a clean deposit-liability counterpart.",
    ]
    if quarterly_alignment.get("status") == "available":
        counterparts = dict(quarterly_alignment.get("counterparts", {}))
        broad_corr = dict(counterparts.get(_BROAD_SUPPORT_COLUMN, {})).get("contemporaneous_corr")
        direct_corr = dict(counterparts.get(_DIRECT_DEPOSIT_COLUMN, {})).get("contemporaneous_corr")
        if broad_corr is not None and direct_corr is not None:
            if float(broad_corr) > float(direct_corr) + 0.05:
                takeaways.append(
                    "Quarter by quarter, the combined TOC/ROW bundle lines up more closely with the broad-support counterpart than with the TGA-anchored direct-deposit counterpart: "
                    f"broad-support corr ≈ {float(broad_corr):.2f}, direct-deposit corr ≈ {float(direct_corr):.2f}."
                )
            elif float(direct_corr) > float(broad_corr) + 0.05:
                takeaways.append(
                    "Quarter by quarter, the combined TOC/ROW bundle also co-moves strongly with the TGA-anchored direct-deposit counterpart, not just the broader support bundle: "
                    f"broad-support corr ≈ {float(broad_corr):.2f}, direct-deposit corr ≈ {float(direct_corr):.2f}."
                )
            else:
                takeaways.append(
                    "Quarter by quarter, the combined TOC/ROW bundle lines up similarly with both the broad-support and TGA-anchored direct-deposit counterparts: "
                    f"broad-support corr ≈ {float(broad_corr):.2f}, direct-deposit corr ≈ {float(direct_corr):.2f}."
                )
    h0 = key_horizons.get("h0", {})
    bundle_response = h0.get("toc_row_bundle_response")
    broad_support = h0.get("broad_support_counterpart_response")
    liquidity_external = h0.get("liquidity_external_counterpart_response")
    direct_deposit = h0.get("direct_deposit_counterpart_response")
    if bundle_response is not None and broad_support is not None and liquidity_external is not None:
        takeaways.append(
            "At h0, the combined TOC/ROW bundle loads like a broader support block: "
            f"bundle ≈ {float(bundle_response['beta']):.2f}, broad support ≈ {float(broad_support['beta']):.2f}, "
            f"liquidity+external support ≈ {float(liquidity_external['beta']):.2f}."
        )
    if direct_deposit is not None:
        takeaways.append(
            f"The narrow direct-deposit counterpart is smaller at h0: direct deposit counterpart ≈ {float(direct_deposit['beta']):.2f}."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "Does the combined TOC-plus-ROW treatment block behave like a broad support bundle or like a clean deposit-liability counterpart?",
        "estimation_path": {
            "lp_spec_name": "toc_row_bundle_audit",
            "summary_artifact": "toc_row_bundle_audit_summary.json",
        },
        "quarterly_alignment": quarterly_alignment,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
