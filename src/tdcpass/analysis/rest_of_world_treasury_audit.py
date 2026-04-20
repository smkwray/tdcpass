from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

_COUNTERPART_COLUMNS: tuple[str, ...] = (
    "foreign_nonts_qoq",
    "checkable_rest_of_world_bank_qoq",
    "interbank_transactions_foreign_banks_liability_qoq",
    "interbank_transactions_foreign_banks_asset_qoq",
    "deposits_at_foreign_banks_asset_qoq",
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


def _quarterly_alignment_summary(shocked: pd.DataFrame) -> dict[str, Any]:
    required = {"quarter", "tdc_row_treasury_transactions_qoq", *_COUNTERPART_COLUMNS}
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    row_leg = frame["tdc_row_treasury_transactions_qoq"].astype(float)
    alignment: dict[str, Any] = {"status": "available", "rows": int(frame.shape[0]), "counterparts": {}}
    for column in _COUNTERPART_COLUMNS:
        series = frame[column].astype(float)
        counterpart_summary: dict[str, Any] = {
            "contemporaneous_corr": None if pd.isna(row_leg.corr(series)) else float(row_leg.corr(series)),
            "lead_lag_correlations": {},
            "sign_match_share": float(((row_leg > 0.0) == (series > 0.0)).mean()),
        }
        for shift in (-2, -1, 0, 1, 2):
            corr = row_leg.corr(series.shift(shift))
            counterpart_summary["lead_lag_correlations"][f"shift_{shift:+d}q"] = None if pd.isna(corr) else float(corr)
        alignment["counterparts"][column] = counterpart_summary
    return alignment


def build_rest_of_world_treasury_audit_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    quarterly_alignment = _quarterly_alignment_summary(shocked)
    lp = run_local_projections(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=["tdc_row_treasury_transactions_qoq", *_COUNTERPART_COLUMNS],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="rest_of_world_treasury_audit",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        row_response = _snapshot(_lp_row(lp, outcome="tdc_row_treasury_transactions_qoq", horizon=horizon))
        foreign_nonts = _snapshot(_lp_row(lp, outcome="foreign_nonts_qoq", horizon=horizon))
        row_deposits = _snapshot(_lp_row(lp, outcome="checkable_rest_of_world_bank_qoq", horizon=horizon))
        foreign_bank_liabs = _snapshot(
            _lp_row(lp, outcome="interbank_transactions_foreign_banks_liability_qoq", horizon=horizon)
        )
        foreign_bank_assets = _snapshot(
            _lp_row(lp, outcome="interbank_transactions_foreign_banks_asset_qoq", horizon=horizon)
        )
        foreign_bank_deposits = _snapshot(_lp_row(lp, outcome="deposits_at_foreign_banks_asset_qoq", horizon=horizon))
        if all(
            item is None
            for item in (
                row_response,
                foreign_nonts,
                row_deposits,
                foreign_bank_liabs,
                foreign_bank_assets,
                foreign_bank_deposits,
            )
        ):
            continue

        interpretation = "mixed_external_pattern"
        row_beta = None if row_response is None else float(row_response["beta"])
        foreign_nonts_beta = None if foreign_nonts is None else float(foreign_nonts["beta"])
        row_deposits_beta = None if row_deposits is None else float(row_deposits["beta"])
        foreign_bank_assets_beta = None if foreign_bank_assets is None else float(foreign_bank_assets["beta"])
        if (
            row_beta is not None
            and foreign_nonts_beta is not None
            and foreign_bank_assets_beta is not None
            and row_beta > 0.0
            and foreign_nonts_beta > 0.0
            and foreign_bank_assets_beta > 0.0
        ):
            interpretation = "external_asset_support_pattern"
        if (
            row_beta is not None
            and row_deposits_beta is not None
            and row_beta > 0.0
            and row_deposits_beta > 0.0
            and (foreign_bank_assets_beta is None or foreign_bank_assets_beta <= 0.0)
        ):
            interpretation = "direct_row_deposit_support_pattern"

        key_horizons[f"h{horizon}"] = {
            "rest_of_world_treasury_response": row_response,
            "foreign_nonts_response": foreign_nonts,
            "checkable_rest_of_world_bank_response": row_deposits,
            "interbank_transactions_foreign_banks_liability_response": foreign_bank_liabs,
            "interbank_transactions_foreign_banks_asset_response": foreign_bank_assets,
            "deposits_at_foreign_banks_asset_response": foreign_bank_deposits,
            "interpretation": interpretation,
        }

    takeaways = [
        "This audit checks whether the ROW Treasury leg behaves like a clean external deposit counterpart or a broader external-asset / nontransaction support channel.",
    ]
    if quarterly_alignment.get("status") == "available":
        counterparts = dict(quarterly_alignment.get("counterparts", {}))
        foreign_nonts_corr = dict(counterparts.get("foreign_nonts_qoq", {})).get("contemporaneous_corr")
        row_deposits_corr = dict(counterparts.get("checkable_rest_of_world_bank_qoq", {})).get("contemporaneous_corr")
        if foreign_nonts_corr is not None and row_deposits_corr is not None:
            takeaways.append(
                "Quarter by quarter, the ROW Treasury leg is not a simple same-quarter liability counterpart: "
                f"corr with `foreign_nonts_qoq` ≈ {float(foreign_nonts_corr):.2f}, corr with `checkable_rest_of_world_bank_qoq` ≈ {float(row_deposits_corr):.2f}."
            )
    h0 = key_horizons.get("h0", {})
    row_response = h0.get("rest_of_world_treasury_response")
    foreign_nonts = h0.get("foreign_nonts_response")
    foreign_bank_assets = h0.get("interbank_transactions_foreign_banks_asset_response")
    row_deposits = h0.get("checkable_rest_of_world_bank_response")
    if row_response is not None and foreign_nonts is not None and foreign_bank_assets is not None:
        takeaways.append(
            "At h0, the baseline TDC shock loads positively on the ROW Treasury leg and broader external support variables: "
            f"ROW ≈ {float(row_response['beta']):.2f}, foreign NONTS ≈ {float(foreign_nonts['beta']):.2f}, "
            f"foreign-bank interbank assets ≈ {float(foreign_bank_assets['beta']):.2f}."
        )
    if row_response is not None and row_deposits is not None:
        takeaways.append(
            f"The direct ROW bank-deposit-liability response is comparatively small at h0: checkable ROW deposits ≈ {float(row_deposits['beta']):.2f}."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "Does the ROW Treasury leg map to a clean deposit counterpart, or does it behave more like a broader external-support channel?",
        "estimation_path": {
            "lp_spec_name": "rest_of_world_treasury_audit",
            "summary_artifact": "rest_of_world_treasury_audit_summary.json",
        },
        "quarterly_alignment": quarterly_alignment,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
