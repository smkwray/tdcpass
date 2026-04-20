from __future__ import annotations

import math
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from tdcpass.pipeline.build_panel import (
    Z1_SERIES,
    _load_fred_series,
    _normalize_z1_levels_frame,
    _qoq_change,
    _quarter_end_level,
)

_TX_START_QUARTER = "2002Q4"
_HISTORICAL_START_QUARTER = "1990Q1"
_PRE_SHIFT_END_QUARTER = "2008Q2"

_REQUIRED_PANEL_COLUMNS = (
    "quarter",
    "tdc_treasury_operating_cash_qoq",
    "federal_govt_cash_balance_proxy_qoq",
)

_REQUIRED_SOURCE_COLUMNS = (
    "quarter",
    "tdc_bank_only_extended_1990",
)


def _quarter_sort_key(value: str) -> pd.Period:
    return pd.Period(str(value), freq="Q")


def _load_extended_tdc_source(path: Path | None) -> pd.DataFrame | None:
    if path is None or not path.exists():
        return None
    frame = pd.read_csv(path)
    if "date" in frame.columns and "quarter" not in frame.columns:
        dates = pd.to_datetime(frame["date"], errors="coerce")
        frame["quarter"] = dates.dt.to_period("Q").astype(str)
    if not set(_REQUIRED_SOURCE_COLUMNS).issubset(frame.columns):
        return None
    out = frame[list(_REQUIRED_SOURCE_COLUMNS)].copy()
    out["tdc_bank_only_extended_1990"] = pd.to_numeric(
        out["tdc_bank_only_extended_1990"], errors="coerce"
    ) / 1000.0
    out = out.dropna(subset=["quarter"]).sort_values("quarter").reset_index(drop=True)
    return out


def _build_raw_historical_cash_frame(root: Path) -> pd.DataFrame | None:
    raw_root = root / "data" / "raw"
    fred_path = raw_root / "fred" / "BOGZ1FU313024000Q.csv"
    z1_zip_path = raw_root / "z1" / "z1_csv_files.zip"
    if not fred_path.exists() or not z1_zip_path.exists():
        return None
    toc_qoq = _quarter_end_level(_load_fred_series(fred_path)) / 1000.0
    z1_keys = {
        "checkable_federal_govt_bank_level": Z1_SERIES["checkable_federal_govt_bank_level"],
        "federal_govt_checkable_total_level": Z1_SERIES["federal_govt_checkable_total_level"],
        "federal_govt_time_savings_total_level": Z1_SERIES["federal_govt_time_savings_total_level"],
    }
    with zipfile.ZipFile(z1_zip_path) as archive:
        with archive.open("csv/all_sectors_levels_q.csv") as handle:
            z1_frame = pd.read_csv(handle)
    z1_levels = _normalize_z1_levels_frame(z1_frame, z1_keys)
    z1_levels["checkable_federal_govt_bank_qoq"] = _qoq_change(z1_levels["checkable_federal_govt_bank_level"])
    z1_levels["federal_govt_checkable_total_qoq"] = _qoq_change(z1_levels["federal_govt_checkable_total_level"])
    z1_levels["federal_govt_time_savings_total_qoq"] = _qoq_change(z1_levels["federal_govt_time_savings_total_level"])
    z1_levels["federal_govt_cash_balance_proxy_qoq"] = (
        z1_levels["federal_govt_checkable_total_qoq"] + z1_levels["federal_govt_time_savings_total_qoq"]
    )
    cash_frame = pd.DataFrame({"quarter": toc_qoq.index.astype(str), "tdc_treasury_operating_cash_qoq": toc_qoq.to_numpy()})
    return cash_frame.merge(
        z1_levels[
            [
                "quarter",
                "checkable_federal_govt_bank_qoq",
                "federal_govt_checkable_total_qoq",
                "federal_govt_time_savings_total_qoq",
                "federal_govt_cash_balance_proxy_qoq",
            ]
        ],
        on="quarter",
        how="outer",
    ).sort_values("quarter").reset_index(drop=True)


def _snapshot(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "rows": 0,
            "mean_abs_adjustment": None,
            "median_abs_adjustment": None,
            "max_abs_adjustment": None,
            "current_vs_candidate_corr": None,
            "candidate_minus_current_mean": None,
        }
    adjustment = frame["candidate_minus_current_tdc_qoq"].astype(float)
    current = frame["current_tdc_extended_qoq"].astype(float)
    candidate = frame["candidate_tdc_extended_qoq"].astype(float)
    corr = current.corr(candidate)
    return {
        "rows": int(frame.shape[0]),
        "mean_abs_adjustment": float(adjustment.abs().mean()),
        "median_abs_adjustment": float(adjustment.abs().median()),
        "max_abs_adjustment": float(adjustment.abs().max()),
        "current_vs_candidate_corr": None if pd.isna(corr) else float(corr),
        "candidate_minus_current_mean": float(adjustment.mean()),
    }


def _classify(mean_abs_adjustment: float | None, max_abs_adjustment: float | None) -> str:
    if mean_abs_adjustment is None or max_abs_adjustment is None:
        return "not_enough_information"
    if mean_abs_adjustment >= 10.0 or max_abs_adjustment >= 40.0:
        return "historical_backfill_changes_materially"
    if mean_abs_adjustment >= 3.0 or max_abs_adjustment >= 15.0:
        return "historical_backfill_changes_nontrivially"
    return "historical_backfill_changes_only_modestly"


def build_historical_cash_term_reestimation_summary(
    *,
    shocked: pd.DataFrame,
    canonical_tdc_source_path: Path | None,
    root: Path | None = None,
) -> dict[str, Any]:
    if not set(_REQUIRED_PANEL_COLUMNS).issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    source = _load_extended_tdc_source(canonical_tdc_source_path)
    if source is None:
        return {"status": "not_available", "reason": "missing_extended_tdc_source"}

    panel = shocked[list(_REQUIRED_PANEL_COLUMNS)].copy()
    panel = panel.dropna(subset=["quarter"]).sort_values("quarter").reset_index(drop=True)
    if root is not None:
        raw_panel = _build_raw_historical_cash_frame(root)
        if raw_panel is not None:
            panel = raw_panel
    merged = source.merge(panel, on="quarter", how="left")
    if merged.empty:
        return {"status": "not_available", "reason": "no_overlap_between_panel_and_extended_source"}

    merged["quarter_period"] = merged["quarter"].map(_quarter_sort_key)
    tx_start = pd.Period(_TX_START_QUARTER, freq="Q")
    historical_start = pd.Period(_HISTORICAL_START_QUARTER, freq="Q")
    pre_shift_end = pd.Period(_PRE_SHIFT_END_QUARTER, freq="Q")

    merged["candidate_cash_term_qoq"] = merged["tdc_treasury_operating_cash_qoq"]
    historical_mask = (merged["quarter_period"] >= historical_start) & (
        merged["quarter_period"] < tx_start
    )
    pre_shift_mask = (merged["quarter_period"] >= historical_start) & (
        merged["quarter_period"] <= pre_shift_end
    )
    merged.loc[historical_mask, "candidate_cash_term_qoq"] = merged.loc[
        historical_mask, "federal_govt_cash_balance_proxy_qoq"
    ]
    merged["candidate_minus_current_tdc_qoq"] = 0.0
    merged.loc[historical_mask, "candidate_minus_current_tdc_qoq"] = (
        merged.loc[historical_mask, "tdc_treasury_operating_cash_qoq"]
        - merged.loc[historical_mask, "candidate_cash_term_qoq"]
    )
    merged["current_tdc_extended_qoq"] = merged["tdc_bank_only_extended_1990"].astype(float)
    merged["candidate_tdc_extended_qoq"] = (
        merged["current_tdc_extended_qoq"] + merged["candidate_minus_current_tdc_qoq"]
    )

    historical_window = merged.loc[historical_mask].copy()
    pre_shift_window = merged.loc[pre_shift_mask & historical_mask].copy()

    top_adjustments = (
        historical_window.assign(abs_adjustment=historical_window["candidate_minus_current_tdc_qoq"].abs())
        .sort_values(["abs_adjustment", "quarter"], ascending=[False, True])
        .head(8)
    )
    top_adjustment_rows = [
        {
            "quarter": str(row["quarter"]),
            "current_tdc_extended_qoq": float(row["current_tdc_extended_qoq"]),
            "candidate_tdc_extended_qoq": float(row["candidate_tdc_extended_qoq"]),
            "candidate_minus_current_tdc_qoq": float(row["candidate_minus_current_tdc_qoq"]),
            "current_cash_term_qoq": float(row["tdc_treasury_operating_cash_qoq"]),
            "candidate_cash_term_qoq": float(row["candidate_cash_term_qoq"]),
        }
        for _, row in top_adjustments.iterrows()
        if not any(pd.isna(row[col]) for col in [
            "current_tdc_extended_qoq",
            "candidate_tdc_extended_qoq",
            "candidate_minus_current_tdc_qoq",
            "tdc_treasury_operating_cash_qoq",
            "candidate_cash_term_qoq",
        ])
    ]

    pre_snapshot = _snapshot(pre_shift_window)
    historical_snapshot = _snapshot(historical_window)
    classification = _classify(
        historical_snapshot.get("mean_abs_adjustment"),
        historical_snapshot.get("max_abs_adjustment"),
    )

    takeaways = [
        "This branch re-estimates only the historical backfill by swapping the pre-transaction Treasury-cash term from the current operating-cash series to an explicit TT&L-era broad federal cash-balance proxy.",
        "The transaction-era headline itself is unchanged; only the 1990-to-2002 backfill is being stress-tested.",
    ]
    pre_mean = historical_snapshot.get("mean_abs_adjustment")
    pre_max = historical_snapshot.get("max_abs_adjustment")
    if pre_mean is not None and pre_max is not None:
        takeaways.append(
            "Across the historical backfill window, the alternative TT&L-aware cash term changes TDC by "
            f"about {float(pre_mean):.2f} on average in absolute q/q terms, with a max absolute change around {float(pre_max):.2f}."
        )
    pre_corr = historical_snapshot.get("current_vs_candidate_corr")
    if pre_corr is not None:
        takeaways.append(
            f"The current versus TT&L-aware historical extensions stay correlated at about {float(pre_corr):.2f} over the backfill window."
        )

    return {
        "status": "available",
        "headline_question": "How much does the 1990-to-2002 historical TDC backfill change when the pre-transaction Treasury-cash term is replaced with an explicit TT&L-era cash-balance proxy?",
        "estimation_path": {
            "summary_artifact": "historical_cash_term_reestimation_summary.json",
            "canonical_extended_source_path": (
                None
                if canonical_tdc_source_path is None
                else f"tdcest/{canonical_tdc_source_path.name}"
            ),
            "historical_extension_source_column": "tdc_bank_only_extended_1990",
            "candidate_formula": "candidate_tdc_extended_qoq = current_tdc_extended_qoq + current_cash_term_qoq - candidate_cash_term_qoq, applied only before 2002Q4",
        },
        "definitions": {
            "current_cash_term_qoq": "Current Treasury-cash term used in the historical backfill comparison (`tdc_treasury_operating_cash_qoq`).",
            "candidate_cash_term_qoq": "TT&L-aware pre-transaction cash term = broad federal cash-balance proxy before 2002Q4; current cash term otherwise.",
            "current_tdc_extended_qoq": "Current tdcest historical extension (`tdc_bank_only_extended_1990`).",
            "candidate_tdc_extended_qoq": "Historical extension after replacing the pre-2002 Treasury-cash term with the TT&L-aware cash proxy.",
        },
        "windows": {
            "historical_backfill_window": {"start": _HISTORICAL_START_QUARTER, "end": "2002Q3"},
            "pre_shift_ttl_regime": {"start": _HISTORICAL_START_QUARTER, "end": _PRE_SHIFT_END_QUARTER},
            "transaction_headline_start": _TX_START_QUARTER,
        },
        "comparison": {
            "historical_backfill_window": historical_snapshot,
            "pre_shift_ttl_regime": pre_snapshot,
        },
        "top_adjustment_quarters": top_adjustment_rows,
        "classification": {
            "historical_adjustment_classification": classification,
        },
        "recommendation": {
            "status": "promote_regime_aware_historical_cash_term" if classification != "historical_backfill_changes_only_modestly" else "historical_cash_term_difference_is_small_but_document_it",
            "next_branch": "decide_whether_to_replace_current_historical_backfill_cash_term",
        },
        "takeaways": takeaways,
    }
