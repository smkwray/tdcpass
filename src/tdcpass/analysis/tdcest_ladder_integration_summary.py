from __future__ import annotations

from typing import Any

import pandas as pd


_SERIES_ROLE_MAP: dict[str, dict[str, str]] = {
    "tdc_bank_only_qoq": {
        "role": "broad_headline_anchor",
        "tier": "default_headline",
        "description": "Canonical broad bank-only TDC headline imported from tdcest.",
    },
    "tdc_tier2_bank_only_qoq": {
        "role": "broad_corrected_comparison",
        "tier": "comparison_only",
        "description": "Interest-cleaned broad bank-only comparison from the tdcest Tier 2 ladder.",
    },
    "tdc_tier3_bank_only_qoq": {
        "role": "broad_corrected_comparison",
        "tier": "comparison_only_partial_receipt_cells",
        "description": "Fiscal-corrected broad bank-only comparison from the tdcest Tier 3 ladder.",
    },
    "tdc_tier3_broad_depository_qoq": {
        "role": "broad_perimeter_comparison",
        "tier": "comparison_only_partial_receipt_cells",
        "description": "Fiscal-corrected broad-depository comparison from the tdcest Tier 3 ladder.",
    },
    "tdc_bank_receipt_historical_overlay_qoq": {
        "role": "historical_only_overlay",
        "tier": "historical_only",
        "description": "Historical bank-receipt overlay candidate from tdcest's age-eligible window.",
    },
    "tdc_row_mrv_nondefault_pilot_qoq": {
        "role": "nondefault_row_sensitivity",
        "tier": "bounded_nondefault_only",
        "description": "Bounded MRV/CBSP ROW receipt pilot imported as a nondefault sensitivity only.",
    },
}


def _latest_snapshot(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    working = frame[["quarter", column]].copy()
    working[column] = pd.to_numeric(working[column], errors="coerce")
    working = working.loc[working[column].notna()]
    if working.empty:
        return {
            "latest_quarter": None,
            "latest_value": None,
            "latest_nonzero_quarter": None,
            "latest_nonzero_value": None,
        }
    row = working.iloc[-1]
    nonzero = working.loc[working[column].ne(0)]
    if nonzero.empty:
        latest_nonzero_quarter = None
        latest_nonzero_value = None
    else:
        latest_nonzero_row = nonzero.iloc[-1]
        latest_nonzero_quarter = str(latest_nonzero_row["quarter"])
        latest_nonzero_value = float(latest_nonzero_row[column])
    return {
        "latest_quarter": str(row["quarter"]),
        "latest_value": float(row[column]),
        "latest_nonzero_quarter": latest_nonzero_quarter,
        "latest_nonzero_value": latest_nonzero_value,
    }


def build_tdcest_ladder_integration_summary(
    panel: pd.DataFrame | None,
) -> dict[str, Any]:
    if panel is None or panel.empty:
        return {"status": "not_available", "reason": "missing_panel"}
    required = {"quarter", *list(_SERIES_ROLE_MAP)}
    if not required.issubset(panel.columns):
        return {"status": "not_available", "reason": "missing_integration_columns"}

    available_series = []
    for key, meta in _SERIES_ROLE_MAP.items():
        snapshot = _latest_snapshot(panel, key)
        available_series.append(
            {
                "series_key": key,
                "role": meta["role"],
                "tier": meta["tier"],
                "description": meta["description"],
                **snapshot,
            }
        )

    tier2 = next(item for item in available_series if item["series_key"] == "tdc_tier2_bank_only_qoq")
    tier3 = next(item for item in available_series if item["series_key"] == "tdc_tier3_bank_only_qoq")
    hist = next(item for item in available_series if item["series_key"] == "tdc_bank_receipt_historical_overlay_qoq")
    mrv = next(item for item in available_series if item["series_key"] == "tdc_row_mrv_nondefault_pilot_qoq")

    takeaways = [
        "tdcpass already uses the canonical tdcest broad headline; this integration adds the richer corrected ladder and bounded downstream surfaces rather than replacing the strict framework.",
        "Tier 2 and Tier 3 are comparison rows for the broad object, not new strict deposit-component defaults.",
        "The historical bank-receipt overlay is useful historical-only context, and the MRV ROW branch remains bounded nondefault sensitivity only.",
    ]
    if None not in (tier2.get("latest_value"), tier3.get("latest_value")):
        takeaways.append(
            "Latest corrected broad-bank read: "
            f"Tier 2 ≈ {float(tier2['latest_value']):.2f}, "
            f"Tier 3 ≈ {float(tier3['latest_value']):.2f}."
        )
    if hist.get("latest_quarter") is not None:
        takeaways.append(
            "Historical overlay is present and should stay fenced to the historical-age window: "
            f"latest nonzero quarter = {str(hist.get('latest_nonzero_quarter'))}."
        )
    if mrv.get("latest_nonzero_quarter") is not None:
        takeaways.append(
            "The ROW MRV pilot is now directly importable, but it remains explicitly nondefault: "
            f"latest nonzero quarter = {str(mrv.get('latest_nonzero_quarter'))}."
        )

    return {
        "status": "available",
        "headline_question": "How should tdcpass use the newer tdcest corrected ladder and receipt-side proxy surfaces?",
        "estimation_path": {
            "summary_artifact": "tdcest_ladder_integration_summary.json",
            "source_artifacts": [
                "quarterly_panel.csv",
                "../tdcest/data/processed/tdc_estimates.csv",
                "../tdcest/data/processed/tdc_downstream_deposit_effect_series_panel.csv",
            ],
        },
        "classification": {
            "decision": "selective_integration_not_wholesale_pivot",
            "strict_framework_effect": "unchanged",
            "broad_object_effect": "richer_comparison_ladder_available",
        },
        "series_roles": available_series,
        "recommendation": {
            "status": "import_selected_tdcest_ladder_rows_only",
            "broad_corrected_comparisons": [
                "tdc_tier2_bank_only_qoq",
                "tdc_tier3_bank_only_qoq",
                "tdc_tier3_broad_depository_qoq",
            ],
            "historical_only_overlay": "tdc_bank_receipt_historical_overlay_qoq",
            "nondefault_row_sensitivity": "tdc_row_mrv_nondefault_pilot_qoq",
            "do_not_promote": [
                "tdc_tier3_bank_only_qoq_as_strict_object",
                "tdc_row_mrv_nondefault_pilot_qoq_as_default_tdc_leg",
                "tdc_bank_receipt_historical_overlay_qoq_outside_historical_window",
            ],
        },
        "takeaways": takeaways,
    }
