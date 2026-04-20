from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


_BROAD_COMPARISON_SERIES = (
    "tdc_bank_only_qoq",
    "tdc_tier2_bank_only_qoq",
    "tdc_tier3_bank_only_qoq",
    "tdc_tier3_broad_depository_qoq",
)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_tdcest_broad_object_comparison_summary(
    panel: pd.DataFrame | None,
    *,
    tdcest_ladder_integration_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if panel is None or panel.empty:
        return {"status": "not_available", "reason": "missing_panel"}
    required = {"quarter", *_BROAD_COMPARISON_SERIES, "tdc_bank_receipt_historical_overlay_qoq", "tdc_row_mrv_nondefault_pilot_qoq"}
    if not required.issubset(panel.columns):
        return {"status": "not_available", "reason": "missing_comparison_columns"}
    if (
        tdcest_ladder_integration_summary is None
        or str(tdcest_ladder_integration_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "missing_ladder_summary"}

    working = panel[["quarter", *required.difference({"quarter"})]].copy()
    for column in working.columns:
        if column != "quarter":
            working[column] = pd.to_numeric(working[column], errors="coerce")

    broad_available = working.dropna(subset=list(_BROAD_COMPARISON_SERIES), how="any").copy()
    latest_common_quarter = None if broad_available.empty else str(broad_available["quarter"].iloc[-1])
    latest_common_row = broad_available.iloc[-1] if not broad_available.empty else None

    latest_common_comparison = {}
    if latest_common_row is not None:
        headline_value = _safe_float(latest_common_row["tdc_bank_only_qoq"])
        latest_common_comparison = {
            "quarter": latest_common_quarter,
            "headline_bank_only_beta": headline_value,
            "tier2_bank_only_beta": _safe_float(latest_common_row["tdc_tier2_bank_only_qoq"]),
            "tier3_bank_only_beta": _safe_float(latest_common_row["tdc_tier3_bank_only_qoq"]),
            "tier3_broad_depository_beta": _safe_float(latest_common_row["tdc_tier3_broad_depository_qoq"]),
            "tier2_minus_headline_beta": None
            if headline_value is None
            else _safe_float(latest_common_row["tdc_tier2_bank_only_qoq"]) - headline_value,
            "tier3_minus_headline_beta": None
            if headline_value is None
            else _safe_float(latest_common_row["tdc_tier3_bank_only_qoq"]) - headline_value,
            "tier3_broad_minus_headline_beta": None
            if headline_value is None
            else _safe_float(latest_common_row["tdc_tier3_broad_depository_qoq"]) - headline_value,
        }

    overlay = working[["quarter", "tdc_bank_receipt_historical_overlay_qoq"]].dropna()
    overlay_payload = {
        "available": not overlay.empty,
        "latest_quarter": None if overlay.empty else str(overlay["quarter"].iloc[-1]),
        "latest_value": None if overlay.empty else _safe_float(overlay["tdc_bank_receipt_historical_overlay_qoq"].iloc[-1]),
        "latest_nonzero_quarter": None,
        "latest_nonzero_value": None,
    }
    overlay_nonzero = overlay.loc[overlay["tdc_bank_receipt_historical_overlay_qoq"].ne(0)]
    if not overlay_nonzero.empty:
        overlay_payload["latest_nonzero_quarter"] = str(overlay_nonzero["quarter"].iloc[-1])
        overlay_payload["latest_nonzero_value"] = _safe_float(
            overlay_nonzero["tdc_bank_receipt_historical_overlay_qoq"].iloc[-1]
        )

    mrv = working[["quarter", "tdc_row_mrv_nondefault_pilot_qoq"]].dropna()
    mrv_payload = {
        "available": not mrv.empty,
        "latest_quarter": None if mrv.empty else str(mrv["quarter"].iloc[-1]),
        "latest_value": None if mrv.empty else _safe_float(mrv["tdc_row_mrv_nondefault_pilot_qoq"].iloc[-1]),
        "latest_nonzero_quarter": None,
        "latest_nonzero_value": None,
    }
    mrv_nonzero = mrv.loc[mrv["tdc_row_mrv_nondefault_pilot_qoq"].ne(0)]
    if not mrv_nonzero.empty:
        mrv_payload["latest_nonzero_quarter"] = str(mrv_nonzero["quarter"].iloc[-1])
        mrv_payload["latest_nonzero_value"] = _safe_float(mrv_nonzero["tdc_row_mrv_nondefault_pilot_qoq"].iloc[-1])

    takeaways = [
        "The sibling tdcest ladder is now fully visible inside tdcpass as broad-object comparison rows rather than a replacement strict framework.",
        "Tier 2 and Tier 3 stay in the broad-object comparison tier; they are not promoted into the strict deposit-component object.",
        "The historical bank-receipt overlay and the MRV ROW pilot are now live in the quarterly panel, but they remain fenced to historical-only and nondefault-sensitivity roles.",
    ]
    if latest_common_comparison:
        takeaways.append(
            "Latest common broad-object quarter "
            f"{latest_common_comparison['quarter']}: headline ≈ {latest_common_comparison['headline_bank_only_beta']:.2f}, "
            f"Tier 2 ≈ {latest_common_comparison['tier2_bank_only_beta']:.2f}, "
            f"Tier 3 ≈ {latest_common_comparison['tier3_bank_only_beta']:.2f}, "
            f"Tier 3 broad-depository ≈ {latest_common_comparison['tier3_broad_depository_beta']:.2f}."
        )
    if overlay_payload["latest_nonzero_quarter"] is not None:
        takeaways.append(
            "Historical overlay remains active only in a narrow historical window: "
            f"latest nonzero quarter = {overlay_payload['latest_nonzero_quarter']}."
        )
    if mrv_payload["latest_nonzero_quarter"] is not None:
        takeaways.append(
            "The bounded ROW MRV pilot is live but explicitly nondefault: "
            f"latest nonzero quarter = {mrv_payload['latest_nonzero_quarter']}."
        )

    return {
        "status": "available",
        "headline_question": "How do the newly integrated tdcest corrected-ladder rows compare to the canonical broad headline inside tdcpass?",
        "estimation_path": {
            "summary_artifact": "tdcest_broad_object_comparison_summary.json",
            "source_artifacts": [
                "quarterly_panel.csv",
                "tdcest_ladder_integration_summary.json",
            ],
        },
        "latest_common_broad_comparison": latest_common_comparison,
        "supplemental_surfaces": {
            "historical_bank_receipt_overlay": overlay_payload,
            "row_mrv_nondefault_pilot": mrv_payload,
        },
        "classification": {
            "role": "broad_object_comparison_only",
            "strict_framework_effect": "unchanged",
            "headline_object_status": "canonical_bank_only_headline_retained",
        },
        "recommendation": {
            "status": "use_as_broad_object_comparison_layer_only",
            "standard_broad_comparison_rows": [
                "tdc_tier2_bank_only_qoq",
                "tdc_tier3_bank_only_qoq",
                "tdc_tier3_broad_depository_qoq",
            ],
            "supplemental_only": [
                "tdc_bank_receipt_historical_overlay_qoq",
                "tdc_row_mrv_nondefault_pilot_qoq",
            ],
        },
        "takeaways": takeaways,
    }
