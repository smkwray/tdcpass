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
        "role": "legacy_broad_corrected_comparison",
        "tier": "legacy_sensitivity_only",
        "description": "Legacy H15/WAMEST interest-cleaned broad bank-only comparison from the tdcest Tier 2 ladder.",
    },
    "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq": {
        "role": "preferred_long_history_broad_comparison",
        "tier": "preferred_historical_candidate_with_method_tiers",
        "description": "Long-history Tier 2 regression-interest bank-only comparison with proportional MMF/RRP add-back.",
    },
    "tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq": {
        "role": "matched_perimeter_long_history_comparison",
        "tier": "historical_candidate_with_method_tiers",
        "description": "Long-history Tier 2 regression-interest DI NP-CU companion with proportional MMF/RRP add-back.",
    },
    "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq": {
        "role": "short_modern_measurement_benchmark",
        "tier": "modern_measurement_default_short_history",
        "description": "Short modern canonical DI/MMF-RRP Tier 2 measurement row; benchmark only inside long-history tdcpass.",
    },
    "tdc_tier2_di_np_cu_qoq": {
        "role": "broad_depository_scope_candidate",
        "tier": "candidate_only",
        "description": "Interest-cleaned depository-institution candidate with natural-person credit unions and a CU interest correction when available.",
    },
    "tdc_tier2_mmf_rrp_prop_bank_only_qoq": {
        "role": "broad_mmf_rrp_candidate",
        "tier": "candidate_only",
        "description": "Tier 2 bank-only comparison plus the preferred proportional MMF/RRP source-of-funds adjustment.",
    },
    "tdc_tier2_mmf_rrp_lb_bank_only_qoq": {
        "role": "broad_mmf_rrp_bound",
        "tier": "candidate_bound",
        "description": "Tier 2 bank-only comparison plus the lower-bound MMF/RRP source-of-funds adjustment.",
    },
    "tdc_tier2_mmf_rrp_ub_bank_only_qoq": {
        "role": "broad_mmf_rrp_bound",
        "tier": "candidate_bound",
        "description": "Tier 2 bank-only comparison plus the upper-bound MMF/RRP source-of-funds adjustment.",
    },
    "tdc_tier2_mmf_rrp_prop_di_np_cu_qoq": {
        "role": "broad_mmf_rrp_scope_candidate",
        "tier": "candidate_only",
        "description": "Tier 2 depository-institution NP-CU candidate plus the preferred proportional MMF/RRP source-of-funds adjustment.",
    },
    "tdc_tier2_treasury_interest_robust_bank_only_qoq": {
        "role": "bill_discount_interest_robustness",
        "tier": "robustness_only",
        "description": "Tier 2 bank-only row with estimated bank and ROW Treasury bill-discount interest subtracted.",
    },
    "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq": {
        "role": "bill_discount_interest_mmf_robustness",
        "tier": "robustness_only",
        "description": "Preferred Tier 2+MMF bank-scope row with estimated bank and ROW Treasury bill-discount interest subtracted.",
    },
    "tdc_tier2_treasury_interest_robust_di_np_cu_qoq": {
        "role": "bill_discount_interest_scope_robustness",
        "tier": "robustness_only",
        "description": "DI NP-CU row with estimated bank, ROW, and credit-union Treasury bill-discount interest subtracted.",
    },
    "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_di_np_cu_qoq": {
        "role": "bill_discount_interest_mmf_scope_robustness",
        "tier": "robustness_only",
        "description": "DI NP-CU plus preferred MMF/RRP row with estimated bill-discount interest subtracted.",
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
    required = {
        "quarter",
        "tdc_bank_only_qoq",
        "tdc_tier2_bank_only_qoq",
        "tdc_tier3_bank_only_qoq",
        "tdc_tier3_broad_depository_qoq",
        "tdc_bank_receipt_historical_overlay_qoq",
        "tdc_row_mrv_nondefault_pilot_qoq",
    }
    if not required.issubset(panel.columns):
        return {"status": "not_available", "reason": "missing_integration_columns"}
    panel = panel.copy()
    for key in _SERIES_ROLE_MAP:
        if key not in panel.columns:
            panel[key] = None

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
    long_tier2 = next(
        item for item in available_series if item["series_key"] == "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq"
    )
    modern_tier2 = next(
        item for item in available_series if item["series_key"] == "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq"
    )
    tier3 = next(item for item in available_series if item["series_key"] == "tdc_tier3_bank_only_qoq")
    hist = next(item for item in available_series if item["series_key"] == "tdc_bank_receipt_historical_overlay_qoq")
    mrv = next(item for item in available_series if item["series_key"] == "tdc_row_mrv_nondefault_pilot_qoq")

    takeaways = [
        "tdcpass already uses the canonical tdcest broad headline; this integration adds the richer corrected ladder and bounded downstream surfaces rather than replacing the strict framework.",
        "The preferred long-history broad comparison is now the regression-interest plus MMF/RRP bank-only row; legacy H15/WAMEST Tier 2 stays sensitivity only.",
        "The short modern canonical DI/MMF/RRP row is the best current-period measurement benchmark, but it is too short to replace the long-history comparison row in tdcpass.",
        "Tier 2 regression/MMF-RRP, modern canonical, legacy Tier 2, and Tier 3 are comparison rows for the broad object, not new strict deposit-component defaults.",
        "The historical bank-receipt overlay is useful historical-only context, and the MRV ROW branch remains bounded nondefault sensitivity only.",
    ]
    if None not in (tier2.get("latest_value"), tier3.get("latest_value")):
        takeaways.append(
            "Latest legacy corrected broad-bank read: "
            f"H15/WAMEST Tier 2 ≈ {float(tier2['latest_value']):.2f}, "
            f"Tier 3 ≈ {float(tier3['latest_value']):.2f}."
        )
    if long_tier2.get("latest_value") is not None:
        takeaways.append(
            "Latest preferred long-history Tier 2 regression/MMF-RRP bank-only read: "
            f"{float(long_tier2['latest_value']):.2f}."
        )
    if modern_tier2.get("latest_value") is not None:
        takeaways.append(
            "Latest short modern canonical DI/MMF-RRP benchmark read: "
            f"{float(modern_tier2['latest_value']):.2f}."
        )
    robust = next(
        (
            item
            for item in available_series
            if item["series_key"] == "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq"
        ),
        None,
    )
    if robust is not None and robust.get("latest_value") is not None:
        takeaways.append(
            "Latest bill-discount-interest robustness read for Tier 2+MMF bank-only: "
            f"{float(robust['latest_value']):.2f}."
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
                "../tdcest/data/processed/tdc_tier2_regression_series.csv",
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
            "preferred_long_history_tier2_bank_scope_candidate": "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq",
            "matched_perimeter_long_history_candidate": "tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq",
            "short_modern_measurement_benchmark": "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq",
            "legacy_tier2_mmf_bank_scope_candidate": "tdc_tier2_mmf_rrp_prop_bank_only_qoq",
            "mmf_bounds": [
                "tdc_tier2_mmf_rrp_lb_bank_only_qoq",
                "tdc_tier2_mmf_rrp_ub_bank_only_qoq",
            ],
            "depository_scope_check": "tdc_tier2_mmf_rrp_prop_di_np_cu_qoq",
            "bill_discount_interest_robustness": "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq",
            "broad_corrected_comparisons": [
                "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq",
                "tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq",
                "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq",
                "tdc_tier2_bank_only_qoq",
                "tdc_tier2_mmf_rrp_prop_bank_only_qoq",
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
