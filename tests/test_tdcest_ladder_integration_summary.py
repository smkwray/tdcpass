from __future__ import annotations

import pandas as pd

from tdcpass.analysis.tdcest_ladder_integration_summary import (
    build_tdcest_ladder_integration_summary,
)


def test_tdcest_ladder_integration_summary_classifies_selective_import_roles() -> None:
    panel = pd.DataFrame(
        {
            "quarter": ["2024Q4", "2025Q3", "2025Q4"],
            "tdc_bank_only_qoq": [10.0, 12.0, 13.0],
            "tdc_tier2_bank_only_qoq": [-20.0, -30.0, -38.09],
            "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq": [-18.0, -28.0, -33.0],
            "tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq": [-18.5, -28.5, -33.5],
            "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq": [-17.0, -27.0, -32.0],
            "tdc_tier2_mmf_rrp_prop_bank_only_qoq": [-10.0, -12.0, -19.1],
            "tdc_tier2_mmf_rrp_lb_bank_only_qoq": [-11.0, -13.0, -20.0],
            "tdc_tier2_mmf_rrp_ub_bank_only_qoq": [-9.0, -11.0, -18.0],
            "tdc_tier2_mmf_rrp_prop_di_np_cu_qoq": [-10.5, -12.5, -19.6],
            "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq": [-14.0, -16.0, -25.0],
            "tdc_tier3_bank_only_qoq": [-21.0, -31.0, -40.16],
            "tdc_tier3_broad_depository_qoq": [-19.0, -29.0, -39.94],
            "tdc_bank_receipt_historical_overlay_qoq": [103.07, 0.0, 0.0],
            "tdc_row_mrv_nondefault_pilot_qoq": [0.0, 0.58, 0.0],
        }
    )

    payload = build_tdcest_ladder_integration_summary(panel)

    assert payload["status"] == "available"
    assert payload["classification"]["decision"] == "selective_integration_not_wholesale_pivot"
    assert (
        payload["recommendation"]["preferred_long_history_tier2_bank_scope_candidate"]
        == "tdc_tier2_regression_mmf_rrp_prop_bank_only_qoq"
    )
    assert (
        payload["recommendation"]["matched_perimeter_long_history_candidate"]
        == "tdc_tier2_regression_mmf_rrp_prop_di_np_cu_qoq"
    )
    assert (
        payload["recommendation"]["short_modern_measurement_benchmark"]
        == "tdc_tier2_modern_canonical_di_mmf_rrp_prop_qoq"
    )
    assert (
        payload["recommendation"]["bill_discount_interest_robustness"]
        == "tdc_tier2_treasury_interest_robust_mmf_rrp_prop_bank_only_qoq"
    )
    assert payload["recommendation"]["historical_only_overlay"] == "tdc_bank_receipt_historical_overlay_qoq"
    assert payload["recommendation"]["nondefault_row_sensitivity"] == "tdc_row_mrv_nondefault_pilot_qoq"
    hist = next(item for item in payload["series_roles"] if item["series_key"] == "tdc_bank_receipt_historical_overlay_qoq")
    mrv = next(item for item in payload["series_roles"] if item["series_key"] == "tdc_row_mrv_nondefault_pilot_qoq")
    assert hist["latest_nonzero_quarter"] == "2024Q4"
    assert mrv["latest_nonzero_quarter"] == "2025Q3"
