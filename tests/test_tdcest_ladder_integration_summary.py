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
            "tdc_tier3_bank_only_qoq": [-21.0, -31.0, -40.16],
            "tdc_tier3_broad_depository_qoq": [-19.0, -29.0, -39.94],
            "tdc_bank_receipt_historical_overlay_qoq": [103.07, 0.0, 0.0],
            "tdc_row_mrv_nondefault_pilot_qoq": [0.0, 0.58, 0.0],
        }
    )

    payload = build_tdcest_ladder_integration_summary(panel)

    assert payload["status"] == "available"
    assert payload["classification"]["decision"] == "selective_integration_not_wholesale_pivot"
    assert payload["recommendation"]["historical_only_overlay"] == "tdc_bank_receipt_historical_overlay_qoq"
    assert payload["recommendation"]["nondefault_row_sensitivity"] == "tdc_row_mrv_nondefault_pilot_qoq"
    hist = next(item for item in payload["series_roles"] if item["series_key"] == "tdc_bank_receipt_historical_overlay_qoq")
    mrv = next(item for item in payload["series_roles"] if item["series_key"] == "tdc_row_mrv_nondefault_pilot_qoq")
    assert hist["latest_nonzero_quarter"] == "2024Q4"
    assert mrv["latest_nonzero_quarter"] == "2025Q3"
