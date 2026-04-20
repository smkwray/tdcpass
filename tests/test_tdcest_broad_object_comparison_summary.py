from __future__ import annotations

import pandas as pd

from tdcpass.analysis.tdcest_broad_object_comparison_summary import (
    build_tdcest_broad_object_comparison_summary,
)


def test_tdcest_broad_object_comparison_summary_reports_latest_ladder_and_supplements() -> None:
    panel = pd.DataFrame(
        {
            "quarter": ["2024Q4", "2025Q1", "2025Q2"],
            "tdc_bank_only_qoq": [207.499, 543.292, 112.379],
            "tdc_tier2_bank_only_qoq": [10.0, 20.0, 30.0],
            "tdc_tier3_bank_only_qoq": [11.0, 21.0, 31.0],
            "tdc_tier3_broad_depository_qoq": [12.0, 22.0, 32.0],
            "tdc_bank_receipt_historical_overlay_qoq": [103.07105627851948, pd.NA, pd.NA],
            "tdc_row_mrv_nondefault_pilot_qoq": [0.6210987081222047, 0.6680884373253256, 0.6198451468805906],
        }
    )

    payload = build_tdcest_broad_object_comparison_summary(
        panel,
        tdcest_ladder_integration_summary={"status": "available"},
    )

    assert payload["status"] == "available"
    latest = payload["latest_common_broad_comparison"]
    assert latest["quarter"] == "2025Q2"
    assert latest["headline_bank_only_beta"] == 112.379
    assert latest["tier2_minus_headline_beta"] == 30.0 - 112.379
    assert (
        payload["supplemental_surfaces"]["historical_bank_receipt_overlay"]["latest_nonzero_quarter"]
        == "2024Q4"
    )
    assert payload["supplemental_surfaces"]["row_mrv_nondefault_pilot"]["latest_nonzero_quarter"] == "2025Q2"
    assert (
        payload["classification"]["headline_object_status"]
        == "canonical_bank_only_headline_retained"
    )
    assert (
        payload["recommendation"]["status"]
        == "use_as_broad_object_comparison_layer_only"
    )
