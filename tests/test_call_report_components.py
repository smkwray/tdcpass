from __future__ import annotations

from pathlib import Path

import pandas as pd

from tdcpass.pipeline.call_report_components import build_call_report_deposit_components


def test_call_report_components_returns_empty_non_blocking_artifact_when_missing(tmp_path: Path) -> None:
    frame, summary = build_call_report_deposit_components(root=tmp_path)

    assert frame.empty
    assert summary["status"] == "not_available"
    assert summary["row_count"] == 0


def test_call_report_components_normalizes_fixture_layout(tmp_path: Path) -> None:
    fixture_path = tmp_path / "fixture" / "call_reports" / "call_report_deposit_components.csv"
    fixture_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "quarter": "2024Q1",
                "account_type": "transaction",
                "depositor_class": "individuals_partnerships_corporations",
                "amount_usd": 2_000_000_000.0,
                "institution_count": 10,
                "universe_basis": "insured_institutions_aggregate",
            },
            {
                "quarter": "2024Q1",
                "account_type": "transaction",
                "depositor_class": "individuals_partnerships_corporations",
                "amount_usd": 1_000_000_000.0,
                "institution_count": 12,
                "universe_basis": "insured_institutions_aggregate",
            },
            {
                "quarter": "2024Q1",
                "account_type": "nontransaction",
                "depositor_class": "us_government",
                "amount_bil_usd": 4.5,
                "institution_count": 12,
                "universe_basis": "insured_institutions_aggregate",
            },
        ]
    ).to_csv(fixture_path, index=False)

    frame, summary = build_call_report_deposit_components(root=tmp_path, fixture_root=tmp_path / "fixture")

    assert frame["amount_bil_usd"].tolist() == [4.5, 3.0]
    assert summary["status"] == "available"
    assert summary["qa"]["universe_consistent"] is True
    assert summary["qa"]["quarterly_aggregation_confirmed"] is True
    assert summary["source_kind"] == "fixture_call_report_components_csv"
