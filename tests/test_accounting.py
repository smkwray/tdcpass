import pandas as pd

from tdcpass.analysis.accounting import (
    build_accounting_summary,
    build_quarters_tdc_exceeds_total,
    compute_other_component,
)


def test_other_component_identity():
    df = pd.DataFrame(
        {
            "tdc_bank_only_qoq": [1.0, 2.0, 3.0],
            "total_deposits_bank_qoq": [1.5, 1.0, 5.0],
        }
    )
    out = compute_other_component(df)
    assert list(out["other_component_qoq"]) == [0.5, -1.0, 2.0]


def test_accounting_summary_runs():
    df = pd.DataFrame(
        {
            "tdc_bank_only_qoq": [1.0, 2.0, 3.0, 4.0],
            "total_deposits_bank_qoq": [2.0, 1.0, 2.5, 5.0],
            "other_component_qoq": [1.0, -1.0, -0.5, 1.0],
        }
    )
    summary = build_accounting_summary(df)
    assert 0 <= summary.share_other_negative <= 1


def test_accounting_quarters_tdc_exceeds_total_contract_columns():
    df = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2", "2020Q3"],
            "tdc_bank_only_qoq": [2.0, 1.0, 5.0],
            "total_deposits_bank_qoq": [1.0, 1.5, 4.0],
            "other_component_qoq": [-1.0, 0.5, -1.0],
        }
    )
    out = build_quarters_tdc_exceeds_total(df)
    assert list(out.columns) == [
        "quarter",
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "other_component_qoq",
    ]
    assert out["quarter"].tolist() == ["2020Q1", "2020Q3"]


def test_accounting_supports_legacy_column_aliases():
    df = pd.DataFrame(
        {
            "quarter": ["2020Q1", "2020Q2"],
            "tdc_qoq": [3.0, 1.0],
            "total_deposits_qoq": [2.0, 2.0],
        }
    )
    out = compute_other_component(df)
    quarters = build_quarters_tdc_exceeds_total(out)
    assert out["other_component_qoq"].tolist() == [-1.0, 1.0]
    assert quarters["quarter"].tolist() == ["2020Q1"]
