from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

CANONICAL_TDC_COL = "tdc_bank_only_qoq"
CANONICAL_TOTAL_COL = "total_deposits_bank_qoq"
CANONICAL_OTHER_COL = "other_component_qoq"
CANONICAL_QUARTER_COL = "quarter"

_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    CANONICAL_TDC_COL: ("tdc_qoq",),
    CANONICAL_TOTAL_COL: ("total_deposits_qoq",),
}


@dataclass
class AccountingSummary:
    mean_tdc: float
    mean_total_deposits: float
    mean_other_component: float
    share_other_negative: float
    correlation_tdc_total: float
    correlation_tdc_other: float


def _resolve_column(df: pd.DataFrame, preferred: str) -> str:
    if preferred in df.columns:
        return preferred
    for alias in _COLUMN_ALIASES.get(preferred, ()):
        if alias in df.columns:
            return alias
    raise KeyError(f"Missing required column: {preferred}")


def compute_other_component(
    df: pd.DataFrame,
    *,
    total_col: str = CANONICAL_TOTAL_COL,
    tdc_col: str = CANONICAL_TDC_COL,
    out_col: str = CANONICAL_OTHER_COL,
) -> pd.DataFrame:
    resolved_total_col = _resolve_column(df, total_col)
    resolved_tdc_col = _resolve_column(df, tdc_col)
    out = df.copy()
    out[out_col] = out[resolved_total_col] - out[resolved_tdc_col]
    return out


def build_accounting_summary(
    df: pd.DataFrame,
    *,
    total_col: str = CANONICAL_TOTAL_COL,
    tdc_col: str = CANONICAL_TDC_COL,
    other_col: str = CANONICAL_OTHER_COL,
) -> AccountingSummary:
    resolved_total_col = _resolve_column(df, total_col)
    resolved_tdc_col = _resolve_column(df, tdc_col)
    resolved_other_col = _resolve_column(df, other_col)

    sample = df[[resolved_total_col, resolved_tdc_col, resolved_other_col]].dropna()
    if sample.empty:
        raise ValueError("No non-missing rows available for accounting summary.")
    share_other_negative = float((sample[resolved_other_col] < 0).mean())
    corr_total = float(sample[resolved_tdc_col].corr(sample[resolved_total_col]))
    corr_other = float(sample[resolved_tdc_col].corr(sample[resolved_other_col]))
    return AccountingSummary(
        mean_tdc=float(sample[resolved_tdc_col].mean()),
        mean_total_deposits=float(sample[resolved_total_col].mean()),
        mean_other_component=float(sample[resolved_other_col].mean()),
        share_other_negative=share_other_negative,
        correlation_tdc_total=corr_total,
        correlation_tdc_other=corr_other,
    )


def summary_to_frame(summary: AccountingSummary) -> pd.DataFrame:
    rows = [
        ("mean_tdc", summary.mean_tdc, "Average quarterly TDC change."),
        ("mean_total_deposits", summary.mean_total_deposits, "Average quarterly total-deposit change."),
        ("mean_other_component", summary.mean_other_component, "Average quarterly non-TDC component."),
        ("share_other_negative", summary.share_other_negative, "Share of quarters with negative non-TDC component."),
        ("corr_tdc_total", summary.correlation_tdc_total, "Correlation between TDC and total deposits."),
        ("corr_tdc_other", summary.correlation_tdc_other, "Correlation between TDC and non-TDC component."),
    ]
    return pd.DataFrame(rows, columns=["metric", "value", "notes"])


def build_quarters_tdc_exceeds_total(
    df: pd.DataFrame,
    *,
    quarter_col: str = CANONICAL_QUARTER_COL,
    total_col: str = CANONICAL_TOTAL_COL,
    tdc_col: str = CANONICAL_TDC_COL,
    other_col: str = CANONICAL_OTHER_COL,
) -> pd.DataFrame:
    resolved_tdc_col = _resolve_column(df, tdc_col)
    resolved_total_col = _resolve_column(df, total_col)
    resolved_other_col = _resolve_column(df, other_col)
    if quarter_col not in df.columns:
        raise KeyError(f"Missing required column: {quarter_col}")

    out = df.loc[
        df[resolved_tdc_col] > df[resolved_total_col],
        [quarter_col, resolved_tdc_col, resolved_total_col, resolved_other_col],
    ].copy()
    out = out.rename(
        columns={
            resolved_tdc_col: CANONICAL_TDC_COL,
            resolved_total_col: CANONICAL_TOTAL_COL,
            resolved_other_col: CANONICAL_OTHER_COL,
        }
    )
    return out.reset_index(drop=True)
