from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

NORMALIZED_COLUMNS: tuple[str, ...] = (
    "quarter",
    "account_type",
    "depositor_class",
    "amount_bil_usd",
    "institution_count",
    "source_quarter",
    "source_kind",
    "universe_basis",
)

_NORMALIZED_ALIASES = {
    "quarter": ("quarter", "report_quarter", "quarter_end"),
    "account_type": ("account_type", "account_category"),
    "depositor_class": ("depositor_class", "holder_class", "counterparty_class"),
    "amount_bil_usd": ("amount_bil_usd", "amount_billions_usd"),
    "amount_usd": ("amount_usd", "amount", "value_usd"),
    "institution_count": ("institution_count", "bank_count"),
    "source_quarter": ("source_quarter", "raw_quarter"),
    "source_kind": ("source_kind",),
    "universe_basis": ("universe_basis",),
}
_QUARTER_PATTERN = re.compile(r"^\d{4}Q[1-4]$")


def _relative_locator(path: Path, *, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=NORMALIZED_COLUMNS)


def _empty_summary(*, root: Path, source_path: Path | None) -> dict[str, Any]:
    return {
        "status": "not_available",
        "source_locator": None if source_path is None else _relative_locator(source_path, root=root),
        "row_count": 0,
        "quarter_start": None,
        "quarter_end": None,
        "account_types": [],
        "depositor_classes": [],
        "qa": {
            "quarter_format_valid": True,
            "duplicate_key_rows": 0,
            "negative_amount_rows": 0,
            "missing_amount_rows": 0,
            "universe_consistent": None,
            "quarterly_aggregation_confirmed": None,
        },
        "takeaways": [
            "No Call Report deposit-component source file was available, so the normalized artifact remains empty and non-blocking."
        ],
    }


def _resolve_source_path(*, root: Path, fixture_root: Path | None) -> tuple[Path | None, str]:
    if fixture_root is not None:
        fixture_path = fixture_root / "call_reports" / "call_report_deposit_components.csv"
        if fixture_path.exists():
            return fixture_path, "fixture_call_report_components_csv"
    raw_path = root / "data" / "raw" / "call_reports" / "call_report_deposit_components.csv"
    if raw_path.exists():
        return raw_path, "raw_call_report_components_csv"
    return None, "not_available"


def _first_present(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    for alias in aliases:
        if alias in frame.columns:
            return alias
    return None


def _normalize_source(frame: pd.DataFrame, *, source_kind: str) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame()
    out = pd.DataFrame()
    quarter_col = _first_present(frame, _NORMALIZED_ALIASES["quarter"])
    account_col = _first_present(frame, _NORMALIZED_ALIASES["account_type"])
    depositor_col = _first_present(frame, _NORMALIZED_ALIASES["depositor_class"])
    amount_bil_col = _first_present(frame, _NORMALIZED_ALIASES["amount_bil_usd"])
    amount_usd_col = _first_present(frame, _NORMALIZED_ALIASES["amount_usd"])
    if quarter_col is None or account_col is None or depositor_col is None or (amount_bil_col is None and amount_usd_col is None):
        return _empty_frame()

    out["quarter"] = frame[quarter_col].astype(str).str.strip()
    out["account_type"] = frame[account_col].astype(str).str.strip()
    out["depositor_class"] = frame[depositor_col].astype(str).str.strip()
    amount_bil_series = (
        pd.to_numeric(frame[amount_bil_col], errors="coerce")
        if amount_bil_col is not None
        else pd.Series(pd.NA, index=frame.index, dtype="Float64")
    )
    amount_usd_series = (
        pd.to_numeric(frame[amount_usd_col], errors="coerce") / 1_000_000_000.0
        if amount_usd_col is not None
        else pd.Series(pd.NA, index=frame.index, dtype="Float64")
    )
    out["amount_bil_usd"] = amount_bil_series.fillna(amount_usd_series)
    institution_col = _first_present(frame, _NORMALIZED_ALIASES["institution_count"])
    out["institution_count"] = (
        pd.to_numeric(frame[institution_col], errors="coerce") if institution_col is not None else pd.Series(pd.NA, index=frame.index)
    )
    source_quarter_col = _first_present(frame, _NORMALIZED_ALIASES["source_quarter"])
    out["source_quarter"] = (
        frame[source_quarter_col].astype(str).str.strip() if source_quarter_col is not None else out["quarter"]
    )
    source_kind_col = _first_present(frame, _NORMALIZED_ALIASES["source_kind"])
    out["source_kind"] = (
        frame[source_kind_col].astype(str).str.strip() if source_kind_col is not None else source_kind
    )
    universe_col = _first_present(frame, _NORMALIZED_ALIASES["universe_basis"])
    out["universe_basis"] = (
        frame[universe_col].astype(str).str.strip()
        if universe_col is not None
        else "insured_institutions_aggregate"
    )
    out = out.groupby(["quarter", "account_type", "depositor_class"], as_index=False, dropna=False).agg(
        {
            "amount_bil_usd": "sum",
            "institution_count": "max",
            "source_quarter": "first",
            "source_kind": "first",
            "universe_basis": "first",
        }
    )
    return out[list(NORMALIZED_COLUMNS)]


def _build_summary(frame: pd.DataFrame, *, root: Path, source_path: Path, source_kind: str) -> dict[str, Any]:
    if frame.empty:
        return _empty_summary(root=root, source_path=source_path)
    duplicate_key_rows = int(frame.duplicated(subset=["quarter", "account_type", "depositor_class"]).sum())
    negative_amount_rows = int(frame["amount_bil_usd"].lt(0).sum())
    missing_amount_rows = int(frame["amount_bil_usd"].isna().sum())
    quarter_format_valid = bool(frame["quarter"].astype(str).map(lambda value: bool(_QUARTER_PATTERN.match(value))).all())
    universe_basis_values = sorted({str(value) for value in frame["universe_basis"].dropna().tolist() if str(value)})
    universe_consistent = len(universe_basis_values) <= 1
    takeaways = [
        "Call Report deposit components are normalized as a separate aggregate artifact so they can mature without blocking the Z.1-first liability split."
    ]
    if not universe_consistent:
        takeaways.append("Universe basis varies across rows; do not promote these components into the headline panel yet.")
    if negative_amount_rows > 0:
        takeaways.append("Some normalized Call Report component rows are negative; review the raw aggregation before headline use.")
    return {
        "status": "available",
        "source_locator": _relative_locator(source_path, root=root),
        "source_kind": source_kind,
        "row_count": int(len(frame)),
        "quarter_start": str(frame["quarter"].iloc[0]),
        "quarter_end": str(frame["quarter"].iloc[-1]),
        "account_types": sorted(frame["account_type"].dropna().astype(str).unique().tolist()),
        "depositor_classes": sorted(frame["depositor_class"].dropna().astype(str).unique().tolist()),
        "qa": {
            "quarter_format_valid": quarter_format_valid,
            "duplicate_key_rows": duplicate_key_rows,
            "negative_amount_rows": negative_amount_rows,
            "missing_amount_rows": missing_amount_rows,
            "universe_consistent": universe_consistent,
            "quarterly_aggregation_confirmed": True,
        },
        "takeaways": takeaways,
    }


def build_call_report_deposit_components(
    *,
    root: Path,
    fixture_root: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_path, source_kind = _resolve_source_path(root=root, fixture_root=fixture_root)
    if source_path is None:
        return _empty_frame(), _empty_summary(root=root, source_path=None)
    frame = pd.read_csv(source_path)
    normalized = _normalize_source(frame, source_kind=source_kind)
    if normalized.empty:
        summary = _empty_summary(root=root, source_path=source_path)
        summary["status"] = "invalid_source_layout"
        summary["takeaways"] = [
            "A Call Report source file was found, but it did not match the normalized aggregate layout expected by the current builder."
        ]
        return normalized, summary
    normalized = normalized.sort_values(["quarter", "account_type", "depositor_class"]).reset_index(drop=True)
    return normalized, _build_summary(normalized, root=root, source_path=source_path, source_kind=source_kind)
