from __future__ import annotations

from typing import Any

import pandas as pd


def _period_bucket(quarter: Any) -> str:
    text = str(quarter)
    try:
        year = int(text[:4])
    except (TypeError, ValueError):
        return "unknown"
    if year <= 2008:
        return "pre_gfc"
    if year <= 2013:
        return "post_gfc_early"
    if year <= 2019:
        return "pre_covid"
    return "covid_post"


def _share(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _sign_label(value: float) -> str:
    if value > 0.0:
        return "positive"
    if value < 0.0:
        return "negative"
    return "zero"


def _dominant_leg(row_abs_share: float, toc_abs_share: float) -> str:
    if row_abs_share >= 0.65:
        return "row_dominant"
    if toc_abs_share >= 0.65:
        return "toc_dominant"
    return "mixed"


def _contribution_pattern(row_leg: float, toc_leg: float) -> str:
    if row_leg == 0.0 or toc_leg == 0.0:
        return "single_leg"
    if row_leg * toc_leg > 0.0:
        return "reinforcing"
    return "offsetting"


def _top_gap_quarter_rows(frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sample = frame.sort_values("abs_shock_gap", ascending=False).head(limit).copy()
    for _, row in sample.iterrows():
        row_leg = float(row["row_leg_qoq"])
        toc_leg = float(row["toc_signed_contribution_qoq"])
        leg_abs_total = abs(row_leg) + abs(toc_leg)
        row_leg_abs_share = _share(abs(row_leg), leg_abs_total)
        toc_leg_abs_share = _share(abs(toc_leg), leg_abs_total)
        rows.append(
            {
                "quarter": str(row["quarter"]),
                "period_bucket": str(row["period_bucket"]),
                "baseline_shock": float(row["tdc_residual_z"]),
                "toc_row_excluded_shock": float(row["tdc_no_toc_no_row_bank_only_residual_z"]),
                "shock_gap": float(row["shock_gap"]),
                "abs_shock_gap": float(row["abs_shock_gap"]),
                "bundle_qoq": float(row["bundle_qoq"]),
                "bundle_sign": _sign_label(float(row["bundle_qoq"])),
                "row_leg_qoq": row_leg,
                "toc_signed_contribution_qoq": toc_leg,
                "row_leg_abs_share": row_leg_abs_share,
                "toc_leg_abs_share": toc_leg_abs_share,
                "row_leg_alignment_to_bundle": _sign_label(row_leg * float(row["bundle_qoq"])),
                "toc_leg_alignment_to_bundle": _sign_label(toc_leg * float(row["bundle_qoq"])),
                "dominant_leg": _dominant_leg(row_leg_abs_share or 0.0, toc_leg_abs_share or 0.0),
                "contribution_pattern": _contribution_pattern(row_leg, toc_leg),
            }
        )
    return rows


def _weighted_distribution(rows: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    total_weight = sum(abs(float(row["shock_gap"])) for row in rows)
    by_key: dict[str, float] = {}
    for row in rows:
        bucket = str(row[key])
        by_key[bucket] = by_key.get(bucket, 0.0) + abs(float(row["shock_gap"]))
    output = [
        {
            key: bucket,
            "abs_gap_weight": weight,
            "abs_gap_share": _share(weight, total_weight),
        }
        for bucket, weight in by_key.items()
    ]
    output.sort(key=lambda item: float(item["abs_gap_weight"]), reverse=True)
    return output


def _interpretation(
    *,
    dominant_leg_summary: list[dict[str, Any]],
    contribution_pattern_summary: list[dict[str, Any]],
) -> str:
    dominant = dominant_leg_summary[0]["dominant_leg"] if dominant_leg_summary else None
    dominant_share = (
        float(dominant_leg_summary[0]["abs_gap_share"])
        if dominant_leg_summary and dominant_leg_summary[0].get("abs_gap_share") is not None
        else None
    )
    offsetting_share = None
    for row in contribution_pattern_summary:
        if str(row["contribution_pattern"]) == "offsetting":
            offsetting_share = row.get("abs_gap_share")
            break
    if offsetting_share is not None and float(offsetting_share) >= 0.4:
        return "top_gap_quarters_are_mixed_or_offsetting_toc_row_bundles"
    if dominant in {"row_dominant", "toc_dominant"} and dominant_share is not None and dominant_share >= 0.65:
        return f"top_gap_quarters_are_mostly_{dominant}"
    return "top_gap_quarters_have_no_single_dominant_leg"


def build_strict_top_gap_quarter_audit_summary(
    *,
    shocked: pd.DataFrame,
    limit: int = 5,
    baseline_shock_column: str = "tdc_residual_z",
    excluded_shock_column: str = "tdc_no_toc_no_row_bank_only_residual_z",
) -> dict[str, Any]:
    required = {
        "quarter",
        baseline_shock_column,
        excluded_shock_column,
        "tdc_bank_only_qoq",
        "tdc_no_toc_no_row_bank_only_qoq",
        "tdc_row_treasury_transactions_qoq",
        "tdc_treasury_operating_cash_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_top_gap_quarter_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    frame["period_bucket"] = frame["quarter"].map(_period_bucket)
    frame["shock_gap"] = frame[excluded_shock_column] - frame[baseline_shock_column]
    frame["abs_shock_gap"] = frame["shock_gap"].abs()
    frame["bundle_qoq"] = frame["tdc_bank_only_qoq"] - frame["tdc_no_toc_no_row_bank_only_qoq"]
    frame["row_leg_qoq"] = frame["tdc_row_treasury_transactions_qoq"]
    frame["toc_signed_contribution_qoq"] = -frame["tdc_treasury_operating_cash_qoq"]

    top_gap_quarters = _top_gap_quarter_rows(frame, limit=limit)
    dominant_leg_summary = _weighted_distribution(top_gap_quarters, key="dominant_leg")
    contribution_pattern_summary = _weighted_distribution(top_gap_quarters, key="contribution_pattern")
    interpretation = _interpretation(
        dominant_leg_summary=dominant_leg_summary,
        contribution_pattern_summary=contribution_pattern_summary,
    )

    takeaways: list[str] = []
    if dominant_leg_summary:
        lead = dominant_leg_summary[0]
        takeaways.append(
            "Across the top-gap quarters, the largest dominant-leg bucket is "
            f"`{lead['dominant_leg']}` with abs-gap share ≈ {float(lead['abs_gap_share'] or 0.0):.2f}."
        )
    if contribution_pattern_summary:
        lead = contribution_pattern_summary[0]
        takeaways.append(
            "Across the top-gap quarters, the largest contribution pattern is "
            f"`{lead['contribution_pattern']}` with abs-gap share ≈ {float(lead['abs_gap_share'] or 0.0):.2f}."
        )
    if top_gap_quarters:
        first = top_gap_quarters[0]
        takeaways.append(
            "The largest top-gap quarter is "
            f"`{first['quarter']}` with dominant leg `{first['dominant_leg']}` and contribution pattern "
            f"`{first['contribution_pattern']}`."
        )

    return {
        "status": "available",
        "headline_question": "Within the largest baseline-versus-excluded shock-gap quarters, is the TOC/ROW bundle mainly TOC-driven, ROW-driven, or mixed?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_top_gap_quarter_audit_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
            "top_gap_limit": int(limit),
        },
        "top_gap_quarters": top_gap_quarters,
        "dominant_leg_summary": dominant_leg_summary,
        "contribution_pattern_summary": contribution_pattern_summary,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
