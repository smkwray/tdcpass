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


def _sign_label(value: float) -> str:
    if value > 0.0:
        return "positive"
    if value < 0.0:
        return "negative"
    return "zero"


def _signed_alignment(left: float, right: float) -> str:
    product = float(left) * float(right)
    if product > 0.0:
        return "aligned"
    if product < 0.0:
        return "opposed"
    return "neutral"


def _share(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _directional_driver(*, row_alignment_to_gap: str, toc_alignment_to_gap: str) -> str:
    if row_alignment_to_gap == "aligned" and toc_alignment_to_gap == "opposed":
        return "row_driven_gap_direction"
    if toc_alignment_to_gap == "aligned" and row_alignment_to_gap == "opposed":
        return "toc_driven_gap_direction"
    if row_alignment_to_gap == "aligned" and toc_alignment_to_gap == "aligned":
        return "both_legs_align_gap"
    if row_alignment_to_gap == "opposed" and toc_alignment_to_gap == "opposed":
        return "both_legs_oppose_gap"
    return "neutral_or_single_leg"


def _weighted_distribution(rows: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    total_weight = sum(abs(float(row["shock_gap"])) for row in rows)
    by_key: dict[str, float] = {}
    for row in rows:
        bucket = str(row[key])
        by_key[bucket] = by_key.get(bucket, 0.0) + abs(float(row["shock_gap"]))
    out = [
        {
            key: bucket,
            "abs_gap_weight": weight,
            "abs_gap_share": _share(weight, total_weight),
        }
        for bucket, weight in by_key.items()
    ]
    out.sort(key=lambda item: float(item["abs_gap_weight"]), reverse=True)
    return out


def _top_gap_quarter_rows(frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sample = frame.sort_values("abs_shock_gap", ascending=False).head(limit).copy()
    for _, row in sample.iterrows():
        shock_gap = float(row["shock_gap"])
        bundle = float(row["bundle_qoq"])
        row_leg = float(row["row_leg_qoq"])
        toc_leg = float(row["toc_signed_contribution_qoq"])
        row_alignment_to_gap = _signed_alignment(row_leg, shock_gap)
        toc_alignment_to_gap = _signed_alignment(toc_leg, shock_gap)
        rows.append(
            {
                "quarter": str(row["quarter"]),
                "period_bucket": str(row["period_bucket"]),
                "baseline_shock": float(row["tdc_residual_z"]),
                "toc_row_excluded_shock": float(row["tdc_no_toc_no_row_bank_only_residual_z"]),
                "shock_gap": shock_gap,
                "shock_gap_sign": _sign_label(shock_gap),
                "abs_shock_gap": float(row["abs_shock_gap"]),
                "bundle_qoq": bundle,
                "bundle_sign": _sign_label(bundle),
                "gap_alignment_to_bundle": _signed_alignment(bundle, shock_gap),
                "row_leg_qoq": row_leg,
                "row_leg_alignment_to_gap": row_alignment_to_gap,
                "toc_signed_contribution_qoq": toc_leg,
                "toc_leg_alignment_to_gap": toc_alignment_to_gap,
                "directional_driver": _directional_driver(
                    row_alignment_to_gap=row_alignment_to_gap,
                    toc_alignment_to_gap=toc_alignment_to_gap,
                ),
            }
        )
    return rows


def _interpretation(
    *,
    gap_bundle_alignment_summary: list[dict[str, Any]],
    directional_driver_summary: list[dict[str, Any]],
) -> str:
    lead_gap_alignment = gap_bundle_alignment_summary[0] if gap_bundle_alignment_summary else {}
    lead_driver = directional_driver_summary[0] if directional_driver_summary else {}
    if str(lead_driver.get("directional_driver")) == "both_legs_oppose_gap" and float(
        lead_driver.get("abs_gap_share") or 0.0
    ) >= 0.35:
        return "top_gap_gap_direction_often_opposes_both_toc_and_row_legs"
    if str(lead_gap_alignment.get("gap_alignment_to_bundle")) == "opposed" and float(
        lead_gap_alignment.get("abs_gap_share") or 0.0
    ) >= 0.5:
        return "top_gap_gap_direction_often_opposes_bundle_sign"
    if str(lead_driver.get("directional_driver")) in {
        "row_driven_gap_direction",
        "toc_driven_gap_direction",
    } and float(lead_driver.get("abs_gap_share") or 0.0) >= 0.5:
        return f"top_gap_gap_direction_is_mostly_{str(lead_driver.get('directional_driver'))}"
    return "top_gap_gap_direction_is_mixed_across_quarters"


def build_strict_top_gap_quarter_direction_summary(
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
        return {"status": "not_available", "reason": "missing_required_top_gap_direction_columns"}

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
    gap_bundle_alignment_summary = _weighted_distribution(top_gap_quarters, key="gap_alignment_to_bundle")
    directional_driver_summary = _weighted_distribution(top_gap_quarters, key="directional_driver")
    interpretation = _interpretation(
        gap_bundle_alignment_summary=gap_bundle_alignment_summary,
        directional_driver_summary=directional_driver_summary,
    )

    takeaways: list[str] = []
    if gap_bundle_alignment_summary:
        lead = gap_bundle_alignment_summary[0]
        takeaways.append(
            "Across the top-gap quarters, the leading gap-versus-bundle alignment is "
            f"`{lead['gap_alignment_to_bundle']}` with abs-gap share ≈ {float(lead['abs_gap_share'] or 0.0):.2f}."
        )
    if directional_driver_summary:
        lead = directional_driver_summary[0]
        takeaways.append(
            "Across the top-gap quarters, the leading directional-driver bucket is "
            f"`{lead['directional_driver']}` with abs-gap share ≈ {float(lead['abs_gap_share'] or 0.0):.2f}."
        )
    if top_gap_quarters:
        first = top_gap_quarters[0]
        takeaways.append(
            "The largest top-gap quarter is "
            f"`{first['quarter']}` with gap-versus-bundle alignment `{first['gap_alignment_to_bundle']}` and "
            f"directional driver `{first['directional_driver']}`."
        )

    return {
        "status": "available",
        "headline_question": "In the top shock-gap quarters, does the gap direction line up with the TOC/ROW bundle sign, the ROW leg, the TOC leg, or oppose the whole bundle?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_top_gap_quarter_direction_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
            "top_gap_limit": int(limit),
        },
        "top_gap_quarters": top_gap_quarters,
        "gap_bundle_alignment_summary": gap_bundle_alignment_summary,
        "directional_driver_summary": directional_driver_summary,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
