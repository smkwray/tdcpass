from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.strict_top_gap_quarter_direction import (
    build_strict_top_gap_quarter_direction_summary,
)


def _share(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _residual_strict_pattern(*, residual: float, strict_total: float) -> str:
    residual_sign = "positive" if residual > 0.0 else "negative" if residual < 0.0 else "zero"
    strict_sign = "positive" if strict_total > 0.0 else "negative" if strict_total < 0.0 else "zero"
    return f"{residual_sign}_residual_{strict_sign}_strict"


def _quarter_rows(
    *,
    direction_rows: list[dict[str, Any]],
    shocked: pd.DataFrame,
) -> list[dict[str, Any]]:
    by_quarter = shocked.set_index("quarter")
    rows: list[dict[str, Any]] = []
    for direction_row in direction_rows:
        quarter = str(direction_row["quarter"])
        if quarter not in by_quarter.index:
            continue
        panel_row = by_quarter.loc[quarter]
        excluded_residual = float(panel_row["other_component_no_toc_no_row_bank_only_qoq"])
        strict_total = float(panel_row["strict_identifiable_total_qoq"])
        strict_net_after_funding = float(panel_row["strict_identifiable_net_after_funding_qoq"])
        rows.append(
            {
                **direction_row,
                "excluded_other_component_qoq": excluded_residual,
                "strict_identifiable_total_qoq": strict_total,
                "excluded_strict_gap_qoq": excluded_residual - strict_total,
                "strict_identifiable_net_after_funding_qoq": strict_net_after_funding,
                "excluded_strict_gap_after_funding_qoq": excluded_residual - strict_net_after_funding,
                "foreign_nonts_qoq": float(panel_row["foreign_nonts_qoq"]),
                "tga_qoq": float(panel_row["tga_qoq"]),
                "reserves_qoq": float(panel_row["reserves_qoq"]),
                "residual_strict_pattern": _residual_strict_pattern(
                    residual=excluded_residual,
                    strict_total=strict_total,
                ),
            }
        )
    return rows


def _pattern_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total_weight = sum(abs(float(row["shock_gap"])) for row in rows)
    by_pattern: dict[str, float] = {}
    for row in rows:
        pattern = str(row["residual_strict_pattern"])
        by_pattern[pattern] = by_pattern.get(pattern, 0.0) + abs(float(row["shock_gap"]))
    output = [
        {
            "residual_strict_pattern": pattern,
            "abs_gap_weight": weight,
            "abs_gap_share": _share(weight, total_weight),
        }
        for pattern, weight in by_pattern.items()
    ]
    output.sort(key=lambda item: float(item["abs_gap_weight"]), reverse=True)
    return output


def _leading_pattern_share(rows: list[dict[str, Any]], *, pattern: str) -> float | None:
    total_weight = sum(abs(float(row["shock_gap"])) for row in rows)
    matched_weight = sum(
        abs(float(row["shock_gap"])) for row in rows if str(row["residual_strict_pattern"]) == pattern
    )
    return _share(matched_weight, total_weight)


def _directional_driver_context_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total_weight = sum(abs(float(row["shock_gap"])) for row in rows)
    output: list[dict[str, Any]] = []
    for directional_driver in sorted({str(row["directional_driver"]) for row in rows}):
        bucket = [row for row in rows if str(row["directional_driver"]) == directional_driver]
        weight = sum(abs(float(row["shock_gap"])) for row in bucket)
        if weight == 0.0:
            continue

        def weighted_mean(key: str) -> float:
            return sum(float(row[key]) * abs(float(row["shock_gap"])) for row in bucket) / weight

        pattern_summary = _pattern_summary(bucket)
        output.append(
            {
                "directional_driver": directional_driver,
                "abs_gap_weight": weight,
                "abs_gap_share": _share(weight, total_weight),
                "weighted_mean_excluded_other_component_qoq": weighted_mean("excluded_other_component_qoq"),
                "weighted_mean_strict_identifiable_total_qoq": weighted_mean("strict_identifiable_total_qoq"),
                "weighted_mean_excluded_strict_gap_qoq": weighted_mean("excluded_strict_gap_qoq"),
                "weighted_mean_strict_identifiable_net_after_funding_qoq": weighted_mean(
                    "strict_identifiable_net_after_funding_qoq"
                ),
                "weighted_mean_excluded_strict_gap_after_funding_qoq": weighted_mean(
                    "excluded_strict_gap_after_funding_qoq"
                ),
                "weighted_mean_foreign_nonts_qoq": weighted_mean("foreign_nonts_qoq"),
                "weighted_mean_tga_qoq": weighted_mean("tga_qoq"),
                "weighted_mean_reserves_qoq": weighted_mean("reserves_qoq"),
                "leading_residual_strict_pattern": (
                    None if not pattern_summary else str(pattern_summary[0]["residual_strict_pattern"])
                ),
                "leading_residual_strict_pattern_share": (
                    None if not pattern_summary else pattern_summary[0].get("abs_gap_share")
                ),
            }
        )
    output.sort(key=lambda item: float(item["abs_gap_weight"]), reverse=True)
    return output


def _interpretation(
    *,
    driver_summary: list[dict[str, Any]],
    pattern_summary: list[dict[str, Any]],
) -> str:
    lead_driver = driver_summary[0] if driver_summary else {}
    lead_pattern = pattern_summary[0] if pattern_summary else {}
    if (
        str(lead_driver.get("directional_driver")) == "both_legs_oppose_gap"
        and float(lead_driver.get("weighted_mean_excluded_other_component_qoq") or 0.0) > 0.0
        and float(lead_driver.get("weighted_mean_strict_identifiable_total_qoq") or 0.0) > 0.0
    ):
        return "both_leg_inversion_quarters_still_tend_to_show_positive_residual_and_positive_strict_support"
    if str(lead_pattern.get("residual_strict_pattern")) == "negative_residual_positive_strict":
        return "top_gap_inversion_quarters_often_show_negative_residual_but_positive_strict_support"
    if str(lead_pattern.get("residual_strict_pattern")) == "negative_residual_negative_strict":
        return "top_gap_inversion_quarters_often_show_joint_negative_residual_and_strict_support"
    return "top_gap_inversion_profiles_are_mixed"


def build_strict_top_gap_inversion_summary(
    *,
    shocked: pd.DataFrame,
    limit: int = 5,
    baseline_shock_column: str = "tdc_residual_z",
    excluded_shock_column: str = "tdc_no_toc_no_row_bank_only_residual_z",
) -> dict[str, Any]:
    required = {
        "quarter",
        "other_component_no_toc_no_row_bank_only_qoq",
        "strict_identifiable_total_qoq",
        "strict_identifiable_net_after_funding_qoq",
        "foreign_nonts_qoq",
        "tga_qoq",
        "reserves_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_top_gap_inversion_columns"}

    direction_summary = build_strict_top_gap_quarter_direction_summary(
        shocked=shocked,
        limit=limit,
        baseline_shock_column=baseline_shock_column,
        excluded_shock_column=excluded_shock_column,
    )
    if str(direction_summary.get("status", "not_available")) != "available":
        return {
            "status": str(direction_summary.get("status", "not_available")),
            "reason": str(direction_summary.get("reason", "direction_summary_unavailable")),
        }

    rows = _quarter_rows(
        direction_rows=list(direction_summary.get("top_gap_quarters", [])),
        shocked=shocked[list(required)].copy(),
    )
    if not rows:
        return {"status": "not_available", "reason": "no_top_gap_rows_after_merge"}

    directional_driver_context_summary = _directional_driver_context_summary(rows)
    residual_strict_pattern_summary = _pattern_summary(rows)
    interpretation = _interpretation(
        driver_summary=directional_driver_context_summary,
        pattern_summary=residual_strict_pattern_summary,
    )

    takeaways: list[str] = []
    if directional_driver_context_summary:
        lead = directional_driver_context_summary[0]
        takeaways.append(
            "Across the top-gap quarters, the leading inversion bucket is "
            f"`{lead['directional_driver']}` with abs-gap share ≈ {float(lead['abs_gap_share'] or 0.0):.2f}; "
            f"its weighted excluded residual is ≈ {float(lead['weighted_mean_excluded_other_component_qoq']):.2f} "
            f"versus weighted strict total ≈ {float(lead['weighted_mean_strict_identifiable_total_qoq']):.2f}."
        )
        if lead.get("leading_residual_strict_pattern") is not None:
            takeaways.append(
                "Inside that leading inversion bucket, the largest residual-versus-strict pattern is "
                f"`{str(lead['leading_residual_strict_pattern'])}` with abs-gap share ≈ "
                f"{float(lead.get('leading_residual_strict_pattern_share') or 0.0):.2f}."
            )
    toc_driven = next(
        (row for row in rows if str(row["directional_driver"]) == "toc_driven_gap_direction"),
        None,
    )
    if toc_driven is not None:
        takeaways.append(
            "The TOC-driven exception is "
            f"`{toc_driven['quarter']}`: excluded residual ≈ {float(toc_driven['excluded_other_component_qoq']):.2f}, "
            f"strict total ≈ {float(toc_driven['strict_identifiable_total_qoq']):.2f}, "
            f"foreign NONTS ≈ {float(toc_driven['foreign_nonts_qoq']):.2f}."
        )
    row_driven = next(
        (row for row in rows if str(row["directional_driver"]) == "row_driven_gap_direction"),
        None,
    )
    if row_driven is not None:
        takeaways.append(
            "The ROW-driven aligned exception is "
            f"`{row_driven['quarter']}`: excluded residual ≈ {float(row_driven['excluded_other_component_qoq']):.2f}, "
            f"strict total ≈ {float(row_driven['strict_identifiable_total_qoq']):.2f}, "
            f"TGA ≈ {float(row_driven['tga_qoq']):.2f}."
        )

    return {
        "status": "available",
        "headline_question": "Inside the top shock-gap quarters, what realized excluded-residual versus strict-lane profile sits under the inversion buckets and single-leg exceptions?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_top_gap_inversion_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
            "top_gap_limit": int(limit),
        },
        "top_gap_quarters": rows,
        "directional_driver_context_summary": directional_driver_context_summary,
        "residual_strict_pattern_summary": residual_strict_pattern_summary,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
