from __future__ import annotations

from typing import Any

import pandas as pd

_COMPONENTS: tuple[str, ...] = (
    "other_component_qoq",
    "other_component_no_toc_no_row_bank_only_qoq",
    "total_deposits_bank_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_source_qoq",
    "strict_non_treasury_securities_qoq",
    "strict_identifiable_total_qoq",
    "strict_identifiable_net_after_funding_qoq",
)

_DRIVER_COLUMNS: tuple[str, ...] = (
    "tdc_bank_only_qoq",
    "tdc_no_toc_no_row_bank_only_qoq",
    "tdc_row_treasury_transactions_qoq",
    "tdc_treasury_operating_cash_qoq",
)


def _signed_corr(frame: pd.DataFrame, left: str, right: str) -> float | None:
    if left not in frame.columns or right not in frame.columns:
        return None
    sample = frame[[left, right]].dropna()
    if len(sample) < 3:
        return None
    value = sample[left].corr(sample[right])
    if pd.isna(value):
        return None
    return float(value)


def _top_gap_rows(frame: pd.DataFrame, *, gap_column: str, limit: int = 10) -> list[dict[str, Any]]:
    if gap_column not in frame.columns:
        return []
    columns = [
        "quarter",
        gap_column,
        "tdc_residual_z",
        "tdc_no_toc_no_row_bank_only_residual_z",
        "period_bucket",
        "baseline_minus_excluded_target_qoq",
        "row_leg_qoq",
        "toc_signed_contribution_qoq",
    ]
    available_columns = [column for column in columns if column in frame.columns]
    sample = frame[available_columns].dropna(subset=[gap_column, "tdc_residual_z", "tdc_no_toc_no_row_bank_only_residual_z"]).copy()
    if sample.empty:
        return []
    sample["_abs_gap"] = sample[gap_column].abs()
    rows: list[dict[str, Any]] = []
    for _, row in sample.sort_values("_abs_gap", ascending=False).head(limit).iterrows():
        rows.append(
            {
                "quarter": str(row["quarter"]),
                "period_bucket": row.get("period_bucket"),
                "baseline_shock": float(row["tdc_residual_z"]),
                "toc_row_excluded_shock": float(row["tdc_no_toc_no_row_bank_only_residual_z"]),
                "shock_gap": float(row[gap_column]),
                "abs_shock_gap": float(row["_abs_gap"]),
                "baseline_minus_excluded_target_qoq": (
                    None
                    if pd.isna(row.get("baseline_minus_excluded_target_qoq"))
                    else float(row["baseline_minus_excluded_target_qoq"])
                ),
                "row_leg_qoq": None if pd.isna(row.get("row_leg_qoq")) else float(row["row_leg_qoq"]),
                "toc_signed_contribution_qoq": (
                    None
                    if pd.isna(row.get("toc_signed_contribution_qoq"))
                    else float(row["toc_signed_contribution_qoq"])
                ),
            }
        )
    return rows


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


def _quarter_concentration(frame: pd.DataFrame, *, gap_column: str) -> dict[str, Any]:
    if gap_column not in frame.columns:
        return {}
    sample = frame[["quarter", "period_bucket", gap_column]].dropna().copy()
    if sample.empty:
        return {}
    sample["_abs_gap"] = sample[gap_column].abs()
    total_abs_gap = float(sample["_abs_gap"].sum())
    sorted_sample = sample.sort_values("_abs_gap", ascending=False).reset_index(drop=True)
    top3_abs_gap = float(sorted_sample.head(3)["_abs_gap"].sum())
    top5_abs_gap = float(sorted_sample.head(5)["_abs_gap"].sum())
    bucket_rows: list[dict[str, Any]] = []
    for bucket, bucket_sample in sample.groupby("period_bucket", dropna=False):
        bucket_abs_gap = float(bucket_sample["_abs_gap"].sum())
        top_row = bucket_sample.sort_values("_abs_gap", ascending=False).iloc[0]
        bucket_rows.append(
            {
                "period_bucket": str(bucket),
                "quarter_count": int(len(bucket_sample)),
                "abs_gap_sum": bucket_abs_gap,
                "abs_gap_share": _share(bucket_abs_gap, total_abs_gap),
                "top_quarter": str(top_row["quarter"]),
                "top_abs_gap": float(top_row["_abs_gap"]),
            }
        )
    bucket_rows.sort(key=lambda row: row["abs_gap_sum"], reverse=True)
    return {
        "total_abs_gap": total_abs_gap,
        "top3_abs_gap_share": _share(top3_abs_gap, total_abs_gap),
        "top5_abs_gap_share": _share(top5_abs_gap, total_abs_gap),
        "dominant_period_bucket": bucket_rows[0]["period_bucket"] if bucket_rows else None,
        "period_bucket_abs_gap_breakdown": bucket_rows,
    }


def _gap_driver_alignment(frame: pd.DataFrame, *, gap_column: str) -> dict[str, Any]:
    if gap_column not in frame.columns:
        return {}
    correlations = {
        "baseline_minus_excluded_target_qoq": _signed_corr(frame, gap_column, "baseline_minus_excluded_target_qoq"),
        "row_leg_qoq": _signed_corr(frame, gap_column, "row_leg_qoq"),
        "toc_signed_contribution_qoq": _signed_corr(frame, gap_column, "toc_signed_contribution_qoq"),
    }
    available = {key: value for key, value in correlations.items() if value is not None}
    dominant_driver = None
    if available:
        dominant_driver = max(available.items(), key=lambda item: abs(float(item[1])))[0]
    return {
        "shock_gap_driver_correlations": correlations,
        "dominant_driver_by_abs_corr": dominant_driver,
    }


def _interpretation_label(
    *,
    shock_corr: float | None,
    same_sign_share: float | None,
    baseline_total_corr: float | None,
    excluded_total_corr: float | None,
    baseline_direct_core_corr: float | None,
    excluded_direct_core_corr: float | None,
) -> str:
    if shock_corr is None:
        return "missing_shock_alignment_inputs"
    if (
        baseline_total_corr is not None
        and excluded_total_corr is not None
        and baseline_direct_core_corr is not None
        and excluded_direct_core_corr is not None
        and baseline_total_corr < 0.0
        and excluded_total_corr > 0.0
        and baseline_direct_core_corr < 0.0
        and excluded_direct_core_corr > 0.0
    ):
        return "excluded_shock_rotates_toward_positive_direct_count_channels"
    if same_sign_share is not None and same_sign_share < 0.75:
        return "excluded_shock_only_partially_aligned_with_baseline"
    if shock_corr < 0.6:
        return "excluded_shock_moderately_aligned_but_distinct"
    return "excluded_shock_close_to_baseline"


def build_strict_sign_mismatch_audit_summary(
    *,
    shocked: pd.DataFrame,
    strict_missing_channel_summary: dict[str, Any] | None = None,
    baseline_shock_column: str = "tdc_residual_z",
    excluded_shock_column: str = "tdc_no_toc_no_row_bank_only_residual_z",
) -> dict[str, Any]:
    required = {"quarter", baseline_shock_column, excluded_shock_column, *_COMPONENTS, *_DRIVER_COLUMNS}
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_shock_or_component_columns"}

    frame = shocked[list(required)].dropna(subset=[baseline_shock_column, excluded_shock_column]).copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_overlapping_shock_rows"}

    frame["same_sign"] = frame[baseline_shock_column] * frame[excluded_shock_column] > 0
    frame["shock_gap"] = frame[excluded_shock_column] - frame[baseline_shock_column]
    frame["period_bucket"] = frame["quarter"].map(_period_bucket)
    frame["baseline_minus_excluded_target_qoq"] = (
        frame["tdc_bank_only_qoq"] - frame["tdc_no_toc_no_row_bank_only_qoq"]
    )
    frame["row_leg_qoq"] = frame["tdc_row_treasury_transactions_qoq"]
    frame["toc_signed_contribution_qoq"] = -frame["tdc_treasury_operating_cash_qoq"]

    shock_corr = _signed_corr(frame, baseline_shock_column, excluded_shock_column)
    same_sign_share = float(frame["same_sign"].mean()) if not frame.empty else None
    quarter_concentration = _quarter_concentration(frame, gap_column="shock_gap")
    gap_driver_alignment = _gap_driver_alignment(frame, gap_column="shock_gap")

    component_alignment: dict[str, Any] = {}
    for outcome in _COMPONENTS:
        baseline_corr = _signed_corr(frame, baseline_shock_column, outcome)
        excluded_corr = _signed_corr(frame, excluded_shock_column, outcome)
        component_alignment[outcome] = {
            "baseline_shock_corr": baseline_corr,
            "toc_row_excluded_shock_corr": excluded_corr,
            "excluded_minus_baseline_corr": (
                None if baseline_corr is None or excluded_corr is None else float(excluded_corr) - float(baseline_corr)
            ),
        }

    interpretation = _interpretation_label(
        shock_corr=shock_corr,
        same_sign_share=same_sign_share,
        baseline_total_corr=component_alignment["strict_identifiable_total_qoq"]["baseline_shock_corr"],
        excluded_total_corr=component_alignment["strict_identifiable_total_qoq"]["toc_row_excluded_shock_corr"],
        baseline_direct_core_corr=component_alignment["strict_loan_core_min_qoq"]["baseline_shock_corr"],
        excluded_direct_core_corr=component_alignment["strict_loan_core_min_qoq"]["toc_row_excluded_shock_corr"],
    )

    h0 = {}
    if strict_missing_channel_summary is not None:
        h0 = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}))

    takeaways = []
    if shock_corr is not None and same_sign_share is not None:
        takeaways.append(
            f"The TOC/ROW-excluded shock is not just a small perturbation of baseline: overlap correlation ≈ {float(shock_corr):.2f}, same-sign share ≈ {float(same_sign_share):.2f}."
        )
    direct_core_block = component_alignment.get("strict_loan_core_min_qoq", {})
    total_block = component_alignment.get("strict_identifiable_total_qoq", {})
    if (
        direct_core_block.get("baseline_shock_corr") is not None
        and direct_core_block.get("toc_row_excluded_shock_corr") is not None
        and total_block.get("baseline_shock_corr") is not None
        and total_block.get("toc_row_excluded_shock_corr") is not None
    ):
        takeaways.append(
            "The excluded shock rotates materially toward positive direct-count channels: "
            f"headline direct-core corr goes from {float(direct_core_block['baseline_shock_corr']):.2f} to {float(direct_core_block['toc_row_excluded_shock_corr']):.2f}, "
            f"strict identifiable total corr goes from {float(total_block['baseline_shock_corr']):.2f} to {float(total_block['toc_row_excluded_shock_corr']):.2f}."
        )
    top5_share = quarter_concentration.get("top5_abs_gap_share")
    dominant_bucket = quarter_concentration.get("dominant_period_bucket")
    if top5_share is not None and dominant_bucket is not None:
        takeaways.append(
            f"The rotation is concentrated rather than diffuse: the top five shock-gap quarters explain about {float(top5_share):.2f} of absolute gap mass, with the largest share in `{str(dominant_bucket)}`."
        )
    target_gap_corr = dict(gap_driver_alignment.get("shock_gap_driver_correlations", {})).get(
        "baseline_minus_excluded_target_qoq"
    )
    row_corr = dict(gap_driver_alignment.get("shock_gap_driver_correlations", {})).get("row_leg_qoq")
    toc_corr = dict(gap_driver_alignment.get("shock_gap_driver_correlations", {})).get("toc_signed_contribution_qoq")
    if target_gap_corr is not None and row_corr is not None and toc_corr is not None:
        takeaways.append(
            "The shock-gap lines up with the TOC/ROW target bundle itself: "
            f"corr with baseline-minus-excluded target ≈ {float(target_gap_corr):.2f}, "
            f"ROW leg ≈ {float(row_corr):.2f}, TOC signed contribution ≈ {float(toc_corr):.2f}."
        )
    if h0:
        excluded_h0 = dict(h0.get("toc_row_excluded", {}))
        residual = dict(excluded_h0.get("residual_response", {}) or {}).get("beta")
        total = dict(excluded_h0.get("strict_identifiable_total_response", {}) or {}).get("beta")
        if residual is not None and total is not None:
            takeaways.append(
                "At h0 this shows up as a sign mismatch in the strict surface: "
                f"TOC/ROW-excluded residual ≈ {float(residual):.2f}, strict identifiable total ≈ {float(total):.2f}."
            )

    return {
        "status": "available",
        "headline_question": "Why does the TOC/ROW-excluded strict lane flip positive while the remaining residual stays slightly negative?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_sign_mismatch_audit_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
        },
        "shock_alignment": {
            "overlap_rows": int(len(frame)),
            "shock_corr": shock_corr,
            "same_sign_share": same_sign_share,
            "largest_gap_quarters": _top_gap_rows(frame, gap_column="shock_gap"),
        },
        "quarter_concentration": quarter_concentration,
        "gap_driver_alignment": gap_driver_alignment,
        "component_alignment": component_alignment,
        "h0_strict_context": h0,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
