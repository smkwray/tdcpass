from __future__ import annotations

from typing import Any

import pandas as pd

_ROTATION_LABEL = "excluded_shock_rotates_toward_positive_direct_count_channels"


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


def _rotation_label(
    *,
    shock_corr: float | None,
    same_sign_share: float | None,
    baseline_total_corr: float | None,
    excluded_total_corr: float | None,
    baseline_loan_corr: float | None,
    excluded_loan_corr: float | None,
) -> str:
    if shock_corr is None:
        return "missing_shock_alignment_inputs"
    if (
        baseline_total_corr is not None
        and excluded_total_corr is not None
        and baseline_loan_corr is not None
        and excluded_loan_corr is not None
        and baseline_total_corr < 0.0
        and excluded_total_corr > 0.0
        and baseline_loan_corr < 0.0
        and excluded_loan_corr > 0.0
    ):
        return _ROTATION_LABEL
    if same_sign_share is not None and same_sign_share < 0.75:
        return "excluded_shock_only_partially_aligned_with_baseline"
    if shock_corr < 0.6:
        return "excluded_shock_moderately_aligned_but_distinct"
    return "excluded_shock_close_to_baseline"


def _subset_rotation_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "rows": 0,
            "shock_corr": None,
            "same_sign_share": None,
            "baseline_loan_corr": None,
            "excluded_loan_corr": None,
            "baseline_total_corr": None,
            "excluded_total_corr": None,
            "interpretation": "empty_subset",
        }
    shock_corr = _signed_corr(frame, "tdc_residual_z", "tdc_no_toc_no_row_bank_only_residual_z")
    same_sign_share = float((frame["tdc_residual_z"] * frame["tdc_no_toc_no_row_bank_only_residual_z"] > 0).mean())
    baseline_loan_corr = _signed_corr(frame, "tdc_residual_z", "strict_loan_source_qoq")
    excluded_loan_corr = _signed_corr(frame, "tdc_no_toc_no_row_bank_only_residual_z", "strict_loan_source_qoq")
    baseline_total_corr = _signed_corr(frame, "tdc_residual_z", "strict_identifiable_total_qoq")
    excluded_total_corr = _signed_corr(frame, "tdc_no_toc_no_row_bank_only_residual_z", "strict_identifiable_total_qoq")
    return {
        "rows": int(len(frame)),
        "shock_corr": shock_corr,
        "same_sign_share": same_sign_share,
        "baseline_loan_corr": baseline_loan_corr,
        "excluded_loan_corr": excluded_loan_corr,
        "baseline_total_corr": baseline_total_corr,
        "excluded_total_corr": excluded_total_corr,
        "interpretation": _rotation_label(
            shock_corr=shock_corr,
            same_sign_share=same_sign_share,
            baseline_total_corr=baseline_total_corr,
            excluded_total_corr=excluded_total_corr,
            baseline_loan_corr=baseline_loan_corr,
            excluded_loan_corr=excluded_loan_corr,
        ),
    }


def _top_gap_quarter_profiles(frame: pd.DataFrame, *, limit: int = 8) -> list[dict[str, Any]]:
    sample = frame[
        [
            "quarter",
            "period_bucket",
            "tdc_residual_z",
            "tdc_no_toc_no_row_bank_only_residual_z",
            "shock_gap",
            "abs_shock_gap",
            "baseline_minus_excluded_target_qoq",
            "row_leg_qoq",
            "toc_signed_contribution_qoq",
        ]
    ].copy()
    rows: list[dict[str, Any]] = []
    for _, row in sample.sort_values("abs_shock_gap", ascending=False).head(limit).iterrows():
        bundle = float(row["baseline_minus_excluded_target_qoq"])
        row_leg = float(row["row_leg_qoq"])
        toc_signed = float(row["toc_signed_contribution_qoq"])
        bundle_abs = abs(bundle)
        rows.append(
            {
                "quarter": str(row["quarter"]),
                "period_bucket": str(row["period_bucket"]),
                "baseline_shock": float(row["tdc_residual_z"]),
                "toc_row_excluded_shock": float(row["tdc_no_toc_no_row_bank_only_residual_z"]),
                "shock_gap": float(row["shock_gap"]),
                "abs_shock_gap": float(row["abs_shock_gap"]),
                "baseline_minus_excluded_target_qoq": bundle,
                "row_leg_qoq": row_leg,
                "toc_signed_contribution_qoq": toc_signed,
                "row_share_of_bundle_abs": _share(abs(row_leg), bundle_abs),
                "toc_share_of_bundle_abs": _share(abs(toc_signed), bundle_abs),
            }
        )
    return rows


def _period_bucket_profiles(frame: pd.DataFrame) -> list[dict[str, Any]]:
    total_abs_gap = float(frame["abs_shock_gap"].sum())
    rows: list[dict[str, Any]] = []
    for bucket, bucket_frame in frame.groupby("period_bucket", dropna=False):
        subset = _subset_rotation_summary(bucket_frame)
        abs_gap_sum = float(bucket_frame["abs_shock_gap"].sum())
        rows.append(
            {
                "period_bucket": str(bucket),
                "rows": int(len(bucket_frame)),
                "abs_gap_sum": abs_gap_sum,
                "abs_gap_share": _share(abs_gap_sum, total_abs_gap),
                "subset_rotation": subset,
            }
        )
    rows.sort(key=lambda row: row["abs_gap_sum"], reverse=True)
    return rows


def _trim_diagnostics(frame: pd.DataFrame) -> dict[str, Any]:
    top3 = frame.sort_values("abs_shock_gap", ascending=False).head(3)["quarter"].astype(str).tolist()
    top5 = frame.sort_values("abs_shock_gap", ascending=False).head(5)["quarter"].astype(str).tolist()
    subsets = {
        "full_sample": frame,
        "drop_top3_gap_quarters": frame[~frame["quarter"].astype(str).isin(top3)].copy(),
        "drop_top5_gap_quarters": frame[~frame["quarter"].astype(str).isin(top5)].copy(),
        "drop_covid_post": frame[frame["period_bucket"] != "covid_post"].copy(),
    }
    return {
        name: {
            **_subset_rotation_summary(subset),
            "dropped_quarters": top3 if name == "drop_top3_gap_quarters" else top5 if name == "drop_top5_gap_quarters" else [],
        }
        for name, subset in subsets.items()
    }


def _composition_interpretation(*, trim_diagnostics: dict[str, Any]) -> str:
    full = str(dict(trim_diagnostics.get("full_sample", {})).get("interpretation", ""))
    drop_top5 = str(dict(trim_diagnostics.get("drop_top5_gap_quarters", {})).get("interpretation", ""))
    drop_covid = str(dict(trim_diagnostics.get("drop_covid_post", {})).get("interpretation", ""))
    if full != _ROTATION_LABEL:
        return "no_rotation_detected_in_full_sample"
    if drop_top5 == _ROTATION_LABEL and drop_covid == _ROTATION_LABEL:
        return "rotation_persists_after_top_quarter_and_covid_post_trims"
    if drop_top5 != _ROTATION_LABEL and drop_covid == _ROTATION_LABEL:
        return "rotation_concentrated_in_top_gap_quarters"
    if drop_top5 == _ROTATION_LABEL and drop_covid != _ROTATION_LABEL:
        return "rotation_is_mostly_covid_post_specific"
    return "rotation_fragile_under_both_top_quarter_and_covid_post_trims"


def build_strict_shock_composition_summary(
    *,
    shocked: pd.DataFrame,
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
        "strict_loan_source_qoq",
        "strict_identifiable_total_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_shock_composition_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    frame["period_bucket"] = frame["quarter"].map(_period_bucket)
    frame["shock_gap"] = frame[excluded_shock_column] - frame[baseline_shock_column]
    frame["abs_shock_gap"] = frame["shock_gap"].abs()
    frame["baseline_minus_excluded_target_qoq"] = frame["tdc_bank_only_qoq"] - frame["tdc_no_toc_no_row_bank_only_qoq"]
    frame["row_leg_qoq"] = frame["tdc_row_treasury_transactions_qoq"]
    frame["toc_signed_contribution_qoq"] = -frame["tdc_treasury_operating_cash_qoq"]

    top_gap_quarters = _top_gap_quarter_profiles(frame)
    period_bucket_profiles = _period_bucket_profiles(frame)
    trim_diagnostics = _trim_diagnostics(frame)
    interpretation = _composition_interpretation(trim_diagnostics=trim_diagnostics)

    takeaways = []
    dominant_bucket = period_bucket_profiles[0]["period_bucket"] if period_bucket_profiles else None
    if dominant_bucket is not None:
        takeaways.append(
            f"The largest absolute shock-gap share sits in `{dominant_bucket}`, but the rotation is not limited to a single episode."
        )
    full = dict(trim_diagnostics.get("full_sample", {}))
    top5 = dict(trim_diagnostics.get("drop_top5_gap_quarters", {}))
    drop_covid = dict(trim_diagnostics.get("drop_covid_post", {}))
    if full and top5 and top5.get("shock_corr") is not None and top5.get("same_sign_share") is not None:
        takeaways.append(
            "Dropping the five largest shock-gap quarters changes the overlap structure to "
            f"corr ≈ {float(top5.get('shock_corr')):.2f}, same-sign share ≈ {float(top5.get('same_sign_share')):.2f}, "
            f"interpretation = `{str(top5.get('interpretation'))}`."
        )
    if drop_covid and drop_covid.get("shock_corr") is not None and drop_covid.get("same_sign_share") is not None:
        takeaways.append(
            "Dropping the full `covid_post` bucket changes the overlap structure to "
            f"corr ≈ {float(drop_covid.get('shock_corr')):.2f}, same-sign share ≈ {float(drop_covid.get('same_sign_share')):.2f}, "
            f"interpretation = `{str(drop_covid.get('interpretation'))}`."
        )
    if top_gap_quarters:
        first = top_gap_quarters[0]
        takeaways.append(
            "The largest single quarter right now is "
            f"`{first['quarter']}` ({first['period_bucket']}), with shock gap ≈ {float(first['shock_gap']):.2f} "
            f"and TOC/ROW bundle ≈ {float(first['baseline_minus_excluded_target_qoq']):.2f}."
        )

    return {
        "status": "available",
        "headline_question": "Is the baseline-versus-excluded shock rotation driven by a handful of quarters or by a broader sample-composition problem?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_shock_composition_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
        },
        "top_gap_quarters": top_gap_quarters,
        "period_bucket_profiles": period_bucket_profiles,
        "trim_diagnostics": trim_diagnostics,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
