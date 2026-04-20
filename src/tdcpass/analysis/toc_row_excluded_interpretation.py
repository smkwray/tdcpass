from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.identity_baseline import build_identity_baseline_irf
from tdcpass.analysis.local_projections import run_local_projections


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _share(numerator: dict[str, Any] | None, denominator: dict[str, Any] | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    denominator_beta = float(denominator["beta"])
    if denominator_beta == 0.0:
        return None
    return abs(float(numerator["beta"])) / abs(denominator_beta)


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _comparison_controls(
    *,
    baseline_controls: Sequence[str],
    baseline_shock_spec: dict[str, Any],
    comparison_shock_spec: dict[str, Any],
) -> list[str]:
    baseline_predictors = {str(value) for value in baseline_shock_spec.get("predictors", [])}
    macro_controls = [str(value) for value in baseline_controls if str(value) not in baseline_predictors]
    comparison_predictors = [str(value) for value in comparison_shock_spec.get("predictors", [])]
    return _dedupe([*comparison_predictors, *macro_controls])


def _interpretation_label(
    *,
    baseline_residual: dict[str, Any] | None,
    excluded_residual: dict[str, Any] | None,
    baseline_gap_share: float | None,
    excluded_gap_share: float | None,
) -> str:
    if baseline_residual is None or excluded_residual is None:
        return "missing_identity_inputs"
    baseline_beta = abs(float(baseline_residual["beta"]))
    excluded_beta = abs(float(excluded_residual["beta"]))
    if baseline_beta == 0.0:
        return "baseline_residual_zero"
    residual_ratio = excluded_beta / baseline_beta
    if residual_ratio <= 0.25:
        if excluded_gap_share is not None and excluded_gap_share <= 0.25:
            return "toc_row_exclusion_removes_most_residual_and_strict_gap"
        return "toc_row_exclusion_removes_most_residual_but_strict_gap_remains"
    if residual_ratio <= 0.6:
        if excluded_gap_share is not None and baseline_gap_share is not None and excluded_gap_share < baseline_gap_share:
            return "toc_row_exclusion_materially_relaxes_residual_and_strict_gap"
        return "toc_row_exclusion_materially_relaxes_residual"
    if (
        excluded_gap_share is not None
        and baseline_gap_share is not None
        and excluded_gap_share >= baseline_gap_share - 0.05
    ):
        return "toc_row_exclusion_changes_residual_but_not_strict_gap_story"
    return "toc_row_exclusion_partial_shift"


def build_toc_row_excluded_interpretation_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    excluded_shock_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
    bootstrap_reps: int = 40,
    bootstrap_block_length: int = 4,
) -> dict[str, Any]:
    baseline_controls = [str(value) for value in baseline_lp_spec.get("controls", [])]
    baseline_horizons = [int(value) for value in baseline_lp_spec.get("horizons", [])]
    cumulative = bool(baseline_lp_spec.get("cumulative", True))
    include_lagged_outcome = bool(baseline_lp_spec.get("include_lagged_outcome", False))
    nw_lags = int(baseline_lp_spec.get("nw_lags", 4))

    baseline_identity = build_identity_baseline_irf(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        tdc_outcome_col=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
        total_outcome_col="total_deposits_bank_qoq",
        controls=baseline_controls,
        horizons=baseline_horizons,
        cumulative=cumulative,
        spec_name="toc_row_excluded_baseline_reference",
        bootstrap_reps=bootstrap_reps,
        bootstrap_block_length=bootstrap_block_length,
        nested_shock_spec=dict(baseline_shock_spec),
    )

    excluded_controls = _comparison_controls(
        baseline_controls=baseline_controls,
        baseline_shock_spec=baseline_shock_spec,
        comparison_shock_spec=excluded_shock_spec,
    )
    excluded_identity = build_identity_baseline_irf(
        shocked,
        shock_col=str(excluded_shock_spec.get("standardized_column", "tdc_no_toc_no_row_bank_only_residual_z")),
        tdc_outcome_col=str(excluded_shock_spec.get("target", "tdc_no_toc_no_row_bank_only_qoq")),
        total_outcome_col="total_deposits_bank_qoq",
        controls=excluded_controls,
        horizons=baseline_horizons,
        cumulative=cumulative,
        spec_name="toc_row_excluded_identity_reference",
        bootstrap_reps=bootstrap_reps,
        bootstrap_block_length=bootstrap_block_length,
        nested_shock_spec=dict(excluded_shock_spec),
    )

    strict_reference = run_local_projections(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=["strict_identifiable_total_qoq", "strict_identifiable_gap_qoq"],
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="toc_row_excluded_baseline_strict_reference",
    )

    excluded_frame = shocked.copy()
    excluded_frame["strict_identifiable_gap_no_toc_no_row_qoq"] = (
        excluded_frame["other_component_no_toc_no_row_bank_only_qoq"] - excluded_frame["strict_identifiable_total_qoq"]
    )
    excluded_frame["lag_strict_identifiable_gap_no_toc_no_row_qoq"] = excluded_frame[
        "strict_identifiable_gap_no_toc_no_row_qoq"
    ].shift(1)
    strict_excluded = run_local_projections(
        excluded_frame,
        shock_col=str(excluded_shock_spec.get("standardized_column", "tdc_no_toc_no_row_bank_only_residual_z")),
        outcome_cols=["strict_identifiable_total_qoq", "strict_identifiable_gap_no_toc_no_row_qoq"],
        controls=excluded_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="toc_row_excluded_strict_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_tdc = _snapshot(
            _lp_row(baseline_identity, outcome=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")), horizon=horizon)
        )
        baseline_total = _snapshot(_lp_row(baseline_identity, outcome="total_deposits_bank_qoq", horizon=horizon))
        baseline_residual = _snapshot(_lp_row(baseline_identity, outcome="other_component_qoq", horizon=horizon))
        baseline_strict_total = _snapshot(_lp_row(strict_reference, outcome="strict_identifiable_total_qoq", horizon=horizon))
        baseline_strict_gap = _snapshot(_lp_row(strict_reference, outcome="strict_identifiable_gap_qoq", horizon=horizon))

        excluded_tdc = _snapshot(
            _lp_row(
                excluded_identity,
                outcome=str(excluded_shock_spec.get("target", "tdc_no_toc_no_row_bank_only_qoq")),
                horizon=horizon,
            )
        )
        excluded_total = _snapshot(_lp_row(excluded_identity, outcome="total_deposits_bank_qoq", horizon=horizon))
        excluded_residual = _snapshot(_lp_row(excluded_identity, outcome="other_component_qoq", horizon=horizon))
        excluded_strict_total = _snapshot(_lp_row(strict_excluded, outcome="strict_identifiable_total_qoq", horizon=horizon))
        excluded_strict_gap = _snapshot(
            _lp_row(strict_excluded, outcome="strict_identifiable_gap_no_toc_no_row_qoq", horizon=horizon)
        )

        if all(
            item is None
            for item in (
                baseline_tdc,
                baseline_total,
                baseline_residual,
                baseline_strict_total,
                baseline_strict_gap,
                excluded_tdc,
                excluded_total,
                excluded_residual,
                excluded_strict_total,
                excluded_strict_gap,
            )
        ):
            continue

        baseline_gap_share = _share(baseline_strict_gap, baseline_residual)
        excluded_gap_share = _share(excluded_strict_gap, excluded_residual)
        key_horizons[f"h{horizon}"] = {
            "baseline": {
                "tdc_response": baseline_tdc,
                "total_deposits_response": baseline_total,
                "residual_response": baseline_residual,
                "strict_identifiable_total_response": baseline_strict_total,
                "strict_identifiable_gap_response": baseline_strict_gap,
                "strict_gap_share_of_residual": baseline_gap_share,
            },
            "toc_row_excluded": {
                "tdc_response": excluded_tdc,
                "total_deposits_response": excluded_total,
                "residual_response": excluded_residual,
                "strict_identifiable_total_response": excluded_strict_total,
                "strict_identifiable_gap_response": excluded_strict_gap,
                "strict_gap_share_of_residual": excluded_gap_share,
            },
            "excluded_minus_baseline_beta": {
                "tdc_response": None
                if baseline_tdc is None or excluded_tdc is None
                else float(excluded_tdc["beta"]) - float(baseline_tdc["beta"]),
                "total_deposits_response": None
                if baseline_total is None or excluded_total is None
                else float(excluded_total["beta"]) - float(baseline_total["beta"]),
                "residual_response": None
                if baseline_residual is None or excluded_residual is None
                else float(excluded_residual["beta"]) - float(baseline_residual["beta"]),
                "strict_identifiable_total_response": None
                if baseline_strict_total is None or excluded_strict_total is None
                else float(excluded_strict_total["beta"]) - float(baseline_strict_total["beta"]),
                "strict_identifiable_gap_response": None
                if baseline_strict_gap is None or excluded_strict_gap is None
                else float(excluded_strict_gap["beta"]) - float(baseline_strict_gap["beta"]),
                "strict_gap_share_of_residual": None
                if baseline_gap_share is None or excluded_gap_share is None
                else float(excluded_gap_share) - float(baseline_gap_share),
            },
            "interpretation": _interpretation_label(
                baseline_residual=baseline_residual,
                excluded_residual=excluded_residual,
                baseline_gap_share=baseline_gap_share,
                excluded_gap_share=excluded_gap_share,
            ),
        }

    if not key_horizons:
        return {
            "status": "not_available",
            "headline_question": "How different does the residual and strict-gap read look if TOC/ROW is excluded as a secondary comparison object?",
            "estimation_path": {
                "summary_artifact": "toc_row_excluded_interpretation_summary.json",
            },
            "takeaways": ["TOC/ROW-excluded interpretation diagnostics were not available in the current run."],
        }

    takeaways = [
        "This is a secondary comparison surface only: it excludes the combined TOC/ROW bundle from the treatment object to test how much of the current residual and strict-gap story is tied to that suspect block, without relabeling the headline treatment.",
    ]
    h0 = key_horizons.get("h0", {})
    baseline_h0 = dict(h0.get("baseline", {}))
    excluded_h0 = dict(h0.get("toc_row_excluded", {}))
    baseline_h0_residual = dict(baseline_h0.get("residual_response", {}) or {})
    excluded_h0_residual = dict(excluded_h0.get("residual_response", {}) or {})
    baseline_h0_gap_share = baseline_h0.get("strict_gap_share_of_residual")
    excluded_h0_gap_share = excluded_h0.get("strict_gap_share_of_residual")
    if baseline_h0_residual and excluded_h0_residual:
        takeaways.append(
            f"At h0, excluding TOC/ROW changes the non-TDC residual response from about {float(baseline_h0_residual['beta']):.2f} to about {float(excluded_h0_residual['beta']):.2f}."
        )
    if baseline_h0_gap_share is not None and excluded_h0_gap_share is not None:
        takeaways.append(
            f"At h0, the strict direct-count gap share moves from about {float(baseline_h0_gap_share):.2f} under the baseline treatment to about {float(excluded_h0_gap_share):.2f} under the TOC/ROW-excluded comparison."
        )
    h4 = key_horizons.get("h4", {})
    excluded_h4_gap_share = dict(h4.get("toc_row_excluded", {})).get("strict_gap_share_of_residual")
    if excluded_h4_gap_share is not None:
        takeaways.append(
            f"At h4, the TOC/ROW-excluded strict gap share is about {float(excluded_h4_gap_share):.2f}, which helps separate treatment-object-definition risk from missing direct-count channels."
        )

    return {
        "status": "available",
        "headline_question": "How different does the residual and strict-gap read look if TOC/ROW is excluded as a secondary comparison object?",
        "estimation_path": {
            "baseline_identity_spec_name": "toc_row_excluded_baseline_reference",
            "toc_row_excluded_identity_spec_name": "toc_row_excluded_identity_reference",
            "baseline_strict_spec_name": "toc_row_excluded_baseline_strict_reference",
            "toc_row_excluded_strict_spec_name": "toc_row_excluded_strict_reference",
            "baseline_shock_column": str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
            "toc_row_excluded_shock_column": str(
                excluded_shock_spec.get("standardized_column", "tdc_no_toc_no_row_bank_only_residual_z")
            ),
            "summary_artifact": "toc_row_excluded_interpretation_summary.json",
        },
        "comparison_definition": {
            "headline_treatment": str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
            "secondary_toc_row_excluded_treatment": str(
                excluded_shock_spec.get("target", "tdc_no_toc_no_row_bank_only_qoq")
            ),
            "strict_gap_secondary_outcome": "strict_identifiable_gap_no_toc_no_row_qoq",
            "release_role": "secondary_interpretation_only",
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
