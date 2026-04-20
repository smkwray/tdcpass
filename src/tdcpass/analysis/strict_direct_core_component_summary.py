from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

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


def _abs_gap(candidate: dict[str, Any] | None, residual: dict[str, Any] | None) -> float | None:
    if candidate is None or residual is None:
        return None
    return abs(float(candidate["beta"]) - float(residual["beta"]))


def _dominant_component(
    *,
    mortgages: dict[str, Any] | None,
    consumer_credit: dict[str, Any] | None,
) -> str:
    if mortgages is None and consumer_credit is None:
        return "not_available"
    if mortgages is None:
        return "strict_loan_consumer_credit_qoq"
    if consumer_credit is None:
        return "strict_loan_mortgages_qoq"
    mort_beta = abs(float(mortgages["beta"]))
    cc_beta = abs(float(consumer_credit["beta"]))
    if mort_beta > cc_beta:
        return "strict_loan_mortgages_qoq"
    if cc_beta > mort_beta:
        return "strict_loan_consumer_credit_qoq"
    return "tie"


def _component_payload(
    *,
    residual: dict[str, Any] | None,
    mortgages: dict[str, Any] | None,
    consumer_credit: dict[str, Any] | None,
    direct_core: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "residual_response": residual,
        "mortgages_response": mortgages,
        "consumer_credit_response": consumer_credit,
        "direct_core_response": direct_core,
        "candidate_abs_gap_to_residual_beta": {
            "strict_loan_mortgages_qoq": _abs_gap(mortgages, residual),
            "strict_loan_consumer_credit_qoq": _abs_gap(consumer_credit, residual),
            "strict_loan_core_min_qoq": _abs_gap(direct_core, residual),
        },
        "dominant_component_by_abs_beta": _dominant_component(
            mortgages=mortgages,
            consumer_credit=consumer_credit,
        ),
    }


def build_strict_direct_core_component_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_release_framing_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    if (
        strict_release_framing_summary is None
        or str(strict_release_framing_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_release_framing_summary_not_available"}

    baseline_controls = [str(value) for value in baseline_lp_spec.get("controls", [])]
    core_controls = _comparison_controls(
        baseline_controls=baseline_controls,
        baseline_shock_spec=baseline_shock_spec,
        comparison_shock_spec=core_shock_spec,
    )
    baseline_horizons = [int(value) for value in baseline_lp_spec.get("horizons", [])]
    cumulative = bool(baseline_lp_spec.get("cumulative", True))
    include_lagged_outcome = bool(baseline_lp_spec.get("include_lagged_outcome", False))
    nw_lags = int(baseline_lp_spec.get("nw_lags", 4))

    baseline_lp_irf = run_local_projections(
        shocked,
        shock_col=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
        outcome_cols=[
            "other_component_qoq",
            "strict_loan_mortgages_qoq",
            "strict_loan_consumer_credit_qoq",
            "strict_loan_core_min_qoq",
        ],
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_direct_core_component_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        shocked,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=[
            "other_component_core_deposit_proximate_bank_only_qoq",
            "strict_loan_mortgages_qoq",
            "strict_loan_consumer_credit_qoq",
            "strict_loan_core_min_qoq",
        ],
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_direct_core_component_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _component_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            mortgages=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_mortgages_qoq", horizon=horizon)),
            consumer_credit=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_loan_consumer_credit_qoq", horizon=horizon)
            ),
            direct_core=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
        )
        core_payload = _component_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            mortgages=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_mortgages_qoq", horizon=horizon)),
            consumer_credit=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_loan_consumer_credit_qoq", horizon=horizon)
            ),
            direct_core=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
        )
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "core_deposit_proximate": core_payload,
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    gaps = dict(h0_core.get("candidate_abs_gap_to_residual_beta", {}) or {})
    mortgage_gap = gaps.get("strict_loan_mortgages_qoq")
    consumer_gap = gaps.get("strict_loan_consumer_credit_qoq")
    direct_gap = gaps.get("strict_loan_core_min_qoq")
    dominant_h0 = str(h0_core.get("dominant_component_by_abs_beta", "not_available"))

    recommendation_status = "keep_bundled_direct_core"
    if (
        direct_gap is not None
        and consumer_gap is not None
        and mortgage_gap is not None
        and float(consumer_gap) < float(direct_gap)
        and float(consumer_gap) <= float(mortgage_gap)
    ):
        recommendation_status = "consumer_credit_only_candidate_deserves_review"
    elif (
        direct_gap is not None
        and consumer_gap is not None
        and mortgage_gap is not None
        and float(mortgage_gap) < float(direct_gap)
        and float(mortgage_gap) < float(consumer_gap)
    ):
        recommendation_status = "mortgages_only_candidate_deserves_review"

    takeaways = [
        "This summary asks whether the current headline strict direct core should stay bundled or narrow further inside the current non-TOC/ROW framework.",
        "The comparison is intentionally narrow: mortgages only, consumer credit only, and the current bundled direct core, all judged against the same residual object.",
    ]
    residual_h0 = dict(h0_core.get("residual_response", {}) or {})
    mortgages_h0 = dict(h0_core.get("mortgages_response", {}) or {})
    consumer_h0 = dict(h0_core.get("consumer_credit_response", {}) or {})
    direct_h0 = dict(h0_core.get("direct_core_response", {}) or {})
    if residual_h0 and mortgages_h0 and consumer_h0 and direct_h0:
        takeaways.append(
            "At h0 under the core-deposit-proximate shock: "
            f"residual ≈ {float(residual_h0['beta']):.2f}, "
            f"mortgages ≈ {float(mortgages_h0['beta']):.2f}, "
            f"consumer credit ≈ {float(consumer_h0['beta']):.2f}, "
            f"bundled direct core ≈ {float(direct_h0['beta']):.2f}."
        )
    if dominant_h0 != "not_available":
        takeaways.append(f"The dominant direct-core subcomponent at h0 is `{dominant_h0}`.")
    takeaways.append(
        "Current recommendation: "
        f"`{recommendation_status}`."
    )

    return {
        "status": "available",
        "headline_question": "Should the current strict direct core remain bundled, or is one direct-core subcomponent the real headline candidate under the current non-TOC/ROW framework?",
        "estimation_path": {
            "summary_artifact": "strict_direct_core_component_summary.json",
            "baseline_spec_name": "strict_direct_core_component_baseline_reference",
            "core_spec_name": "strict_direct_core_component_core_reference",
            "source_artifacts": [
                "strict_release_framing_summary.json",
            ],
        },
        "candidate_definitions": {
            "headline_direct_core": "strict_loan_core_min_qoq",
            "mortgages_only_candidate": "strict_loan_mortgages_qoq",
            "consumer_credit_only_candidate": "strict_loan_consumer_credit_qoq",
        },
        "key_horizons": key_horizons,
        "classification": {
            "h0_dominant_component": dominant_h0,
            "recommendation_status": recommendation_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "headline_direct_core": "strict_loan_core_min_qoq",
            "next_branch": (
                "keep_bundled_core_and_reassess_other_strict_creator_channels"
                if recommendation_status == "keep_bundled_direct_core"
                else "test_narrower_single_component_direct_core_candidate"
            ),
        },
        "takeaways": takeaways,
    }
