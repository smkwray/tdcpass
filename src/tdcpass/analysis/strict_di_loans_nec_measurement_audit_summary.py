from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

TARGET_OUTCOME = "strict_loan_di_loans_nec_qoq"

CROSS_SCOPE_TRANSACTION_BRIDGE_OUTCOMES: tuple[str, ...] = (
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
    "strict_di_loans_nec_systemwide_liability_total_qoq",
)

SAME_SCOPE_PROXY_OUTCOMES: tuple[str, ...] = (
    "loans_to_commercial_banks_qoq",
    "loans_to_nondepository_financial_institutions_qoq",
    "loans_for_purchasing_or_carrying_securities_qoq",
)


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


def _abs_gap(candidate: dict[str, Any] | None, target: dict[str, Any] | None) -> float | None:
    if candidate is None or target is None:
        return None
    return abs(float(candidate["beta"]) - float(target["beta"]))


def _same_sign(candidate: dict[str, Any] | None, target: dict[str, Any] | None) -> bool | None:
    if candidate is None or target is None:
        return None
    candidate_beta = float(candidate["beta"])
    target_beta = float(target["beta"])
    return (candidate_beta >= 0.0 and target_beta >= 0.0) or (candidate_beta <= 0.0 and target_beta <= 0.0)


def _rank_group(
    *,
    lp_irf: pd.DataFrame,
    horizon: int,
    outcomes: Sequence[str],
) -> dict[str, Any]:
    target = _snapshot(_lp_row(lp_irf, outcome=TARGET_OUTCOME, horizon=horizon))
    ranked: list[dict[str, Any]] = []
    for outcome in outcomes:
        candidate = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if candidate is None:
            continue
        ranked.append(
            {
                "outcome": outcome,
                "response": candidate,
                "abs_gap_to_target_beta": _abs_gap(candidate, target),
                "same_sign_as_target": _same_sign(candidate, target),
            }
        )
    ranked.sort(
        key=lambda item: float("inf") if item["abs_gap_to_target_beta"] is None else float(item["abs_gap_to_target_beta"])
    )
    return {
        "target_response": target,
        "ranked_candidates": ranked,
        "best_candidate": ranked[0] if ranked else None,
    }


def build_strict_di_loans_nec_measurement_audit_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_release_framing_summary: dict[str, Any] | None,
    strict_di_bucket_bridge_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if (
        strict_release_framing_summary is None
        or str(strict_release_framing_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_release_framing_summary_not_available"}
    if (
        strict_di_bucket_bridge_summary is None
        or str(strict_di_bucket_bridge_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_di_bucket_bridge_summary_not_available"}

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

    outcome_cols = _dedupe(
        [
            TARGET_OUTCOME,
            *CROSS_SCOPE_TRANSACTION_BRIDGE_OUTCOMES,
            *SAME_SCOPE_PROXY_OUTCOMES,
        ]
    )
    available_outcomes = [outcome for outcome in outcome_cols if outcome in shocked.columns]

    baseline_lp_irf = run_local_projections(
        shocked,
        shock_col=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
        outcome_cols=available_outcomes,
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_di_loans_nec_measurement_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        shocked,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=available_outcomes,
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_di_loans_nec_measurement_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        key_horizons[f"h{horizon}"] = {
            "baseline": {
                "cross_scope_transaction_bridges": _rank_group(
                    lp_irf=baseline_lp_irf,
                    horizon=horizon,
                    outcomes=CROSS_SCOPE_TRANSACTION_BRIDGE_OUTCOMES,
                ),
                "same_scope_proxies": _rank_group(
                    lp_irf=baseline_lp_irf,
                    horizon=horizon,
                    outcomes=SAME_SCOPE_PROXY_OUTCOMES,
                ),
            },
            "core_deposit_proximate": {
                "cross_scope_transaction_bridges": _rank_group(
                    lp_irf=core_lp_irf,
                    horizon=horizon,
                    outcomes=CROSS_SCOPE_TRANSACTION_BRIDGE_OUTCOMES,
                ),
                "same_scope_proxies": _rank_group(
                    lp_irf=core_lp_irf,
                    horizon=horizon,
                    outcomes=SAME_SCOPE_PROXY_OUTCOMES,
                ),
            },
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    best_cross_scope = dict(h0_core.get("cross_scope_transaction_bridges", {}) or {}).get("best_candidate") or {}
    best_proxy = dict(h0_core.get("same_scope_proxies", {}) or {}).get("best_candidate") or {}

    recommendation_status = "no_promotable_same_scope_transaction_subcomponent_supported"
    next_branch = "freeze_framework_and_move_to_writeup_if_no_new_public_transaction_split_appears"

    takeaways = [
        "This audit asks a narrower measurement question than the bridge surfaces: can the broad DI-loans-n.e.c. lender transaction row be split into any clean same-scope transaction-based subcomponent using current public data?",
        "The answer is constrained by the current data map itself: the repo has one direct lender-side transaction row for the DI bucket, a cross-scope borrower-counterpart family, and only proxy-level same-scope slices for likely noncore pieces.",
    ]
    target_h0 = dict(h0_core.get("cross_scope_transaction_bridges", {}) or {}).get("target_response") or {}
    if target_h0 and best_cross_scope:
        best_cross_scope_response = dict(best_cross_scope.get("response", {}) or {})
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the closest cross-scope transaction bridge is "
            f"`{best_cross_scope.get('outcome', 'not_available')}` with beta ≈ {float(best_cross_scope_response.get('beta', 0.0)):.2f} "
            f"against DI aggregate ≈ {float(target_h0.get('beta', 0.0)):.2f}."
        )
    if target_h0 and best_proxy:
        best_proxy_response = dict(best_proxy.get("response", {}) or {})
        takeaways.append(
            "The closest same-scope proxy slice is "
            f"`{best_proxy.get('outcome', 'not_available')}` with beta ≈ {float(best_proxy_response.get('beta', 0.0)):.2f}, "
            "but it remains a proxy-level level-change family rather than a clean lender-side transaction split."
        )
    takeaways.append(
        "So the current public data do not support promoting any DI-loans-n.e.c. subcomponent into the strict object: the exact same-scope transaction subcomponent is unavailable, the best bridges are cross-scope, and the best same-scope slices are proxy-only."
    )

    return {
        "status": "available",
        "headline_question": "Does current public data isolate any same-scope transaction-based subcomponent inside `strict_loan_di_loans_nec_qoq` that deserves promotion into the strict object?",
        "estimation_path": {
            "summary_artifact": "strict_di_loans_nec_measurement_audit_summary.json",
            "target_outcome": TARGET_OUTCOME,
            "source_families": [
                "cross_scope_transaction_bridges",
                "same_scope_proxies",
            ],
        },
        "candidate_groups": {
            "same_scope_transaction_subcomponents": [],
            "cross_scope_transaction_bridges": list(CROSS_SCOPE_TRANSACTION_BRIDGE_OUTCOMES),
            "same_scope_proxies": list(SAME_SCOPE_PROXY_OUTCOMES),
        },
        "classification": {
            "same_scope_transaction_subcomponent_status": "not_available_from_current_public_data",
            "h0_best_cross_scope_transaction_bridge": best_cross_scope.get("outcome"),
            "h0_best_same_scope_proxy": best_proxy.get("outcome"),
            "promotion_gate": recommendation_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "strict_rule": "keep_di_bucket_out_of_strict_object",
            "next_branch": next_branch,
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
