from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

VALIDATION_PROXY_OUTCOMES: tuple[str, ...] = (
    "closed_end_residential_loans_qoq",
    "closed_end_residential_loans_ex_chargeoffs_qoq",
    "consumer_loans_qoq",
    "consumer_loans_ex_chargeoffs_qoq",
)

EXTENSION_CANDIDATE_OUTCOMES: tuple[str, ...] = (
    "commercial_industrial_loans_qoq",
    "commercial_industrial_loans_ex_chargeoffs_qoq",
    "construction_land_development_loans_qoq",
    "cre_multifamily_loans_qoq",
    "cre_nonfarm_nonresidential_loans_qoq",
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


def _abs_gap(candidate: dict[str, Any] | None, residual: dict[str, Any] | None) -> float | None:
    if candidate is None or residual is None:
        return None
    return abs(float(candidate["beta"]) - float(residual["beta"]))


def _same_sign(candidate: dict[str, Any] | None, residual: dict[str, Any] | None) -> bool | None:
    if candidate is None or residual is None:
        return None
    candidate_beta = float(candidate["beta"])
    residual_beta = float(residual["beta"])
    return (candidate_beta >= 0.0 and residual_beta >= 0.0) or (candidate_beta <= 0.0 and residual_beta <= 0.0)


def _rank_group(
    *,
    lp_irf: pd.DataFrame,
    residual_outcome: str,
    horizon: int,
    outcomes: Sequence[str],
) -> dict[str, Any]:
    residual = _snapshot(_lp_row(lp_irf, outcome=residual_outcome, horizon=horizon))
    ranked: list[dict[str, Any]] = []
    for outcome in outcomes:
        candidate = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if candidate is None:
            continue
        ranked.append(
            {
                "outcome": outcome,
                "response": candidate,
                "abs_gap_to_residual_beta": _abs_gap(candidate, residual),
                "same_sign_as_residual": _same_sign(candidate, residual),
            }
        )
    ranked.sort(
        key=lambda item: (
            float("inf") if item["abs_gap_to_residual_beta"] is None else float(item["abs_gap_to_residual_beta"]),
            item["outcome"],
        )
    )
    return {
        "residual_response": residual,
        "ranked_candidates": ranked,
        "best_candidate": ranked[0] if ranked else None,
    }


def build_strict_additional_creator_candidate_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_release_framing_summary: dict[str, Any] | None,
    strict_direct_core_horizon_stability_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    if (
        strict_release_framing_summary is None
        or str(strict_release_framing_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_release_framing_summary_not_available"}
    if (
        strict_direct_core_horizon_stability_summary is None
        or str(strict_direct_core_horizon_stability_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_direct_core_horizon_stability_summary_not_available"}

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
            "other_component_qoq",
            "other_component_core_deposit_proximate_bank_only_qoq",
            "strict_loan_core_min_qoq",
            "strict_loan_mortgages_qoq",
            *VALIDATION_PROXY_OUTCOMES,
            *EXTENSION_CANDIDATE_OUTCOMES,
        ]
    )
    available_outcomes = [outcome for outcome in outcome_cols if outcome in shocked.columns]

    baseline_lp_irf = run_local_projections(
        shocked,
        shock_col=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
        outcome_cols=[outcome for outcome in available_outcomes if outcome != "other_component_core_deposit_proximate_bank_only_qoq"],
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_additional_creator_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        shocked,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=[outcome for outcome in available_outcomes if outcome != "other_component_qoq"],
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_additional_creator_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_validation = _rank_group(
            lp_irf=baseline_lp_irf,
            residual_outcome="other_component_qoq",
            horizon=horizon,
            outcomes=VALIDATION_PROXY_OUTCOMES,
        )
        baseline_extension = _rank_group(
            lp_irf=baseline_lp_irf,
            residual_outcome="other_component_qoq",
            horizon=horizon,
            outcomes=EXTENSION_CANDIDATE_OUTCOMES,
        )
        core_validation = _rank_group(
            lp_irf=core_lp_irf,
            residual_outcome="other_component_core_deposit_proximate_bank_only_qoq",
            horizon=horizon,
            outcomes=VALIDATION_PROXY_OUTCOMES,
        )
        core_extension = _rank_group(
            lp_irf=core_lp_irf,
            residual_outcome="other_component_core_deposit_proximate_bank_only_qoq",
            horizon=horizon,
            outcomes=EXTENSION_CANDIDATE_OUTCOMES,
        )
        key_horizons[f"h{horizon}"] = {
            "baseline": {
                "validation_proxies": baseline_validation,
                "extension_candidates": baseline_extension,
            },
            "core_deposit_proximate": {
                "validation_proxies": core_validation,
                "extension_candidates": core_extension,
            },
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    best_validation = dict(h0_core.get("validation_proxies", {}) or {}).get("best_candidate") or {}
    best_extension = dict(h0_core.get("extension_candidates", {}) or {}).get("best_candidate") or {}
    impact_horizon_candidate = str(
        dict(strict_direct_core_horizon_stability_summary.get("recommendation", {}) or {}).get(
            "impact_candidate", "not_available"
        )
    )

    recommendation_status = "no_additional_extension_candidate_supported"
    next_branch = "freeze_creator_search_and_only_reopen_if_new_same_scope_channel_appears"
    if best_extension:
        same_sign = bool(best_extension.get("same_sign_as_residual"))
        response = dict(best_extension.get("response", {}) or {})
        abs_gap = best_extension.get("abs_gap_to_residual_beta")
        if same_sign and response.get("ci_excludes_zero") and abs_gap is not None and float(abs_gap) <= 5.0:
            recommendation_status = "extension_candidate_deserves_review"
            next_branch = "test_best_extension_candidate_against_direct_core_release_rule"

    takeaways = [
        "This surface asks a narrower question than the earlier bridge work: are there any additional mechanical creator candidates beyond the current direct core, or only broader validation proxies?",
        "Validation proxies are broad all-bank residential and consumer loan families that overlap the current direct core conceptually; extension candidates are business, CRE, and clearly noncore loan families outside the current direct core.",
    ]
    residual_h0 = dict((h0_core.get("validation_proxies") or {}).get("residual_response") or {})
    if residual_h0 and best_validation:
        best_validation_response = dict(best_validation.get("response", {}) or {})
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the closest broad validation proxy is "
            f"`{best_validation.get('outcome', 'not_available')}` with beta ≈ {float(best_validation_response.get('beta', 0.0)):.2f} "
            f"against residual ≈ {float(residual_h0.get('beta', 0.0)):.2f}."
        )
    if best_extension:
        best_extension_response = dict(best_extension.get("response", {}) or {})
        takeaways.append(
            "The closest additional extension candidate at h0 is "
            f"`{best_extension.get('outcome', 'not_available')}` with beta ≈ {float(best_extension_response.get('beta', 0.0)):.2f}; "
            "this is the best remaining non-core creator candidate, not a promoted strict channel."
        )
    if impact_horizon_candidate != "not_available":
        takeaways.append(
            f"The impact-horizon direct-core candidate remains `{impact_horizon_candidate}`, so broad validation proxies should be read mainly as cross-checks on that existing channel rather than as new strict components."
        )
    takeaways.append(f"Current recommendation: `{recommendation_status}`.")

    return {
        "status": "available",
        "headline_question": "Beyond the current direct core, do any remaining creator channels deserve strict-candidate status, or do they only serve as broader validation proxies?",
        "estimation_path": {
            "summary_artifact": "strict_additional_creator_candidate_summary.json",
            "baseline_spec_name": "strict_additional_creator_baseline_reference",
            "core_spec_name": "strict_additional_creator_core_reference",
            "source_artifacts": [
                "strict_release_framing_summary.json",
                "strict_direct_core_horizon_stability_summary.json",
            ],
        },
        "candidate_groups": {
            "validation_proxies": list(VALIDATION_PROXY_OUTCOMES),
            "extension_candidates": list(EXTENSION_CANDIDATE_OUTCOMES),
        },
        "key_horizons": key_horizons,
        "classification": {
            "impact_horizon_direct_core_candidate": impact_horizon_candidate,
            "h0_best_validation_proxy": best_validation.get("outcome"),
            "h0_best_extension_candidate": best_extension.get("outcome"),
            "recommendation_status": recommendation_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "headline_direct_core": "strict_loan_core_min_qoq",
            "impact_horizon_candidate": impact_horizon_candidate,
            "best_validation_proxy": best_validation.get("outcome"),
            "best_extension_candidate": best_extension.get("outcome"),
            "next_branch": next_branch,
        },
        "takeaways": takeaways,
    }
