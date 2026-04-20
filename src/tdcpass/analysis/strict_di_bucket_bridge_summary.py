from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_loan_di_loans_nec_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
    "strict_di_loans_nec_systemwide_liability_total_qoq",
    "strict_di_loans_nec_systemwide_borrower_total_qoq",
    "strict_di_loans_nec_systemwide_borrower_gap_qoq",
    "strict_loan_other_advances_qoq",
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_loan_di_loans_nec_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
    "strict_di_loans_nec_systemwide_liability_total_qoq",
    "strict_di_loans_nec_systemwide_borrower_total_qoq",
    "strict_di_loans_nec_systemwide_borrower_gap_qoq",
    "strict_loan_other_advances_qoq",
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


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _bridge_payload(
    *,
    residual: dict[str, Any] | None,
    di_asset: dict[str, Any] | None,
    private_borrower: dict[str, Any] | None,
    noncore_system: dict[str, Any] | None,
    systemwide_liability_total: dict[str, Any] | None,
    systemwide_borrower_total: dict[str, Any] | None,
    systemwide_borrower_gap: dict[str, Any] | None,
    other_advances: dict[str, Any] | None,
) -> dict[str, Any]:
    di_beta = None if di_asset is None else float(di_asset["beta"])
    private_beta = None if private_borrower is None else float(private_borrower["beta"])
    noncore_beta = None if noncore_system is None else float(noncore_system["beta"])
    liability_beta = None if systemwide_liability_total is None else float(systemwide_liability_total["beta"])
    borrower_total_beta = None if systemwide_borrower_total is None else float(systemwide_borrower_total["beta"])
    borrower_gap_beta = None if systemwide_borrower_gap is None else float(systemwide_borrower_gap["beta"])
    other_adv_beta = None if other_advances is None else float(other_advances["beta"])

    bridge_residual_beta = None
    if di_beta is not None and borrower_total_beta is not None:
        bridge_residual_beta = di_beta - borrower_total_beta

    private_share = _ratio(private_beta, di_beta)
    noncore_share = _ratio(noncore_beta, di_beta)
    borrower_total_share = _ratio(borrower_total_beta, di_beta)
    bridge_residual_share = _ratio(bridge_residual_beta, di_beta)
    usc_share_of_liability = _ratio(di_beta, liability_beta)

    interpretation = "bridge_not_classified"
    if bridge_residual_share is not None and abs(float(bridge_residual_share)) >= 0.5:
        interpretation = "cross_scope_bridge_residual_large"
    elif private_beta is not None and noncore_beta is not None:
        interpretation = (
            "private_borrower_bridge_dominant"
            if abs(float(private_beta)) >= abs(float(noncore_beta))
            else "noncore_system_bridge_dominant"
        )

    return {
        "core_residual_response": residual,
        "di_asset_response": di_asset,
        "private_borrower_bridge_response": private_borrower,
        "noncore_system_bridge_response": noncore_system,
        "systemwide_liability_total_response": systemwide_liability_total,
        "systemwide_borrower_total_response": systemwide_borrower_total,
        "systemwide_borrower_gap_response": systemwide_borrower_gap,
        "other_advances_response": other_advances,
        "bridge_residual_beta": bridge_residual_beta,
        "private_borrower_share_of_di_asset_beta": private_share,
        "noncore_system_share_of_di_asset_beta": noncore_share,
        "systemwide_borrower_total_share_of_di_asset_beta": borrower_total_share,
        "bridge_residual_share_of_di_asset_beta": bridge_residual_share,
        "us_chartered_di_asset_share_of_systemwide_liability_beta": usc_share_of_liability,
        "interpretation": interpretation,
        "borrower_gap_beta": borrower_gap_beta,
        "other_advances_beta": other_adv_beta,
    }


def build_strict_di_bucket_bridge_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_di_bucket_role_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_di_bucket_role_summary is None:
        return {"status": "not_available", "reason": "strict_di_bucket_role_summary_not_available"}
    if str(strict_di_bucket_role_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_di_bucket_role_summary_not_available"}

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
        outcome_cols=list(BASELINE_OUTCOMES),
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_di_bucket_bridge_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        shocked,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=list(CORE_OUTCOMES),
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_di_bucket_bridge_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _bridge_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            di_asset=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_di_loans_nec_qoq", horizon=horizon)),
            private_borrower=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_noncore_system_borrower_qoq", horizon=horizon)
            ),
            systemwide_liability_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_systemwide_liability_total_qoq", horizon=horizon)
            ),
            systemwide_borrower_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_total_qoq", horizon=horizon)
            ),
            systemwide_borrower_gap=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_gap_qoq", horizon=horizon)
            ),
            other_advances=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_other_advances_qoq", horizon=horizon)),
        )
        core_payload = _bridge_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            di_asset=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_di_loans_nec_qoq", horizon=horizon)),
            private_borrower=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_noncore_system_borrower_qoq", horizon=horizon)
            ),
            systemwide_liability_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_systemwide_liability_total_qoq", horizon=horizon)
            ),
            systemwide_borrower_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_total_qoq", horizon=horizon)
            ),
            systemwide_borrower_gap=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_gap_qoq", horizon=horizon)
            ),
            other_advances=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_other_advances_qoq", horizon=horizon)),
        )
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "core_deposit_proximate": core_payload,
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    h0_interpretation = str(h0_core.get("interpretation", "bridge_not_classified"))
    next_branch = {
        "cross_scope_bridge_residual_large": "build_counterpart_alignment_surface",
        "private_borrower_bridge_dominant": "build_narrow_private_borrower_bridge_surface",
        "noncore_system_bridge_dominant": "build_noncore_system_bridge_surface",
    }.get(h0_interpretation, "build_counterpart_alignment_surface")

    takeaways = [
        "This surface converts the broad DI-loans-n.e.c. diagnostic into an explicit bridge problem instead of a vague subtotal.",
        "The bridge rows remain mixed-scope by construction: the DI asset row is U.S.-chartered source-side, while the borrower rows are systemwide F.215 counterparts.",
    ]
    di_beta = dict(h0_core.get("di_asset_response", {}) or {}).get("beta")
    private_beta = dict(h0_core.get("private_borrower_bridge_response", {}) or {}).get("beta")
    noncore_beta = dict(h0_core.get("noncore_system_bridge_response", {}) or {}).get("beta")
    bridge_beta = h0_core.get("bridge_residual_beta")
    if None not in (di_beta, private_beta, noncore_beta, bridge_beta):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the DI bridge is now explicit: "
            f"DI asset ≈ {float(di_beta):.2f}, private-borrower bridge ≈ {float(private_beta):.2f}, "
            f"noncore/system bridge ≈ {float(noncore_beta):.2f}, bridge residual ≈ {float(bridge_beta):.2f}."
        )
    residual_share = h0_core.get("bridge_residual_share_of_di_asset_beta")
    usc_share = h0_core.get("us_chartered_di_asset_share_of_systemwide_liability_beta")
    if residual_share is not None and usc_share is not None:
        takeaways.append(
            f"The unresolved bridge residual is about {float(residual_share):.2f} of the signed DI-asset response at h0, while the U.S.-chartered DI asset is about {float(usc_share):.2f} of the signed systemwide liability counterpart."
        )

    return {
        "status": "available",
        "headline_question": "What does the broad DI-loans-n.e.c. bucket mean once it is treated as a bridge problem rather than a headline strict component?",
        "estimation_path": {
            "summary_artifact": "strict_di_bucket_bridge_summary.json",
            "baseline_spec": "identity_baseline",
            "comparison_spec": "unexpected_tdc_core_deposit_proximate_bank_only",
        },
        "bridge_definitions": {
            "di_asset": "strict_loan_di_loans_nec_qoq",
            "private_borrower_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
            "noncore_system_bridge": "strict_di_loans_nec_noncore_system_borrower_qoq",
            "systemwide_borrower_total": "strict_di_loans_nec_systemwide_borrower_total_qoq",
            "bridge_residual_formula": "strict_loan_di_loans_nec_qoq - strict_di_loans_nec_systemwide_borrower_total_qoq",
        },
        "recommendation": {
            "status": "bridge_surface_first",
            "keep_di_bucket_release_role": "diagnostic_only",
            "next_branch": next_branch,
            "headline_direct_core": "strict_loan_core_min_qoq",
            "standard_secondary_comparison": "strict_loan_core_plus_private_borrower_qoq",
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
