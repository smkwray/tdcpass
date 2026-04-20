from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections
from tdcpass.analysis.strict_private_offset_residual_summary import _ensure_offset_columns

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_core_plus_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_private_offset_residual_qoq",
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_core_plus_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_private_offset_residual_qoq",
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


def _signed_gap(candidate: dict[str, Any] | None, residual: dict[str, Any] | None) -> float | None:
    if candidate is None or residual is None:
        return None
    return float(candidate["beta"]) - float(residual["beta"])


def _comparison_payload(
    *,
    residual: dict[str, Any] | None,
    direct_core: dict[str, Any] | None,
    core_plus_private: dict[str, Any] | None,
    core_plus_corporate: dict[str, Any] | None,
    private_offset: dict[str, Any] | None,
) -> dict[str, Any]:
    candidates = {
        "headline_direct_core": direct_core,
        "core_plus_private_bridge": core_plus_private,
        "core_plus_nonfinancial_corporate": core_plus_corporate,
    }
    return {
        "core_residual_response": residual,
        "headline_direct_core_response": direct_core,
        "core_plus_private_bridge_response": core_plus_private,
        "core_plus_nonfinancial_corporate_response": core_plus_corporate,
        "private_offset_residual_response": private_offset,
        "candidate_abs_gap_to_core_residual_beta": {
            key: _abs_gap(candidate, residual) for key, candidate in candidates.items()
        },
        "candidate_signed_gap_to_core_residual_beta": {
            key: _signed_gap(candidate, residual) for key, candidate in candidates.items()
        },
    }


def build_strict_corporate_bridge_secondary_comparison_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_private_offset_residual_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_private_offset_residual_summary is None:
        return {"status": "not_available", "reason": "strict_private_offset_residual_summary_not_available"}
    if str(strict_private_offset_residual_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_private_offset_residual_summary_not_available"}

    panel = _ensure_offset_columns(shocked)
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
        panel,
        shock_col=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
        outcome_cols=list(BASELINE_OUTCOMES),
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_corporate_bridge_secondary_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        panel,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=list(CORE_OUTCOMES),
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_corporate_bridge_secondary_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _comparison_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            direct_core=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            core_plus_private=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            core_plus_corporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_loan_core_plus_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            private_offset=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_offset_residual_qoq", horizon=horizon)
            ),
        )
        core_payload = _comparison_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            direct_core=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            core_plus_private=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            core_plus_corporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_loan_core_plus_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            private_offset=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_offset_residual_qoq", horizon=horizon)
            ),
        )
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "core_deposit_proximate": core_payload,
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    gaps = dict(h0_core.get("candidate_abs_gap_to_core_residual_beta", {}) or {})
    corporate_gap = gaps.get("core_plus_nonfinancial_corporate")
    private_gap = gaps.get("core_plus_private_bridge")
    direct_gap = gaps.get("headline_direct_core")

    fit_preferred_secondary = "not_available"
    if corporate_gap is not None and private_gap is not None:
        fit_preferred_secondary = (
            "strict_loan_core_plus_nonfinancial_corporate_qoq"
            if float(corporate_gap) <= float(private_gap)
            else "strict_loan_core_plus_private_borrower_qoq"
        )

    recommendation = {
        "status": "promote_corporate_bridge_for_strict_role",
        "headline_direct_core": "strict_loan_core_min_qoq",
        "standard_secondary_candidate": "strict_loan_core_plus_nonfinancial_corporate_qoq",
        "secondary_comparison_retained_for_diagnostics": "strict_loan_core_plus_private_borrower_qoq",
        "fit_preferred_secondary_candidate": fit_preferred_secondary,
        "role_decision_basis": "strict_design_over_fit_heuristic",
        "private_offset_role": "diagnostic_only",
        "next_branch": "tighten_framework_roles_and_start_toc_row_incidence_audit",
    }

    takeaways = [
        "This surface asks whether the standard secondary strict comparison should remain the broad private bridge or narrow to the nonfinancial-corporate bridge.",
        "It keeps the headline direct core fixed and compares only the secondary role candidates against the same residual target.",
    ]
    residual_beta = dict(h0_core.get("core_residual_response", {}) or {}).get("beta")
    direct_beta = dict(h0_core.get("headline_direct_core_response", {}) or {}).get("beta")
    private_beta = dict(h0_core.get("core_plus_private_bridge_response", {}) or {}).get("beta")
    corporate_beta = dict(h0_core.get("core_plus_nonfinancial_corporate_response", {}) or {}).get("beta")
    if None not in (residual_beta, direct_beta, private_beta, corporate_beta):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the direct comparison is explicit: "
            f"core residual ≈ {float(residual_beta):.2f}, headline direct core ≈ {float(direct_beta):.2f}, "
            f"core + private bridge ≈ {float(private_beta):.2f}, core + nonfinancial corporate ≈ {float(corporate_beta):.2f}."
        )
    if direct_gap is not None and private_gap is not None and corporate_gap is not None:
        takeaways.append(
            "Absolute h0 gaps to the core residual are now explicit: "
            f"direct core ≈ {float(direct_gap):.2f}, core + private bridge ≈ {float(private_gap):.2f}, "
            f"core + nonfinancial corporate ≈ {float(corporate_gap):.2f}."
        )
    takeaways.append(
        "Under a strict design rule, the standard bridge comparison should narrow to the nonfinancial-corporate bridge, while the broader private bridge becomes a wider diagnostic envelope."
    )
    if fit_preferred_secondary == "strict_loan_core_plus_private_borrower_qoq":
        takeaways.append(
            "The broader private bridge is only slightly closer numerically at h0 because it includes a known small opposing offset; that is treated as a fit heuristic, not the strict role-assignment rule."
        )
    elif fit_preferred_secondary == "strict_loan_core_plus_nonfinancial_corporate_qoq":
        takeaways.append(
            "The fit comparison and the strict role-assignment rule point in the same direction: the corporate bridge is both narrower and closer at h0."
        )

    return {
        "status": "available",
        "headline_question": "Should the standard secondary strict comparison remain the broad private bridge, or narrow to the nonfinancial-corporate bridge once the small offset block is separated out?",
        "estimation_path": {
            "summary_artifact": "strict_corporate_bridge_secondary_comparison_summary.json",
            "baseline_spec": "identity_baseline",
            "comparison_spec": "unexpected_tdc_core_deposit_proximate_bank_only",
        },
        "candidate_definitions": {
            "headline_direct_core": "strict_loan_core_min_qoq",
            "core_plus_private_bridge": "strict_loan_core_plus_private_borrower_qoq",
            "core_plus_nonfinancial_corporate": "strict_loan_core_plus_nonfinancial_corporate_qoq",
            "private_offset_residual": "strict_di_loans_nec_private_offset_residual_qoq",
        },
        "recommendation": recommendation,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
