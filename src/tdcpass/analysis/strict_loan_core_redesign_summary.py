from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_loan_source_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_noncore_system_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_loan_source_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_noncore_system_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
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


def _candidate_payload(
    *,
    residual: dict[str, Any] | None,
    broad_loan_source: dict[str, Any] | None,
    direct_min_core: dict[str, Any] | None,
    private_borrower_augmented_core: dict[str, Any] | None,
    noncore_system: dict[str, Any] | None,
    private_borrower_component: dict[str, Any] | None,
    noncore_system_component: dict[str, Any] | None,
) -> dict[str, Any]:
    candidates = {
        "current_broad_loan_source": broad_loan_source,
        "redesigned_direct_min_core": direct_min_core,
        "private_borrower_augmented_core": private_borrower_augmented_core,
        "noncore_system_diagnostic": noncore_system,
    }
    return {
        "core_residual_response": residual,
        "current_broad_loan_source_response": broad_loan_source,
        "redesigned_direct_min_core_response": direct_min_core,
        "private_borrower_augmented_core_response": private_borrower_augmented_core,
        "noncore_system_diagnostic_response": noncore_system,
        "private_borrower_component_response": private_borrower_component,
        "noncore_system_component_response": noncore_system_component,
        "candidate_abs_gap_to_core_residual_beta": {
            key: _abs_gap(candidate, residual) for key, candidate in candidates.items()
        },
        "candidate_signed_gap_to_core_residual_beta": {
            key: _signed_gap(candidate, residual) for key, candidate in candidates.items()
        },
    }


def build_strict_loan_core_redesign_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_redesign_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_redesign_summary is None or str(strict_redesign_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_redesign_summary_not_available"}

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
        spec_name="strict_loan_core_redesign_baseline_reference",
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
        spec_name="strict_loan_core_redesign_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _candidate_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            broad_loan_source=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon)),
            direct_min_core=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            private_borrower_augmented_core=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(_lp_row(baseline_lp_irf, outcome="strict_loan_noncore_system_qoq", horizon=horizon)),
            private_borrower_component=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            noncore_system_component=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_noncore_system_borrower_qoq", horizon=horizon)
            ),
        )
        core_payload = _candidate_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            broad_loan_source=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon)),
            direct_min_core=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            private_borrower_augmented_core=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(_lp_row(core_lp_irf, outcome="strict_loan_noncore_system_qoq", horizon=horizon)),
            private_borrower_component=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            noncore_system_component=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_noncore_system_borrower_qoq", horizon=horizon)
            ),
        )
        if all(value is None for value in baseline_payload.values()) and all(value is None for value in core_payload.values()):
            continue
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "core_deposit_proximate": core_payload,
        }

    h0 = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    h0_gaps = dict(h0.get("candidate_abs_gap_to_core_residual_beta", {}) or {})
    direct_gap = h0_gaps.get("redesigned_direct_min_core")
    broad_gap = h0_gaps.get("current_broad_loan_source")
    headline_candidate_status = "not_available"
    if direct_gap is not None and broad_gap is not None:
        headline_candidate_status = (
            "direct_min_core_improves_alignment"
            if float(direct_gap) < float(broad_gap)
            else "direct_min_core_still_safer_but_not_closer"
        )

    recommendation = {
        "status": "promote_direct_core_role_design",
        "release_headline_candidate": "strict_loan_core_min_qoq",
        "standard_secondary_candidate": "strict_loan_core_plus_private_borrower_qoq",
        "diagnostic_augmented_candidate": "strict_loan_core_plus_private_borrower_qoq",
        "diagnostic_broad_loan_subtotal": "strict_loan_source_qoq",
        "diagnostic_di_bucket": "strict_loan_di_loans_nec_qoq",
        "noncore_system_diagnostic": "strict_loan_noncore_system_qoq",
        "di_split_target": "strict_loan_di_loans_nec_qoq",
        "headline_candidate_status": headline_candidate_status,
    }
    published_roles = {
        "headline_direct_core": {
            "series": "strict_loan_core_min_qoq",
            "release_role": "headline",
            "definition": "Conservative same-scope direct core: mortgages plus consumer credit only.",
            "why": "Best current same-scope direct-count candidate after removing the unresolved DI-loans-n.e.c. bucket from the headline core.",
        },
        "standard_secondary_comparison": {
            "series": "strict_loan_core_plus_private_borrower_qoq",
            "release_role": "standard_secondary",
            "definition": "Headline direct core plus the private-domestic borrower slice of DI loans n.e.c.",
            "why": "Useful bounded comparison for how much private-borrower DI activity changes the headline read without promoting the full DI bucket into the core.",
        },
        "broad_loan_subtotal_diagnostic": {
            "series": "strict_loan_source_qoq",
            "release_role": "diagnostic_only",
            "definition": "Historical broad loan subtotal including DI loans n.e.c. and other advances.",
            "why": "Still informative as a subtotal, but no longer suitable as the headline direct core because it embeds the unresolved DI-loans-n.e.c. classification problem.",
        },
        "di_bucket_diagnostic": {
            "series": "strict_loan_di_loans_nec_qoq",
            "release_role": "diagnostic_only",
            "definition": "Broad DI-loans-n.e.c. bucket prior to a settled core-vs-noncore split.",
            "why": "Should be interpreted through borrower and noncore/system diagnostics, not as a headline-capable direct core component.",
        },
        "noncore_system_diagnostic": {
            "series": "strict_loan_noncore_system_qoq",
            "release_role": "diagnostic_only",
            "definition": "State/local, domestic-financial, ROW, borrower-gap, and other-advances subtotal.",
            "why": "Tracks the system/noncore portion of the old broad loan subtotal and helps keep the redesigned headline core narrow.",
        },
    }

    takeaways = [
        "This artifact turns the strict-redesign plan into a concrete loan-core comparison under both the baseline and core-deposit-proximate shocks.",
        "The redesigned direct minimum core is intentionally conservative: mortgages plus consumer credit only, with the broad DI-loans-n.e.c. bucket removed from the headline direct core until it is subdivided.",
    ]
    core_h0 = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    residual_h0 = dict(core_h0.get("core_residual_response", {}) or {}).get("beta")
    broad_h0 = dict(core_h0.get("current_broad_loan_source_response", {}) or {}).get("beta")
    direct_h0 = dict(core_h0.get("redesigned_direct_min_core_response", {}) or {}).get("beta")
    private_aug_h0 = dict(core_h0.get("private_borrower_augmented_core_response", {}) or {}).get("beta")
    noncore_h0 = dict(core_h0.get("noncore_system_diagnostic_response", {}) or {}).get("beta")
    if (
        residual_h0 is not None
        and broad_h0 is not None
        and direct_h0 is not None
        and private_aug_h0 is not None
        and noncore_h0 is not None
    ):
        takeaways.append(
            "Under the core-deposit-proximate shock at h0, the strict loan-core redesign comparison is now explicit: "
            f"core residual ≈ {float(residual_h0):.2f}, current broad loan source ≈ {float(broad_h0):.2f}, "
            f"direct minimum core ≈ {float(direct_h0):.2f}, private-borrower-augmented core ≈ {float(private_aug_h0):.2f}, "
            f"noncore/system diagnostic ≈ {float(noncore_h0):.2f}."
        )
    if direct_gap is not None and broad_gap is not None:
        takeaways.append(
            "At h0, the redesigned direct minimum core should be read as the safer headline direct core because it keeps same-scope direct measurement explicit while the broad current loan source still embeds the unresolved DI-loans-n.e.c. bucket "
            f"(direct-core abs gap to core residual ≈ {float(direct_gap):.2f}, current broad-loan abs gap ≈ {float(broad_gap):.2f})."
        )
    takeaways.append(
        "The published role design should now be explicit: `strict_loan_core_min_qoq` is the headline direct core, `strict_loan_core_plus_private_borrower_qoq` is the standard secondary comparison, and both `strict_loan_source_qoq` and `strict_loan_di_loans_nec_qoq` remain diagnostic-only until the DI bucket is redesigned more fully."
    )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "How should the strict loan core be redesigned now that the treatment side is split and the broad DI-loans-n.e.c. bucket is the remaining classification problem?",
        "estimation_path": {
            "summary_artifact": "strict_loan_core_redesign_summary.json",
            "baseline_shock_column": str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
            "core_shock_column": str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
            "release_role": "strict_loan_core_redesign_surface",
        },
        "candidate_definitions": {
            "current_broad_loan_source": "strict_loan_source_qoq",
            "redesigned_direct_min_core": "strict_loan_core_min_qoq",
            "private_borrower_augmented_core": "strict_loan_core_plus_private_borrower_qoq",
            "private_borrower_component": "strict_di_loans_nec_private_domestic_borrower_qoq",
            "noncore_system_diagnostic": "strict_loan_noncore_system_qoq",
            "noncore_system_component": "strict_di_loans_nec_noncore_system_borrower_qoq",
            "core_residual_outcome": "other_component_core_deposit_proximate_bank_only_qoq",
        },
        "published_roles": published_roles,
        "recommendation": recommendation,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
