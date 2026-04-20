from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

STRICT_COMPONENT_OUTCOMES: tuple[str, ...] = (
    "strict_loan_source_qoq",
    "strict_loan_mortgages_qoq",
    "strict_loan_consumer_credit_qoq",
    "strict_loan_di_loans_nec_qoq",
    "strict_loan_other_advances_qoq",
    "strict_non_treasury_agency_gse_qoq",
    "strict_non_treasury_municipal_qoq",
    "strict_non_treasury_corporate_foreign_bonds_qoq",
    "strict_non_treasury_securities_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_noncore_system_borrower_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_noncore_system_qoq",
)
STRICT_LOAN_COMPONENTS: tuple[str, ...] = (
    "strict_loan_mortgages_qoq",
    "strict_loan_consumer_credit_qoq",
    "strict_loan_di_loans_nec_qoq",
    "strict_loan_other_advances_qoq",
)
STRICT_DI_LOANS_NEC_BORROWER_COMPONENTS: tuple[str, ...] = (
    "strict_di_loans_nec_households_nonprofits_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
    "strict_di_loans_nec_state_local_qoq",
    "strict_di_loans_nec_domestic_financial_qoq",
    "strict_di_loans_nec_rest_of_world_qoq",
)
STRICT_FUNDING_COMPONENTS: tuple[str, ...] = (
    "strict_funding_fedfunds_repo_qoq",
    "strict_funding_debt_securities_qoq",
    "strict_funding_fhlb_advances_qoq",
)
MEASUREMENT_COMPARISON_OUTCOMES: tuple[str, ...] = (
    "total_deposits_bank_qoq",
    "other_component_qoq",
)
STRICT_IDENTITY_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    *STRICT_COMPONENT_OUTCOMES,
    "strict_di_loans_nec_systemwide_liability_total_qoq",
    *STRICT_DI_LOANS_NEC_BORROWER_COMPONENTS,
    "strict_di_loans_nec_systemwide_borrower_total_qoq",
    "strict_di_loans_nec_systemwide_borrower_gap_qoq",
    "strict_identifiable_total_qoq",
    "strict_identifiable_gap_qoq",
    *STRICT_FUNDING_COMPONENTS,
    "strict_funding_offset_total_qoq",
    "strict_identifiable_net_after_funding_qoq",
    "strict_gap_after_funding_qoq",
)


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


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


def _identity_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _measurement_variant_row(
    df: pd.DataFrame,
    *,
    treatment_variant: str,
    outcome: str,
    horizon: int,
) -> dict[str, Any] | None:
    required = {"treatment_variant", "outcome", "horizon"}
    if df.empty or not required.issubset(df.columns):
        return None
    sample = df[
        (df["treatment_variant"] == treatment_variant) & (df["outcome"] == outcome) & (df["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def slice_strict_identifiable_lp_irf(lp_irf: pd.DataFrame) -> pd.DataFrame:
    if lp_irf.empty or "outcome" not in lp_irf.columns:
        return pd.DataFrame(columns=list(lp_irf.columns))
    subset = lp_irf[lp_irf["outcome"].isin(STRICT_IDENTITY_OUTCOMES)].copy()
    if subset.empty:
        return pd.DataFrame(columns=list(lp_irf.columns))
    order = {name: idx for idx, name in enumerate(STRICT_IDENTITY_OUTCOMES)}
    subset["_outcome_order"] = subset["outcome"].map(order).fillna(len(order))
    subset = subset.sort_values(["_outcome_order", "horizon"]).drop(columns="_outcome_order")
    return subset.reset_index(drop=True)


def build_strict_identifiable_alignment_frame(
    lp_irf: pd.DataFrame,
    *,
    horizons: Iterable[int] | None = None,
) -> pd.DataFrame:
    if horizons is None:
        if lp_irf.empty or "horizon" not in lp_irf.columns:
            horizons = ()
        else:
            horizons = sorted(
                set(
                    int(value)
                    for value in lp_irf.loc[
                        lp_irf["outcome"].isin(
                            ("other_component_qoq", "strict_identifiable_total_qoq", "strict_identifiable_gap_qoq")
                        ),
                        "horizon",
                    ].dropna()
                )
            )

    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        residual = _snapshot(_lp_row(lp_irf, outcome="other_component_qoq", horizon=int(horizon)))
        direct_core = _snapshot(_lp_row(lp_irf, outcome="strict_loan_core_min_qoq", horizon=int(horizon)))
        loan = _snapshot(_lp_row(lp_irf, outcome="strict_loan_source_qoq", horizon=int(horizon)))
        securities = _snapshot(_lp_row(lp_irf, outcome="strict_non_treasury_securities_qoq", horizon=int(horizon)))
        total = _snapshot(_lp_row(lp_irf, outcome="strict_identifiable_total_qoq", horizon=int(horizon)))
        gap = _snapshot(_lp_row(lp_irf, outcome="strict_identifiable_gap_qoq", horizon=int(horizon)))
        if residual is None and total is None and gap is None:
            continue
        arithmetic_gap = None if residual is None or total is None else float(residual["beta"]) - float(total["beta"])
        gap_share = (
            None
            if gap is None or residual is None or float(residual["beta"]) == 0.0
            else abs(float(gap["beta"])) / abs(float(residual["beta"]))
        )
        if gap_share is None:
            interpretation = "missing_alignment_inputs"
        elif gap_share <= 0.1:
            interpretation = "near_full_identifiable_coverage"
        elif gap_share <= 0.5:
            interpretation = "partial_identifiable_coverage"
        else:
            interpretation = "large_unidentified_remainder"
        rows.append(
            {
                "horizon": int(horizon),
                "residual_beta": None if residual is None else float(residual["beta"]),
                "strict_loan_core_min_beta": None if direct_core is None else float(direct_core["beta"]),
                "strict_loan_source_beta": None if loan is None else float(loan["beta"]),
                "strict_non_treasury_securities_beta": None if securities is None else float(securities["beta"]),
                "strict_identifiable_total_beta": None if total is None else float(total["beta"]),
                "strict_identifiable_gap_beta": None if gap is None else float(gap["beta"]),
                "arithmetic_residual_minus_total_beta": arithmetic_gap,
                "strict_gap_share_of_residual": gap_share,
                "residual_n": None if residual is None else int(residual["n"]),
                "strict_total_n": None if total is None else int(total["n"]),
                "strict_gap_n": None if gap is None else int(gap["n"]),
                "interpretation": interpretation,
            }
        )

    columns = [
        "horizon",
        "residual_beta",
        "strict_loan_core_min_beta",
        "strict_loan_source_beta",
        "strict_non_treasury_securities_beta",
        "strict_identifiable_total_beta",
        "strict_identifiable_gap_beta",
        "arithmetic_residual_minus_total_beta",
        "strict_gap_share_of_residual",
        "residual_n",
        "strict_total_n",
        "strict_gap_n",
        "interpretation",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def build_strict_funding_offset_alignment_frame(
    lp_irf: pd.DataFrame,
    *,
    horizons: Iterable[int] | None = None,
) -> pd.DataFrame:
    if horizons is None:
        if lp_irf.empty or "horizon" not in lp_irf.columns:
            horizons = ()
        else:
            horizons = sorted(
                set(
                    int(value)
                    for value in lp_irf.loc[
                        lp_irf["outcome"].isin(
                            (
                                "strict_identifiable_total_qoq",
                                "strict_funding_offset_total_qoq",
                                "strict_identifiable_net_after_funding_qoq",
                                "strict_gap_after_funding_qoq",
                            )
                        ),
                        "horizon",
                    ].dropna()
                )
            )

    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        identifiable_total = _snapshot(_lp_row(lp_irf, outcome="strict_identifiable_total_qoq", horizon=int(horizon)))
        funding_total = _snapshot(_lp_row(lp_irf, outcome="strict_funding_offset_total_qoq", horizon=int(horizon)))
        net_after_funding = _snapshot(
            _lp_row(lp_irf, outcome="strict_identifiable_net_after_funding_qoq", horizon=int(horizon))
        )
        gap_after_funding = _snapshot(_lp_row(lp_irf, outcome="strict_gap_after_funding_qoq", horizon=int(horizon)))
        if identifiable_total is None and funding_total is None and net_after_funding is None and gap_after_funding is None:
            continue
        funding_share = _share(funding_total, identifiable_total)
        if funding_share is None:
            interpretation = "missing_alignment_inputs"
        elif abs(float(funding_share)) < 0.25:
            interpretation = "funding_offsets_small_relative_to_identifiable_total"
        elif abs(float(funding_share)) < 0.75:
            interpretation = "funding_offsets_material_relative_to_identifiable_total"
        else:
            interpretation = "funding_offsets_dominate_identifiable_total"
        rows.append(
            {
                "horizon": int(horizon),
                "strict_identifiable_total_beta": None if identifiable_total is None else float(identifiable_total["beta"]),
                "strict_funding_offset_total_beta": None if funding_total is None else float(funding_total["beta"]),
                "strict_funding_offset_share_of_identifiable_total_beta": funding_share,
                "strict_identifiable_net_after_funding_beta": (
                    None if net_after_funding is None else float(net_after_funding["beta"])
                ),
                "strict_gap_after_funding_beta": None if gap_after_funding is None else float(gap_after_funding["beta"]),
                "identifiable_total_n": None if identifiable_total is None else int(identifiable_total["n"]),
                "funding_total_n": None if funding_total is None else int(funding_total["n"]),
                "net_after_funding_n": None if net_after_funding is None else int(net_after_funding["n"]),
                "gap_after_funding_n": None if gap_after_funding is None else int(gap_after_funding["n"]),
                "interpretation": interpretation,
            }
        )

    columns = [
        "horizon",
        "strict_identifiable_total_beta",
        "strict_funding_offset_total_beta",
        "strict_funding_offset_share_of_identifiable_total_beta",
        "strict_identifiable_net_after_funding_beta",
        "strict_gap_after_funding_beta",
        "identifiable_total_n",
        "funding_total_n",
        "net_after_funding_n",
        "gap_after_funding_n",
        "interpretation",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def build_strict_identifiable_summary(
    *,
    lp_irf: pd.DataFrame,
    strict_source_kind: str = "not_available",
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    primary_lp_irf = slice_strict_identifiable_lp_irf(lp_irf)
    key_horizons: dict[str, Any] = {}
    observed_components: set[str] = set()

    for horizon in horizons:
        component_payload: dict[str, Any] = {}
        for outcome in STRICT_COMPONENT_OUTCOMES:
            snapshot = _snapshot(_lp_row(primary_lp_irf, outcome=outcome, horizon=horizon))
            if snapshot is not None:
                component_payload[outcome] = snapshot
                observed_components.add(outcome)
        residual = _snapshot(_lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon))
        direct_core = _snapshot(_lp_row(primary_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon))
        loan = _snapshot(_lp_row(primary_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon))
        securities = _snapshot(_lp_row(primary_lp_irf, outcome="strict_non_treasury_securities_qoq", horizon=horizon))
        total = _snapshot(_lp_row(primary_lp_irf, outcome="strict_identifiable_total_qoq", horizon=horizon))
        gap = _snapshot(_lp_row(primary_lp_irf, outcome="strict_identifiable_gap_qoq", horizon=horizon))
        arithmetic_gap = None if residual is None or total is None else float(residual["beta"]) - float(total["beta"])
        gap_share = (
            None
            if gap is None or residual is None or float(residual["beta"]) == 0.0
            else abs(float(gap["beta"])) / abs(float(residual["beta"]))
        )
        if gap_share is None:
            interpretation = "missing_alignment_inputs"
        elif gap_share <= 0.1:
            interpretation = "near_full_identifiable_coverage"
        elif gap_share <= 0.5:
            interpretation = "partial_identifiable_coverage"
        else:
            interpretation = "large_unidentified_remainder"
        key_horizons[f"h{horizon}"] = {
            "other_component": residual,
            "strict_headline_direct_core": direct_core,
            "strict_loan_source": loan,
            "strict_non_treasury_securities": securities,
            "strict_identifiable_total": total,
            "strict_identifiable_gap": gap,
            "strict_components": component_payload,
            "arithmetic_residual_minus_total_beta": arithmetic_gap,
            "strict_gap_share_of_residual": gap_share,
            "interpretation": interpretation,
        }

    status = "not_available"
    if key_horizons and any(payload["strict_identifiable_total"] is not None for payload in key_horizons.values()):
        status = "available"

    observed_interpretations = [
        str(payload.get("interpretation", "missing_alignment_inputs"))
        for payload in key_horizons.values()
        if payload.get("strict_identifiable_total") is not None
    ]

    takeaways = [
        "This strict lane is source-side and gross: it is meant to count directly measured non-Treasury bank asset transactions without using the residual to close the books.",
        "The headline strict direct core is `strict_loan_core_min_qoq`; the older broad `strict_loan_source_qoq` remains published as a diagnostic because it still embeds the unresolved DI-loans-n.e.c. bucket.",
    ]
    if strict_source_kind != "not_available":
        takeaways.append(f"Strict source kind: {strict_source_kind}.")
    if observed_components:
        takeaways.append(
            f"Observed strict component outcomes currently present: {', '.join(sorted(observed_components))}."
        )
    if observed_interpretations and all(item == "large_unidentified_remainder" for item in observed_interpretations):
        takeaways.append(
            "All reported horizons still show a large unidentified remainder. That is compatible with the design: the strict lane is intentionally incomplete and leaves funding substitutions, portfolio reallocations, and other hard-to-measure channels in the gap."
        )
    elif observed_interpretations and any(item == "partial_identifiable_coverage" for item in observed_interpretations):
        takeaways.append(
            "At least one reported horizon shows partial identifiable coverage, so the strict direct lane is explaining a meaningful but incomplete share of the non-TDC residual."
        )
    elif observed_interpretations and all(item == "near_full_identifiable_coverage" for item in observed_interpretations):
        takeaways.append(
            "All reported horizons show near-full identifiable coverage in the strict lane. Read that as unusually strong alignment of the direct source-side counts, not as license to collapse the strict lane and the closure-oriented accounting lane into the same object."
        )

    return {
        "status": status,
        "source_kind": strict_source_kind,
        "headline_question": "How much of the non-TDC deposit residual is covered by the direct source-side strict lane before any closure-oriented reconstruction?",
        "estimation_path": {
            "primary_artifact": "lp_irf_strict_identifiable.csv",
            "alignment_artifact": "strict_identifiable_alignment.csv",
            "summary_artifact": "strict_identifiable_summary.json",
        },
        "component_outcomes_present": sorted(observed_components),
        "horizons": key_horizons,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }


def _build_measurement_variant_comparison(
    *,
    identity_baseline_lp_irf: pd.DataFrame,
    identity_measurement_ladder: pd.DataFrame,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    comparison_variants = sorted(
        {
            str(value)
            for value in identity_measurement_ladder.get("treatment_variant", pd.Series(dtype="object")).dropna()
            if str(value)
        }
    )
    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_outcomes = {
            outcome: _snapshot(_identity_row(identity_baseline_lp_irf, outcome=outcome, horizon=horizon))
            for outcome in MEASUREMENT_COMPARISON_OUTCOMES
        }
        baseline_target_response = _snapshot(
            _identity_row(identity_baseline_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)
        )
        variant_payloads: dict[str, Any] = {}
        for variant in comparison_variants:
            variant_target_rows = identity_measurement_ladder[
                (identity_measurement_ladder.get("treatment_variant") == variant)
                & (identity_measurement_ladder.get("horizon") == horizon)
            ]
            target_name = (
                None
                if variant_target_rows.empty or "target" not in variant_target_rows.columns
                else str(variant_target_rows["target"].dropna().iloc[0])
            )
            target_response = None
            if target_name:
                target_response = _snapshot(
                    _measurement_variant_row(
                        identity_measurement_ladder,
                        treatment_variant=variant,
                        outcome=target_name,
                        horizon=horizon,
                    )
                )
            variant_outcomes = {
                outcome: _snapshot(
                    _measurement_variant_row(
                        identity_measurement_ladder,
                        treatment_variant=variant,
                        outcome=outcome,
                        horizon=horizon,
                    )
                )
                for outcome in MEASUREMENT_COMPARISON_OUTCOMES
            }
            if target_response is None and not any(payload is not None for payload in variant_outcomes.values()):
                continue
            differences = {
                outcome: None
                if baseline_outcomes.get(outcome) is None or variant_outcomes.get(outcome) is None
                else float(variant_outcomes[outcome]["beta"]) - float(baseline_outcomes[outcome]["beta"])
                for outcome in MEASUREMENT_COMPARISON_OUTCOMES
            }
            differences["target_response"] = (
                None
                if baseline_target_response is None or target_response is None
                else float(target_response["beta"]) - float(baseline_target_response["beta"])
            )
            variant_payloads[variant] = {
                "target": target_name,
                "target_response": target_response,
                "outcomes": variant_outcomes,
                "differences_vs_baseline_beta": differences,
            }
        if baseline_target_response is not None or any(payload is not None for payload in baseline_outcomes.values()) or variant_payloads:
            key_horizons[f"h{horizon}"] = {
                "baseline_bank_only": {
                    "target": "tdc_bank_only_qoq",
                    "target_response": baseline_target_response,
                    "outcomes": baseline_outcomes,
                },
                "measurement_variants": variant_payloads,
            }
    return {
        "baseline_variant": "bank_only",
        "comparison_variants": comparison_variants,
        "key_horizons": key_horizons,
    }


def _recommended_measurement_comparison(
    measurement_variant_comparison: dict[str, Any],
) -> dict[str, Any]:
    h0 = dict(measurement_variant_comparison.get("key_horizons", {}).get("h0", {}))
    variants = dict(h0.get("measurement_variants", {}))
    preferred = dict(variants.get("us_chartered_bank_only", {}))
    secondary = dict(variants.get("domestic_bank_only", {}))
    preferred_other_delta = (
        preferred.get("differences_vs_baseline_beta", {}).get("other_component_qoq")
    )
    secondary_other_delta = (
        secondary.get("differences_vs_baseline_beta", {}).get("other_component_qoq")
    )
    headline_read = (
        "Use the U.S.-chartered bank-leg match as the standard scope-check comparison and keep the no-ROW variant as a secondary sensitivity."
    )
    if preferred_other_delta is not None and secondary_other_delta is not None:
        headline_read = (
            "Use the U.S.-chartered bank-leg match as the standard scope-check comparison: at h0 it makes "
            f"`other_component_qoq` about {abs(float(preferred_other_delta)):.2f} "
            f"{'less' if float(preferred_other_delta) > 0 else 'more'} negative than baseline, versus about "
            f"{abs(float(secondary_other_delta)):.2f} for the no-ROW sensitivity."
        )
    return {
        "decision_status": "standardize_us_chartered_scope_check",
        "headline_outcome": "total_deposits_bank_qoq",
        "baseline_variant": "bank_only",
        "preferred_variant": "us_chartered_bank_only",
        "secondary_variant": "domestic_bank_only",
        "headline_read": headline_read,
        "rationale": (
            "The U.S.-chartered bank-leg-matched variant isolates bank-scope matching directly, while `domestic_bank_only` "
            "changes the treatment by removing the rest-of-world term only. That makes the former the cleaner standard "
            "scope comparison for the current headline outcome."
        ),
    }


def _build_scope_check_gap_assessment(
    *,
    measurement_variant_comparison: dict[str, Any],
    strict_component_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    comparison_variants = list(measurement_variant_comparison.get("comparison_variants", []))
    for horizon_key, strict_payload in dict(strict_component_diagnostics.get("key_horizons", {})).items():
        measurement_payload = dict(measurement_variant_comparison.get("key_horizons", {}).get(horizon_key, {}))
        baseline_residual = strict_payload.get("other_component")
        strict_total = strict_payload.get("strict_identifiable_total")
        baseline_gap = strict_payload.get("strict_identifiable_gap")
        variant_payloads: dict[str, Any] = {}
        for variant in comparison_variants:
            variant_measurement_payload = dict(measurement_payload.get("measurement_variants", {}).get(variant, {}))
            residual_shift = (
                variant_measurement_payload.get("differences_vs_baseline_beta", {}).get("other_component_qoq")
            )
            descriptive_residual = (
                None
                if baseline_residual is None or residual_shift is None
                else float(baseline_residual["beta"]) + float(residual_shift)
            )
            descriptive_gap = (
                None
                if descriptive_residual is None or strict_total is None
                else descriptive_residual - float(strict_total["beta"])
            )
            relief_share = (
                None
                if baseline_gap is None or float(baseline_gap["beta"]) == 0.0 or descriptive_gap is None
                else (abs(float(baseline_gap["beta"])) - abs(float(descriptive_gap)))
                / abs(float(baseline_gap["beta"]))
            )
            remaining_share = (
                None
                if baseline_gap is None or float(baseline_gap["beta"]) == 0.0 or descriptive_gap is None
                else abs(float(descriptive_gap)) / abs(float(baseline_gap["beta"]))
            )
            if remaining_share is None:
                interpretation = "missing_alignment_inputs"
            elif remaining_share > 0.8:
                interpretation = "scope_check_relief_small_relative_to_baseline_strict_gap"
            elif remaining_share > 0.4:
                interpretation = "scope_check_relief_partial_but_incomplete"
            else:
                interpretation = "scope_check_relief_eliminates_most_of_baseline_strict_gap"
            variant_payloads[variant] = {
                "residual_shift_vs_baseline_beta": residual_shift,
                "descriptive_residual_if_shift_applied_to_strict_baseline_beta": descriptive_residual,
                "descriptive_gap_if_strict_total_held_fixed_beta": descriptive_gap,
                "relief_share_of_baseline_strict_gap": relief_share,
                "remaining_share_of_baseline_strict_gap": remaining_share,
                "interpretation": interpretation,
            }
        if baseline_gap is not None or variant_payloads:
            key_horizons[horizon_key] = {
                "baseline_strict_gap_beta": None if baseline_gap is None else float(baseline_gap["beta"]),
                "baseline_residual_beta": None if baseline_residual is None else float(baseline_residual["beta"]),
                "strict_identifiable_total_beta": None if strict_total is None else float(strict_total["beta"]),
                "variant_gap_assessments": variant_payloads,
            }
    return {
        "assumption": (
            "Descriptive only: this block holds the baseline strict identifiable total fixed and asks how much the "
            "residual-side scope-check shift could mechanically relieve the current strict gap on its own."
        ),
        "key_horizons": key_horizons,
    }


def _dominant_component(components: dict[str, dict[str, Any] | None]) -> str | None:
    ranked = [
        (name, abs(float(payload["beta"])))
        for name, payload in components.items()
        if payload is not None and payload.get("beta") is not None
    ]
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[0][0]


def _share(component: dict[str, Any] | None, total: dict[str, Any] | None) -> float | None:
    if component is None or total is None:
        return None
    denominator = float(total["beta"])
    if denominator == 0.0:
        return None
    return float(component["beta"]) / denominator


def _build_strict_component_diagnostics(
    *,
    strict_lp_irf: pd.DataFrame,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        residual = _snapshot(_lp_row(strict_lp_irf, outcome="other_component_qoq", horizon=horizon))
        direct_core = _snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon))
        loan_source = _snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon))
        private_augmented_core = _snapshot(
            _lp_row(strict_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
        )
        noncore_system = _snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_noncore_system_qoq", horizon=horizon))
        securities = _snapshot(_lp_row(strict_lp_irf, outcome="strict_non_treasury_securities_qoq", horizon=horizon))
        total = _snapshot(_lp_row(strict_lp_irf, outcome="strict_identifiable_total_qoq", horizon=horizon))
        gap = _snapshot(_lp_row(strict_lp_irf, outcome="strict_identifiable_gap_qoq", horizon=horizon))
        loan_components = {
            outcome: _snapshot(_lp_row(strict_lp_irf, outcome=outcome, horizon=horizon))
            for outcome in STRICT_LOAN_COMPONENTS
        }
        loan_component_shares = {
            outcome: _share(payload, loan_source) for outcome, payload in loan_components.items()
        }
        if not any(
            payload is not None
            for payload in [
                residual,
                direct_core,
                loan_source,
                private_augmented_core,
                noncore_system,
                securities,
                total,
                gap,
                *loan_components.values(),
            ]
        ):
            continue
        key_horizons[f"h{horizon}"] = {
            "other_component": residual,
            "strict_headline_direct_core": direct_core,
            "strict_loan_source": loan_source,
            "strict_loan_core_plus_private_borrower": private_augmented_core,
            "strict_loan_noncore_system": noncore_system,
            "strict_non_treasury_securities": securities,
            "strict_identifiable_total": total,
            "strict_identifiable_gap": gap,
            "loan_components": loan_components,
            "loan_component_shares_of_loan_source_beta": loan_component_shares,
            "strict_loan_di_loans_nec_share_of_loan_source_beta": loan_component_shares.get(
                "strict_loan_di_loans_nec_qoq"
            ),
            "dominant_loan_component": _dominant_component(loan_components),
            "strict_non_treasury_securities_share_of_identifiable_total_beta": _share(securities, total),
        }
    return {"key_horizons": key_horizons}


def _build_di_loans_nec_borrower_diagnostics(
    *,
    lp_irf: pd.DataFrame,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        us_chartered_aggregate = _snapshot(_lp_row(lp_irf, outcome="strict_loan_di_loans_nec_qoq", horizon=horizon))
        systemwide_total = _snapshot(
            _lp_row(lp_irf, outcome="strict_di_loans_nec_systemwide_liability_total_qoq", horizon=horizon)
        )
        borrower_components = {
            outcome: _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
            for outcome in STRICT_DI_LOANS_NEC_BORROWER_COMPONENTS
        }
        borrower_total = _snapshot(
            _lp_row(lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_total_qoq", horizon=horizon)
        )
        borrower_gap = _snapshot(
            _lp_row(lp_irf, outcome="strict_di_loans_nec_systemwide_borrower_gap_qoq", horizon=horizon)
        )
        if not any(
            payload is not None
            for payload in [us_chartered_aggregate, systemwide_total, borrower_total, borrower_gap, *borrower_components.values()]
        ):
            continue
        borrower_component_shares = {
            outcome: _share(payload, systemwide_total) for outcome, payload in borrower_components.items()
        }
        key_horizons[f"h{horizon}"] = {
            "strict_loan_di_loans_nec": us_chartered_aggregate,
            "strict_di_loans_nec_systemwide_liability_total": systemwide_total,
            "borrower_components": borrower_components,
            "borrower_component_shares_of_systemwide_liability_total_beta": borrower_component_shares,
            "systemwide_borrower_total": borrower_total,
            "systemwide_borrower_gap": borrower_gap,
            "systemwide_borrower_total_share_of_systemwide_liability_beta": _share(borrower_total, systemwide_total),
            "systemwide_borrower_gap_share_of_systemwide_liability_beta": _share(borrower_gap, systemwide_total),
            "us_chartered_share_of_systemwide_liability_beta": _share(us_chartered_aggregate, systemwide_total),
            "dominant_borrower_component": _dominant_component(borrower_components),
        }
    return {"key_horizons": key_horizons}


def _build_funding_offset_sensitivity(
    *,
    lp_irf: pd.DataFrame,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        identifiable_total = _snapshot(_lp_row(lp_irf, outcome="strict_identifiable_total_qoq", horizon=horizon))
        funding_components = {
            outcome: _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
            for outcome in STRICT_FUNDING_COMPONENTS
        }
        funding_total = _snapshot(_lp_row(lp_irf, outcome="strict_funding_offset_total_qoq", horizon=horizon))
        net_after_funding = _snapshot(
            _lp_row(lp_irf, outcome="strict_identifiable_net_after_funding_qoq", horizon=horizon)
        )
        gap_after_funding = _snapshot(_lp_row(lp_irf, outcome="strict_gap_after_funding_qoq", horizon=horizon))
        if not any(
            payload is not None
            for payload in [identifiable_total, funding_total, net_after_funding, gap_after_funding, *funding_components.values()]
        ):
            continue
        funding_component_shares = {
            outcome: _share(payload, funding_total) for outcome, payload in funding_components.items()
        }
        key_horizons[f"h{horizon}"] = {
            "strict_identifiable_total": identifiable_total,
            "funding_components": funding_components,
            "funding_component_shares_of_offset_total_beta": funding_component_shares,
            "strict_funding_offset_total": funding_total,
            "strict_funding_offset_share_of_identifiable_total_beta": _share(funding_total, identifiable_total),
            "strict_identifiable_net_after_funding": net_after_funding,
            "strict_gap_after_funding": gap_after_funding,
            "dominant_funding_component": _dominant_component(funding_components),
        }
    return {"key_horizons": key_horizons}


def build_strict_identifiable_followup_summary(
    *,
    identity_baseline_lp_irf: pd.DataFrame,
    identity_measurement_ladder: pd.DataFrame,
    lp_irf: pd.DataFrame,
    strict_source_kind: str = "not_available",
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    measurement_variant_comparison = _build_measurement_variant_comparison(
        identity_baseline_lp_irf=identity_baseline_lp_irf,
        identity_measurement_ladder=identity_measurement_ladder,
        horizons=horizons,
    )
    strict_component_diagnostics = _build_strict_component_diagnostics(
        strict_lp_irf=lp_irf,
        horizons=horizons,
    )
    di_loans_nec_borrower_diagnostics = _build_di_loans_nec_borrower_diagnostics(
        lp_irf=lp_irf,
        horizons=horizons,
    )
    funding_offset_sensitivity = _build_funding_offset_sensitivity(
        lp_irf=lp_irf,
        horizons=horizons,
    )
    recommended_measurement_comparison = _recommended_measurement_comparison(
        measurement_variant_comparison,
    )
    scope_check_gap_assessment = _build_scope_check_gap_assessment(
        measurement_variant_comparison=measurement_variant_comparison,
        strict_component_diagnostics=strict_component_diagnostics,
    )

    available = bool(
        measurement_variant_comparison["key_horizons"]
        or strict_component_diagnostics["key_horizons"]
        or di_loans_nec_borrower_diagnostics["key_horizons"]
        or funding_offset_sensitivity["key_horizons"]
        or scope_check_gap_assessment["key_horizons"]
    )
    takeaways = [
        "This follow-up artifact is meant to answer the next diagnostic question after the strict lane: is the big strict gap coming from treatment measurement, from missing direct-count channels, or from both?",
        "The measurement-variant comparison uses the exact identity-preserving baseline and its measurement-family variants; the strict component block uses the direct source-side strict lane only.",
    ]
    if strict_source_kind != "not_available":
        takeaways.append(f"Strict source kind: {strict_source_kind}.")

    domestic_h0 = (
        measurement_variant_comparison["key_horizons"]
        .get("h0", {})
        .get("measurement_variants", {})
        .get("domestic_bank_only", {})
    )
    domestic_h0_diff = domestic_h0.get("differences_vs_baseline_beta", {})
    domestic_other_diff = domestic_h0_diff.get("other_component_qoq")
    if domestic_other_diff is not None:
        direction = "less negative" if domestic_other_diff > 0 else "more negative"
        takeaways.append(
            f"At h0, the upstream no-ROW `domestic_bank_only` treatment makes `other_component_qoq` {direction} than the baseline bank-only treatment by about {abs(float(domestic_other_diff)):.2f}."
        )
    us_chartered_h0 = (
        measurement_variant_comparison["key_horizons"]
        .get("h0", {})
        .get("measurement_variants", {})
        .get("us_chartered_bank_only", {})
    )
    us_chartered_h0_diff = us_chartered_h0.get("differences_vs_baseline_beta", {})
    us_chartered_other_diff = us_chartered_h0_diff.get("other_component_qoq")
    if us_chartered_other_diff is not None:
        direction = "less negative" if us_chartered_other_diff > 0 else "more negative"
        takeaways.append(
            f"At h0, the local U.S.-chartered bank-leg-matched treatment makes `other_component_qoq` {direction} than the baseline bank-only treatment by about {abs(float(us_chartered_other_diff)):.2f}."
        )
        takeaways.append(
            "For the current headline outcome, the U.S.-chartered bank-leg match should be read as the standard scope check, while the no-ROW variant remains secondary."
        )
    scope_gap_h0 = scope_check_gap_assessment["key_horizons"].get("h0", {})
    us_chartered_gap_h0 = scope_gap_h0.get("variant_gap_assessments", {}).get("us_chartered_bank_only", {})
    remaining_gap_share_h0 = us_chartered_gap_h0.get("remaining_share_of_baseline_strict_gap")
    if remaining_gap_share_h0 is not None:
        takeaways.append(
            f"Even after applying the standard U.S.-chartered scope-check shift at h0, about {float(remaining_gap_share_h0):.2f} of the baseline strict gap remains if the direct-count strict total is held fixed."
        )
        if float(remaining_gap_share_h0) > 0.8:
            takeaways.append(
                "So the current strict-gap story still looks dominated by missing direct-count channels or remaining treatment misspecification, not by the headline bank-leg scope mismatch alone."
            )

    strict_h0 = strict_component_diagnostics["key_horizons"].get("h0", {})
    dominant_h0 = strict_h0.get("dominant_loan_component")
    direct_core_h0 = dict(strict_h0.get("strict_headline_direct_core", {}) or {}).get("beta")
    broad_loan_h0 = dict(strict_h0.get("strict_loan_source", {}) or {}).get("beta")
    private_aug_h0 = dict(strict_h0.get("strict_loan_core_plus_private_borrower", {}) or {}).get("beta")
    noncore_h0 = dict(strict_h0.get("strict_loan_noncore_system", {}) or {}).get("beta")
    di_share_h0 = strict_h0.get("strict_loan_di_loans_nec_share_of_loan_source_beta")
    if (
        direct_core_h0 is not None
        and broad_loan_h0 is not None
        and private_aug_h0 is not None
        and noncore_h0 is not None
    ):
        takeaways.append(
            "At h0, the strict loan-core redesign is already visible inside the strict lane: "
            f"headline direct core ≈ {float(direct_core_h0):.2f}, current broad loan source ≈ {float(broad_loan_h0):.2f}, "
            f"private-borrower-augmented core ≈ {float(private_aug_h0):.2f}, noncore/system diagnostic ≈ {float(noncore_h0):.2f}."
        )
    if dominant_h0 is not None:
        takeaways.append(f"At h0, the dominant strict loan subcomponent is `{dominant_h0}`.")
    if di_share_h0 is not None:
        takeaways.append(
            f"At h0, `strict_loan_di_loans_nec_qoq` contributes about {float(di_share_h0):.2f} of the signed strict loan-source response."
        )
    borrower_h0 = di_loans_nec_borrower_diagnostics["key_horizons"].get("h0", {})
    us_chartered_share_h0 = borrower_h0.get("us_chartered_share_of_systemwide_liability_beta")
    systemwide_gap_h0 = borrower_h0.get("systemwide_borrower_gap_share_of_systemwide_liability_beta")
    if us_chartered_share_h0 is not None:
        takeaways.append(
            f"At h0, the U.S.-chartered DI-loans-n.e.c. asset response is about {float(us_chartered_share_h0):.2f} of the signed systemwide borrower-liability total, so the borrower-side block should be read as a cross-scope counterpart diagnostic rather than as a same-scope decomposition."
        )
    if systemwide_gap_h0 is not None:
        takeaways.append(
            f"At h0, the named borrower rows leave about {float(systemwide_gap_h0):.2f} of the signed systemwide borrower-liability total outside the explicitly materialized borrower buckets."
        )
    funding_h0 = funding_offset_sensitivity["key_horizons"].get("h0", {})
    funding_share_h0 = funding_h0.get("strict_funding_offset_share_of_identifiable_total_beta")
    if funding_share_h0 is not None:
        takeaways.append(
            f"At h0, the strict funding-offset block is about {float(funding_share_h0):.2f} of the signed strict identifiable total, so netting funding can materially change the direct-count read even before any closure-oriented reconstruction."
        )

    return {
        "status": "available" if available else "not_available",
        "strict_source_kind": strict_source_kind,
        "headline_question": "How much of the current strict-gap story looks like treatment measurement versus missing direct-count source-side channels?",
        "estimation_path": {
            "baseline_identity_artifact": "lp_irf_identity_baseline.csv",
            "measurement_ladder_artifact": "identity_measurement_ladder.csv",
            "strict_primary_artifact": "lp_irf_strict_identifiable.csv",
            "funding_alignment_artifact": "strict_funding_offset_alignment.csv",
            "summary_artifact": "strict_identifiable_followup_summary.json",
        },
        "measurement_variant_comparison": measurement_variant_comparison,
        "recommended_measurement_comparison": recommended_measurement_comparison,
        "scope_check_gap_assessment": scope_check_gap_assessment,
        "strict_component_diagnostics": strict_component_diagnostics,
        "di_loans_nec_borrower_diagnostics": di_loans_nec_borrower_diagnostics,
        "funding_offset_sensitivity": funding_offset_sensitivity,
        "takeaways": takeaways,
    }
