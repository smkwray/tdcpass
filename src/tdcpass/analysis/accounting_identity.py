from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

ACCOUNTING_COMPONENT_OUTCOMES: tuple[str, ...] = (
    "accounting_deposit_substitution_qoq",
    "accounting_bank_balance_sheet_qoq",
    "accounting_public_liquidity_qoq",
    "accounting_external_flow_qoq",
)
ACCOUNTING_IDENTITY_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    *ACCOUNTING_COMPONENT_OUTCOMES,
    "accounting_identity_total_qoq",
    "accounting_identity_gap_qoq",
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


def slice_accounting_identity_lp_irf(lp_irf: pd.DataFrame) -> pd.DataFrame:
    if lp_irf.empty or "outcome" not in lp_irf.columns:
        return pd.DataFrame(columns=list(lp_irf.columns))
    subset = lp_irf[lp_irf["outcome"].isin(ACCOUNTING_IDENTITY_OUTCOMES)].copy()
    if subset.empty:
        return pd.DataFrame(columns=list(lp_irf.columns))
    order = {name: idx for idx, name in enumerate(ACCOUNTING_IDENTITY_OUTCOMES)}
    subset["_outcome_order"] = subset["outcome"].map(order).fillna(len(order))
    subset = subset.sort_values(["_outcome_order", "horizon"]).drop(columns="_outcome_order")
    return subset.reset_index(drop=True)


def build_accounting_identity_alignment_frame(
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
                            ("other_component_qoq", "accounting_identity_total_qoq", "accounting_identity_gap_qoq")
                        ),
                        "horizon",
                    ].dropna()
                )
            )

    rows: list[dict[str, Any]] = []
    for horizon in horizons:
        residual = _snapshot(_lp_row(lp_irf, outcome="other_component_qoq", horizon=int(horizon)))
        total = _snapshot(_lp_row(lp_irf, outcome="accounting_identity_total_qoq", horizon=int(horizon)))
        gap = _snapshot(_lp_row(lp_irf, outcome="accounting_identity_gap_qoq", horizon=int(horizon)))
        if residual is None and total is None and gap is None:
            continue
        arithmetic_gap = (
            None
            if residual is None or total is None
            else float(residual["beta"]) - float(total["beta"])
        )
        gap_share = (
            None
            if gap is None or residual is None or float(residual["beta"]) == 0.0
            else abs(float(gap["beta"])) / abs(float(residual["beta"]))
        )
        if gap_share is None:
            interpretation = "missing_alignment_inputs"
        elif gap_share <= 0.1:
            interpretation = "tight_closure"
        elif gap_share <= 0.5:
            interpretation = "partial_closure"
        else:
            interpretation = "large_closure_gap"
        rows.append(
            {
                "horizon": int(horizon),
                "residual_beta": None if residual is None else float(residual["beta"]),
                "accounting_total_beta": None if total is None else float(total["beta"]),
                "identity_gap_beta": None if gap is None else float(gap["beta"]),
                "arithmetic_residual_minus_total_beta": arithmetic_gap,
                "identity_gap_share_of_residual": gap_share,
                "residual_n": None if residual is None else int(residual["n"]),
                "accounting_total_n": None if total is None else int(total["n"]),
                "identity_gap_n": None if gap is None else int(gap["n"]),
                "interpretation": interpretation,
            }
        )
    columns = [
        "horizon",
        "residual_beta",
        "accounting_total_beta",
        "identity_gap_beta",
        "arithmetic_residual_minus_total_beta",
        "identity_gap_share_of_residual",
        "residual_n",
        "accounting_total_n",
        "identity_gap_n",
        "interpretation",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def build_accounting_identity_summary(
    *,
    lp_irf: pd.DataFrame,
    accounting_source_kind: str = "not_available",
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    primary_lp_irf = slice_accounting_identity_lp_irf(lp_irf)
    key_horizons: dict[str, Any] = {}
    observed_components: set[str] = set()

    for horizon in horizons:
        components: dict[str, Any] = {}
        for outcome in ACCOUNTING_COMPONENT_OUTCOMES:
            snapshot = _snapshot(_lp_row(primary_lp_irf, outcome=outcome, horizon=horizon))
            if snapshot is not None:
                components[outcome] = snapshot
                observed_components.add(outcome)
        residual = _snapshot(_lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon))
        total = _snapshot(_lp_row(primary_lp_irf, outcome="accounting_identity_total_qoq", horizon=horizon))
        gap = _snapshot(_lp_row(primary_lp_irf, outcome="accounting_identity_gap_qoq", horizon=horizon))
        arithmetic_gap = None if residual is None or total is None else float(residual["beta"]) - float(total["beta"])
        gap_share = None if gap is None or residual is None or float(residual["beta"]) == 0.0 else abs(
            float(gap["beta"])
        ) / abs(float(residual["beta"]))
        if gap_share is None:
            interpretation = "missing_alignment_inputs"
        elif gap_share <= 0.1:
            interpretation = "tight_closure"
        elif gap_share <= 0.5:
            interpretation = "partial_closure"
        else:
            interpretation = "large_closure_gap"
        key_horizons[f"h{horizon}"] = {
            "other_component": residual,
            "accounting_identity_total": total,
            "accounting_identity_gap": gap,
            "accounting_components": components,
            "arithmetic_residual_minus_total_beta": arithmetic_gap,
            "identity_gap_share_of_residual": gap_share,
            "interpretation": interpretation,
        }

    status = "not_available"
    if key_horizons and any(payload["accounting_identity_total"] is not None for payload in key_horizons.values()):
        status = "available"

    observed_interpretations = [
        str(payload.get("interpretation", "missing_alignment_inputs"))
        for payload in key_horizons.values()
        if payload.get("accounting_identity_total") is not None
    ]

    takeaways = [
        "This accounting lane is separate from the current public counterpart/proxy scorecard and should be read as an imported reconstruction check on the non-TDC residual."
    ]
    if accounting_source_kind != "not_available":
        takeaways.append(
            f"Imported accounting source kind: {accounting_source_kind}."
        )
    if observed_components:
        takeaways.append(
            f"Observed accounting component outcomes currently present: {', '.join(sorted(observed_components))}."
        )
        takeaways.append(
            "Arithmetic residual-minus-total and the direct gap response need not match exactly because LP outcomes can use outcome-specific samples."
        )
        if observed_interpretations and all(item == "large_closure_gap" for item in observed_interpretations):
            takeaways.append(
                "The imported accounting reconstruction currently shows a large closure gap at all reported horizons, so it should be treated as a draft or scale-mismatched secondary check rather than a validated measurement of the non-TDC residual."
            )
        elif observed_interpretations and all(item == "tight_closure" for item in observed_interpretations):
            takeaways.append(
                "All reported horizons show tight accounting closure in the imported lane. Read that as a closure-oriented cross-check on the non-TDC residual, not by itself as independent validation of each accounting component."
            )
        elif observed_interpretations and any(item == "tight_closure" for item in observed_interpretations):
            takeaways.append(
                "At least one reported horizon shows tight accounting closure, so the imported reconstruction is partly lining up with the non-TDC residual."
            )
    else:
        takeaways.append("No imported accounting component outcomes are materialized yet.")

    return {
        "status": status,
        "source_kind": accounting_source_kind,
        "headline_question": "Does an imported accounting reconstruction line up with the non-TDC deposit component under the baseline TDC shock design?",
        "estimation_path": {
            "primary_artifact": "lp_irf_accounting_identity.csv",
            "alignment_artifact": "accounting_identity_alignment.csv",
            "summary_artifact": "accounting_identity_summary.json",
        },
        "component_outcomes_present": sorted(observed_components),
        "horizons": key_horizons,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
