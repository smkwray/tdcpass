from __future__ import annotations

from typing import Any

import pandas as pd

DEPOSIT_COMPONENT_OUTCOMES: tuple[str, ...] = (
    "checkable_deposits_bank_qoq",
    "interbank_transactions_bank_qoq",
    "time_savings_deposits_bank_qoq",
    "checkable_federal_govt_bank_qoq",
    "checkable_state_local_bank_qoq",
    "checkable_rest_of_world_bank_qoq",
    "checkable_private_domestic_bank_qoq",
)
CREATOR_LENDING_OUTCOMES: tuple[str, ...] = (
    "commercial_industrial_loans_qoq",
    "construction_land_development_loans_qoq",
    "cre_multifamily_loans_qoq",
    "cre_nonfarm_nonresidential_loans_qoq",
    "consumer_loans_qoq",
    "credit_card_revolving_loans_qoq",
    "auto_loans_qoq",
    "other_consumer_loans_qoq",
    "heloc_loans_qoq",
    "closed_end_residential_loans_qoq",
    "commercial_industrial_loans_ex_chargeoffs_qoq",
    "consumer_loans_ex_chargeoffs_qoq",
    "credit_card_revolving_loans_ex_chargeoffs_qoq",
    "other_consumer_loans_ex_chargeoffs_qoq",
    "closed_end_residential_loans_ex_chargeoffs_qoq",
    "loans_to_commercial_banks_qoq",
    "loans_to_nondepository_financial_institutions_qoq",
    "loans_for_purchasing_or_carrying_securities_qoq",
    "treasury_securities_bank_qoq",
    "agency_gse_backed_securities_bank_qoq",
    "municipal_securities_bank_qoq",
    "corporate_foreign_bonds_bank_qoq",
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


def _component_rows(lp_irf: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for outcome in DEPOSIT_COMPONENT_OUTCOMES:
        snapshot = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if snapshot is not None:
            rows[outcome] = snapshot
    return rows


def _creator_rows(lp_irf: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for outcome in CREATOR_LENDING_OUTCOMES:
        snapshot = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if snapshot is not None:
            rows[outcome] = snapshot
    return rows


def build_deposit_component_scorecard(
    *,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    proxy_coverage_summary: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    primary_lp_irf = identity_lp_irf if identity_lp_irf is not None and not identity_lp_irf.empty else lp_irf
    primary_decomposition_mode = (
        "exact_identity_baseline"
        if identity_lp_irf is not None and not identity_lp_irf.empty
        else "approximate_dynamic_decomposition"
    )
    key_horizons: dict[str, Any] = {}
    observed_components: set[str] = set()
    observed_creators: set[str] = set()

    for horizon in horizons:
        components = _component_rows(lp_irf, horizon=horizon)
        creators = _creator_rows(lp_irf, horizon=horizon)
        observed_components.update(components)
        observed_creators.update(creators)
        proxy_context = (
            {}
            if proxy_coverage_summary is None
            else dict(proxy_coverage_summary.get("key_horizons", {}).get(f"h{horizon}", {}))
        )
        key_horizons[f"h{horizon}"] = {
            "tdc": _snapshot(_lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)),
            "total": _snapshot(_lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)),
            "other_component": _snapshot(_lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            "z1_deposit_components": components,
            "creator_lending_channels": creators,
            "proxy_bundle_beta_sum": proxy_context.get("proxy_bundle_beta_sum"),
            "unexplained_beta": proxy_context.get("unexplained_beta"),
            "unexplained_share_of_other_beta": proxy_context.get("unexplained_share_of_other_beta"),
            "proxy_coverage_label": proxy_context.get("coverage_label"),
            "uncovered_channel_families": list(proxy_coverage_summary.get("major_uncovered_channel_families", []))
            if proxy_coverage_summary is not None
            else [],
        }

    status = "not_available"
    if key_horizons and any(payload["tdc"] is not None for payload in key_horizons.values()):
        status = "available"

    takeaways = [
        "This scorecard is a secondary side read that pairs the exact-baseline TDC/total/other decomposition with observed deposit-type LP responses and the current proxy uncovered remainder."
    ]
    if observed_components:
        takeaways.append(
            f"Observed deposit-component outcomes currently present: {', '.join(sorted(observed_components))}."
        )
    elif lp_irf.empty:
        takeaways.append("No baseline LP evidence is available yet for deposit-component outcomes.")
    else:
        takeaways.append("Deposit-component outcomes are not yet materialized in the baseline LP output.")
    if observed_creators:
        takeaways.append(
            f"Observed creator-lending outcomes currently present: {', '.join(sorted(observed_creators))}."
        )

    return {
        "status": status,
        "headline_question": "As a secondary side read, which observable deposit types move alongside the non-TDC deposit response?",
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": (
                "lp_irf_identity_baseline.csv" if primary_decomposition_mode == "exact_identity_baseline" else "lp_irf.csv"
            ),
            "component_artifact": "lp_irf.csv",
            "proxy_coverage_artifact": "proxy_coverage_summary.json",
        },
        "component_outcomes_present": sorted(observed_components),
        "creator_channel_outcomes_present": sorted(observed_creators),
        "horizons": key_horizons,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
