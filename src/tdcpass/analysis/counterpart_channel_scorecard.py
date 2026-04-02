from __future__ import annotations

from typing import Any

import pandas as pd

CORE_CREATOR_LENDING_OUTCOMES: tuple[str, ...] = (
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
)
NONCORE_CREATOR_LENDING_OUTCOMES: tuple[str, ...] = (
    "loans_to_commercial_banks_qoq",
    "loans_to_nondepository_financial_institutions_qoq",
    "loans_for_purchasing_or_carrying_securities_qoq",
)
ASSET_PURCHASE_CREATOR_OUTCOMES: tuple[str, ...] = (
    "treasury_securities_bank_qoq",
    "agency_gse_backed_securities_bank_qoq",
    "municipal_securities_bank_qoq",
    "corporate_foreign_bonds_bank_qoq",
)
ACTIVE_FUNDING_ACCOMMODATION_OUTCOMES: tuple[str, ...] = (
    "interbank_transactions_bank_qoq",
    "fedfunds_repo_liabilities_bank_qoq",
    "commercial_bank_borrowings_qoq",
    "fed_borrowings_depository_institutions_qoq",
    "debt_securities_bank_liability_qoq",
    "fhlb_advances_sallie_mae_loans_bank_qoq",
)
FUNDING_ACCOMMODATION_CLEANUP_CANDIDATES: tuple[str, ...] = (
    "holding_company_parent_funding_bank_qoq",
)
ESCAPE_SUPPORT_OUTCOMES: tuple[str, ...] = (
    "domestic_nonfinancial_mmf_reallocation_qoq",
    "domestic_nonfinancial_repo_reallocation_qoq",
    "on_rrp_reallocation_qoq",
    "household_treasury_securities_reallocation_qoq",
    "mmf_treasury_bills_reallocation_qoq",
    "currency_reallocation_qoq",
)
EXTERNAL_ESCAPE_OUTCOMES: tuple[str, ...] = (
    "foreign_nonts_qoq",
    "checkable_rest_of_world_bank_qoq",
    "interbank_transactions_foreign_banks_liability_qoq",
    "interbank_transactions_foreign_banks_asset_qoq",
    "deposits_at_foreign_banks_asset_qoq",
)
PLUMBING_CONTEXT_OUTCOMES: tuple[str, ...] = (
    "tga_qoq",
    "reserves_qoq",
    "cb_nonts_qoq",
)
CREATOR_CHANNEL_OUTCOMES: tuple[str, ...] = (
    *CORE_CREATOR_LENDING_OUTCOMES,
    *NONCORE_CREATOR_LENDING_OUTCOMES,
    *ASSET_PURCHASE_CREATOR_OUTCOMES,
)
CORE_TARGET_MAPPINGS: tuple[dict[str, str], ...] = (
    {
        "scientific_target": "ci_us_qoq",
        "current_live_proxy": "commercial_industrial_loans_qoq",
        "status": "scope_mismatch_current_public_path",
        "scope": "all_commercial_bank_c_and_i",
        "interpretation": "Treat as a broad all-bank creator proxy, not exact U.S.-addressee domestic nonfinancial C&I.",
    },
    {
        "scientific_target": "construction_land_dev_qoq",
        "current_live_proxy": "construction_land_development_loans_qoq",
        "status": "near_match_current_public_path",
        "scope": "all_commercial_bank_construction_and_land_development",
        "interpretation": "Usable first-pass public creator lane, but still on all-commercial-bank scope rather than a narrower domestic counterpart object.",
    },
    {
        "scientific_target": "cre_multifamily_qoq",
        "current_live_proxy": "cre_multifamily_loans_qoq",
        "status": "near_match_current_public_path",
        "scope": "all_commercial_bank_multifamily",
        "interpretation": "Close public category match, but still broader than a bank-only domestic counterpart-accounting object.",
    },
    {
        "scientific_target": "cre_nonfarm_nonres_qoq",
        "current_live_proxy": "cre_nonfarm_nonresidential_loans_qoq",
        "status": "near_match_current_public_path",
        "scope": "all_commercial_bank_nonfarm_nonresidential_cre",
        "interpretation": "Close public category match, but still on broad all-commercial-bank scope.",
    },
    {
        "scientific_target": "consumer_credit_qoq",
        "current_live_proxy": "consumer_loans_qoq",
        "status": "near_match_current_public_path",
        "scope": "all_commercial_bank_consumer_loans",
        "interpretation": "Broad public consumer creator lane, with narrower subcomponents available as secondary detail.",
    },
)
NONCORE_TARGET_MAPPINGS: tuple[dict[str, str], ...] = (
    {
        "excluded_family": "loans_to_depositories_qoq",
        "current_live_proxy": "loans_to_commercial_banks_qoq",
        "status": "materialized",
        "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
    },
    {
        "excluded_family": "loans_to_ndfis_qoq",
        "current_live_proxy": "loans_to_nondepository_financial_institutions_qoq",
        "status": "materialized",
        "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
    },
    {
        "excluded_family": "securities_purpose_loans_qoq",
        "current_live_proxy": "loans_for_purchasing_or_carrying_securities_qoq",
        "status": "materialized",
        "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
    },
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


def _creator_rows(
    lp_irf: pd.DataFrame,
    *,
    horizon: int,
    outcomes: tuple[str, ...] = CREATOR_CHANNEL_OUTCOMES,
) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for outcome in outcomes:
        snapshot = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if snapshot is not None:
            rows[outcome] = snapshot
    return rows


def _decisive_creator_channels(creator_rows: dict[str, Any], *, sign: str) -> list[str]:
    if sign not in {"positive", "negative"}:
        raise ValueError(f"Unsupported sign: {sign}")
    threshold = 1.0 if sign == "positive" else -1.0
    return sorted(
        outcome
        for outcome, snapshot in creator_rows.items()
        if snapshot["ci_excludes_zero"] and snapshot["beta"] * threshold > 0.0
    )


def _decisive_snapshot_channels(rows: dict[str, Any], *, sign: str) -> list[str]:
    if sign not in {"positive", "negative"}:
        raise ValueError(f"Unsupported sign: {sign}")
    threshold = 1.0 if sign == "positive" else -1.0
    return sorted(
        outcome
        for outcome, snapshot in rows.items()
        if snapshot["ci_excludes_zero"] and snapshot["beta"] * threshold > 0.0
    )


def _plumbing_rows(lp_irf: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for outcome in PLUMBING_CONTEXT_OUTCOMES:
        snapshot = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if snapshot is not None:
            rows[outcome] = snapshot
    return rows


def _escape_rows(
    lp_irf: pd.DataFrame,
    *,
    horizon: int,
    outcomes: tuple[str, ...],
) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for outcome in outcomes:
        snapshot = _snapshot(_lp_row(lp_irf, outcome=outcome, horizon=horizon))
        if snapshot is not None:
            rows[outcome] = snapshot
    return rows


def _asset_purchase_plumbing_interpretation(
    *,
    decisive_positive_asset_purchase_channels: list[str],
    plumbing_rows: dict[str, Any],
) -> str:
    if not decisive_positive_asset_purchase_channels:
        return "no_decisive_asset_purchase_creator_signal"
    tga = plumbing_rows.get("tga_qoq")
    reserves = plumbing_rows.get("reserves_qoq")
    cb_nonts = plumbing_rows.get("cb_nonts_qoq")
    tga_positive = bool(tga and tga["ci_excludes_zero"] and tga["beta"] > 0.0)
    reserves_positive = bool(reserves and reserves["ci_excludes_zero"] and reserves["beta"] > 0.0)
    cb_positive = bool(cb_nonts and cb_nonts["ci_excludes_zero"] and cb_nonts["beta"] > 0.0)
    cb_negative = bool(cb_nonts and cb_nonts["ci_excludes_zero"] and cb_nonts["beta"] < 0.0)
    if tga_positive and reserves_positive:
        return "mixed_treasury_drain_and_reserve_support"
    if tga_positive:
        return "treasury_drain_context"
    if reserves_positive:
        return "reserve_support_context"
    if cb_positive:
        return "positive_combined_central_bank_plumbing_context"
    if cb_negative:
        return "negative_combined_central_bank_plumbing_context"
    return "mixed_or_nondecisive_plumbing_context"


def _escape_support_interpretation(escape_rows: dict[str, Any]) -> str:
    decisive_positive = _decisive_snapshot_channels(escape_rows, sign="positive")
    decisive_negative = _decisive_snapshot_channels(escape_rows, sign="negative")
    if decisive_positive and decisive_negative:
        return "mixed_escape_and_support_signals"
    if decisive_positive:
        return "deposit_retention_support_signal"
    if decisive_negative:
        return "escape_pressure_signal"
    return "escape_support_unresolved"


def _funding_accommodation_interpretation(funding_rows: dict[str, Any]) -> str:
    decisive_positive = _decisive_snapshot_channels(funding_rows, sign="positive")
    decisive_negative = _decisive_snapshot_channels(funding_rows, sign="negative")
    if decisive_positive and decisive_negative:
        return "mixed_funding_accommodation_signals"
    if decisive_positive:
        return "positive_funding_accommodation_signal"
    if decisive_negative:
        return "negative_funding_accommodation_signal"
    return "funding_accommodation_unresolved"


def _target_mapping_payload() -> dict[str, Any]:
    return {
        "priority_gap": "ci_us_qoq",
        "next_build_priority": "funding_accommodation_lane_then_external_banking_or_narrower_us_addressee_mapping",
        "core_creator_targets": [dict(item) for item in CORE_TARGET_MAPPINGS],
        "excluded_or_noncore_families": [dict(item) for item in NONCORE_TARGET_MAPPINGS],
        "bank_asset_purchase_lane": {
            "status": "materialized",
            "outcomes": list(ASSET_PURCHASE_CREATOR_OUTCOMES),
            "interpretation": "Potential creator evidence from quarterly bank asset accumulation; pair with TGA and Fed context rather than treating raw securities growth as direct deposit creation by itself.",
        },
        "destroyer_escape_lane": {
            "status": "materialized",
            "outcomes": list(ESCAPE_SUPPORT_OUTCOMES),
            "interpretation": "Sign-normalized reallocation support channels spanning MMFs, repo, ON RRP, direct Treasury / bill absorption, and public currency; positive means less deposit escape / more retention support, negative means escape pressure out of bank deposits.",
        },
        "external_escape_lane": {
            "status": "expanded",
            "outcomes": list(EXTERNAL_ESCAPE_OUTCOMES),
            "interpretation": "External banking context now spans broad foreign nontransaction liabilities plus bank-specific foreign interbank liabilities, foreign interbank assets, and deposits at foreign banks; keep these separate from the domestic escape-support subtotal.",
        },
        "funding_accommodation_lane": {
            "status": "materialized_with_cleanup_candidate",
            "active_outcomes": list(ACTIVE_FUNDING_ACCOMMODATION_OUTCOMES),
            "cleanup_candidates": list(FUNDING_ACCOMMODATION_CLEANUP_CANDIDATES),
            "interpretation": "Separate bank funding-accommodation channels spanning interbank liabilities, repo/fed-funds-style liabilities, total borrowings, Fed borrowing, debt securities, and FHLB advances; parent/affiliate funding remains a cleanup candidate rather than an active interpreted lane.",
        },
    }


def build_counterpart_channel_scorecard(
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
    observed_creators: set[str] = set()
    observed_funding: set[str] = set()

    for horizon in horizons:
        creators = _creator_rows(lp_irf, horizon=horizon)
        core_creators = _creator_rows(lp_irf, horizon=horizon, outcomes=CORE_CREATOR_LENDING_OUTCOMES)
        noncore_creators = _creator_rows(lp_irf, horizon=horizon, outcomes=NONCORE_CREATOR_LENDING_OUTCOMES)
        asset_purchase_creators = _creator_rows(lp_irf, horizon=horizon, outcomes=ASSET_PURCHASE_CREATOR_OUTCOMES)
        funding_accommodation_rows = _escape_rows(
            lp_irf, horizon=horizon, outcomes=ACTIVE_FUNDING_ACCOMMODATION_OUTCOMES
        )
        funding_cleanup_candidate_rows = _escape_rows(
            lp_irf, horizon=horizon, outcomes=FUNDING_ACCOMMODATION_CLEANUP_CANDIDATES
        )
        escape_support_rows = _escape_rows(lp_irf, horizon=horizon, outcomes=ESCAPE_SUPPORT_OUTCOMES)
        external_escape_rows = _escape_rows(lp_irf, horizon=horizon, outcomes=EXTERNAL_ESCAPE_OUTCOMES)
        plumbing_rows = _plumbing_rows(lp_irf, horizon=horizon)
        observed_creators.update(creators)
        observed_funding.update(funding_accommodation_rows)
        decisive_positive_asset_purchase_channels = _decisive_creator_channels(asset_purchase_creators, sign="positive")
        decisive_negative_asset_purchase_channels = _decisive_creator_channels(asset_purchase_creators, sign="negative")
        proxy_context = (
            {}
            if proxy_coverage_summary is None
            else dict(proxy_coverage_summary.get("key_horizons", {}).get(f"h{horizon}", {}))
        )
        key_horizons[f"h{horizon}"] = {
            "tdc": _snapshot(_lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)),
            "total": _snapshot(_lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)),
            "other_component": _snapshot(_lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            "legacy_private_credit_proxy": {
                "role": "coarse_legacy_creator_proxy",
                "snapshot": _snapshot(_lp_row(lp_irf, outcome="bank_credit_private_qoq", horizon=horizon)),
            },
            "creator_lending_channels": creators,
            "core_domestic_creator_lending_channels": core_creators,
            "noncore_creator_lending_channels": noncore_creators,
            "creator_asset_purchase_channels": asset_purchase_creators,
            "funding_accommodation_channels": funding_accommodation_rows,
            "funding_accommodation_cleanup_candidates": funding_cleanup_candidate_rows,
            "deposit_retention_support_channels": escape_support_rows,
            "external_escape_channels": external_escape_rows,
            "funding_accommodation_context": {
                "channels": funding_accommodation_rows,
                "decisive_positive_channels": _decisive_snapshot_channels(
                    funding_accommodation_rows, sign="positive"
                ),
                "decisive_negative_channels": _decisive_snapshot_channels(
                    funding_accommodation_rows, sign="negative"
                ),
                "interpretation": _funding_accommodation_interpretation(
                    funding_accommodation_rows
                ),
            },
            "escape_support_context": {
                "channels": escape_support_rows,
                "decisive_positive_channels": _decisive_snapshot_channels(escape_support_rows, sign="positive"),
                "decisive_negative_channels": _decisive_snapshot_channels(escape_support_rows, sign="negative"),
                "interpretation": _escape_support_interpretation(escape_support_rows),
            },
            "asset_purchase_plumbing_context": {
                "channels": plumbing_rows,
                "decisive_positive_channels": _decisive_snapshot_channels(plumbing_rows, sign="positive"),
                "decisive_negative_channels": _decisive_snapshot_channels(plumbing_rows, sign="negative"),
                "treasury_drain_signal": bool(
                    plumbing_rows.get("tga_qoq")
                    and plumbing_rows["tga_qoq"]["ci_excludes_zero"]
                    and plumbing_rows["tga_qoq"]["beta"] > 0.0
                ),
                "reserve_support_signal": bool(
                    plumbing_rows.get("reserves_qoq")
                    and plumbing_rows["reserves_qoq"]["ci_excludes_zero"]
                    and plumbing_rows["reserves_qoq"]["beta"] > 0.0
                ),
                "interpretation": _asset_purchase_plumbing_interpretation(
                    decisive_positive_asset_purchase_channels=decisive_positive_asset_purchase_channels,
                    plumbing_rows=plumbing_rows,
                ),
            },
            "decisive_positive_creator_channels": _decisive_creator_channels(creators, sign="positive"),
            "decisive_negative_creator_channels": _decisive_creator_channels(creators, sign="negative"),
            "decisive_positive_core_creator_channels": _decisive_creator_channels(core_creators, sign="positive"),
            "decisive_negative_core_creator_channels": _decisive_creator_channels(core_creators, sign="negative"),
            "decisive_positive_noncore_creator_channels": _decisive_creator_channels(noncore_creators, sign="positive"),
            "decisive_negative_noncore_creator_channels": _decisive_creator_channels(noncore_creators, sign="negative"),
            "decisive_positive_asset_purchase_channels": decisive_positive_asset_purchase_channels,
            "decisive_negative_asset_purchase_channels": decisive_negative_asset_purchase_channels,
            "decisive_positive_retention_support_channels": _decisive_snapshot_channels(
                escape_support_rows, sign="positive"
            ),
            "decisive_negative_retention_support_channels": _decisive_snapshot_channels(
                escape_support_rows, sign="negative"
            ),
            "decisive_positive_external_escape_channels": _decisive_snapshot_channels(
                external_escape_rows, sign="positive"
            ),
            "decisive_negative_external_escape_channels": _decisive_snapshot_channels(
                external_escape_rows, sign="negative"
            ),
            "decisive_positive_funding_accommodation_channels": _decisive_snapshot_channels(
                funding_accommodation_rows, sign="positive"
            ),
            "decisive_negative_funding_accommodation_channels": _decisive_snapshot_channels(
                funding_accommodation_rows, sign="negative"
            ),
            "proxy_coverage_label": proxy_context.get("coverage_label"),
            "uncovered_channel_families": list(proxy_coverage_summary.get("major_uncovered_channel_families", []))
            if proxy_coverage_summary is not None
            else [],
        }

    status = "not_available"
    if key_horizons and any(payload["tdc"] is not None for payload in key_horizons.values()):
        status = "available"

    takeaways = [
        "This scorecard reframes the non-TDC question around counterpart creator channels rather than deposit types."
    ]
    if observed_creators:
        takeaways.append(
            f"First-wave creator outcomes currently present: {', '.join(sorted(observed_creators))}."
        )
        if any(name.endswith("_ex_chargeoffs_qoq") for name in observed_creators):
            takeaways.append(
                "Charge-off-adjusted creator lanes add back an approximate quarterly destruction flow using lagged balances and official annualized charge-off rates."
            )
        if any(name in observed_creators for name in NONCORE_CREATOR_LENDING_OUTCOMES):
            takeaways.append(
                "Intermediary, depository, and securities-purpose lending channels are now separated from the core domestic nonfinancial creator lanes."
            )
        if any(name in observed_creators for name in ASSET_PURCHASE_CREATOR_OUTCOMES):
            takeaways.append(
                "The first bank asset-purchase creator lane is now live via quarterly Z.1 holdings for Treasury, agency/GSE-backed, municipal, and corporate/foreign bond assets."
            )
            takeaways.append(
                "Asset-purchase creator signals are now paired with raw TGA, reserves, and combined central-bank plumbing responses so positive securities accumulation is not interpreted without Treasury/Fed context."
            )
        if any(
            name in key_horizon_payload.get("deposit_retention_support_channels", {})
            for key_horizon_payload in key_horizons.values()
            for name in ESCAPE_SUPPORT_OUTCOMES
        ):
            takeaways.append(
                "The first destroyer / escape block is now live through sign-normalized MMF, repo, ON RRP, direct Treasury / bill, and currency reallocation support channels."
            )
        if observed_funding:
            takeaways.append(
                "Funding accommodations are now tracked separately from creator and escape lanes so borrowing-side absorption is visible without being mislabeled as deposit creation."
            )
        takeaways.append(
            "Holding-company / parent funding remains a cleanup candidate rather than an active interpreted accommodation lane."
        )
        if any(
            name in key_horizon_payload.get("external_escape_channels", {})
            for key_horizon_payload in key_horizons.values()
            for name in EXTERNAL_ESCAPE_OUTCOMES
        ):
            takeaways.append(
                "The external banking lane now includes broad foreign liabilities plus bank-specific foreign interbank positions and deposits at foreign banks."
            )
        takeaways.append(
            "The biggest remaining creator-target gap is ci_us_qoq: the live public path still uses all-commercial-bank C&I rather than exact U.S.-addressee domestic nonfinancial C&I."
        )
    else:
        takeaways.append("No creator-channel LP outcomes are materialized yet.")

    return {
        "status": status,
        "headline_question": "Which first-wave creator and escape counterpart channels line up with the exact non-TDC deposit response?",
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": (
                "lp_irf_identity_baseline.csv"
                if primary_decomposition_mode == "exact_identity_baseline"
                else "lp_irf.csv"
            ),
            "creator_channel_artifact": "lp_irf.csv",
            "legacy_private_credit_artifact": "lp_irf.csv",
            "proxy_coverage_artifact": "proxy_coverage_summary.json",
        },
        "target_mapping": _target_mapping_payload(),
        "legacy_private_credit_proxy_role": "coarse_legacy_creator_proxy",
        "creator_channel_outcomes_present": sorted(observed_creators),
        "funding_accommodation_outcomes_present": sorted(observed_funding),
        "funding_accommodation_cleanup_candidates": list(FUNDING_ACCOMMODATION_CLEANUP_CANDIDATES),
        "horizons": key_horizons,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
