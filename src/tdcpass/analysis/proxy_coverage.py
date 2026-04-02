from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.structural_proxy_evidence import PROXY_OUTCOMES


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


def _beta_sign(snapshot: dict[str, Any] | None) -> str:
    if snapshot is None:
        return "missing"
    beta = float(snapshot["beta"])
    if beta > 0.0:
        return "positive"
    if beta < 0.0:
        return "negative"
    return "zero"


def _sign_alignment(other: dict[str, Any] | None, proxy: dict[str, Any] | None) -> str:
    other_sign = _beta_sign(other)
    proxy_sign = _beta_sign(proxy)
    if "missing" in {other_sign, proxy_sign}:
        return "missing"
    if "zero" in {other_sign, proxy_sign}:
        return "zero_involved"
    if other_sign == proxy_sign:
        return "same_sign"
    return "opposite_sign"


def _build_context(df: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    other = _snapshot(_lp_row(df, outcome="other_component_qoq", horizon=horizon))
    proxy_rows: list[dict[str, Any]] = []
    for proxy_outcome in PROXY_OUTCOMES:
        proxy = _snapshot(_lp_row(df, outcome=proxy_outcome, horizon=horizon))
        proxy_rows.append(
            {
                "proxy_outcome": proxy_outcome,
                "snapshot": proxy,
                "sign_alignment": _sign_alignment(other, proxy),
            }
        )

    proxy_bundle_beta_sum = sum(
        float(row["snapshot"]["beta"]) for row in proxy_rows if row["snapshot"] is not None
    )
    proxy_bundle_share_of_other_beta = None
    unexplained_beta = None
    unexplained_share_of_other_beta = None
    if other is not None and abs(float(other["beta"])) > 1e-12:
        proxy_bundle_share_of_other_beta = proxy_bundle_beta_sum / float(other["beta"])
        unexplained_beta = float(other["beta"]) - proxy_bundle_beta_sum
        unexplained_share_of_other_beta = unexplained_beta / float(other["beta"])

    same_sign_proxy_count = sum(1 for row in proxy_rows if row["sign_alignment"] == "same_sign")
    opposite_sign_proxy_count = sum(1 for row in proxy_rows if row["sign_alignment"] == "opposite_sign")
    decisive_same_sign_proxy_count = sum(
        1
        for row in proxy_rows
        if row["sign_alignment"] == "same_sign"
        and row["snapshot"] is not None
        and bool(row["snapshot"]["ci_excludes_zero"])
    )
    decisive_opposite_sign_proxy_count = sum(
        1
        for row in proxy_rows
        if row["sign_alignment"] == "opposite_sign"
        and row["snapshot"] is not None
        and bool(row["snapshot"]["ci_excludes_zero"])
    )
    decisive_proxy_count = decisive_same_sign_proxy_count + decisive_opposite_sign_proxy_count

    coverage_label = "proxy_bundle_weak"
    if other is None:
        coverage_label = "missing_other_component_response"
    elif not bool(other["ci_excludes_zero"]):
        coverage_label = "other_component_not_decisive"
    elif decisive_opposite_sign_proxy_count > 0:
        coverage_label = "proxy_bundle_opposite_direction"
    elif (
        same_sign_proxy_count >= 2
        and proxy_bundle_share_of_other_beta is not None
        and abs(proxy_bundle_share_of_other_beta) >= 0.6
        and decisive_same_sign_proxy_count == 0
    ):
        coverage_label = "proxy_bundle_same_sign_but_not_decisive"
    elif (
        decisive_same_sign_proxy_count >= 1
        and proxy_bundle_share_of_other_beta is not None
        and abs(proxy_bundle_share_of_other_beta) >= 0.6
    ):
        coverage_label = "proxy_bundle_mostly_covers_other"
    elif (
        same_sign_proxy_count >= 1
        and proxy_bundle_share_of_other_beta is not None
        and abs(proxy_bundle_share_of_other_beta) >= 0.4
    ):
        coverage_label = "proxy_bundle_partial_same_sign_support"
    elif (
        unexplained_share_of_other_beta is not None
        and abs(unexplained_share_of_other_beta) >= 0.75
    ):
        coverage_label = "proxy_bundle_uncovered_remainder_large"

    return {
        "other_component": other,
        "proxy_bundle_beta_sum": proxy_bundle_beta_sum,
        "proxy_bundle_share_of_other_beta": proxy_bundle_share_of_other_beta,
        "unexplained_beta": unexplained_beta,
        "unexplained_share_of_other_beta": unexplained_share_of_other_beta,
        "same_sign_proxy_count": same_sign_proxy_count,
        "opposite_sign_proxy_count": opposite_sign_proxy_count,
        "decisive_proxy_count": decisive_proxy_count,
        "decisive_same_sign_proxy_count": decisive_same_sign_proxy_count,
        "decisive_opposite_sign_proxy_count": decisive_opposite_sign_proxy_count,
        "coverage_label": coverage_label,
        "proxy_rows": [
            {
                "proxy_outcome": row["proxy_outcome"],
                "beta": None if row["snapshot"] is None else float(row["snapshot"]["beta"]),
                "se": None if row["snapshot"] is None else float(row["snapshot"]["se"]),
                "lower95": None if row["snapshot"] is None else float(row["snapshot"]["lower95"]),
                "upper95": None if row["snapshot"] is None else float(row["snapshot"]["upper95"]),
                "n": None if row["snapshot"] is None else int(row["snapshot"]["n"]),
                "ci_excludes_zero": False if row["snapshot"] is None else bool(row["snapshot"]["ci_excludes_zero"]),
                "sign_alignment": row["sign_alignment"],
            }
            for row in proxy_rows
        ],
    }


def _status_from_key_horizons(key_horizons: dict[str, dict[str, Any]]) -> str:
    labels = {str(payload.get("coverage_label", "")) for payload in key_horizons.values()}
    if labels & {"proxy_bundle_opposite_direction"}:
        return "mixed"
    if labels and labels <= {
        "proxy_bundle_uncovered_remainder_large",
        "proxy_bundle_weak",
        "other_component_not_decisive",
        "missing_other_component_response",
    }:
        return "weak"
    if labels <= {
        "proxy_bundle_mostly_covers_other",
        "proxy_bundle_partial_same_sign_support",
        "proxy_bundle_same_sign_but_not_decisive",
    }:
        return "partial_support"
    return "mixed"


def _regime_contexts(
    *,
    lp_irf_regimes: pd.DataFrame,
    horizons: tuple[int, ...],
    regime_diagnostics: dict[str, Any] | None,
    regime_specs: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if lp_irf_regimes.empty:
        return []

    diagnostic_rows: dict[str, dict[str, Any]] = {}
    if regime_diagnostics is not None:
        for row in regime_diagnostics.get("regimes", []):
            if isinstance(row, dict) and "regime" in row:
                diagnostic_rows[str(row["regime"])] = row
    publication_roles: dict[str, str] = {}
    if regime_specs is not None:
        for name, definition in regime_specs.get("regimes", {}).items():
            if isinstance(definition, dict):
                publication_roles[str(name)] = str(definition.get("publication_role", "published"))

    base_names = sorted({str(name).rsplit("_", 1)[0] for name in lp_irf_regimes["regime"].drop_duplicates().tolist()})
    rows: list[dict[str, Any]] = []
    for base_name in base_names:
        diag = diagnostic_rows.get(base_name, {})
        publication_role = publication_roles.get(base_name, str(diag.get("publication_role", "published")))
        if publication_role == "diagnostic_only":
            continue
        if not bool(diag.get("stable_for_interpretation", False)):
            continue
        regime_payload: dict[str, Any] = {
            "regime": base_name,
            "stable_for_interpretation": True,
            "publication_role": publication_role,
            "stability_warnings": list(diag.get("stability_warnings", [])),
            "horizons": {},
        }
        for horizon in horizons:
            high_df = lp_irf_regimes[lp_irf_regimes["regime"] == f"{base_name}_high"]
            low_df = lp_irf_regimes[lp_irf_regimes["regime"] == f"{base_name}_low"]
            regime_payload["horizons"][f"h{horizon}"] = {
                "high": _build_context(high_df, horizon=horizon),
                "low": _build_context(low_df, horizon=horizon),
            }
        rows.append(regime_payload)
    return rows


def build_proxy_coverage_summary(
    *,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    lp_irf_regimes: pd.DataFrame,
    regime_diagnostics: dict[str, Any] | None = None,
    regime_specs: dict[str, Any] | None = None,
    proxy_unit_audit: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    primary_lp_irf = identity_lp_irf if identity_lp_irf is not None and not identity_lp_irf.empty else lp_irf
    primary_decomposition_mode = (
        "exact_identity_baseline"
        if identity_lp_irf is not None and not identity_lp_irf.empty
        else "approximate_dynamic_decomposition"
    )
    key_horizons = {f"h{horizon}": _build_context(primary_lp_irf, horizon=horizon) for horizon in horizons}
    status = _status_from_key_horizons(key_horizons)
    large_gap_horizons = [
        name for name, payload in key_horizons.items() if payload["coverage_label"] == "proxy_bundle_uncovered_remainder_large"
    ]
    same_sign_not_decisive_horizons = [
        name
        for name, payload in key_horizons.items()
        if payload["coverage_label"] == "proxy_bundle_same_sign_but_not_decisive"
    ]
    mostly_covered_horizons = [
        name
        for name, payload in key_horizons.items()
        if payload["coverage_label"] == "proxy_bundle_mostly_covers_other"
    ]

    takeaways = [
        "Proxy coverage compares the non-TDC residual with the summed structural proxy response, so it can show whether the residual is mostly covered or still leaves a large unexplained remainder.",
    ]
    if mostly_covered_horizons:
        takeaways.append(
            f"At {', '.join(mostly_covered_horizons)}, the proxy bundle covers most of the non-TDC response with at least one decisive same-sign proxy."
        )
    if same_sign_not_decisive_horizons:
        takeaways.append(
            f"At {', '.join(same_sign_not_decisive_horizons)}, the proxy bundle lines up in sign and size but remains statistically non-decisive."
        )
    if large_gap_horizons:
        takeaways.append(
            f"At {', '.join(large_gap_horizons)}, the proxy bundle leaves a large uncovered remainder, so the current mechanism bundle is incomplete."
        )

    history_limits: list[dict[str, Any]] = []
    if proxy_unit_audit is not None:
        for row in proxy_unit_audit.get("derived_proxies", []):
            if isinstance(row, dict):
                history_limits.append(
                    {
                        "proxy_outcome": str(row.get("proxy_outcome", row.get("proxy", ""))),
                        "start_quarter": row.get("start_quarter"),
                        "non_missing_obs": row.get("non_missing_obs"),
                    }
                )

    return {
        "status": status,
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": "lp_irf_identity_baseline.csv"
            if primary_decomposition_mode == "exact_identity_baseline"
            else "lp_irf.csv",
            "secondary_artifact": "lp_irf.csv" if primary_decomposition_mode == "exact_identity_baseline" else None,
        },
        "headline_question": "How much of the non-TDC response is covered by the current structural proxy bundle?",
        "covered_channel_families": [
            {
                "proxy_outcome": "bank_credit_private_qoq",
                "channel_family": "private_bank_credit",
                "description": "Bank-side private credit expansion net of Treasury and agency securities.",
            },
            {
                "proxy_outcome": "cb_nonts_qoq",
                "channel_family": "fed_liquidity_plumbing",
                "description": "Central-bank and liquidity-plumbing counterpart movements proxied by reserves plus the TGA.",
            },
            {
                "proxy_outcome": "foreign_nonts_qoq",
                "channel_family": "foreign_deposit_absorption",
                "description": "Foreign-sector total-deposit movements captured from Z.1.",
            },
            {
                "proxy_outcome": "domestic_nonfinancial_mmf_reallocation_qoq",
                "channel_family": "domestic_mmf_portfolio_shift",
                "description": "Sign-normalized declines in domestic nonfinancial money market fund holdings, interpreted as cash reallocations toward deposits.",
            },
            {
                "proxy_outcome": "domestic_nonfinancial_repo_reallocation_qoq",
                "channel_family": "domestic_repo_portfolio_shift",
                "description": "Sign-normalized declines in domestic nonfinancial repo assets, interpreted as cash reallocations toward deposits.",
            },
        ],
        "major_uncovered_channel_families": [
            "within_bank_deposit_substitution_across_holders_or_products",
            "omitted_bank_asset_side_channels_outside_private_credit",
            "domestic_public_sector_and_wholesale_deposit_channels",
            "additional_cash_management_channels_outside_mmf_and_repo",
        ],
        "history_limits": history_limits,
        "key_horizons": key_horizons,
        "published_regime_contexts": _regime_contexts(
            lp_irf_regimes=lp_irf_regimes,
            horizons=horizons,
            regime_diagnostics=regime_diagnostics,
            regime_specs=regime_specs,
        ),
        "release_caveat": "The current public proxy bundle spans a limited set of channel families and still does not exhaust the bank-only non-TDC residual.",
        "takeaways": takeaways,
    }
