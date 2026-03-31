from __future__ import annotations

from typing import Any

import pandas as pd


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _state_row(
    df: pd.DataFrame,
    *,
    state_variant: str,
    state_label: str,
    outcome: str,
    horizon: int,
) -> dict[str, Any] | None:
    if df.empty:
        return None
    sample = df[
        (df["state_variant"] == state_variant)
        & (df["state_label"] == state_label)
        & (df["outcome"] == outcome)
        & (df["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _snapshot(row: dict[str, Any] | None, *, beta_key: str = "beta", lower_key: str = "lower95", upper_key: str = "upper95") -> dict[str, Any] | None:
    if row is None:
        return None
    beta = float(row[beta_key])
    lower95 = float(row[lower_key])
    upper95 = float(row[upper_key])
    return {
        "beta": beta,
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]) if "n" in row and row["n"] is not None else None,
        "sign": "positive" if beta > 0.0 else "negative" if beta < 0.0 else "zero",
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _state_payload(
    lp_irf_state_dependence: pd.DataFrame,
    *,
    state_variant: str,
    horizon: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for outcome in ("total_deposits_bank_qoq", "other_component_qoq"):
        low = _snapshot(
            _state_row(
                lp_irf_state_dependence,
                state_variant=state_variant,
                state_label="low",
                outcome=outcome,
                horizon=horizon,
            )
        )
        high = _snapshot(
            _state_row(
                lp_irf_state_dependence,
                state_variant=state_variant,
                state_label="high",
                outcome=outcome,
                horizon=horizon,
            )
        )
        payload[outcome] = {"low": low, "high": high}
    return payload


def _best_core_factor_variant(
    factor_control_diagnostics: dict[str, Any] | None,
) -> str | None:
    if factor_control_diagnostics is None:
        return None
    candidates = [
        row
        for row in factor_control_diagnostics.get("factor_variants", [])
        if isinstance(row, dict) and str(row.get("factor_role", "")) == "core"
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda row: float(row.get("min_key_horizon_n_ratio", -1.0)), reverse=True)
    return str(candidates[0]["factor_variant"])


def _factor_row(
    df: pd.DataFrame,
    *,
    factor_variant: str,
    outcome: str,
    horizon: int,
) -> dict[str, Any] | None:
    if df.empty:
        return None
    sample = df[
        (df["factor_variant"] == factor_variant)
        & (df["outcome"] == outcome)
        & (df["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def build_research_dashboard_summary(
    *,
    readiness: dict[str, Any],
    direct_identification: dict[str, Any] | None,
    shock_diagnostics: dict[str, Any] | None,
    lp_irf: pd.DataFrame,
    smoothed_lp_diagnostics: dict[str, Any] | None,
    lp_irf_state_dependence: pd.DataFrame,
    factor_control_sensitivity: pd.DataFrame,
    factor_control_diagnostics: dict[str, Any] | None,
    proxy_factor_summary: dict[str, Any] | None,
    state_proxy_factor_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    best_factor_variant = _best_core_factor_variant(factor_control_diagnostics)

    for horizon in horizons:
        horizon_key = f"h{horizon}"
        baseline_total = _snapshot(_lp_row(lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon))
        baseline_other = _snapshot(_lp_row(lp_irf, outcome="other_component_qoq", horizon=horizon))

        smoothed_horizon = {} if smoothed_lp_diagnostics is None else dict(smoothed_lp_diagnostics.get("key_horizons", {}).get(horizon_key, {}))

        factor_payload = None
        if best_factor_variant is not None:
            factor_payload = {
                "factor_variant": best_factor_variant,
                "total_deposits": _snapshot(
                    _factor_row(
                        factor_control_sensitivity,
                        factor_variant=best_factor_variant,
                        outcome="total_deposits_bank_qoq",
                        horizon=horizon,
                    )
                ),
                "other_component": _snapshot(
                    _factor_row(
                        factor_control_sensitivity,
                        factor_variant=best_factor_variant,
                        outcome="other_component_qoq",
                        horizon=horizon,
                    )
                ),
            }

        proxy_families = {}
        if proxy_factor_summary is not None:
            proxy_families = dict(proxy_factor_summary.get("key_horizons", {}).get(horizon_key, {}).get("families", {}))
        state_proxy_contexts = []
        if state_proxy_factor_summary is not None:
            for regime_row in state_proxy_factor_summary.get("regimes", []):
                if not isinstance(regime_row, dict):
                    continue
                horizon_payload = dict(regime_row.get("horizons", {}).get(horizon_key, {}))
                if not horizon_payload:
                    continue
                state_proxy_contexts.append(
                    {
                        "regime": str(regime_row.get("regime", "")),
                        "stable_for_interpretation": bool(regime_row.get("stable_for_interpretation", False)),
                        "publication_role": str(regime_row.get("publication_role", "unknown")),
                        "high": horizon_payload.get("high"),
                        "low": horizon_payload.get("low"),
                    }
                )

        key_horizons[horizon_key] = {
            "baseline": {
                "total_deposits": baseline_total,
                "other_component": baseline_other,
            },
            "smoothed": smoothed_horizon,
            "best_core_factor_control": factor_payload,
            "state_dependence": {
                "bank_absorption": _state_payload(lp_irf_state_dependence, state_variant="bank_absorption", horizon=horizon),
                "reserve_drain": _state_payload(lp_irf_state_dependence, state_variant="reserve_drain", horizon=horizon),
            },
            "proxy_families": proxy_families,
            "state_proxy_contexts": state_proxy_contexts,
        }

    statuses = {
        "readiness": str(readiness.get("status", "not_ready")),
        "direct_identification": None if direct_identification is None else str(direct_identification.get("status", "not_ready")),
        "treatment_quality": None if shock_diagnostics is None else str(shock_diagnostics.get("treatment_quality_status", "not_evaluated")),
        "smoothed_lp": None if smoothed_lp_diagnostics is None else str(smoothed_lp_diagnostics.get("status", "no_smoothed_rows")),
        "factor_controls": None if factor_control_diagnostics is None else str(factor_control_diagnostics.get("status", "no_factor_rows")),
        "proxy_factors": None if proxy_factor_summary is None else str(proxy_factor_summary.get("status", "weak")),
        "state_proxy_factors": None if state_proxy_factor_summary is None else str(state_proxy_factor_summary.get("status", "weak")),
    }

    takeaways = [
        "This dashboard compares the fixed-shock baseline against backend-only extensions: smoothed LPs, state dependence, factor-augmented controls, and grouped proxy baskets.",
    ]
    if statuses["factor_controls"] == "core_adequate":
        takeaways.append("A core factor-control specification now preserves the baseline LP sample and does not overturn the headline deposit-response signs at h0/h4.")
    if statuses["smoothed_lp"] == "stable":
        takeaways.append("The local smoothed LP overlay reduces horizon wobble without changing the headline deposit-response signs at h0/h4.")
    if statuses["proxy_factors"] == "weak":
        takeaways.append("Grouped funding-side and asset-side proxy baskets still do not provide decisive mechanism support at the key horizons.")
    state_proxy_status = statuses["state_proxy_factors"]
    if state_proxy_status in {"published_supportive", "published_mixed"}:
        takeaways.append("Some published regime-state contexts sharpen the grouped mechanism read beyond the flat full-sample proxy baskets.")
    elif state_proxy_status in {"diagnostic_only_supportive", "diagnostic_only_mixed"}:
        takeaways.append("Only diagnostic-only regime-state contexts currently sharpen the grouped mechanism read beyond the flat full-sample proxy baskets.")
    if statuses["readiness"] != "ready_for_interpretation":
        takeaways.append("Even with the backend extensions, the quarterly bundle still does not clear the project’s readiness bar for a clean pass-through versus crowd-out claim.")

    next_questions = [
        "Does bank_absorption state dependence remain the strongest heterogeneity pattern after adding the smoothed and factor-control overlays?",
        "Do any future factor-control variants improve precision without materially reducing support or changing signs?",
        "Do grouped proxy baskets become decisive only in specific states, or do they remain weak even after the LP-layer upgrades?",
    ]

    return {
        "status": str(readiness.get("status", "not_ready")),
        "headline_question": "What do the backend methodological extensions collectively say about the quarterly fixed-shock design?",
        "status_board": statuses,
        "best_core_factor_variant": best_factor_variant,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
        "next_questions": next_questions,
    }
