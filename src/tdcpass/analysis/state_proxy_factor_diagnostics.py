from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.proxy_factor_diagnostics import PROXY_FAMILIES


def _lp_row(df: pd.DataFrame, *, regime: str, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty:
        return None
    sample = df[(df["regime"] == regime) & (df["outcome"] == outcome) & (df["horizon"] == horizon)]
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


def _sign_weight(other_snapshot: dict[str, Any] | None) -> float | None:
    sign = _beta_sign(other_snapshot)
    if sign == "positive":
        return 1.0
    if sign == "negative":
        return -1.0
    return None


def _family_label(
    *,
    other_snapshot: dict[str, Any] | None,
    normalized_beta_sum: float | None,
    decisive_same_direction_count: int,
    decisive_opposite_direction_count: int,
) -> str:
    if other_snapshot is None:
        return "missing_other_component"
    if not bool(other_snapshot["ci_excludes_zero"]):
        return "other_component_not_decisive"
    if decisive_opposite_direction_count > 0:
        return "opposite_direction"
    if decisive_same_direction_count > 0 and normalized_beta_sum is not None and normalized_beta_sum > 0.0:
        return "supportive"
    if normalized_beta_sum is not None and normalized_beta_sum > 0.0:
        return "same_direction_not_decisive"
    return "weak"


def _context_bucket(*, publication_role: str, stable_for_interpretation: bool) -> str:
    if not stable_for_interpretation:
        return "unstable"
    if publication_role == "diagnostic_only":
        return "diagnostic_only"
    return "published"


def build_state_proxy_factor_diagnostics(
    *,
    lp_irf_regimes: pd.DataFrame,
    regime_diagnostics: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4),
) -> tuple[pd.DataFrame, dict[str, Any]]:
    regime_meta: dict[str, dict[str, Any]] = {}
    if regime_diagnostics is not None:
        for row in regime_diagnostics.get("regimes", []):
            if isinstance(row, dict) and "regime" in row:
                regime_meta[str(row["regime"])] = row

    base_regimes = sorted({str(name).rsplit("_", 1)[0] for name in lp_irf_regimes.get("regime", pd.Series(dtype=str)).dropna().tolist()})
    rows: list[dict[str, Any]] = []
    summary_regimes: list[dict[str, Any]] = []
    supportive_contexts: list[str] = []
    contradictory_contexts: list[str] = []
    published_supportive_contexts: list[str] = []
    diagnostic_only_supportive_contexts: list[str] = []
    unstable_supportive_contexts: list[str] = []
    published_contradictory_contexts: list[str] = []
    diagnostic_only_contradictory_contexts: list[str] = []
    unstable_contradictory_contexts: list[str] = []

    for base_name in base_regimes:
        stable_for_interpretation = bool(regime_meta.get(base_name, {}).get("stable_for_interpretation", False))
        publication_role = str(regime_meta.get(base_name, {}).get("publication_role", "unknown"))
        bucket = _context_bucket(
            publication_role=publication_role,
            stable_for_interpretation=stable_for_interpretation,
        )
        regime_payload: dict[str, Any] = {
            "regime": base_name,
            "stable_for_interpretation": stable_for_interpretation,
            "publication_role": publication_role,
            "horizons": {},
        }
        for horizon in horizons:
            horizon_payload: dict[str, Any] = {}
            for state_label in ("high", "low"):
                regime_name = f"{base_name}_{state_label}"
                other_snapshot = _snapshot(_lp_row(lp_irf_regimes, regime=regime_name, outcome="other_component_qoq", horizon=horizon))
                sign_weight = _sign_weight(other_snapshot)
                family_payloads: dict[str, Any] = {}
                for family_name, family_outcomes in PROXY_FAMILIES.items():
                    family_rows: list[dict[str, Any]] = []
                    normalized_beta_sum = 0.0
                    decisive_same_direction_count = 0
                    decisive_opposite_direction_count = 0
                    observed_proxy_count = 0
                    for proxy_outcome in family_outcomes:
                        proxy_snapshot = _snapshot(_lp_row(lp_irf_regimes, regime=regime_name, outcome=proxy_outcome, horizon=horizon))
                        normalized_beta = None
                        normalized_sign = "missing"
                        if proxy_snapshot is not None and sign_weight is not None:
                            normalized_beta = sign_weight * float(proxy_snapshot["beta"])
                            if normalized_beta > 0.0:
                                normalized_sign = "same_direction"
                            elif normalized_beta < 0.0:
                                normalized_sign = "opposite_direction"
                            else:
                                normalized_sign = "zero"
                        if normalized_beta is not None:
                            normalized_beta_sum += normalized_beta
                        if proxy_snapshot is not None:
                            observed_proxy_count += 1
                            if bool(proxy_snapshot["ci_excludes_zero"]) and normalized_sign == "same_direction":
                                decisive_same_direction_count += 1
                            if bool(proxy_snapshot["ci_excludes_zero"]) and normalized_sign == "opposite_direction":
                                decisive_opposite_direction_count += 1
                        row = {
                            "regime": base_name,
                            "state_label": state_label,
                            "horizon": int(horizon),
                            "family": family_name,
                            "proxy_outcome": proxy_outcome,
                            "other_beta": None if other_snapshot is None else float(other_snapshot["beta"]),
                            "other_ci_excludes_zero": False if other_snapshot is None else bool(other_snapshot["ci_excludes_zero"]),
                            "proxy_beta": None if proxy_snapshot is None else float(proxy_snapshot["beta"]),
                            "proxy_se": None if proxy_snapshot is None else float(proxy_snapshot["se"]),
                            "proxy_lower95": None if proxy_snapshot is None else float(proxy_snapshot["lower95"]),
                            "proxy_upper95": None if proxy_snapshot is None else float(proxy_snapshot["upper95"]),
                            "proxy_ci_excludes_zero": False if proxy_snapshot is None else bool(proxy_snapshot["ci_excludes_zero"]),
                            "normalized_beta": normalized_beta,
                            "normalized_sign": normalized_sign,
                        }
                        rows.append(row)
                        family_rows.append(row)

                    family_label = _family_label(
                        other_snapshot=other_snapshot,
                        normalized_beta_sum=None if observed_proxy_count == 0 else normalized_beta_sum,
                        decisive_same_direction_count=decisive_same_direction_count,
                        decisive_opposite_direction_count=decisive_opposite_direction_count,
                    )
                    context_name = f"{base_name}_{state_label}_{'h'+str(horizon)}"
                    if family_label == "supportive":
                        context_key = f"{context_name}:{family_name}"
                        supportive_contexts.append(context_key)
                        if bucket == "published":
                            published_supportive_contexts.append(context_key)
                        elif bucket == "diagnostic_only":
                            diagnostic_only_supportive_contexts.append(context_key)
                        else:
                            unstable_supportive_contexts.append(context_key)
                    elif family_label == "opposite_direction":
                        context_key = f"{context_name}:{family_name}"
                        contradictory_contexts.append(context_key)
                        if bucket == "published":
                            published_contradictory_contexts.append(context_key)
                        elif bucket == "diagnostic_only":
                            diagnostic_only_contradictory_contexts.append(context_key)
                        else:
                            unstable_contradictory_contexts.append(context_key)
                    family_payloads[family_name] = {
                        "family_label": family_label,
                        "normalized_beta_sum": None if observed_proxy_count == 0 else normalized_beta_sum,
                        "observed_proxy_count": observed_proxy_count,
                        "decisive_same_direction_count": decisive_same_direction_count,
                        "decisive_opposite_direction_count": decisive_opposite_direction_count,
                    }
                horizon_payload[state_label] = {
                    "other_component": other_snapshot,
                    "families": family_payloads,
                }
            regime_payload["horizons"][f"h{horizon}"] = horizon_payload
        summary_regimes.append(regime_payload)

    status = "weak"
    if published_supportive_contexts and not published_contradictory_contexts:
        status = "published_supportive"
    elif published_supportive_contexts or published_contradictory_contexts:
        status = "published_mixed"
    elif diagnostic_only_supportive_contexts and not diagnostic_only_contradictory_contexts:
        status = "diagnostic_only_supportive"
    elif diagnostic_only_supportive_contexts or diagnostic_only_contradictory_contexts:
        status = "diagnostic_only_mixed"

    takeaways = [
        "State-by-mechanism diagnostics group the proxy bundle into funding-side and asset-side baskets within each regime-state LP context.",
    ]
    if published_supportive_contexts:
        takeaways.append("Published supportive grouped mechanism contexts: " + ", ".join(published_supportive_contexts) + ".")
    if diagnostic_only_supportive_contexts:
        takeaways.append(
            "Diagnostic-only supportive grouped mechanism contexts: "
            + ", ".join(diagnostic_only_supportive_contexts)
            + "."
        )
    if published_contradictory_contexts:
        takeaways.append("Published contradictory grouped mechanism contexts: " + ", ".join(published_contradictory_contexts) + ".")
    if diagnostic_only_contradictory_contexts:
        takeaways.append(
            "Diagnostic-only contradictory grouped mechanism contexts: "
            + ", ".join(diagnostic_only_contradictory_contexts)
            + "."
        )
    if unstable_supportive_contexts:
        takeaways.append("Unstable supportive grouped mechanism contexts: " + ", ".join(unstable_supportive_contexts) + ".")
    if unstable_contradictory_contexts:
        takeaways.append("Unstable contradictory grouped mechanism contexts: " + ", ".join(unstable_contradictory_contexts) + ".")
    if not supportive_contexts and not contradictory_contexts:
        takeaways.append("No regime-state context yet shows decisive grouped mechanism support at the key horizons.")

    frame = pd.DataFrame(
        rows,
        columns=[
            "regime",
            "state_label",
            "horizon",
            "family",
            "proxy_outcome",
            "other_beta",
            "other_ci_excludes_zero",
            "proxy_beta",
            "proxy_se",
            "proxy_lower95",
            "proxy_upper95",
            "proxy_ci_excludes_zero",
            "normalized_beta",
            "normalized_sign",
        ],
    )
    return frame, {
        "status": status,
        "headline_question": "Do grouped proxy families become more decisive in specific regime states?",
        "regimes": summary_regimes,
        "supportive_contexts": supportive_contexts,
        "contradictory_contexts": contradictory_contexts,
        "published_supportive_contexts": published_supportive_contexts,
        "diagnostic_only_supportive_contexts": diagnostic_only_supportive_contexts,
        "unstable_supportive_contexts": unstable_supportive_contexts,
        "published_contradictory_contexts": published_contradictory_contexts,
        "diagnostic_only_contradictory_contexts": diagnostic_only_contradictory_contexts,
        "unstable_contradictory_contexts": unstable_contradictory_contexts,
        "takeaways": takeaways,
    }
