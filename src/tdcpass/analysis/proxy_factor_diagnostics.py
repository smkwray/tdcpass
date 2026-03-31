from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.structural_proxy_evidence import PROXY_OUTCOMES

PROXY_FAMILIES: dict[str, tuple[str, ...]] = {
    "funding_side": (
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
    ),
    "asset_side": (
        "bank_credit_private_qoq",
        "cb_nonts_qoq",
    ),
}


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


def build_proxy_factor_diagnostics(
    *,
    lp_irf: pd.DataFrame,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    key_horizons: dict[str, Any] = {}

    for horizon in horizons:
        other_snapshot = _snapshot(_lp_row(lp_irf, outcome="other_component_qoq", horizon=horizon))
        other_sign_weight = _sign_weight(other_snapshot)
        horizon_family_payloads: dict[str, Any] = {}

        for family_name, family_outcomes in PROXY_FAMILIES.items():
            family_rows: list[dict[str, Any]] = []
            normalized_beta_sum = 0.0
            decisive_same_direction_count = 0
            decisive_opposite_direction_count = 0
            observed_proxy_count = 0

            for proxy_outcome in family_outcomes:
                proxy_snapshot = _snapshot(_lp_row(lp_irf, outcome=proxy_outcome, horizon=horizon))
                normalized_beta = None
                normalized_sign = "missing"
                if proxy_snapshot is not None and other_sign_weight is not None:
                    normalized_beta = other_sign_weight * float(proxy_snapshot["beta"])
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
                    "family_member_count": len(family_outcomes),
                }
                rows.append(row)
                family_rows.append(row)

            family_label = _family_label(
                other_snapshot=other_snapshot,
                normalized_beta_sum=None if observed_proxy_count == 0 else normalized_beta_sum,
                decisive_same_direction_count=decisive_same_direction_count,
                decisive_opposite_direction_count=decisive_opposite_direction_count,
            )
            horizon_family_payloads[family_name] = {
                "family_label": family_label,
                "normalized_beta_sum": None if observed_proxy_count == 0 else normalized_beta_sum,
                "observed_proxy_count": observed_proxy_count,
                "decisive_same_direction_count": decisive_same_direction_count,
                "decisive_opposite_direction_count": decisive_opposite_direction_count,
                "proxy_rows": [
                    {
                        "proxy_outcome": row["proxy_outcome"],
                        "proxy_beta": row["proxy_beta"],
                        "proxy_ci_excludes_zero": row["proxy_ci_excludes_zero"],
                        "normalized_beta": row["normalized_beta"],
                        "normalized_sign": row["normalized_sign"],
                    }
                    for row in family_rows
                ],
            }

        if horizon in (0, 4):
            key_horizons[f"h{horizon}"] = {
                "other_component": other_snapshot,
                "families": horizon_family_payloads,
            }

    supportive_horizons = [
        name
        for name, payload in key_horizons.items()
        if any(family["family_label"] == "supportive" for family in payload["families"].values())
    ]
    contradictory_horizons = [
        name
        for name, payload in key_horizons.items()
        if any(family["family_label"] == "opposite_direction" for family in payload["families"].values())
    ]
    status = "weak"
    if supportive_horizons and not contradictory_horizons:
        status = "supportive"
    elif supportive_horizons or contradictory_horizons:
        status = "mixed"

    takeaways = [
        "Proxy factor diagnostics group the structural-proxy bundle into funding-side and asset-side baskets, then sign-normalize each basket against the non-TDC residual.",
    ]
    if supportive_horizons:
        takeaways.append(
            f"At {', '.join(supportive_horizons)}, at least one grouped counterpart basket moves decisively in the same direction as the non-TDC residual."
        )
    if contradictory_horizons:
        takeaways.append(
            f"At {', '.join(contradictory_horizons)}, at least one grouped counterpart basket moves decisively against the non-TDC residual."
        )
    if not supportive_horizons and not contradictory_horizons:
        takeaways.append("The grouped counterpart baskets do not yet add decisive mechanism support at the key horizons.")

    frame = pd.DataFrame(
        rows,
        columns=[
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
            "family_member_count",
        ],
    )
    return frame, {
        "status": status,
        "headline_question": "Do grouped counterpart baskets add clearer mechanism support than the flat structural-proxy bundle?",
        "families": {name: list(outcomes) for name, outcomes in PROXY_FAMILIES.items()},
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
