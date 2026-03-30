from __future__ import annotations

from typing import Any

import pandas as pd

PROXY_OUTCOMES: tuple[str, ...] = (
    "bank_credit_private_qoq",
    "cb_nonts_qoq",
    "foreign_nonts_qoq",
    "domestic_nonfinancial_mmf_reallocation_qoq",
    "domestic_nonfinancial_repo_reallocation_qoq",
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


def _evidence_label(other: dict[str, Any] | None, proxy: dict[str, Any] | None) -> str:
    if other is None or proxy is None:
        return "missing"
    other_decisive = bool(other["ci_excludes_zero"])
    proxy_decisive = bool(proxy["ci_excludes_zero"])
    alignment = _sign_alignment(other, proxy)
    if other_decisive and proxy_decisive and alignment == "same_sign":
        return "proxy_supports_other_direction"
    if proxy_decisive and alignment == "opposite_sign":
        return "proxy_discordant_with_other_direction"
    if other_decisive and not proxy_decisive:
        return "other_without_proxy_confirmation"
    if proxy_decisive and not other_decisive:
        return "proxy_without_other_confirmation"
    return "ambiguous"


def _rows_for_context(
    *,
    df: pd.DataFrame,
    scope: str,
    context: str,
    horizon: int,
    other_outcome: str = "other_component_qoq",
) -> list[dict[str, Any]]:
    other = _snapshot(_lp_row(df, outcome=other_outcome, horizon=horizon))
    rows: list[dict[str, Any]] = []
    for proxy_outcome in PROXY_OUTCOMES:
        proxy = _snapshot(_lp_row(df, outcome=proxy_outcome, horizon=horizon))
        proxy_share = None
        if proxy is not None and other is not None and abs(float(other["beta"])) > 1e-12:
            proxy_share = float(proxy["beta"]) / float(other["beta"])
        rows.append(
            {
                "scope": scope,
                "context": context,
                "horizon": int(horizon),
                "other_outcome": other_outcome,
                "other_beta": None if other is None else float(other["beta"]),
                "other_se": None if other is None else float(other["se"]),
                "other_lower95": None if other is None else float(other["lower95"]),
                "other_upper95": None if other is None else float(other["upper95"]),
                "other_ci_excludes_zero": False if other is None else bool(other["ci_excludes_zero"]),
                "proxy_outcome": proxy_outcome,
                "proxy_beta": None if proxy is None else float(proxy["beta"]),
                "proxy_se": None if proxy is None else float(proxy["se"]),
                "proxy_lower95": None if proxy is None else float(proxy["lower95"]),
                "proxy_upper95": None if proxy is None else float(proxy["upper95"]),
                "proxy_ci_excludes_zero": False if proxy is None else bool(proxy["ci_excludes_zero"]),
                "other_sign": _beta_sign(other),
                "proxy_sign": _beta_sign(proxy),
                "sign_alignment": _sign_alignment(other, proxy),
                "evidence_label": _evidence_label(other, proxy),
                "proxy_share_of_other_beta": proxy_share,
            }
        )
    return rows


def _summarize_rows(
    rows: list[dict[str, Any]],
    *,
    other_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    decisive = [row for row in rows if bool(row["proxy_ci_excludes_zero"])]
    concordant = [row for row in decisive if row["sign_alignment"] == "same_sign"]
    discordant = [row for row in decisive if row["sign_alignment"] == "opposite_sign"]
    proxy_bundle_beta = sum(float(row["proxy_beta"]) for row in rows if row["proxy_beta"] is not None)
    other_minus_bundle = None
    if other_snapshot is not None:
        other_minus_bundle = float(other_snapshot["beta"]) - proxy_bundle_beta

    interpretation = "proxy_evidence_weak"
    if other_snapshot is None:
        interpretation = "missing_other_component_response"
    elif not bool(other_snapshot["ci_excludes_zero"]):
        interpretation = "other_component_not_decisive"
    elif discordant:
        interpretation = "proxy_evidence_discordant"
    elif concordant:
        interpretation = "proxy_evidence_supportive"

    return {
        "other_component": other_snapshot,
        "decisive_proxy_count": len(decisive),
        "decisive_concordant_proxy_count": len(concordant),
        "decisive_discordant_proxy_count": len(discordant),
        "proxy_bundle_beta_sum": proxy_bundle_beta,
        "other_minus_proxy_bundle_beta": other_minus_bundle,
        "interpretation": interpretation,
    }


def build_structural_proxy_evidence(
    *,
    lp_irf: pd.DataFrame,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    summary_horizons: dict[str, Any] = {}
    for horizon in horizons:
        context_rows = _rows_for_context(df=lp_irf, scope="baseline", context="baseline", horizon=horizon)
        rows.extend(context_rows)
        other_snapshot = _snapshot(_lp_row(lp_irf, outcome="other_component_qoq", horizon=horizon))
        summary_horizons[f"h{horizon}"] = {
            **_summarize_rows(context_rows, other_snapshot=other_snapshot),
            "proxy_rows": [
                {
                    "proxy_outcome": row["proxy_outcome"],
                    "proxy_beta": row["proxy_beta"],
                    "proxy_se": row["proxy_se"],
                    "proxy_lower95": row["proxy_lower95"],
                    "proxy_upper95": row["proxy_upper95"],
                    "proxy_ci_excludes_zero": row["proxy_ci_excludes_zero"],
                    "sign_alignment": row["sign_alignment"],
                    "evidence_label": row["evidence_label"],
                    "proxy_share_of_other_beta": row["proxy_share_of_other_beta"],
                }
                for row in context_rows
            ],
        }

    key_horizons = {name: summary_horizons[name] for name in ("h0", "h4") if name in summary_horizons}
    supportive_key_horizons = [
        name
        for name, payload in key_horizons.items()
        if payload["interpretation"] == "proxy_evidence_supportive"
    ]
    discordant_key_horizons = [
        name
        for name, payload in key_horizons.items()
        if payload["interpretation"] == "proxy_evidence_discordant"
    ]
    weak_key_horizons = [
        name
        for name, payload in key_horizons.items()
        if payload["interpretation"] in {"proxy_evidence_weak", "other_component_not_decisive"}
    ]
    status = "weak"
    if supportive_key_horizons and not discordant_key_horizons:
        status = "supportive"
    elif supportive_key_horizons or discordant_key_horizons:
        status = "mixed"

    takeaways = [
        "Structural proxies are mechanism cross-checks for the non-TDC residual, not exact counterpart accounting.",
    ]
    if supportive_key_horizons:
        takeaways.append(
            f"At {', '.join(supportive_key_horizons)}, at least one decisive structural proxy moves in the same direction as the non-TDC residual."
        )
    if weak_key_horizons:
        takeaways.append(
            f"At {', '.join(weak_key_horizons)}, the non-TDC residual is not backed by decisive structural-proxy evidence."
        )
    if discordant_key_horizons:
        takeaways.append(
            f"At {', '.join(discordant_key_horizons)}, at least one decisive structural proxy moves against the non-TDC residual."
        )

    frame = pd.DataFrame(rows)
    return frame, {
        "status": status,
        "headline_question": "Do structural proxies corroborate the direction of the non-TDC residual after an unexpected TDC shock?",
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
