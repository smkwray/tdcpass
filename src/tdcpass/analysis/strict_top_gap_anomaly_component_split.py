from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly import build_strict_top_gap_anomaly_summary


def _share(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def build_strict_top_gap_anomaly_component_split_summary(
    *,
    shocked: pd.DataFrame,
    strict_top_gap_anomaly_summary: dict[str, Any] | None = None,
    limit: int = 5,
    anomaly_quarter: str = "2009Q4",
) -> dict[str, Any]:
    anomaly_summary = (
        strict_top_gap_anomaly_summary
        if strict_top_gap_anomaly_summary is not None
        else build_strict_top_gap_anomaly_summary(
            shocked=shocked,
            limit=limit,
            anomaly_quarter=anomaly_quarter,
        )
    )
    if str(anomaly_summary.get("status", "not_available")) != "available":
        return {
            "status": str(anomaly_summary.get("status", "not_available")),
            "reason": str(anomaly_summary.get("reason", "anomaly_summary_unavailable")),
        }

    anomaly_payload = dict(anomaly_summary.get("anomaly_quarter", {}) or {})
    peer_rows = list(anomaly_summary.get("peer_quarters", []))
    if not anomaly_payload or not peer_rows:
        return {"status": "not_available", "reason": "missing_anomaly_or_peer_rows"}

    required = {
        "quarter",
        "strict_loan_mortgages_qoq",
        "strict_loan_consumer_credit_qoq",
        "strict_loan_di_loans_nec_qoq",
        "strict_loan_other_advances_qoq",
        "strict_non_treasury_agency_gse_qoq",
        "strict_non_treasury_municipal_qoq",
        "strict_non_treasury_corporate_foreign_bonds_qoq",
        "strict_funding_fedfunds_repo_qoq",
        "strict_funding_debt_securities_qoq",
        "strict_funding_fhlb_advances_qoq",
        "foreign_nonts_qoq",
        "reserves_qoq",
        "tga_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_anomaly_component_split_columns"}

    peer_weights = {
        str(row["quarter"]): abs(float(row["shock_gap"]))
        for row in peer_rows
    }
    if not peer_weights or sum(peer_weights.values()) == 0.0:
        return {"status": "not_available", "reason": "no_peer_gap_weight"}

    panel = shocked[list(required)].dropna(subset=["quarter"]).copy().set_index("quarter")
    quarter = str(anomaly_payload.get("quarter", anomaly_quarter))
    if quarter not in panel.index:
        return {"status": "not_available", "reason": "anomaly_quarter_not_in_panel"}
    if not all(peer_quarter in panel.index for peer_quarter in peer_weights):
        return {"status": "not_available", "reason": "peer_quarter_not_in_panel"}

    total_peer_weight = sum(peer_weights.values())

    def weighted_peer_mean(column: str) -> float:
        return sum(float(panel.loc[q, column]) * peer_weights[q] for q in peer_weights) / total_peer_weight

    def block_rows(columns: list[tuple[str, str]]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for metric, label in columns:
            anomaly_value = float(panel.loc[quarter, metric])
            peer_mean = weighted_peer_mean(metric)
            delta = anomaly_value - peer_mean
            output.append(
                {
                    "metric": metric,
                    "label": label,
                    "anomaly_value": anomaly_value,
                    "weighted_peer_mean": peer_mean,
                    "anomaly_minus_peer_delta": delta,
                    "abs_delta": abs(delta),
                }
            )
        output.sort(key=lambda item: float(item["abs_delta"]), reverse=True)
        return output

    loan_rows = block_rows(
        [
            ("strict_loan_di_loans_nec_qoq", "DI loans n.e.c."),
            ("strict_loan_mortgages_qoq", "Mortgages"),
            ("strict_loan_consumer_credit_qoq", "Consumer credit"),
            ("strict_loan_other_advances_qoq", "Other advances"),
        ]
    )
    securities_rows = block_rows(
        [
            ("strict_non_treasury_corporate_foreign_bonds_qoq", "Corporate and foreign bonds"),
            ("strict_non_treasury_agency_gse_qoq", "Agency / GSE-backed securities"),
            ("strict_non_treasury_municipal_qoq", "Municipal securities"),
        ]
    )
    funding_rows = block_rows(
        [
            ("strict_funding_fedfunds_repo_qoq", "Fed funds / repo funding"),
            ("strict_funding_fhlb_advances_qoq", "FHLB advances"),
            ("strict_funding_debt_securities_qoq", "Debt securities funding"),
        ]
    )
    liquidity_external_rows = block_rows(
        [
            ("reserves_qoq", "Reserves"),
            ("foreign_nonts_qoq", "Foreign NONTS"),
            ("tga_qoq", "TGA"),
        ]
    )

    ranked_component_deltas = sorted(
        [*loan_rows, *securities_rows, *funding_rows, *liquidity_external_rows],
        key=lambda item: float(item["abs_delta"]),
        reverse=True,
    )

    leading_loan = loan_rows[0] if loan_rows else {}
    leading_liquidity = liquidity_external_rows[0] if liquidity_external_rows else {}
    interpretation = "anomaly_component_mix_is_not_classified"
    if (
        str(leading_loan.get("metric")) == "strict_loan_di_loans_nec_qoq"
        and float(leading_loan.get("anomaly_minus_peer_delta") or 0.0) < 0.0
        and float(next((row["anomaly_minus_peer_delta"] for row in liquidity_external_rows if row["metric"] == "reserves_qoq"), 0.0)) < 0.0
        and float(next((row["anomaly_minus_peer_delta"] for row in liquidity_external_rows if row["metric"] == "foreign_nonts_qoq"), 0.0)) < 0.0
    ):
        interpretation = "anomaly_is_di_loans_nec_contraction_with_weaker_liquidity_and_external_support"
    elif float(leading_loan.get("anomaly_minus_peer_delta") or 0.0) < 0.0:
        interpretation = "anomaly_is_loan_led_with_secondary_liquidity_external_gap"

    takeaways = [
        "Within the anomaly quarter "
        f"`{quarter}`, the largest loan-subcomponent gap versus same-bucket peers is "
        f"`{leading_loan.get('label', 'n/a')}` at ≈ {float(leading_loan.get('anomaly_minus_peer_delta') or 0.0):.2f}.",
        "The main liquidity/external comparison shows "
        f"`{leading_liquidity.get('label', 'n/a')}` at ≈ {float(leading_liquidity.get('anomaly_minus_peer_delta') or 0.0):.2f}; "
        f"TGA differs by ≈ {float(next((row['anomaly_minus_peer_delta'] for row in liquidity_external_rows if row['metric'] == 'tga_qoq'), 0.0)):.2f}.",
    ]
    if ranked_component_deltas:
        takeaways.append(
            "Across the detailed anomaly blocks, the largest absolute anomaly-minus-peer gap is "
            f"`{ranked_component_deltas[0]['metric']}` at ≈ {float(ranked_component_deltas[0]['anomaly_minus_peer_delta']):.2f}."
        )

    return {
        "status": "available",
        "headline_question": "What detailed loan, securities, funding, and liquidity/external subcomponents make the main within-bucket anomaly differ from its same-bucket peers?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_strict_components",
            "comparison_artifact": "strict_top_gap_anomaly_component_split_summary.json",
            "anomaly_source_artifact": "strict_top_gap_anomaly_summary.json",
            "top_gap_limit": int(limit),
            "anomaly_quarter": quarter,
        },
        "anomaly_quarter": anomaly_payload,
        "peer_quarters": peer_rows,
        "peer_bucket_weight": float(total_peer_weight),
        "loan_subcomponent_deltas": loan_rows,
        "securities_subcomponent_deltas": securities_rows,
        "funding_subcomponent_deltas": funding_rows,
        "liquidity_external_deltas": liquidity_external_rows,
        "ranked_component_deltas": ranked_component_deltas,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
