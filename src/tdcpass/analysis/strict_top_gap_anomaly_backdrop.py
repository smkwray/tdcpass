from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly import build_strict_top_gap_anomaly_summary


def build_strict_top_gap_anomaly_backdrop_summary(
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
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
        "strict_loan_di_loans_nec_qoq",
        "strict_loan_source_qoq",
        "other_component_no_toc_no_row_bank_only_qoq",
        "reserves_qoq",
        "foreign_nonts_qoq",
        "tga_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_anomaly_backdrop_columns"}

    peer_weights = {str(row["quarter"]): abs(float(row["shock_gap"])) for row in peer_rows}
    total_peer_weight = sum(peer_weights.values())
    if total_peer_weight == 0.0:
        return {"status": "not_available", "reason": "no_peer_gap_weight"}

    panel = shocked[list(required)].dropna(subset=["quarter"]).copy().set_index("quarter")
    quarter = str(anomaly_payload.get("quarter", anomaly_quarter))
    if quarter not in panel.index or not all(peer in panel.index for peer in peer_weights):
        return {"status": "not_available", "reason": "missing_quarter_in_panel"}

    metrics = [
        ("strict_di_loans_nec_nonfinancial_corporate_qoq", "Nonfinancial corporate DI loans n.e.c."),
        ("strict_loan_di_loans_nec_qoq", "U.S.-chartered DI loans n.e.c."),
        ("strict_loan_source_qoq", "Strict loan source"),
        ("reserves_qoq", "Reserves"),
        ("foreign_nonts_qoq", "Foreign NONTS"),
        ("tga_qoq", "TGA"),
        ("other_component_no_toc_no_row_bank_only_qoq", "TOC/ROW-excluded residual"),
    ]

    def weighted_peer_mean(column: str) -> float:
        return sum(float(panel.loc[q, column]) * peer_weights[q] for q in peer_weights) / total_peer_weight

    rows: list[dict[str, Any]] = []
    for metric, label in metrics:
        anomaly_value = float(panel.loc[quarter, metric])
        peer_mean = weighted_peer_mean(metric)
        delta = anomaly_value - peer_mean
        rows.append(
            {
                "metric": metric,
                "label": label,
                "anomaly_value": anomaly_value,
                "weighted_peer_mean": peer_mean,
                "anomaly_minus_peer_delta": delta,
                "abs_delta": abs(delta),
            }
        )

    by_metric = {row["metric"]: row for row in rows}
    corporate = by_metric["strict_di_loans_nec_nonfinancial_corporate_qoq"]
    reserves = by_metric["reserves_qoq"]
    foreign_nonts = by_metric["foreign_nonts_qoq"]
    tga = by_metric["tga_qoq"]
    loan_source = by_metric["strict_loan_source_qoq"]
    residual = by_metric["other_component_no_toc_no_row_bank_only_qoq"]

    liquidity_external_abs = abs(float(reserves["anomaly_minus_peer_delta"])) + abs(
        float(foreign_nonts["anomaly_minus_peer_delta"])
    )
    corporate_abs = abs(float(corporate["anomaly_minus_peer_delta"]))
    backdrop_balance = None
    if corporate_abs != 0.0:
        backdrop_balance = liquidity_external_abs / corporate_abs

    if (
        float(corporate["anomaly_minus_peer_delta"]) < 0.0
        and float(reserves["anomaly_minus_peer_delta"]) < 0.0
        and float(foreign_nonts["anomaly_minus_peer_delta"]) < 0.0
    ):
        if backdrop_balance is not None and backdrop_balance >= 1.5:
            interpretation = "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall"
        else:
            interpretation = "anomaly_combines_corporate_credit_shortfall_with_weak_liquidity_external_support"
    elif float(corporate["anomaly_minus_peer_delta"]) < 0.0:
        interpretation = "anomaly_is_mainly_corporate_credit_shortfall"
    else:
        interpretation = "anomaly_backdrop_not_classified"

    takeaways = [
        "Relative to same-bucket peers, the `2009Q4` anomaly shows "
        f"nonfinancial-corporate DI loans delta ≈ {float(corporate['anomaly_minus_peer_delta']):.2f}, "
        f"reserves delta ≈ {float(reserves['anomaly_minus_peer_delta']):.2f}, "
        f"and foreign NONTS delta ≈ {float(foreign_nonts['anomaly_minus_peer_delta']):.2f}.",
        "The TGA delta is "
        f"≈ {float(tga['anomaly_minus_peer_delta']):.2f}, while the TOC/ROW-excluded residual delta is "
        f"≈ {float(residual['anomaly_minus_peer_delta']):.2f}.",
    ]
    if backdrop_balance is not None:
        takeaways.append(
            "The combined liquidity/external shortfall is about "
            f"{float(backdrop_balance):.2f} times the absolute nonfinancial-corporate DI-loans shortfall."
        )

    return {
        "status": "available",
        "headline_question": "For the main anomaly quarter, is the peer shortfall better described as a narrow corporate-credit problem, a broader liquidity/external-support problem, or both?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_anomaly_backdrop_columns",
            "comparison_artifact": "strict_top_gap_anomaly_backdrop_summary.json",
            "anomaly_source_artifact": "strict_top_gap_anomaly_summary.json",
            "top_gap_limit": int(limit),
            "anomaly_quarter": quarter,
        },
        "anomaly_quarter": anomaly_payload,
        "peer_quarters": peer_rows,
        "peer_bucket_weight": float(total_peer_weight),
        "backdrop_rows": rows,
        "corporate_credit_row": corporate,
        "loan_source_row": loan_source,
        "reserves_row": reserves,
        "foreign_nonts_row": foreign_nonts,
        "tga_row": tga,
        "residual_row": residual,
        "liquidity_external_abs_to_corporate_abs_ratio": backdrop_balance,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
