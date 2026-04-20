from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.strict_top_gap_anomaly import build_strict_top_gap_anomaly_summary


def build_strict_top_gap_anomaly_di_loans_split_summary(
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
        "strict_loan_di_loans_nec_qoq",
        "strict_di_loans_nec_systemwide_liability_total_qoq",
        "strict_di_loans_nec_households_nonprofits_qoq",
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        "strict_di_loans_nec_state_local_qoq",
        "strict_di_loans_nec_domestic_financial_qoq",
        "strict_di_loans_nec_rest_of_world_qoq",
        "strict_di_loans_nec_systemwide_borrower_total_qoq",
        "strict_di_loans_nec_systemwide_borrower_gap_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_di_loans_split_columns"}

    peer_weights = {
        str(row["quarter"]): abs(float(row["shock_gap"]))
        for row in peer_rows
    }
    total_peer_weight = sum(peer_weights.values())
    if total_peer_weight == 0.0:
        return {"status": "not_available", "reason": "no_peer_gap_weight"}

    panel = shocked[list(required)].dropna(subset=["quarter"]).copy().set_index("quarter")
    quarter = str(anomaly_payload.get("quarter", anomaly_quarter))
    if quarter not in panel.index or not all(peer in panel.index for peer in peer_weights):
        return {"status": "not_available", "reason": "missing_quarter_in_panel"}

    def weighted_peer_mean(column: str) -> float:
        return sum(float(panel.loc[q, column]) * peer_weights[q] for q in peer_weights) / total_peer_weight

    rows: list[dict[str, Any]] = []
    metrics = [
        ("strict_loan_di_loans_nec_qoq", "U.S.-chartered DI loans n.e.c."),
        ("strict_di_loans_nec_systemwide_liability_total_qoq", "Systemwide DI loans n.e.c. liability total"),
        ("strict_di_loans_nec_households_nonprofits_qoq", "Households / nonprofits"),
        ("strict_di_loans_nec_nonfinancial_corporate_qoq", "Nonfinancial corporate"),
        ("strict_di_loans_nec_nonfinancial_noncorporate_qoq", "Nonfinancial noncorporate"),
        ("strict_di_loans_nec_state_local_qoq", "State / local"),
        ("strict_di_loans_nec_domestic_financial_qoq", "Domestic financial"),
        ("strict_di_loans_nec_rest_of_world_qoq", "Rest of world"),
        ("strict_di_loans_nec_systemwide_borrower_total_qoq", "Systemwide named borrower total"),
        ("strict_di_loans_nec_systemwide_borrower_gap_qoq", "Systemwide borrower gap"),
    ]
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
    rows.sort(key=lambda item: float(item["abs_delta"]), reverse=True)

    dominant_borrower_component = next(
        (
            row
            for row in rows
            if row["metric"]
            in {
                "strict_di_loans_nec_households_nonprofits_qoq",
                "strict_di_loans_nec_nonfinancial_corporate_qoq",
                "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
                "strict_di_loans_nec_state_local_qoq",
                "strict_di_loans_nec_domestic_financial_qoq",
                "strict_di_loans_nec_rest_of_world_qoq",
            }
        ),
        None,
    )
    borrower_gap_row = next(
        (row for row in rows if row["metric"] == "strict_di_loans_nec_systemwide_borrower_gap_qoq"),
        None,
    )
    interpretation = "di_loans_nec_anomaly_not_classified"
    if dominant_borrower_component is not None:
        if (
            dominant_borrower_component["metric"] == "strict_di_loans_nec_domestic_financial_qoq"
            and float(dominant_borrower_component["anomaly_minus_peer_delta"]) < 0.0
        ):
            interpretation = "di_loans_nec_anomaly_is_domestic_financial_shortfall"
        elif (
            dominant_borrower_component["metric"] == "strict_di_loans_nec_nonfinancial_corporate_qoq"
            and float(dominant_borrower_component["anomaly_minus_peer_delta"]) < 0.0
        ):
            interpretation = "di_loans_nec_anomaly_is_nonfinancial_corporate_shortfall"
    if interpretation == "di_loans_nec_anomaly_not_classified" and borrower_gap_row is not None:
        if float(borrower_gap_row["anomaly_minus_peer_delta"]) > 0.0:
            interpretation = "di_loans_nec_anomaly_has_larger_systemwide_borrower_gap"

    takeaways = []
    if dominant_borrower_component is not None:
        takeaways.append(
            "Inside the DI-loans-n.e.c. borrower-side split, the largest named borrower delta is "
            f"`{dominant_borrower_component['label']}` at ≈ {float(dominant_borrower_component['anomaly_minus_peer_delta']):.2f}."
        )
    if borrower_gap_row is not None:
        takeaways.append(
            "The DI-loans-n.e.c. systemwide borrower-gap delta is "
            f"≈ {float(borrower_gap_row['anomaly_minus_peer_delta']):.2f}."
        )
    takeaways.append(
        "This artifact should be read as a borrower-side counterpart diagnostic for the DI-loans-n.e.c. block, not as a same-scope decomposition of the U.S.-chartered lender asset series."
    )

    return {
        "status": "available",
        "headline_question": "Within the DI-loans-n.e.c. anomaly, which borrower-side counterpart buckets differ most from the same-bucket peers?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_di_loans_nec_components",
            "comparison_artifact": "strict_top_gap_anomaly_di_loans_split_summary.json",
            "anomaly_source_artifact": "strict_top_gap_anomaly_summary.json",
            "top_gap_limit": int(limit),
            "anomaly_quarter": quarter,
        },
        "anomaly_quarter": anomaly_payload,
        "peer_quarters": peer_rows,
        "peer_bucket_weight": float(total_peer_weight),
        "di_loans_nec_component_deltas": rows,
        "dominant_borrower_component": dominant_borrower_component,
        "borrower_gap_row": borrower_gap_row,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
