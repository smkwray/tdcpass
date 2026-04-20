from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.strict_top_gap_inversion import build_strict_top_gap_inversion_summary


def _share(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def build_strict_top_gap_anomaly_summary(
    *,
    shocked: pd.DataFrame,
    limit: int = 5,
    baseline_shock_column: str = "tdc_residual_z",
    excluded_shock_column: str = "tdc_no_toc_no_row_bank_only_residual_z",
    anomaly_quarter: str = "2009Q4",
) -> dict[str, Any]:
    inversion_summary = build_strict_top_gap_inversion_summary(
        shocked=shocked,
        limit=limit,
        baseline_shock_column=baseline_shock_column,
        excluded_shock_column=excluded_shock_column,
    )
    if str(inversion_summary.get("status", "not_available")) != "available":
        return {
            "status": str(inversion_summary.get("status", "not_available")),
            "reason": str(inversion_summary.get("reason", "inversion_summary_unavailable")),
        }

    rows = list(inversion_summary.get("top_gap_quarters", []))
    if not rows:
        return {"status": "not_available", "reason": "no_top_gap_rows"}

    panel_required = {
        "quarter",
        "strict_loan_source_qoq",
        "strict_non_treasury_securities_qoq",
        "strict_funding_offset_total_qoq",
        "strict_identifiable_net_after_funding_qoq",
    }
    if not panel_required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_anomaly_component_columns"}
    panel_by_quarter = shocked[list(panel_required)].set_index("quarter")
    for row in rows:
        quarter = str(row["quarter"])
        if quarter not in panel_by_quarter.index:
            continue
        panel_row = panel_by_quarter.loc[quarter]
        row["strict_loan_source_qoq"] = float(panel_row["strict_loan_source_qoq"])
        row["strict_non_treasury_securities_qoq"] = float(panel_row["strict_non_treasury_securities_qoq"])
        row["strict_funding_offset_total_qoq"] = float(panel_row["strict_funding_offset_total_qoq"])

    anomaly = next((row for row in rows if str(row.get("quarter")) == anomaly_quarter), None)
    if anomaly is None:
        return {"status": "not_available", "reason": "anomaly_quarter_not_in_top_gap_rows"}

    peer_rows = [
        row
        for row in rows
        if str(row.get("directional_driver")) == str(anomaly.get("directional_driver"))
        and str(row.get("quarter")) != anomaly_quarter
    ]
    if not peer_rows:
        return {"status": "not_available", "reason": "no_same_bucket_peers"}

    peer_weight = sum(abs(float(row["shock_gap"])) for row in peer_rows)

    def weighted_peer_mean(key: str) -> float:
        return sum(float(row[key]) * abs(float(row["shock_gap"])) for row in peer_rows) / peer_weight

    comparable_keys = [
        "excluded_other_component_qoq",
        "strict_loan_source_qoq",
        "strict_non_treasury_securities_qoq",
        "strict_identifiable_total_qoq",
        "strict_funding_offset_total_qoq",
        "excluded_strict_gap_qoq",
        "strict_identifiable_net_after_funding_qoq",
        "excluded_strict_gap_after_funding_qoq",
        "bundle_qoq",
        "row_leg_qoq",
        "toc_signed_contribution_qoq",
        "foreign_nonts_qoq",
        "tga_qoq",
        "reserves_qoq",
    ]
    anomaly_vs_peer_deltas = {
        key: float(anomaly[key]) - weighted_peer_mean(key)
        for key in comparable_keys
    }

    peer_pattern_weights: dict[str, float] = {}
    for row in peer_rows:
        pattern = str(row["residual_strict_pattern"])
        peer_pattern_weights[pattern] = peer_pattern_weights.get(pattern, 0.0) + abs(float(row["shock_gap"]))
    peer_pattern_summary = [
        {
            "residual_strict_pattern": pattern,
            "abs_gap_weight": weight,
            "abs_gap_share": _share(weight, peer_weight),
        }
        for pattern, weight in peer_pattern_weights.items()
    ]
    peer_pattern_summary.sort(key=lambda item: float(item["abs_gap_weight"]), reverse=True)

    ranked_deltas = [
        {
            "metric": key,
            "anomaly_minus_peer_delta": delta,
            "abs_delta": abs(float(delta)),
        }
        for key, delta in anomaly_vs_peer_deltas.items()
    ]
    ranked_deltas.sort(key=lambda item: float(item["abs_delta"]), reverse=True)

    interpretation = "anomaly_not_classified"
    if (
        float(anomaly["excluded_other_component_qoq"]) > 0.0
        and float(anomaly["strict_identifiable_total_qoq"]) < 0.0
        and weighted_peer_mean("excluded_other_component_qoq") > 0.0
        and weighted_peer_mean("strict_identifiable_total_qoq") > 0.0
    ):
        if (
            float(anomaly_vs_peer_deltas["strict_loan_source_qoq"]) < 0.0
            and abs(float(anomaly_vs_peer_deltas["strict_loan_source_qoq"]))
            >= abs(float(anomaly_vs_peer_deltas["strict_non_treasury_securities_qoq"]))
        ):
            interpretation = "anomaly_flips_strict_total_negative_mainly_through_loan_contraction_relative_to_peers"
        else:
            interpretation = "anomaly_flips_strict_total_negative_while_peer_bucket_stays_positive"
    elif float(anomaly["strict_identifiable_net_after_funding_qoq"]) > 0.0:
        interpretation = "anomaly_is_mainly_pre_funding_not_post_funding"

    takeaways = [
        "The anomaly quarter is "
        f"`{anomaly_quarter}` inside the `{str(anomaly['directional_driver'])}` bucket, with excluded residual ≈ "
        f"{float(anomaly['excluded_other_component_qoq']):.2f} versus strict total ≈ "
        f"{float(anomaly['strict_identifiable_total_qoq']):.2f}.",
        "Relative to same-bucket peers, the anomaly quarter differs by "
        f"loan source ≈ {float(anomaly_vs_peer_deltas['strict_loan_source_qoq']):.2f}, "
        f"securities ≈ {float(anomaly_vs_peer_deltas['strict_non_treasury_securities_qoq']):.2f}, "
        f"strict total ≈ {float(anomaly_vs_peer_deltas['strict_identifiable_total_qoq']):.2f}, "
        f"funding offset ≈ {float(anomaly_vs_peer_deltas['strict_funding_offset_total_qoq']):.2f}, "
        f"foreign NONTS ≈ {float(anomaly_vs_peer_deltas['foreign_nonts_qoq']):.2f}, "
        f"reserves ≈ {float(anomaly_vs_peer_deltas['reserves_qoq']):.2f}.",
    ]
    if peer_pattern_summary:
        takeaways.append(
            "Among same-bucket peers, the leading residual-versus-strict pattern is "
            f"`{peer_pattern_summary[0]['residual_strict_pattern']}` with abs-gap share ≈ "
            f"{float(peer_pattern_summary[0]['abs_gap_share'] or 0.0):.2f}."
        )
    if ranked_deltas:
        top = ranked_deltas[0]
        takeaways.append(
            "The largest anomaly-minus-peer component delta is "
            f"`{top['metric']}` at ≈ {float(top['anomaly_minus_peer_delta']):.2f}."
        )

    return {
        "status": "available",
        "headline_question": "Why does the main within-bucket anomaly differ from the dominant top-gap inversion profile?",
        "estimation_path": {
            "input_panel": "quarterly_panel_with_shocks",
            "comparison_artifact": "strict_top_gap_anomaly_summary.json",
            "baseline_shock_column": baseline_shock_column,
            "toc_row_excluded_shock_column": excluded_shock_column,
            "top_gap_limit": int(limit),
            "anomaly_quarter": anomaly_quarter,
        },
        "anomaly_quarter": anomaly,
        "peer_quarters": peer_rows,
        "peer_pattern_summary": peer_pattern_summary,
        "peer_bucket_weight": peer_weight,
        "weighted_peer_means": {
            key: weighted_peer_mean(key)
            for key in comparable_keys
        },
        "anomaly_vs_peer_deltas": anomaly_vs_peer_deltas,
        "ranked_anomaly_component_deltas": ranked_deltas,
        "interpretation": interpretation,
        "takeaways": takeaways,
    }
