from __future__ import annotations

from typing import Any, Mapping


def build_strict_direct_core_horizon_stability_summary(
    *,
    strict_direct_core_component_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if (
        strict_direct_core_component_summary is None
        or str(strict_direct_core_component_summary.get("status", "not_available")) != "available"
    ):
        return {"status": "not_available", "reason": "strict_direct_core_component_summary_not_available"}

    winners: dict[str, str] = {}
    key_horizons = dict(strict_direct_core_component_summary.get("key_horizons", {}) or {})
    for horizon_key, payload in key_horizons.items():
        core_payload = dict((payload or {}).get("core_deposit_proximate", {}) or {})
        gaps = dict(core_payload.get("candidate_abs_gap_to_residual_beta", {}) or {})
        available = {k: float(v) for k, v in gaps.items() if v is not None}
        winners[horizon_key] = min(available, key=available.get) if available else "not_available"

    impact_winner = winners.get("h0", "not_available")
    medium_winner = winners.get("h4", "not_available")
    long_winner = winners.get("h8", "not_available")

    recommendation_status = "keep_current_direct_core"
    if (
        impact_winner == "strict_loan_mortgages_qoq"
        and medium_winner == "strict_loan_core_min_qoq"
        and long_winner == "strict_loan_core_min_qoq"
    ):
        recommendation_status = "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
    elif impact_winner == medium_winner == long_winner and impact_winner != "not_available":
        recommendation_status = f"stable_single_component_winner_{impact_winner}"

    takeaways = [
        "This summary asks whether the direct-core winner is stable across horizons or only an impact-horizon result.",
        f"Horizon-by-horizon winners = {winners}.",
    ]
    if recommendation_status == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate":
        takeaways.append(
            "Mortgages are the impact-horizon winner, but the bundled core retakes the closest-to-residual slot by h4 and h8, so the current bundled direct core should stay the multihorizon benchmark for now."
        )
    else:
        takeaways.append(f"Current recommendation: `{recommendation_status}`.")

    return {
        "status": "available",
        "headline_question": "Is the best strict direct-core candidate stable across horizons, or only at impact?",
        "estimation_path": {
            "summary_artifact": "strict_direct_core_horizon_stability_summary.json",
            "source_artifacts": [
                "strict_direct_core_component_summary.json",
            ],
        },
        "horizon_winners": winners,
        "classification": {
            "impact_winner": impact_winner,
            "medium_horizon_winner": medium_winner,
            "long_horizon_winner": long_winner,
            "recommendation_status": recommendation_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "impact_candidate": impact_winner,
            "multihorizon_candidate": (
                "strict_loan_core_min_qoq"
                if recommendation_status == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
                else impact_winner
            ),
            "next_branch": (
                "keep_bundled_direct_core_but_surface_mortgages_as_impact_candidate"
                if recommendation_status == "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"
                else "reassess_direct_core_release_role"
            ),
        },
        "takeaways": takeaways,
    }
