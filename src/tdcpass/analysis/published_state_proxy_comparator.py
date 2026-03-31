from __future__ import annotations

from typing import Any, Mapping


def _family_sets(state_payload: Mapping[str, Any] | None) -> tuple[list[str], list[str]]:
    if not state_payload:
        return [], []
    supportive: list[str] = []
    contradictory: list[str] = []
    for family_name, family_payload in dict(state_payload.get("families", {})).items():
        label = str(family_payload.get("family_label", "weak"))
        if label == "supportive":
            supportive.append(family_name)
        elif label == "opposite_direction":
            contradictory.append(family_name)
    return supportive, contradictory


def build_published_state_proxy_comparator(
    *,
    state_proxy_factor_summary: Mapping[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    horizon_payloads: dict[str, list[dict[str, Any]]] = {f"h{horizon}": [] for horizon in horizons}
    primary_contexts: list[dict[str, Any]] = []

    if state_proxy_factor_summary is None:
        return {
            "status": "no_state_proxy_summary",
            "headline_question": "Which published regime-state contexts materially sharpen the grouped mechanism read?",
            "key_horizons": horizon_payloads,
            "primary_contexts": primary_contexts,
            "takeaways": ["State-proxy diagnostics are missing, so no published comparator can be constructed."],
        }

    for regime_row in state_proxy_factor_summary.get("regimes", []):
        if not isinstance(regime_row, Mapping):
            continue
        if str(regime_row.get("publication_role", "unknown")) != "published":
            continue
        if not bool(regime_row.get("stable_for_interpretation", False)):
            continue
        regime_name = str(regime_row.get("regime", ""))
        for horizon in horizons:
            horizon_key = f"h{horizon}"
            horizon_states = dict(regime_row.get("horizons", {}).get(horizon_key, {}))
            for state_label in ("low", "high"):
                state_payload = horizon_states.get(state_label)
                supportive_families, contradictory_families = _family_sets(state_payload)
                if not supportive_families and not contradictory_families:
                    continue
                other_component = None if not state_payload else state_payload.get("other_component")
                context = {
                    "regime": regime_name,
                    "state_label": state_label,
                    "horizon": horizon,
                    "other_component": other_component,
                    "supportive_families": supportive_families,
                    "contradictory_families": contradictory_families,
                }
                horizon_payloads[horizon_key].append(context)
                primary_contexts.append(context)

    status = "no_published_signal"
    supportive_count = sum(len(row["supportive_families"]) > 0 for row in primary_contexts)
    contradictory_count = sum(len(row["contradictory_families"]) > 0 for row in primary_contexts)
    if supportive_count and not contradictory_count:
        status = "published_signal"
    elif supportive_count or contradictory_count:
        status = "published_mixed_signal"

    primary_contexts.sort(
        key=lambda row: (
            -len(row["supportive_families"]),
            len(row["contradictory_families"]),
            row["horizon"],
            row["regime"],
            row["state_label"],
        )
    )

    takeaways = [
        "This comparator isolates stable published regime-state contexts from the broader state-proxy diagnostic stack.",
    ]
    if not primary_contexts:
        takeaways.append("No stable published regime-state context currently shows supportive or contradictory grouped proxy-family evidence.")
    else:
        lead = primary_contexts[0]
        takeaways.append(
            "Top published context: "
            f"{lead['regime']}_{lead['state_label']}_h{lead['horizon']} "
            f"(supportive={lead['supportive_families']}, contradictory={lead['contradictory_families']})."
        )
        if status == "published_signal":
            takeaways.append("Published regime-state mechanism evidence is one-sided supportive in the current bundle.")
        elif status == "published_mixed_signal":
            takeaways.append("Published regime-state mechanism evidence is mixed, with both supportive and contradictory contexts present.")

    return {
        "status": status,
        "headline_question": "Which published regime-state contexts materially sharpen the grouped mechanism read?",
        "key_horizons": horizon_payloads,
        "primary_contexts": primary_contexts,
        "takeaways": takeaways,
    }
