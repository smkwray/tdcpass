from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def _snapshot_line(label: str, payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return f"- {label}: missing"
    beta = payload.get("beta")
    lower95 = payload.get("lower95")
    upper95 = payload.get("upper95")
    sign = payload.get("sign")
    n = payload.get("n")
    return f"- {label}: beta={beta:.2f}, 95% CI [{lower95:.2f}, {upper95:.2f}], sign={sign}, n={n}"


def _smoothed_line(label: str, payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return f"- {label}: missing"
    return (
        f"- {label}: raw={payload.get('raw_beta'):.2f}, smoothed={payload.get('smoothed_beta'):.2f}, "
        f"adjustment={payload.get('adjustment'):.2f}, raw_sign={payload.get('raw_sign')}, "
        f"smoothed_sign={payload.get('smoothed_sign')}, n={payload.get('n')}"
    )


def _proxy_family_line(family: str, payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return f"- {family}: missing"
    return (
        f"- {family}: label={payload.get('family_label')}, "
        f"normalized_beta_sum={payload.get('normalized_beta_sum')}, "
        f"decisive_same_direction_count={payload.get('decisive_same_direction_count')}, "
        f"decisive_opposite_direction_count={payload.get('decisive_opposite_direction_count')}"
    )


def _state_line(state_name: str, outcome: str, payload: Mapping[str, Any] | None) -> list[str]:
    if not payload:
        return [f"- {state_name} {outcome}: missing"]
    low_payload = payload.get("low") or {}
    high_payload = payload.get("high") or {}
    return [
        f"- {state_name} {outcome} low: beta={low_payload.get('beta')}, sign={low_payload.get('sign')}, ci_excludes_zero={low_payload.get('ci_excludes_zero')}",
        f"- {state_name} {outcome} high: beta={high_payload.get('beta')}, sign={high_payload.get('sign')}, ci_excludes_zero={high_payload.get('ci_excludes_zero')}",
    ]


def _state_proxy_context_lines(context: Mapping[str, Any] | None) -> list[str]:
    if not context:
        return ["- state proxy context: missing"]
    lines = [
        (
            f"- regime={context.get('regime')}, publication_role={context.get('publication_role')}, "
            f"stable_for_interpretation={context.get('stable_for_interpretation')}"
        )
    ]
    for state_label in ("low", "high"):
        state_payload = context.get(state_label)
        if not state_payload:
            lines.append(f"- {state_label}: missing")
            continue
        other_payload = state_payload.get("other_component")
        if other_payload:
            lines.append(
                f"- {state_label} other_component: beta={other_payload.get('beta')}, "
                f"ci_excludes_zero={other_payload.get('ci_excludes_zero')}, n={other_payload.get('n')}"
            )
        else:
            lines.append(f"- {state_label} other_component: missing")
        for family, family_payload in dict(state_payload.get("families", {})).items():
            lines.append(
                f"- {state_label} {family}: label={family_payload.get('family_label')}, "
                f"normalized_beta_sum={family_payload.get('normalized_beta_sum')}, "
                f"decisive_same_direction_count={family_payload.get('decisive_same_direction_count')}, "
                f"decisive_opposite_direction_count={family_payload.get('decisive_opposite_direction_count')}"
            )
    return lines


def render_research_dashboard_report(dashboard: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Internal Research Dashboard")
    lines.append("")
    lines.append(f"Status: `{dashboard.get('status', 'unknown')}`")
    lines.append("")
    lines.append("## Status Board")
    for key, value in dict(dashboard.get("status_board", {})).items():
        lines.append(f"- {key}: `{value}`")
    best_core_factor_variant = dashboard.get("best_core_factor_variant")
    if best_core_factor_variant:
        lines.append(f"- best_core_factor_variant: `{best_core_factor_variant}`")

    for horizon_key, payload in dict(dashboard.get("key_horizons", {})).items():
        lines.append("")
        lines.append(f"## {horizon_key.upper()}")
        baseline = dict(payload.get("baseline", {}))
        lines.append("### Baseline")
        lines.append(_snapshot_line("total_deposits", baseline.get("total_deposits")))
        lines.append(_snapshot_line("other_component", baseline.get("other_component")))

        smoothed = dict(payload.get("smoothed", {}))
        lines.append("### Smoothed")
        lines.append(_smoothed_line("total_deposits", smoothed.get("total_deposits_bank_qoq")))
        lines.append(_smoothed_line("other_component", smoothed.get("other_component_qoq")))

        factor = payload.get("best_core_factor_control")
        lines.append("### Best Core Factor Control")
        if factor:
            lines.append(f"- factor_variant: `{factor.get('factor_variant')}`")
            lines.append(_snapshot_line("total_deposits", factor.get("total_deposits")))
            lines.append(_snapshot_line("other_component", factor.get("other_component")))
        else:
            lines.append("- factor control: missing")

        lines.append("### State Dependence")
        state_dependence = dict(payload.get("state_dependence", {}))
        for state_name, state_payload in state_dependence.items():
            for outcome in ("total_deposits_bank_qoq", "other_component_qoq"):
                lines.extend(_state_line(state_name, outcome, dict(state_payload).get(outcome)))

        lines.append("### Proxy Families")
        for family, family_payload in dict(payload.get("proxy_families", {})).items():
            lines.append(_proxy_family_line(family, family_payload))

        lines.append("### State Proxy Contexts")
        state_proxy_contexts = list(payload.get("state_proxy_contexts", []))
        if state_proxy_contexts:
            for context in state_proxy_contexts:
                lines.extend(_state_proxy_context_lines(context))
        else:
            lines.append("- state proxy contexts: missing")

    lines.append("")
    lines.append("## Takeaways")
    for item in list(dashboard.get("takeaways", [])):
        lines.append(f"- {item}")

    lines.append("")
    lines.append("## Next Questions")
    for item in list(dashboard.get("next_questions", [])):
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def write_research_dashboard_report(path: Path, dashboard: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_research_dashboard_report(dashboard), encoding="utf-8")
    return path
