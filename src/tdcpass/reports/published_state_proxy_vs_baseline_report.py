from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def render_published_state_proxy_vs_baseline_report(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Published Context vs Baseline")
    lines.append("")
    lines.append(f"Status: `{summary.get('status', 'unknown')}`")
    lines.append("")
    lines.append(str(summary.get("headline_question", "")))

    lead_context = summary.get("lead_context")
    if lead_context:
        other_payload = lead_context.get("other_component") or {}
        lines.append("")
        lines.append("## Lead Published Context")
        lines.append(
            f"- {lead_context.get('regime')}_{lead_context.get('state_label')}_h{lead_context.get('horizon')}: "
            f"supportive={lead_context.get('supportive_families')}, "
            f"contradictory={lead_context.get('contradictory_families')}, "
            f"other_beta={other_payload.get('beta')}, "
            f"other_ci_excludes_zero={other_payload.get('ci_excludes_zero')}"
        )

    baseline = summary.get("baseline") or {}
    if baseline:
        total = baseline.get("total_deposits") or {}
        other = baseline.get("other_component") or {}
        lines.append("")
        lines.append("## Matching Baseline Horizon")
        lines.append(
            f"- h{baseline.get('horizon')} total_deposits: beta={total.get('beta')}, "
            f"sign={total.get('sign')}, ci_excludes_zero={total.get('ci_excludes_zero')}"
        )
        lines.append(
            f"- h{baseline.get('horizon')} other_component: beta={other.get('beta')}, "
            f"sign={other.get('sign')}, ci_excludes_zero={other.get('ci_excludes_zero')}"
        )

    lines.append("")
    lines.append("## Takeaways")
    for item in list(summary.get("takeaways", [])):
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def write_published_state_proxy_vs_baseline_report(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_published_state_proxy_vs_baseline_report(summary), encoding="utf-8")
    return path
