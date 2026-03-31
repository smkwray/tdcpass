from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def render_backend_decision_bundle_report(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Backend Decision Bundle")
    lines.append("")
    lines.append(f"Status: `{summary.get('status', 'unknown')}`")
    lines.append(f"Recommended action: `{summary.get('recommended_action', 'unknown')}`")
    lines.append("")
    lines.append(str(summary.get("headline_question", "")))

    lines.append("")
    lines.append("## Status Board")
    for key, value in dict(summary.get("status_board", {})).items():
        lines.append(f"- {key}: `{value}`")

    published_contexts = list(summary.get("published_contexts", []))
    lines.append("")
    lines.append("## Published Contexts")
    if published_contexts:
        for row in published_contexts:
            other_payload = row.get("other_component") or {}
            lines.append(
                f"- {row.get('regime')}_{row.get('state_label')}_h{row.get('horizon')}: "
                f"supportive={row.get('supportive_families')}, "
                f"contradictory={row.get('contradictory_families')}, "
                f"other_beta={other_payload.get('beta')}, "
                f"other_ci_excludes_zero={other_payload.get('ci_excludes_zero')}"
            )
    else:
        lines.append("- none")

    lines.append("")
    lines.append("## Takeaways")
    for item in list(summary.get("takeaways", [])):
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def write_backend_decision_bundle_report(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_backend_decision_bundle_report(summary), encoding="utf-8")
    return path
