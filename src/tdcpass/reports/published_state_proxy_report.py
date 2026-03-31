from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def render_published_state_proxy_report(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Published State-Proxy Comparator")
    lines.append("")
    lines.append(f"Status: `{summary.get('status', 'unknown')}`")
    lines.append("")
    lines.append(str(summary.get("headline_question", "")))

    primary_contexts = list(summary.get("primary_contexts", []))
    lines.append("")
    lines.append("## Primary Contexts")
    if primary_contexts:
        for row in primary_contexts:
            other_payload = row.get("other_component") or {}
            lines.append(
                f"- {row.get('regime')}_{row.get('state_label')}_h{row.get('horizon')}: "
                f"supportive={row.get('supportive_families')}, "
                f"contradictory={row.get('contradictory_families')}, "
                f"other_beta={other_payload.get('beta')}, "
                f"other_ci_excludes_zero={other_payload.get('ci_excludes_zero')}"
            )
    else:
        lines.append("- no published contexts")

    lines.append("")
    lines.append("## By Horizon")
    for horizon_key, rows in dict(summary.get("key_horizons", {})).items():
        lines.append(f"### {horizon_key.upper()}")
        if rows:
            for row in rows:
                other_payload = row.get("other_component") or {}
                lines.append(
                    f"- {row.get('regime')}_{row.get('state_label')}: "
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


def write_published_state_proxy_report(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_published_state_proxy_report(summary), encoding="utf-8")
    return path
