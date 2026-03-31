from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def render_backend_closeout_report(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Backend Closeout")
    lines.append("")
    lines.append(f"Status: `{summary.get('status', 'unknown')}`")
    lines.append(f"Recommended action: `{summary.get('recommended_action', 'unknown')}`")
    lines.append("")
    lines.append(str(summary.get("headline_question", "")))

    lines.append("")
    lines.append("## Settled Points")
    for item in list(summary.get("settled_points", [])):
        lines.append(f"- {item}")

    lines.append("")
    lines.append("## Unsupported Claims")
    for item in list(summary.get("unsupported_claims", [])):
        lines.append(f"- {item}")

    lines.append("")
    lines.append("## Next Lane Options")
    for item in list(summary.get("next_lane_options", [])):
        lines.append(f"- {item}")

    lines.append("")
    lines.append("## Takeaways")
    for item in list(summary.get("takeaways", [])):
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def write_backend_closeout_report(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_backend_closeout_report(summary), encoding="utf-8")
    return path
