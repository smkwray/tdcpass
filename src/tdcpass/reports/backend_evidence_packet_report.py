from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def render_backend_evidence_packet_report(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Backend Evidence Packet")
    lines.append("")
    lines.append(f"Status: `{summary.get('status', 'unknown')}`")
    lines.append(f"Recommended action: `{summary.get('recommended_action', 'unknown')}`")
    lines.append("")
    lines.append(str(summary.get("headline_question", "")))

    lines.append("")
    lines.append("## Reading Order")
    for row in list(summary.get("packet_sections", [])):
        lines.append(f"- {row.get('label')}: {row.get('purpose')}")
        lines.append(f"  json={row.get('json_path')}")
        lines.append(f"  report={row.get('report_path')}")

    lines.append("")
    lines.append("## Takeaways")
    for item in list(summary.get("takeaways", [])):
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def write_backend_evidence_packet_report(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_backend_evidence_packet_report(summary), encoding="utf-8")
    return path
