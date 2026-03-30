from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from tdcpass.core.manifest import sha256_file


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_raw_download_manifest(
    manifest_path: Path,
    *,
    source_key: str,
    source_url: str,
    params: Mapping[str, Any] | None,
    downloaded_at_utc: str,
    file_path: Path,
) -> Path:
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        payload = {"runs": []}

    payload.setdefault("runs", [])
    payload["runs"].append(
        {
            "source_key": source_key,
            "source_url": source_url,
            "params": dict(params or {}),
            "downloaded_at_utc": downloaded_at_utc,
            "file_path": str(file_path),
            "file_sha256": sha256_file(file_path),
            "size_bytes": file_path.stat().st_size,
        }
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return manifest_path
