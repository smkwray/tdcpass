from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(path: Path, *, command: str, outputs: Iterable[Path], extra: Mapping[str, Any] | None = None) -> Path:
    payload = {
        "command": command,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "outputs": [
            {
                "path": str(item),
                "exists": item.exists(),
                "sha256": sha256_file(item) if item.exists() and item.is_file() else None,
            }
            for item in outputs
        ],
    }
    if extra:
        payload["extra"] = dict(extra)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path
