from __future__ import annotations

from pathlib import Path
from typing import Dict


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def ensure_repo_dirs(base_root: Path | None = None) -> Dict[str, Path]:
    root = base_root or repo_root()
    dirs = {
        "data_raw": root / "data" / "raw",
        "data_cache": root / "data" / "cache",
        "data_derived": root / "data" / "derived",
        "data_examples": root / "data" / "examples",
        "output": root / "output",
        "site_data": root / "site" / "data",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs
