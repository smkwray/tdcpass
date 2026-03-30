from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from tdcpass.data.fetchers.frb_common import fetch_frb_release_raw
from tdcpass.data.fetchers.http import DEFAULT_TIMEOUT


def fetch_frb_h8_raw(
    destination: Path,
    *,
    params: Mapping[str, Any] | None = None,
    source_url: str | None = None,
    manifest_path: Path | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Path:
    return fetch_frb_release_raw(
        source_key="frb_h8",
        rel_code="h8",
        destination=destination,
        params=params,
        source_url=source_url,
        manifest_path=manifest_path,
        timeout=timeout,
    )
