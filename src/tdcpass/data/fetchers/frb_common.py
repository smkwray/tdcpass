from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

import requests

from tdcpass.data.fetchers.http import DEFAULT_TIMEOUT
from tdcpass.data.fetchers.raw_manifest import utc_now_iso, write_raw_download_manifest
from tdcpass.data.registry import load_data_sources


def _source_payload(source_key: str) -> dict[str, Any]:
    payload = load_data_sources()
    return payload.get("sources", {}).get(source_key, {})


def _base_download_url(source_key: str) -> str:
    source = _source_payload(source_key)
    base_url = source.get("ddp_choose_url") or source.get("landing_url")
    if not base_url:
        raise ValueError(f"Missing source URL for {source_key}")
    parsed = urlparse(base_url)
    return f"{parsed.scheme}://{parsed.netloc}/datadownload/Output.aspx"


def _rel_from_source(source_key: str, rel_code: str) -> str:
    source = _source_payload(source_key)
    ddp_url = source.get("ddp_choose_url")
    if not ddp_url:
        return rel_code.upper()
    query = parse_qs(urlparse(ddp_url).query)
    rel = query.get("rel", [rel_code])[0]
    return rel.upper()


def default_frb_params(source_key: str, rel_code: str) -> dict[str, str]:
    return {"rel": _rel_from_source(source_key, rel_code), "filetype": "csv"}


def fetch_frb_release_raw(
    *,
    source_key: str,
    rel_code: str,
    destination: Path,
    params: Mapping[str, Any] | None = None,
    source_url: str | None = None,
    manifest_path: Path | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Path:
    download_url = source_url or _base_download_url(source_key)
    request_params = dict(params or default_frb_params(source_key, rel_code))
    downloaded_at = utc_now_iso()

    destination.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(download_url, params=request_params, timeout=timeout)
    response.raise_for_status()
    destination.write_bytes(response.content)

    if manifest_path is not None:
        write_raw_download_manifest(
            manifest_path,
            source_key=source_key,
            source_url=download_url,
            params=request_params,
            downloaded_at_utc=downloaded_at,
            file_path=destination,
        )
    return destination
