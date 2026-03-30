from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

from tdcpass.data.fetchers.http import DEFAULT_TIMEOUT, get_json


def fetch_fiscaldata_endpoint(
    endpoint: str,
    *,
    out_path: Optional[Path] = None,
    page_size: int = 10000,
    extra_params: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    base = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    url = base + endpoint.lstrip("/")
    params: Dict[str, str] = {"page[size]": str(page_size)}
    if extra_params:
        params.update(extra_params)

    rows = []
    while True:
        request_url = requests.Request("GET", url, params=params).prepare().url
        payload = get_json(url, params=params, timeout=timeout)
        rows.extend(payload.get("data", []))
        links = payload.get("links", {}) or {}
        next_url = links.get("next")
        if not next_url:
            break
        if str(next_url).startswith("&"):
            url = f"{request_url}{next_url}"
        else:
            url = urljoin(request_url, str(next_url))
        params = {}

    df = pd.DataFrame(rows)
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
    return df
