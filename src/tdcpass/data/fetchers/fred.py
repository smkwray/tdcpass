from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from tdcpass.data.fetchers.http import get_json


def fetch_fred_observations(
    series_id: str,
    *,
    api_key: Optional[str] = None,
    out_path: Optional[Path] = None,
    observation_start: Optional[str] = None,
) -> pd.DataFrame:
    params = {
        "file_type": "json",
        "series_id": series_id,
    }
    if api_key:
        params["api_key"] = api_key
    if observation_start:
        params["observation_start"] = observation_start

    payload = get_json("https://api.stlouisfed.org/fred/series/observations", params=params)
    observations = payload.get("observations", [])
    df = pd.DataFrame(observations)
    if not df.empty:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
    return df
