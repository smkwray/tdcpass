from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd
import requests

from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.data.fetchers.fiscaldata import fetch_fiscaldata_endpoint
from tdcpass.data.fetchers.http import DEFAULT_TIMEOUT, download_file
from tdcpass.data.fetchers.raw_manifest import utc_now_iso, write_raw_download_manifest
from tdcpass.data.sibling_cache import build_cache_reuse_provenance

FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FISCALDATA_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
FISCALDATA_AUCTIONS_ENDPOINT = "v1/accounting/od/auctions_query"
FRED_SERIES = {
    "tga_level": "WTREGEN",
    "reserves_level": "WRESBAL",
    "bank_credit_level": "TOTBKCR",
    "treasury_agency_level": "TNMACBW027SBOG",
    "fedfunds": "FEDFUNDS",
    "unemployment": "UNRATE",
    "cpi": "CPIAUCSL",
}
FRED_LEVEL_DIVISORS = {
    "tga_level": 1000.0,
    "reserves_level": 1000.0,
    "bank_credit_level": 1.0,
    "treasury_agency_level": 1.0,
}
Z1_SERIES = {
    "tdc_bank_only_level": "FL763123005",
    "total_deposits_bank_level": "FL764100005",
    "federal_government_checkable_level": "FL313020005",
    "federal_government_time_savings_level": "FL313030003",
    "federal_government_other_level": "FL313030505",
    "foreign_total_deposits_level": "FL264000005",
    "domestic_nonfinancial_mmf_level": "FL383034005",
    "domestic_nonfinancial_repo_level": "FL382051005",
}
Z1_TABLE_MEMBERS = {
    "csv/l201.csv": ("total_deposits_bank_level", "foreign_total_deposits_level"),
    "csv/l203.csv": ("tdc_bank_only_level", "federal_government_checkable_level"),
    "csv/l204.csv": ("federal_government_time_savings_level",),
    "csv/l205.csv": ("federal_government_other_level",),
}


@dataclass(frozen=True)
class QuarterlyPanelBuildResult:
    panel_path: Path
    raw_download_manifest_path: Path
    reused_artifacts_path: Path
    proxy_unit_audit_path: Path
    rows: int


def _output_contract() -> Mapping[str, object]:
    return load_yaml(repo_root() / "config" / "output_contract.yml")


def _required_panel_columns() -> list[str]:
    payload = _output_contract()
    for artifact in payload.get("artifacts", []):
        if artifact.get("path") == "data/derived/quarterly_panel.csv":
            return [str(item) for item in artifact.get("required_columns", [])]
    raise KeyError("Quarterly panel contract missing from config/output_contract.yml")


def _headline_sample_columns() -> list[str]:
    payload = _output_contract()
    for artifact in payload.get("artifacts", []):
        if artifact.get("path") == "data/derived/quarterly_panel.csv":
            columns = artifact.get("headline_sample_columns")
            if columns:
                return [str(item) for item in columns]
            return _required_panel_columns()
    raise KeyError("Quarterly panel contract missing from config/output_contract.yml")


def _append_raw_manifest(
    manifest_path: Path,
    *,
    source_key: str,
    source_url: str,
    params: Mapping[str, object] | None,
    file_path: Path,
) -> None:
    write_raw_download_manifest(
        manifest_path,
        source_key=source_key,
        source_url=source_url,
        params=params,
        downloaded_at_utc=utc_now_iso(),
        file_path=file_path,
    )


def _download_current_z1_zip(raw_dir: Path, manifest_path: Path, *, timeout: int = DEFAULT_TIMEOUT) -> Path:
    landing_url = "https://www.federalreserve.gov/releases/z1/"
    html_path = raw_dir / "z1" / "landing.html"
    zip_path = raw_dir / "z1" / "z1_csv_files.zip"

    response = requests.get(landing_url, timeout=timeout)
    response.raise_for_status()
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(response.text, encoding="utf-8")
    _append_raw_manifest(
        manifest_path,
        source_key="frb_z1_page",
        source_url=landing_url,
        params=None,
        file_path=html_path,
    )

    match = re.search(r'href="(?P<path>/releases/z1/\d+/z1_csv_files\.zip)"', response.text, flags=re.IGNORECASE)
    if match is None:
        raise ValueError("Could not locate current Z.1 CSV zip URL on the release page.")
    zip_url = f"https://www.federalreserve.gov{match.group('path')}"
    download_file(zip_url, zip_path, timeout=timeout)
    _append_raw_manifest(
        manifest_path,
        source_key="frb_z1_zip",
        source_url=zip_url,
        params=None,
        file_path=zip_path,
    )
    return zip_path


def _read_z1_levels(zip_path: Path, series_codes: Mapping[str, str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(zip_path) as archive:
        missing_members = [member_name for member_name in Z1_TABLE_MEMBERS if member_name not in archive.namelist()]
        if missing_members:
            with archive.open("csv/all_sectors_levels_q.csv") as handle:
                frame = pd.read_csv(handle)
            frame = frame.rename(columns=lambda value: str(value).removesuffix(".Q"))
            selected = ["date", *series_codes.values()]
            out = frame[selected].rename(columns={code: key for key, code in series_codes.items()})
        else:
            for member_name, keys in Z1_TABLE_MEMBERS.items():
                with archive.open(member_name) as handle:
                    frame = pd.read_csv(handle)
                frame = frame.rename(columns=lambda value: str(value).removesuffix(".Q"))
                selected = ["date", *[series_codes[key] for key in keys]]
                frame = frame[selected].rename(columns={series_codes[key]: key for key in keys})
                frames.append(frame)
            out = frames[0]
            for frame in frames[1:]:
                out = out.merge(frame, on="date", how="outer")
            missing_keys = [key for key in series_codes if key not in out.columns]
            if missing_keys:
                with archive.open("csv/all_sectors_levels_q.csv") as handle:
                    all_sectors = pd.read_csv(handle)
                all_sectors = all_sectors.rename(columns=lambda value: str(value).removesuffix(".Q"))
                supplement = all_sectors[["date", *[series_codes[key] for key in missing_keys]]].rename(
                    columns={series_codes[key]: key for key in missing_keys}
                )
                out = out.merge(supplement, on="date", how="left")

    out["quarter"] = out["date"].astype(str).str.replace(":", "", regex=False)
    out = out.drop(columns=["date"])
    for column in series_codes:
        out[column] = pd.to_numeric(out[column], errors="coerce") / 1000.0
    return out


def _download_fred_csv(series_id: str, raw_dir: Path, manifest_path: Path, *, timeout: int = DEFAULT_TIMEOUT) -> Path:
    destination = raw_dir / "fred" / f"{series_id}.csv"
    url = f"{FRED_GRAPH_URL}?id={series_id}"
    download_file(url, destination, timeout=timeout)
    _append_raw_manifest(
        manifest_path,
        source_key="fred_graph",
        source_url=FRED_GRAPH_URL,
        params={"id": series_id},
        file_path=destination,
    )
    return destination


def _download_fiscaldata_auctions_csv(
    raw_dir: Path,
    manifest_path: Path,
    *,
    start_date: str = "2000-01-01",
    timeout: int = DEFAULT_TIMEOUT,
) -> Path:
    destination = raw_dir / "fiscaldata" / "auctions_query.csv"
    params = {
        "filter": f"auction_date:gte:{start_date}",
        "sort": "auction_date",
    }
    fetch_fiscaldata_endpoint(
        FISCALDATA_AUCTIONS_ENDPOINT,
        out_path=destination,
        page_size=10000,
        extra_params=params,
        timeout=timeout,
    )
    _append_raw_manifest(
        manifest_path,
        source_key="fiscaldata_auctions_query",
        source_url=f"{FISCALDATA_BASE_URL}{FISCALDATA_AUCTIONS_ENDPOINT}",
        params=params,
        file_path=destination,
    )
    return destination


def _load_fred_series(path: Path) -> pd.Series:
    frame = pd.read_csv(path)
    date_column = "DATE" if "DATE" in frame.columns else "observation_date"
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce")
    frame["VALUE"] = pd.to_numeric(frame.iloc[:, 1], errors="coerce")
    frame = frame.dropna(subset=[date_column])
    return pd.Series(frame["VALUE"].to_numpy(), index=frame[date_column], name=path.stem).sort_index()


def _load_bill_share_series(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError("FiscalData auctions extract is empty.")
    frame["issue_date"] = pd.to_datetime(frame["issue_date"], errors="coerce")
    frame["offering_amt"] = pd.to_numeric(frame["offering_amt"], errors="coerce")
    frame["security_type"] = frame["security_type"].astype(str)
    eligible = frame["security_type"].isin({"Bill", "Note", "Bond"}) & frame["issue_date"].notna() & frame["offering_amt"].notna()
    sample = frame.loc[eligible, ["issue_date", "security_type", "offering_amt"]].copy()
    if sample.empty:
        raise ValueError("No eligible Treasury auction rows were available to build bill_share.")
    sample["quarter"] = sample["issue_date"].dt.to_period("Q").astype(str)
    sample["offering_amt_billions"] = sample["offering_amt"] / 1_000_000_000.0

    totals = sample.groupby("quarter")["offering_amt_billions"].sum()
    bills = sample.loc[sample["security_type"] == "Bill"].groupby("quarter")["offering_amt_billions"].sum()
    bill_share = (bills / totals).fillna(0.0).clip(0.0, 1.0)
    return bill_share.rename("bill_share").reset_index()


def _quarter_end_level(series: pd.Series) -> pd.Series:
    grouped = series.groupby(series.index.to_period("Q")).last()
    grouped.index = grouped.index.astype(str)
    return grouped


def _quarter_average_level(series: pd.Series) -> pd.Series:
    grouped = series.groupby(series.index.to_period("Q")).mean()
    grouped.index = grouped.index.astype(str)
    return grouped


def _qoq_change(levels: pd.Series) -> pd.Series:
    return levels.astype(float).diff().round(12)


def _align_quarter_series(series: pd.Series, quarters: pd.Series) -> pd.Series:
    return series.reindex(quarters).reset_index(drop=True)


def _load_reused_tdc_series(reuse_payload: Mapping[str, object]) -> pd.DataFrame | None:
    artifacts = reuse_payload.get("reused_artifacts", [])
    if not isinstance(artifacts, list):
        return None
    for artifact in artifacts:
        if not isinstance(artifact, Mapping):
            continue
        path_value = artifact.get("materialized_path") or artifact.get("source_path")
        if not path_value:
            continue
        path = Path(str(path_value))
        if not path.exists() or path.suffix.lower() != ".csv":
            continue
        frame = pd.read_csv(path)
        if "quarter" not in frame.columns:
            continue
        if "tdc_bank_only_qoq" in frame.columns:
            return frame[["quarter", "tdc_bank_only_qoq"]].copy()
        if "tdc_qoq" in frame.columns:
            return frame[["quarter", "tdc_qoq"]].rename(columns={"tdc_qoq": "tdc_bank_only_qoq"})
    return None


def _write_proxy_unit_audit(
    path: Path,
    *,
    fred_levels_raw: Mapping[str, pd.Series],
    fred_levels_scaled: Mapping[str, pd.Series],
    panel: pd.DataFrame,
) -> Path:
    source_series = []
    for key in ("bank_credit_level", "treasury_agency_level", "tga_level", "reserves_level"):
        raw = fred_levels_raw[key].dropna()
        scaled = fred_levels_scaled[key].dropna()
        source_series.append(
            {
                "series_key": key,
                "series_id": FRED_SERIES[key],
                "scale_divisor": float(FRED_LEVEL_DIVISORS[key]),
                "output_units": "billions_usd",
                "raw_start_date": None if raw.empty else str(raw.index.min()),
                "raw_end_date": None if raw.empty else str(raw.index.max()),
                "latest_raw_level": None if raw.empty else float(raw.iloc[-1]),
                "latest_scaled_level": None if scaled.empty else float(scaled.iloc[-1]),
            }
        )

    derived_proxies = []
    for proxy_name in (
        "bank_credit_private_qoq",
        "cb_nonts_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
    ):
        series = panel[["quarter", proxy_name]].dropna()
        derived_proxies.append(
            {
                "proxy": proxy_name,
                "units": "billions_usd",
                "start_quarter": None if series.empty else str(series["quarter"].iloc[0]),
                "end_quarter": None if series.empty else str(series["quarter"].iloc[-1]),
                "non_missing_obs": int(len(series)),
                "median_abs_qoq": None if series.empty else float(series[proxy_name].abs().median()),
            }
        )

    takeaways = [
        "FRED level series are now scaled with an explicit per-series divisor rather than a blanket /1000 rule.",
        "TOTBKCR and TNMACBW027SBOG are treated as already being in billions_usd, while WTREGEN and WRESBAL are converted from millions_usd to billions_usd.",
    ]
    payload = {
        "status": "ok",
        "source_series": source_series,
        "derived_proxies": derived_proxies,
        "takeaways": takeaways,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _enforce_max_common_sample(frame: pd.DataFrame, required_columns: Iterable[str]) -> pd.DataFrame:
    required = [column for column in required_columns if column in frame.columns]
    sample = frame.dropna(subset=required).copy()
    if sample.empty:
        raise ValueError("No rows survive the max-common-sample requirement for the quarterly panel.")
    return sample.reset_index(drop=True)


def _compute_slr_tight_indicator(frame: pd.DataFrame) -> pd.Series:
    return (
        (frame["bank_absorption_share"] > frame["bank_absorption_share"].median(skipna=True))
        & (frame["reserves_qoq"] < 0)
    ).astype(float)


def load_panel(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def build_public_quarterly_panel(
    base_dir: Path | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    reuse_mode: str | None = None,
) -> QuarterlyPanelBuildResult:
    root = base_dir or repo_root()
    dirs = ensure_repo_dirs(root)
    raw_dir = dirs["data_raw"]
    derived_dir = dirs["data_derived"]
    output_manifest_dir = root / "output" / "manifests"
    output_manifest_dir.mkdir(parents=True, exist_ok=True)

    raw_download_manifest_path = output_manifest_dir / "raw_downloads.json"
    reused_artifacts_path = output_manifest_dir / "reused_artifacts.json"
    proxy_unit_audit_path = root / "output" / "models" / "proxy_unit_audit.json"

    reuse_payload = build_cache_reuse_provenance(reuse_mode=reuse_mode)
    reused_artifacts_path.write_text(
        json.dumps(reuse_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    z1_zip_path = _download_current_z1_zip(raw_dir, raw_download_manifest_path, timeout=timeout)
    z1_levels = _read_z1_levels(z1_zip_path, Z1_SERIES)
    auctions_path = _download_fiscaldata_auctions_csv(raw_dir, raw_download_manifest_path, timeout=timeout)
    bill_share = _load_bill_share_series(auctions_path)

    broad_tdc_level = (
        z1_levels["federal_government_checkable_level"]
        + z1_levels["federal_government_time_savings_level"]
        + z1_levels["federal_government_other_level"]
    )
    panel = pd.DataFrame(
        {
            "quarter": z1_levels["quarter"],
            "tdc_bank_only_qoq": _qoq_change(z1_levels["tdc_bank_only_level"]),
            "tdc_broad_depository_qoq": _qoq_change(broad_tdc_level),
            "total_deposits_bank_qoq": _qoq_change(z1_levels["total_deposits_bank_level"]),
            "foreign_nonts_qoq": _qoq_change(z1_levels["foreign_total_deposits_level"]),
            "domestic_nonfinancial_mmf_reallocation_qoq": -_qoq_change(z1_levels["domestic_nonfinancial_mmf_level"]),
            "domestic_nonfinancial_repo_reallocation_qoq": -_qoq_change(z1_levels["domestic_nonfinancial_repo_level"]),
            "bank_absorption_share": (z1_levels["tdc_bank_only_level"] / broad_tdc_level.replace({0.0: pd.NA})).clip(0.0, 1.0),
        }
    )
    panel = panel.merge(bill_share, on="quarter", how="left")

    reused_tdc = _load_reused_tdc_series(reuse_payload)
    if reused_tdc is not None:
        panel = panel.drop(columns=["tdc_bank_only_qoq"]).merge(reused_tdc, on="quarter", how="left")

    fred_levels_raw: dict[str, pd.Series] = {}
    fred_levels: dict[str, pd.Series] = {}
    for key, series_id in FRED_SERIES.items():
        csv_path = _download_fred_csv(series_id, raw_dir, raw_download_manifest_path, timeout=timeout)
        series = _load_fred_series(csv_path)
        if key in {"fedfunds", "unemployment", "cpi"}:
            fred_levels[key] = _quarter_average_level(series)
        else:
            fred_levels_raw[key] = _quarter_end_level(series)
            fred_levels[key] = fred_levels_raw[key] / float(FRED_LEVEL_DIVISORS[key])

    fred_frame = pd.DataFrame({"quarter": list(fred_levels["tga_level"].index)})
    fred_frame["tga_qoq"] = _align_quarter_series(_qoq_change(fred_levels["tga_level"]), fred_frame["quarter"])
    fred_frame["reserves_qoq"] = _align_quarter_series(_qoq_change(fred_levels["reserves_level"]), fred_frame["quarter"])
    bank_private_level = fred_levels["bank_credit_level"] - fred_levels["treasury_agency_level"]
    fred_frame["bank_credit_private_qoq"] = _align_quarter_series(_qoq_change(bank_private_level), fred_frame["quarter"])
    fred_frame["fedfunds"] = _align_quarter_series(fred_levels["fedfunds"], fred_frame["quarter"])
    fred_frame["unemployment"] = _align_quarter_series(fred_levels["unemployment"], fred_frame["quarter"])
    cpi_quarter = fred_levels["cpi"].reindex(fred_frame["quarter"])
    fred_frame["inflation"] = _align_quarter_series(cpi_quarter.pct_change() * 100.0, fred_frame["quarter"])
    fred_frame["cb_nonts_qoq"] = fred_frame["reserves_qoq"] + fred_frame["tga_qoq"]

    panel = panel.merge(fred_frame, on="quarter", how="outer").sort_values("quarter").reset_index(drop=True)
    panel["other_component_qoq"] = panel["total_deposits_bank_qoq"] - panel["tdc_bank_only_qoq"]
    for column in [
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "bank_credit_private_qoq",
        "tga_qoq",
        "reserves_qoq",
        "bill_share",
        "fedfunds",
        "unemployment",
        "inflation",
    ]:
        panel[f"lag_{column}"] = panel[column].shift(1)

    panel = _enforce_max_common_sample(panel, _headline_sample_columns())
    panel["reserve_drain_pressure"] = -panel["lag_reserves_qoq"]
    panel["quarter_index"] = range(len(panel))
    panel["slr_tight"] = _compute_slr_tight_indicator(panel)
    panel_path = derived_dir / "quarterly_panel.csv"
    panel_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(panel_path, index=False, float_format="%.17g")
    _write_proxy_unit_audit(
        proxy_unit_audit_path,
        fred_levels_raw=fred_levels_raw,
        fred_levels_scaled=fred_levels,
        panel=panel,
    )

    return QuarterlyPanelBuildResult(
        panel_path=panel_path,
        raw_download_manifest_path=raw_download_manifest_path,
        reused_artifacts_path=reused_artifacts_path,
        proxy_unit_audit_path=proxy_unit_audit_path,
        rows=int(len(panel)),
    )
