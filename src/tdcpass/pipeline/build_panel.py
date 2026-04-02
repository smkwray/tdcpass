from __future__ import annotations

import json
import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd
import requests

from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.data.fetchers.fiscaldata import fetch_fiscaldata_endpoint
from tdcpass.data.fetchers.fred import fetch_fred_observations
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
    "on_rrp_level": "RRPONTSYD",
    "currency_component_level": "CURRNS",
    "commercial_bank_borrowings_level": "H8B3094NCBA",
    "fed_borrowings_depository_institutions_level": "BORROW",
    "commercial_industrial_loans_level": "TOTCINSA",
    "construction_land_development_loans_level": "CLDACBW027SBOG",
    "cre_multifamily_loans_level": "SMPACBW027SBOG",
    "cre_nonfarm_nonresidential_loans_level": "SNFACBW027SBOG",
    "consumer_loans_level": "CLSACBW027SBOG",
    "credit_card_revolving_loans_level": "CCLACBW027SBOG",
    "auto_loans_level": "CARACBW027SBOG",
    "other_consumer_loans_level": "OCLACBW027SBOG",
    "heloc_loans_level": "RHEACBW027SBOG",
    "closed_end_residential_loans_level": "CRLACBW027SBOG",
    "loans_to_commercial_banks_level": "LCBACBW027SBOG",
    "loans_to_nondepository_financial_institutions_level": "LNFACBW027SBOG",
    "loans_for_purchasing_or_carrying_securities_level": "BOGZ1FL763067003Q",
    "commercial_industrial_chargeoff_rate": "CORBLACBS",
    "consumer_chargeoff_rate": "CORCACBS",
    "credit_card_revolving_chargeoff_rate": "CORCCACBS",
    "other_consumer_chargeoff_rate": "COROCLACBS",
    "closed_end_residential_chargeoff_rate": "CORSFRMACBS",
    "fedfunds": "FEDFUNDS",
    "unemployment": "UNRATE",
    "cpi": "CPIAUCSL",
}
FRED_LEVEL_DIVISORS = {
    "tga_level": 1000.0,
    "reserves_level": 1000.0,
    "bank_credit_level": 1.0,
    "treasury_agency_level": 1.0,
    "on_rrp_level": 1000.0,
    "currency_component_level": 1.0,
    "commercial_bank_borrowings_level": 1000.0,
    "fed_borrowings_depository_institutions_level": 1000.0,
    "commercial_industrial_loans_level": 1.0,
    "construction_land_development_loans_level": 1.0,
    "cre_multifamily_loans_level": 1.0,
    "cre_nonfarm_nonresidential_loans_level": 1.0,
    "consumer_loans_level": 1.0,
    "credit_card_revolving_loans_level": 1.0,
    "auto_loans_level": 1.0,
    "other_consumer_loans_level": 1.0,
    "heloc_loans_level": 1.0,
    "closed_end_residential_loans_level": 1.0,
    "loans_to_commercial_banks_level": 1.0,
    "loans_to_nondepository_financial_institutions_level": 1.0,
    "loans_for_purchasing_or_carrying_securities_level": 1000.0,
}
CORE_CREATOR_LENDING_FRED_KEYS = {
    "commercial_industrial_loans_qoq": "commercial_industrial_loans_level",
    "construction_land_development_loans_qoq": "construction_land_development_loans_level",
    "cre_multifamily_loans_qoq": "cre_multifamily_loans_level",
    "cre_nonfarm_nonresidential_loans_qoq": "cre_nonfarm_nonresidential_loans_level",
    "consumer_loans_qoq": "consumer_loans_level",
    "credit_card_revolving_loans_qoq": "credit_card_revolving_loans_level",
    "auto_loans_qoq": "auto_loans_level",
    "other_consumer_loans_qoq": "other_consumer_loans_level",
    "heloc_loans_qoq": "heloc_loans_level",
    "closed_end_residential_loans_qoq": "closed_end_residential_loans_level",
}
NONCORE_CREATOR_LENDING_FRED_KEYS = {
    "loans_to_commercial_banks_qoq": "loans_to_commercial_banks_level",
    "loans_to_nondepository_financial_institutions_qoq": "loans_to_nondepository_financial_institutions_level",
    "loans_for_purchasing_or_carrying_securities_qoq": "loans_for_purchasing_or_carrying_securities_level",
}
ASSET_PURCHASE_Z1_KEYS = {
    "treasury_securities_bank_qoq": "treasury_securities_bank_level",
    "agency_gse_backed_securities_bank_qoq": "agency_gse_backed_securities_bank_level",
    "municipal_securities_bank_qoq": "municipal_securities_bank_level",
    "corporate_foreign_bonds_bank_qoq": "corporate_foreign_bonds_bank_level",
}
CHARGEOFF_ADJUSTED_CREATOR_LENDING_KEYS = {
    "commercial_industrial_loans_ex_chargeoffs_qoq": (
        "commercial_industrial_loans_level",
        "commercial_industrial_chargeoff_rate",
    ),
    "consumer_loans_ex_chargeoffs_qoq": (
        "consumer_loans_level",
        "consumer_chargeoff_rate",
    ),
    "credit_card_revolving_loans_ex_chargeoffs_qoq": (
        "credit_card_revolving_loans_level",
        "credit_card_revolving_chargeoff_rate",
    ),
    "other_consumer_loans_ex_chargeoffs_qoq": (
        "other_consumer_loans_level",
        "other_consumer_chargeoff_rate",
    ),
    "closed_end_residential_loans_ex_chargeoffs_qoq": (
        "closed_end_residential_loans_level",
        "closed_end_residential_chargeoff_rate",
    ),
}
CREATOR_LENDING_FRED_KEYS = {
    **CORE_CREATOR_LENDING_FRED_KEYS,
    **NONCORE_CREATOR_LENDING_FRED_KEYS,
}
FRED_AVERAGE_KEYS = {
    "fedfunds",
    "unemployment",
    "cpi",
    "commercial_industrial_chargeoff_rate",
    "consumer_chargeoff_rate",
    "credit_card_revolving_chargeoff_rate",
    "other_consumer_chargeoff_rate",
    "closed_end_residential_chargeoff_rate",
}
Z1_SERIES = {
    "total_deposits_bank_level": "FL764100005",
    "checkable_deposits_bank_level": "FL763127005",
    "interbank_transactions_bank_level": "FL764110005",
    "time_savings_deposits_bank_level": "FL763130005",
    "checkable_federal_govt_bank_level": "FL763123005",
    "checkable_state_local_bank_level": "FL763128000",
    "checkable_rest_of_world_bank_level": "FL763122605",
    "checkable_private_domestic_bank_level": "FL763129205",
    "interbank_transactions_foreign_banks_liability_level": "FL764116005",
    "interbank_transactions_foreign_banks_asset_level": "FL764016005",
    "deposits_at_foreign_banks_asset_level": "FL764016205",
    "treasury_securities_bank_level": "LM763061100",
    "agency_gse_backed_securities_bank_level": "LM763061705",
    "municipal_securities_bank_level": "LM763062005",
    "corporate_foreign_bonds_bank_level": "LM763063005",
    "fedfunds_repo_liabilities_bank_level": "FL762150005",
    "debt_securities_bank_liability_level": "FL764122005",
    "fhlb_advances_sallie_mae_loans_bank_level": "FL763169305",
    "holding_company_parent_funding_bank_level": "FL763194735",
    "household_treasury_securities_level": "LM153061105",
    "mmf_treasury_bills_level": "FL633061110",
    "foreign_total_deposits_level": "FL264000005",
    "domestic_nonfinancial_mmf_level": "FL383034005",
    "domestic_nonfinancial_repo_level": "FL382051005",
}
Z1_TABLE_MEMBERS = {
    "csv/l201.csv": ("total_deposits_bank_level", "foreign_total_deposits_level"),
    "csv/l202.csv": (
        "interbank_transactions_bank_level",
        "interbank_transactions_foreign_banks_liability_level",
        "interbank_transactions_foreign_banks_asset_level",
        "deposits_at_foreign_banks_asset_level",
    ),
    "csv/l203.csv": (
        "checkable_deposits_bank_level",
        "checkable_federal_govt_bank_level",
        "checkable_state_local_bank_level",
        "checkable_rest_of_world_bank_level",
        "checkable_private_domestic_bank_level",
    ),
    "csv/l204.csv": ("time_savings_deposits_bank_level",),
    "csv/l207.csv": ("fedfunds_repo_liabilities_bank_level",),
    "csv/l111.csv": (
        "treasury_securities_bank_level",
        "agency_gse_backed_securities_bank_level",
        "municipal_securities_bank_level",
        "corporate_foreign_bonds_bank_level",
        "debt_securities_bank_liability_level",
        "fhlb_advances_sallie_mae_loans_bank_level",
        "holding_company_parent_funding_bank_level",
    ),
    "csv/l210.csv": (
        "household_treasury_securities_level",
        "mmf_treasury_bills_level",
    ),
}
TDCEST_BANK_ONLY_METHOD = "tdc_base_bank_only_ru_flow"
TDCEST_BROAD_DEPOSITORY_METHOD = "tdc_base_broad_depository_np_cu_ru_flow"
TDCEST_DOMESTIC_BANK_ONLY_METHOD = "tdc_domestic_bank_only_ru_flow"
TDCEST_NO_REMIT_BANK_ONLY_METHOD = "tdc_no_remit_bank_only"
TDCEST_CREDIT_UNION_SENSITIVE_METHOD = "tdc_credit_union_aggregate_sensitivity"
TDCEST_NOMINAL_TO_BILLIONS = 1000.0


@dataclass(frozen=True)
class QuarterlyPanelBuildResult:
    panel_path: Path
    raw_download_manifest_path: Path
    reused_artifacts_path: Path
    proxy_unit_audit_path: Path
    sample_construction_summary_path: Path
    canonical_tdc_source_path: Path | None
    canonical_tdc_source_kind: str
    rows: int


@dataclass(frozen=True)
class CanonicalTdcSeriesResult:
    frame: pd.DataFrame
    source_path: Path | None
    source_kind: str


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


def _copy_fixture_file(
    fixture_path: Path,
    destination: Path,
    manifest_path: Path,
    *,
    source_key: str,
) -> Path:
    if not fixture_path.exists():
        raise FileNotFoundError(f"Missing raw fixture file: {fixture_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fixture_path, destination)
    _append_raw_manifest(
        manifest_path,
        source_key=source_key,
        source_url=fixture_path.resolve().as_uri(),
        params={"mode": "raw_fixture"},
        file_path=destination,
    )
    return destination


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


def _normalize_z1_levels_frame(frame: pd.DataFrame, series_codes: Mapping[str, str]) -> pd.DataFrame:
    frame = frame.rename(columns=lambda value: str(value).removesuffix(".Q"))
    if "date" in frame.columns:
        out = frame[["date"]].copy()
    elif "quarter" in frame.columns:
        out = frame[["quarter"]].copy()
    else:
        raise KeyError("Z.1 levels frame must contain either 'date' or 'quarter'.")
    for key, code in series_codes.items():
        if code in frame.columns:
            out[key] = frame[code]
        else:
            out[key] = pd.NA
    return _finalize_z1_levels_frame(out, series_codes.keys())


def _finalize_z1_levels_frame(frame: pd.DataFrame, series_keys: Iterable[str]) -> pd.DataFrame:
    out = frame.copy()
    if "date" in out.columns:
        out["quarter"] = out["date"].astype(str).str.replace(":", "", regex=False)
        out = out.drop(columns=["date"])
    elif "quarter" not in out.columns:
        raise KeyError("Z.1 levels frame must contain either 'date' or 'quarter'.")
    for column in series_keys:
        out[column] = pd.to_numeric(out[column], errors="coerce") / 1000.0
    return out


def _read_z1_levels(zip_path: Path, series_codes: Mapping[str, str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(zip_path) as archive:
        missing_members = [member_name for member_name in Z1_TABLE_MEMBERS if member_name not in archive.namelist()]
        if missing_members:
            with archive.open("csv/all_sectors_levels_q.csv") as handle:
                frame = pd.read_csv(handle)
            return _normalize_z1_levels_frame(frame, series_codes)
        else:
            for member_name, keys in Z1_TABLE_MEMBERS.items():
                with archive.open(member_name) as handle:
                    frame = pd.read_csv(handle)
                frame = frame.rename(columns=lambda value: str(value).removesuffix(".Q"))
                shaped = frame[["date"]].copy()
                for key in keys:
                    source_column = series_codes[key]
                    shaped[key] = frame[source_column] if source_column in frame.columns else pd.NA
                frames.append(shaped)
            out = frames[0]
            for frame in frames[1:]:
                out = out.merge(frame, on="date", how="outer")
            missing_keys = [key for key in series_codes if key not in out.columns]
            if missing_keys:
                with archive.open("csv/all_sectors_levels_q.csv") as handle:
                    all_sectors = pd.read_csv(handle)
                all_sectors = all_sectors.rename(columns=lambda value: str(value).removesuffix(".Q"))
                supplement = all_sectors[["date"]].copy()
                for key in missing_keys:
                    source_column = series_codes[key]
                    supplement[key] = all_sectors[source_column] if source_column in all_sectors.columns else pd.NA
                out = out.merge(supplement, on="date", how="left")

    return _finalize_z1_levels_frame(out, series_codes.keys())


def _read_z1_levels_from_csv(path: Path, series_codes: Mapping[str, str]) -> pd.DataFrame:
    return _normalize_z1_levels_frame(pd.read_csv(path), series_codes)


def _download_fred_csv(series_id: str, raw_dir: Path, manifest_path: Path, *, timeout: int = DEFAULT_TIMEOUT) -> Path:
    destination = raw_dir / "fred" / f"{series_id}.csv"
    api_key = os.getenv("FRED_API_KEY")
    if api_key:
        try:
            api_frame = fetch_fred_observations(series_id, api_key=api_key)
        except requests.RequestException:
            api_frame = pd.DataFrame()
        if not api_frame.empty:
            api_export = pd.DataFrame(
                {
                    "DATE": api_frame["date"],
                    series_id: pd.to_numeric(api_frame["value"], errors="coerce"),
                }
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            api_export.to_csv(destination, index=False)
            _append_raw_manifest(
                manifest_path,
                source_key="fred_api",
                source_url="https://api.stlouisfed.org/fred/series/observations",
                params={"series_id": series_id, "api_key_present": True, "file_type": "json"},
                file_path=destination,
            )
            return destination
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


def _approximate_chargeoff_flow(levels: pd.Series, annualized_rate: pd.Series) -> pd.Series:
    lagged_levels = levels.astype(float).shift(1)
    aligned_rate = annualized_rate.astype(float).reindex(lagged_levels.index)
    return (lagged_levels * aligned_rate / 400.0).round(12)


def _chargeoff_adjusted_qoq(levels: pd.Series, annualized_rate: pd.Series) -> pd.Series:
    return (_qoq_change(levels) + _approximate_chargeoff_flow(levels, annualized_rate)).round(12)


def _align_quarter_series(series: pd.Series, quarters: pd.Series) -> pd.Series:
    return series.reindex(quarters).reset_index(drop=True)


def _quarterly_series_frame(name: str, series: pd.Series) -> pd.DataFrame:
    frame = series.rename(name).rename_axis("quarter").reset_index()
    return frame.sort_values("quarter").reset_index(drop=True)


def _load_canonical_tdc_series_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists() or path.suffix.lower() != ".csv":
        return None
    frame = pd.read_csv(path)
    if "quarter" not in frame.columns:
        if "date" not in frame.columns:
            return None
        dates = pd.to_datetime(frame["date"], errors="coerce")
        if dates.isna().all():
            return None
        frame = frame.assign(quarter=dates.dt.to_period("Q").astype(str))
    if TDCEST_BANK_ONLY_METHOD in frame.columns:
        out = frame[["quarter", TDCEST_BANK_ONLY_METHOD]].rename(
            columns={TDCEST_BANK_ONLY_METHOD: "tdc_bank_only_qoq"}
        )
        if TDCEST_BROAD_DEPOSITORY_METHOD in frame.columns:
            out["tdc_broad_depository_qoq"] = pd.to_numeric(
                frame[TDCEST_BROAD_DEPOSITORY_METHOD], errors="coerce"
            ) / TDCEST_NOMINAL_TO_BILLIONS
        if TDCEST_DOMESTIC_BANK_ONLY_METHOD in frame.columns:
            out["tdc_domestic_bank_only_qoq"] = pd.to_numeric(
                frame[TDCEST_DOMESTIC_BANK_ONLY_METHOD], errors="coerce"
            ) / TDCEST_NOMINAL_TO_BILLIONS
        if TDCEST_NO_REMIT_BANK_ONLY_METHOD in frame.columns:
            out["tdc_no_remit_bank_only_qoq"] = pd.to_numeric(
                frame[TDCEST_NO_REMIT_BANK_ONLY_METHOD], errors="coerce"
            ) / TDCEST_NOMINAL_TO_BILLIONS
        if TDCEST_CREDIT_UNION_SENSITIVE_METHOD in frame.columns:
            out["tdc_credit_union_sensitive_qoq"] = pd.to_numeric(
                frame[TDCEST_CREDIT_UNION_SENSITIVE_METHOD], errors="coerce"
            ) / TDCEST_NOMINAL_TO_BILLIONS
        out["tdc_bank_only_qoq"] = (
            pd.to_numeric(out["tdc_bank_only_qoq"], errors="coerce") / TDCEST_NOMINAL_TO_BILLIONS
        )
        for column in (
            "tdc_broad_depository_qoq",
            "tdc_domestic_bank_only_qoq",
            "tdc_no_remit_bank_only_qoq",
            "tdc_credit_union_sensitive_qoq",
        ):
            if column not in out.columns:
                out[column] = pd.NA
        return out
    if "tdc_bank_only_qoq" in frame.columns:
        out = frame[["quarter", "tdc_bank_only_qoq"]].copy()
        if "tdc_broad_depository_qoq" in frame.columns:
            out["tdc_broad_depository_qoq"] = pd.to_numeric(frame["tdc_broad_depository_qoq"], errors="coerce")
        if "tdc_domestic_bank_only_qoq" in frame.columns:
            out["tdc_domestic_bank_only_qoq"] = pd.to_numeric(frame["tdc_domestic_bank_only_qoq"], errors="coerce")
        if "tdc_no_remit_bank_only_qoq" in frame.columns:
            out["tdc_no_remit_bank_only_qoq"] = pd.to_numeric(frame["tdc_no_remit_bank_only_qoq"], errors="coerce")
        if "tdc_credit_union_sensitive_qoq" in frame.columns:
            out["tdc_credit_union_sensitive_qoq"] = pd.to_numeric(frame["tdc_credit_union_sensitive_qoq"], errors="coerce")
        for column in (
            "tdc_broad_depository_qoq",
            "tdc_domestic_bank_only_qoq",
            "tdc_no_remit_bank_only_qoq",
            "tdc_credit_union_sensitive_qoq",
        ):
            if column not in out.columns:
                out[column] = pd.NA
        return out
    if "tdc_qoq" in frame.columns:
        out = frame[["quarter", "tdc_qoq"]].rename(columns={"tdc_qoq": "tdc_bank_only_qoq"})
        for column in (
            "tdc_broad_depository_qoq",
            "tdc_domestic_bank_only_qoq",
            "tdc_no_remit_bank_only_qoq",
            "tdc_credit_union_sensitive_qoq",
        ):
            out[column] = pd.NA
        return out
    return None


def _load_reused_tdc_series(reuse_payload: Mapping[str, object]) -> CanonicalTdcSeriesResult | None:
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
        frame = _load_canonical_tdc_series_csv(path)
        if frame is not None:
            return CanonicalTdcSeriesResult(frame=frame, source_path=path, source_kind="reused_artifact")
    return None


def _candidate_tdcest_csv_paths(*, root: Path, fixture_root: Path | None) -> list[Path]:
    candidates: list[Path] = []
    workspace_root = repo_root()
    if fixture_root is not None:
        candidates.append(fixture_root / "tdcest" / "tdc_estimates.csv")
    env_root = os.getenv("TDCEST_ROOT")
    if env_root:
        candidates.append(Path(os.path.expanduser(os.path.expandvars(env_root))) / "data" / "processed" / "tdc_estimates.csv")
    candidates.append(workspace_root.parent / "tdcest" / "data" / "processed" / "tdc_estimates.csv")
    candidates.append(workspace_root.parent.parent / "tdcest" / "data" / "processed" / "tdc_estimates.csv")
    candidates.append(root.parent / "tdcest" / "data" / "processed" / "tdc_estimates.csv")
    candidates.append(root.parent.parent / "tdcest" / "data" / "processed" / "tdc_estimates.csv")
    seen: set[str] = set()
    out: list[Path] = []
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    return out


def _load_canonical_tdc_series(
    *,
    root: Path,
    reuse_payload: Mapping[str, object],
    fixture_root: Path | None,
) -> CanonicalTdcSeriesResult:
    reused = _load_reused_tdc_series(reuse_payload)
    if reused is not None and "tdc_broad_depository_qoq" in reused.frame.columns:
        return reused
    for path in _candidate_tdcest_csv_paths(root=root, fixture_root=fixture_root):
        frame = _load_canonical_tdc_series_csv(path)
        if frame is not None and "tdc_broad_depository_qoq" in frame.columns:
            return CanonicalTdcSeriesResult(frame=frame, source_path=path, source_kind="tdcest_processed_csv")
    for path in _candidate_tdcest_csv_paths(root=root, fixture_root=fixture_root):
        frame = _load_canonical_tdc_series_csv(path)
        if frame is not None:
            return CanonicalTdcSeriesResult(frame=frame, source_path=path, source_kind="tdcest_processed_csv")
    raise FileNotFoundError(
        "Could not locate canonical TDC estimates. Expected a tdcest processed file such as "
        "../tdcest/data/processed/tdc_estimates.csv or $TDCEST_ROOT/data/processed/tdc_estimates.csv."
    )


def _write_proxy_unit_audit(
    path: Path,
    *,
    fred_levels_raw: Mapping[str, pd.Series],
    fred_levels_scaled: Mapping[str, pd.Series],
    panel: pd.DataFrame,
) -> Path:
    source_series = []
    for key in (
        "bank_credit_level",
        "treasury_agency_level",
        "tga_level",
        "reserves_level",
        *CREATOR_LENDING_FRED_KEYS.values(),
    ):
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

    creator_channels = []
    for channel_name in (*CREATOR_LENDING_FRED_KEYS.keys(), *ASSET_PURCHASE_Z1_KEYS.keys()):
        series = panel[["quarter", channel_name]].dropna()
        creator_channels.append(
            {
                "channel": channel_name,
                "units": "billions_usd",
                "start_quarter": None if series.empty else str(series["quarter"].iloc[0]),
                "end_quarter": None if series.empty else str(series["quarter"].iloc[-1]),
                "non_missing_obs": int(len(series)),
                "median_abs_qoq": None if series.empty else float(series[channel_name].abs().median()),
            }
        )

    creator_channel_adjustments = []
    for channel_name, (level_key, rate_key) in CHARGEOFF_ADJUSTED_CREATOR_LENDING_KEYS.items():
        series = panel[["quarter", channel_name]].dropna()
        creator_channel_adjustments.append(
            {
                "channel": channel_name,
                "construction": "raw_qoq_plus_lagged_balance_times_annualized_chargeoff_rate_div_400",
                "base_level_series_id": FRED_SERIES[level_key],
                "chargeoff_rate_series_id": FRED_SERIES[rate_key],
                "units": "billions_usd",
                "start_quarter": None if series.empty else str(series["quarter"].iloc[0]),
                "end_quarter": None if series.empty else str(series["quarter"].iloc[-1]),
                "non_missing_obs": int(len(series)),
                "median_abs_qoq": None if series.empty else float(series[channel_name].abs().median()),
            }
        )

    takeaways = [
        "FRED level series are now scaled with an explicit per-series divisor rather than a blanket /1000 rule.",
        "TOTBKCR and TNMACBW027SBOG are treated as already being in billions_usd, while WTREGEN and WRESBAL are converted from millions_usd to billions_usd.",
        "The creator-lane lending series are H.8/FRED balance levels quarterlyized by taking the last observation in each quarter and then differencing q/q, except for the securities-purpose lane that comes from quarterly Z.1 levels.",
        "The first asset-purchase creator lanes come from quarterly Z.1 L.111 holdings for Treasury, agency/GSE-backed, municipal, and corporate/foreign bond assets.",
        "Charge-off-adjusted creator lanes add back an approximate quarterly destruction flow computed from lagged balances and official annualized charge-off rates.",
    ]
    payload = {
        "status": "ok",
        "source_series": source_series,
        "derived_proxies": derived_proxies,
        "creator_channels": creator_channels,
        "creator_channel_adjustments": creator_channel_adjustments,
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


def _write_sample_construction_summary(
    path: Path,
    *,
    full_panel: pd.DataFrame,
    headline_panel: pd.DataFrame,
    headline_columns: list[str],
    extended_columns: list[str],
) -> Path:
    full_panel = full_panel.sort_values("quarter").reset_index(drop=True)
    headline_panel = headline_panel.sort_values("quarter").reset_index(drop=True)
    truncation_rows = []
    dropped = (
        full_panel.loc[~full_panel["quarter"].isin(headline_panel["quarter"])]
        if len(full_panel) != len(headline_panel)
        else full_panel.iloc[0:0]
    )
    for column in headline_columns:
        if column == "quarter":
            continue
        if column not in full_panel.columns:
            continue
        observed = full_panel[["quarter", column]].dropna()
        truncation_rows.append(
            {
                "column": column,
                "full_panel_non_missing_obs": int(full_panel[column].notna().sum()),
                "first_available_quarter": None if observed.empty else str(observed["quarter"].iloc[0]),
                "last_available_quarter": None if observed.empty else str(observed["quarter"].iloc[-1]),
                "missing_rows_in_full_panel": int(full_panel[column].isna().sum()),
                "dropped_rows_with_column_missing": int(dropped[column].isna().sum()) if column in dropped.columns else 0,
            }
        )

    extended_coverage = []
    for column in extended_columns:
        if column not in headline_panel.columns:
            continue
        observed = headline_panel[["quarter", column]].dropna()
        extended_coverage.append(
            {
                "column": column,
                "headline_sample_non_missing_obs": int(headline_panel[column].notna().sum()),
                "headline_sample_missing_obs": int(headline_panel[column].isna().sum()),
                "coverage_share_within_headline_sample": (
                    float(headline_panel[column].notna().mean()) if len(headline_panel) else None
                ),
                "first_available_quarter": None if observed.empty else str(observed["quarter"].iloc[0]),
                "last_available_quarter": None if observed.empty else str(observed["quarter"].iloc[-1]),
            }
        )

    payload = {
        "full_panel": {
            "rows": int(len(full_panel)),
            "start_quarter": None if full_panel.empty else str(full_panel["quarter"].iloc[0]),
            "end_quarter": None if full_panel.empty else str(full_panel["quarter"].iloc[-1]),
        },
        "headline_sample": {
            "rows": int(len(headline_panel)),
            "start_quarter": None if headline_panel.empty else str(headline_panel["quarter"].iloc[0]),
            "end_quarter": None if headline_panel.empty else str(headline_panel["quarter"].iloc[-1]),
            "required_columns": headline_columns,
        },
        "usable_shock_sample": {
            "rows": 0,
            "start_quarter": None,
            "end_quarter": None,
        },
        "shock_definition": {},
        "headline_sample_truncation": {
            "dropped_rows_from_full_panel": int(len(full_panel) - len(headline_panel)),
            "columns": truncation_rows,
        },
        "extended_column_coverage": extended_coverage,
        "takeaways": [
            "The headline panel is trimmed only on the frozen treatment, outcome, and baseline shock-control columns.",
            "Extended proxy and regime variables remain in the exported panel but no longer silently define the headline sample.",
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def load_panel(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def build_public_quarterly_panel(
    base_dir: Path | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    reuse_mode: str | None = None,
    fixture_root: Path | None = None,
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
    sample_construction_summary_path = root / "output" / "models" / "sample_construction_summary.json"

    reuse_payload = build_cache_reuse_provenance(reuse_mode=reuse_mode)
    reused_artifacts_path.write_text(
        json.dumps(reuse_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    if fixture_root is not None:
        z1_csv_path = _copy_fixture_file(
            fixture_root / "z1" / "all_sectors_levels_q.csv",
            raw_dir / "z1" / "all_sectors_levels_q.csv",
            raw_download_manifest_path,
            source_key="fixture_frb_z1_csv",
        )
        z1_levels = _read_z1_levels_from_csv(z1_csv_path, Z1_SERIES)
        auctions_path = _copy_fixture_file(
            fixture_root / "fiscaldata" / "auctions_query.csv",
            raw_dir / "fiscaldata" / "auctions_query.csv",
            raw_download_manifest_path,
            source_key="fixture_fiscaldata_auctions_query",
        )
    else:
        z1_zip_path = _download_current_z1_zip(raw_dir, raw_download_manifest_path, timeout=timeout)
        z1_levels = _read_z1_levels(z1_zip_path, Z1_SERIES)
        auctions_path = _download_fiscaldata_auctions_csv(raw_dir, raw_download_manifest_path, timeout=timeout)
    bill_share = _load_bill_share_series(auctions_path)

    canonical_tdc = _load_canonical_tdc_series(root=root, reuse_payload=reuse_payload, fixture_root=fixture_root)
    panel = pd.DataFrame(
        {
            "quarter": z1_levels["quarter"],
            "total_deposits_bank_qoq": _qoq_change(z1_levels["total_deposits_bank_level"]),
            "checkable_deposits_bank_qoq": _qoq_change(z1_levels["checkable_deposits_bank_level"]),
            "interbank_transactions_bank_qoq": _qoq_change(z1_levels["interbank_transactions_bank_level"]),
            "time_savings_deposits_bank_qoq": _qoq_change(z1_levels["time_savings_deposits_bank_level"]),
            "checkable_federal_govt_bank_qoq": _qoq_change(z1_levels["checkable_federal_govt_bank_level"]),
            "checkable_state_local_bank_qoq": _qoq_change(z1_levels["checkable_state_local_bank_level"]),
            "checkable_rest_of_world_bank_qoq": _qoq_change(z1_levels["checkable_rest_of_world_bank_level"]),
            "checkable_private_domestic_bank_qoq": _qoq_change(z1_levels["checkable_private_domestic_bank_level"]),
            "interbank_transactions_foreign_banks_liability_qoq": _qoq_change(
                z1_levels["interbank_transactions_foreign_banks_liability_level"]
            ),
            "interbank_transactions_foreign_banks_asset_qoq": _qoq_change(
                z1_levels["interbank_transactions_foreign_banks_asset_level"]
            ),
            "deposits_at_foreign_banks_asset_qoq": _qoq_change(
                z1_levels["deposits_at_foreign_banks_asset_level"]
            ),
            "treasury_securities_bank_qoq": _qoq_change(z1_levels["treasury_securities_bank_level"]),
            "agency_gse_backed_securities_bank_qoq": _qoq_change(z1_levels["agency_gse_backed_securities_bank_level"]),
            "municipal_securities_bank_qoq": _qoq_change(z1_levels["municipal_securities_bank_level"]),
            "corporate_foreign_bonds_bank_qoq": _qoq_change(z1_levels["corporate_foreign_bonds_bank_level"]),
            "fedfunds_repo_liabilities_bank_qoq": _qoq_change(z1_levels["fedfunds_repo_liabilities_bank_level"]),
            "debt_securities_bank_liability_qoq": _qoq_change(z1_levels["debt_securities_bank_liability_level"]),
            "fhlb_advances_sallie_mae_loans_bank_qoq": _qoq_change(
                z1_levels["fhlb_advances_sallie_mae_loans_bank_level"]
            ),
            "holding_company_parent_funding_bank_qoq": _qoq_change(
                z1_levels["holding_company_parent_funding_bank_level"]
            ),
            "household_treasury_securities_reallocation_qoq": -_qoq_change(
                z1_levels["household_treasury_securities_level"]
            ),
            "mmf_treasury_bills_reallocation_qoq": -_qoq_change(z1_levels["mmf_treasury_bills_level"]),
            "foreign_nonts_qoq": _qoq_change(z1_levels["foreign_total_deposits_level"]),
            "domestic_nonfinancial_mmf_reallocation_qoq": -_qoq_change(z1_levels["domestic_nonfinancial_mmf_level"]),
            "domestic_nonfinancial_repo_reallocation_qoq": -_qoq_change(z1_levels["domestic_nonfinancial_repo_level"]),
        }
    )
    panel = panel.merge(bill_share, on="quarter", how="left")
    panel = panel.merge(canonical_tdc.frame, on="quarter", how="left")

    fred_levels_raw: dict[str, pd.Series] = {}
    fred_levels: dict[str, pd.Series] = {}
    for key, series_id in FRED_SERIES.items():
        if fixture_root is not None:
            csv_path = _copy_fixture_file(
                fixture_root / "fred" / f"{series_id}.csv",
                raw_dir / "fred" / f"{series_id}.csv",
                raw_download_manifest_path,
                source_key="fixture_fred_graph",
            )
        else:
            csv_path = _download_fred_csv(series_id, raw_dir, raw_download_manifest_path, timeout=timeout)
        series = _load_fred_series(csv_path)
        if key in FRED_AVERAGE_KEYS:
            fred_levels[key] = _quarter_average_level(series)
        else:
            fred_levels_raw[key] = _quarter_end_level(series)
            fred_levels[key] = fred_levels_raw[key] / float(FRED_LEVEL_DIVISORS[key])

    bank_private_level = fred_levels["bank_credit_level"] - fred_levels["treasury_agency_level"]
    fred_series = {
        "tga_qoq": _qoq_change(fred_levels["tga_level"]),
        "reserves_qoq": _qoq_change(fred_levels["reserves_level"]),
        "bank_credit_private_qoq": _qoq_change(bank_private_level),
        "on_rrp_reallocation_qoq": -_qoq_change(fred_levels["on_rrp_level"]),
        "currency_reallocation_qoq": -_qoq_change(fred_levels["currency_component_level"]),
        "commercial_bank_borrowings_qoq": _qoq_change(fred_levels["commercial_bank_borrowings_level"]),
        "fed_borrowings_depository_institutions_qoq": _qoq_change(
            fred_levels["fed_borrowings_depository_institutions_level"]
        ),
        **{
            outcome_name: _qoq_change(fred_levels[level_key])
            for outcome_name, level_key in CREATOR_LENDING_FRED_KEYS.items()
        },
        **{
            outcome_name: _chargeoff_adjusted_qoq(fred_levels[level_key], fred_levels[rate_key])
            for outcome_name, (level_key, rate_key) in CHARGEOFF_ADJUSTED_CREATOR_LENDING_KEYS.items()
        },
        "fedfunds": fred_levels["fedfunds"],
        "unemployment": fred_levels["unemployment"],
        "inflation": fred_levels["cpi"].pct_change() * 100.0,
    }
    fred_frame: pd.DataFrame | None = None
    for name, series in fred_series.items():
        series_frame = _quarterly_series_frame(name, series)
        fred_frame = series_frame if fred_frame is None else fred_frame.merge(series_frame, on="quarter", how="outer")
    if fred_frame is None:
        fred_frame = pd.DataFrame(columns=["quarter"])
    fred_frame = fred_frame.sort_values("quarter").reset_index(drop=True)
    fred_frame["cb_nonts_qoq"] = fred_frame["reserves_qoq"] + fred_frame["tga_qoq"]

    panel = panel.merge(fred_frame, on="quarter", how="outer").sort_values("quarter").reset_index(drop=True)
    panel["other_component_qoq"] = panel["total_deposits_bank_qoq"] - panel["tdc_bank_only_qoq"]
    for column in [
        "tdc_bank_only_qoq",
        "tdc_broad_depository_qoq",
        "tdc_domestic_bank_only_qoq",
        "tdc_no_remit_bank_only_qoq",
        "tdc_credit_union_sensitive_qoq",
        "total_deposits_bank_qoq",
        "checkable_deposits_bank_qoq",
        "interbank_transactions_bank_qoq",
        "time_savings_deposits_bank_qoq",
        "checkable_federal_govt_bank_qoq",
        "checkable_state_local_bank_qoq",
        "checkable_rest_of_world_bank_qoq",
        "checkable_private_domestic_bank_qoq",
        "interbank_transactions_foreign_banks_liability_qoq",
        "interbank_transactions_foreign_banks_asset_qoq",
        "deposits_at_foreign_banks_asset_qoq",
        "treasury_securities_bank_qoq",
        "agency_gse_backed_securities_bank_qoq",
        "municipal_securities_bank_qoq",
        "corporate_foreign_bonds_bank_qoq",
        "fedfunds_repo_liabilities_bank_qoq",
        "debt_securities_bank_liability_qoq",
        "fhlb_advances_sallie_mae_loans_bank_qoq",
        "holding_company_parent_funding_bank_qoq",
        "commercial_bank_borrowings_qoq",
        "fed_borrowings_depository_institutions_qoq",
        "other_component_qoq",
        "bank_credit_private_qoq",
        "commercial_industrial_loans_qoq",
        "construction_land_development_loans_qoq",
        "cre_multifamily_loans_qoq",
        "cre_nonfarm_nonresidential_loans_qoq",
        "consumer_loans_qoq",
        "credit_card_revolving_loans_qoq",
        "auto_loans_qoq",
        "other_consumer_loans_qoq",
        "heloc_loans_qoq",
        "closed_end_residential_loans_qoq",
        "loans_to_commercial_banks_qoq",
        "loans_to_nondepository_financial_institutions_qoq",
        "loans_for_purchasing_or_carrying_securities_qoq",
        "commercial_industrial_loans_ex_chargeoffs_qoq",
        "consumer_loans_ex_chargeoffs_qoq",
        "credit_card_revolving_loans_ex_chargeoffs_qoq",
        "other_consumer_loans_ex_chargeoffs_qoq",
        "closed_end_residential_loans_ex_chargeoffs_qoq",
        "cb_nonts_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
        "on_rrp_reallocation_qoq",
        "currency_reallocation_qoq",
        "household_treasury_securities_reallocation_qoq",
        "mmf_treasury_bills_reallocation_qoq",
        "tga_qoq",
        "reserves_qoq",
        "bill_share",
        "fedfunds",
        "unemployment",
        "inflation",
    ]:
        panel[f"lag_{column}"] = panel[column].shift(1)

    panel["reserve_drain_pressure"] = -panel["lag_reserves_qoq"]
    headline_columns = _headline_sample_columns()
    full_panel = panel.copy()
    panel = _enforce_max_common_sample(full_panel, headline_columns)
    panel["quarter_index"] = range(len(panel))
    required_columns = _required_panel_columns()
    extended_columns = [column for column in required_columns if column not in headline_columns and column != "quarter_index"]
    panel_path = derived_dir / "quarterly_panel.csv"
    panel_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(panel_path, index=False, float_format="%.17g")
    _write_proxy_unit_audit(
        proxy_unit_audit_path,
        fred_levels_raw=fred_levels_raw,
        fred_levels_scaled=fred_levels,
        panel=panel,
    )
    _write_sample_construction_summary(
        sample_construction_summary_path,
        full_panel=full_panel,
        headline_panel=panel,
        headline_columns=headline_columns,
        extended_columns=extended_columns,
    )

    return QuarterlyPanelBuildResult(
        panel_path=panel_path,
        raw_download_manifest_path=raw_download_manifest_path,
        reused_artifacts_path=reused_artifacts_path,
        proxy_unit_audit_path=proxy_unit_audit_path,
        sample_construction_summary_path=sample_construction_summary_path,
        canonical_tdc_source_path=canonical_tdc.source_path,
        canonical_tdc_source_kind=canonical_tdc.source_kind,
        rows=int(len(panel)),
    )
