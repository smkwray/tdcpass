from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from tdcpass.pipeline import build_panel


def _write_z1_zip(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "date": ["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"],
            "FL763123005": [10.0, 12.0, 16.0, 18.0, 21.0],
            "FL764100005": [100.0, 110.0, 121.0, 127.0, 133.0],
            "FL313020005": [20.0, 22.0, 24.0, 26.0, 28.0],
            "FL313030003": [5.0, 6.0, 7.0, 7.5, 8.0],
            "FL313030505": [3.0, 3.5, 4.0, 4.5, 5.0],
            "FL264000005": [50.0, 51.0, 53.0, 54.0, 56.0],
            "FL383034005": [80.0, 79.0, 78.0, 76.0, 75.0],
            "FL382051005": [40.0, 39.0, 38.0, 37.0, 35.0],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("csv/all_sectors_levels_q.csv", frame.to_csv(index=False))


def _write_z1_multitable_zip(path: Path) -> None:
    all_sectors = pd.DataFrame(
        {
            "date": ["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"],
            "FL763123005": [10.0, 12.0, 16.0, 18.0, 21.0],
            "FL764100005": [100.0, 110.0, 121.0, 127.0, 133.0],
            "FL313020005": [20.0, 22.0, 24.0, 26.0, 28.0],
            "FL313030003": [5.0, 6.0, 7.0, 7.5, 8.0],
            "FL313030505": [3.0, 3.5, 4.0, 4.5, 5.0],
            "FL264000005": [50.0, 51.0, 53.0, 54.0, 56.0],
            "FL383034005": [80.0, 79.0, 78.0, 76.0, 75.0],
            "FL382051005": [40.0, 39.0, 38.0, 37.0, 35.0],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "csv/l201.csv",
            all_sectors[["date", "FL764100005", "FL264000005"]].to_csv(index=False),
        )
        archive.writestr(
            "csv/l203.csv",
            all_sectors[["date", "FL763123005", "FL313020005"]].to_csv(index=False),
        )
        archive.writestr("csv/l204.csv", all_sectors[["date", "FL313030003"]].to_csv(index=False))
        archive.writestr("csv/l205.csv", all_sectors[["date", "FL313030505"]].to_csv(index=False))
        archive.writestr("csv/all_sectors_levels_q.csv", all_sectors.to_csv(index=False))


def _write_fred_csv(path: Path, *, header: str, rows: list[tuple[str, float]]) -> None:
    frame = pd.DataFrame(rows, columns=["DATE", header])
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_auctions_csv(path: Path, rows: list[tuple[str, str, float]]) -> None:
    frame = pd.DataFrame(rows, columns=["issue_date", "security_type", "offering_amt"])
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _fake_canonical_tdc_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "quarter": ["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"],
            "tdc_bank_only_qoq": [None, 2.0, 4.0, 2.0, 3.0],
            "tdc_broad_depository_qoq": [None, 3.0, 5.0, 3.0, 4.0],
        }
    )


def test_build_public_quarterly_panel_writes_contract_columns(tmp_path: Path, monkeypatch) -> None:
    z1_zip = tmp_path / "fixtures" / "z1.zip"
    _write_z1_zip(z1_zip)

    fred_data = {
        "WTREGEN": [("2000-03-31", 1000.0), ("2000-06-30", 1020.0), ("2000-09-30", 980.0), ("2000-12-31", 990.0), ("2001-03-31", 1010.0)],
        "WRESBAL": [("2000-03-31", 2000.0), ("2000-06-30", 2010.0), ("2000-09-30", 1980.0), ("2000-12-31", 1995.0), ("2001-03-31", 2025.0)],
        "TOTBKCR": [("2000-03-31", 3000.0), ("2000-06-30", 3030.0), ("2000-09-30", 3060.0), ("2000-12-31", 3090.0), ("2001-03-31", 3125.0)],
        "TNMACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 420.0), ("2000-12-31", 435.0), ("2001-03-31", 440.0)],
        "FEDFUNDS": [("2000-01-31", 5.5), ("2000-02-29", 5.6), ("2000-03-31", 5.7), ("2000-04-30", 5.8), ("2000-05-31", 5.9), ("2000-06-30", 6.0), ("2000-07-31", 6.1), ("2000-08-31", 6.0), ("2000-09-30", 5.9), ("2000-10-31", 5.8), ("2000-11-30", 5.7), ("2000-12-31", 5.6), ("2001-01-31", 5.5), ("2001-02-28", 5.4), ("2001-03-31", 5.3)],
        "UNRATE": [("2000-01-31", 4.0), ("2000-02-29", 4.1), ("2000-03-31", 4.0), ("2000-04-30", 4.0), ("2000-05-31", 4.1), ("2000-06-30", 4.0), ("2000-07-31", 4.0), ("2000-08-31", 4.1), ("2000-09-30", 4.0), ("2000-10-31", 4.1), ("2000-11-30", 4.2), ("2000-12-31", 4.2), ("2001-01-31", 4.3), ("2001-02-28", 4.3), ("2001-03-31", 4.3)],
        "CPIAUCSL": [("2000-01-31", 168.8), ("2000-02-29", 169.8), ("2000-03-31", 171.2), ("2000-04-30", 171.3), ("2000-05-31", 171.5), ("2000-06-30", 172.4), ("2000-07-31", 172.8), ("2000-08-31", 172.8), ("2000-09-30", 173.7), ("2000-10-31", 174.0), ("2000-11-30", 174.1), ("2000-12-31", 174.0), ("2001-01-31", 175.1), ("2001-02-28", 175.8), ("2001-03-31", 176.2)],
    }
    auctions_rows = [
        ("2000-01-15", "Bill", 60_000_000_000.0),
        ("2000-02-15", "Note", 40_000_000_000.0),
        ("2000-04-15", "Bill", 70_000_000_000.0),
        ("2000-05-15", "Bond", 30_000_000_000.0),
        ("2000-07-15", "Bill", 50_000_000_000.0),
        ("2000-08-15", "Note", 50_000_000_000.0),
        ("2000-10-15", "Bill", 20_000_000_000.0),
        ("2000-11-15", "Bond", 80_000_000_000.0),
        ("2001-01-15", "Bill", 25_000_000_000.0),
        ("2001-02-15", "Note", 75_000_000_000.0),
    ]

    def fake_download_current_z1_zip(raw_dir: Path, manifest_path: Path, *, timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "z1" / "z1_csv_files.zip"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(z1_zip.read_bytes())
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps({"runs": []}) + "\n", encoding="utf-8")
        return target

    def fake_download_fred_csv(series_id: str, raw_dir: Path, manifest_path: Path, *, timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "fred" / f"{series_id}.csv"
        _write_fred_csv(target, header=series_id, rows=fred_data[series_id])
        return target

    def fake_download_fiscaldata_auctions_csv(raw_dir: Path, manifest_path: Path, *, start_date: str = "2000-01-01", timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "fiscaldata" / "auctions_query.csv"
        _write_auctions_csv(target, auctions_rows)
        return target

    monkeypatch.setattr(build_panel, "_download_current_z1_zip", fake_download_current_z1_zip)
    monkeypatch.setattr(build_panel, "_download_fred_csv", fake_download_fred_csv)
    monkeypatch.setattr(build_panel, "_download_fiscaldata_auctions_csv", fake_download_fiscaldata_auctions_csv)
    monkeypatch.setattr(build_panel, "build_cache_reuse_provenance", lambda reuse_mode=None: {"reuse_mode": "discover", "reused_artifacts": [], "fresh_downloads": []})
    monkeypatch.setattr(build_panel, "_load_canonical_tdc_series", lambda **kwargs: _fake_canonical_tdc_frame())

    result = build_panel.build_public_quarterly_panel(base_dir=tmp_path)
    frame = pd.read_csv(result.panel_path)

    assert result.panel_path.exists()
    assert result.raw_download_manifest_path.exists()
    assert result.reused_artifacts_path.exists()
    assert result.proxy_unit_audit_path.exists()
    assert result.sample_construction_summary_path.exists()
    assert set(build_panel._required_panel_columns()).issubset(frame.columns)
    assert frame["quarter"].iloc[0] == "2000Q3"
    assert frame.loc[frame["quarter"] == "2000Q3", "bank_credit_private_qoq"].isna().all()
    assert (frame["other_component_qoq"] == frame["total_deposits_bank_qoq"] - frame["tdc_bank_only_qoq"]).all()
    assert frame["bill_share"].between(0.0, 1.0).all()
    assert (frame["reserve_drain_pressure"] == -frame["lag_reserves_qoq"]).all()
    assert frame["quarter_index"].tolist() == list(range(len(frame)))
    assert frame["quarter"].is_monotonic_increasing
    assert frame["quarter"].is_unique
    proxy_unit_audit = json.loads(result.proxy_unit_audit_path.read_text(encoding="utf-8"))
    bank_credit_source = next(item for item in proxy_unit_audit["source_series"] if item["series_key"] == "bank_credit_level")
    assert bank_credit_source["scale_divisor"] == 1.0
    tga_source = next(item for item in proxy_unit_audit["source_series"] if item["series_key"] == "tga_level")
    assert tga_source["scale_divisor"] == 1000.0
    sample_summary = json.loads(result.sample_construction_summary_path.read_text(encoding="utf-8"))
    assert sample_summary["full_panel"]["rows"] == 5
    assert sample_summary["headline_sample"]["rows"] == len(frame)
    assert sample_summary["headline_sample"]["start_quarter"] == "2000Q3"
    assert all(item["column"] != "quarter" for item in sample_summary["headline_sample_truncation"]["columns"])
    extended_bank_credit = next(item for item in sample_summary["extended_column_coverage"] if item["column"] == "bank_credit_private_qoq")
    assert extended_bank_credit["headline_sample_missing_obs"] > 0


def test_build_public_quarterly_panel_preserves_macro_history_when_tga_starts_later(tmp_path: Path, monkeypatch) -> None:
    z1_zip = tmp_path / "fixtures" / "z1.zip"
    _write_z1_zip(z1_zip)

    fred_data = {
        "WTREGEN": [("2002-03-31", 1000.0), ("2002-06-30", 1020.0), ("2002-09-30", 980.0)],
        "WRESBAL": [("2002-03-31", 2000.0), ("2002-06-30", 2010.0), ("2002-09-30", 1980.0)],
        "TOTBKCR": [("2002-03-31", 3000.0), ("2002-06-30", 3030.0), ("2002-09-30", 3060.0)],
        "TNMACBW027SBOG": [("2002-03-31", 420.0), ("2002-06-30", 435.0), ("2002-09-30", 440.0)],
        "FEDFUNDS": [("2000-01-31", 5.5), ("2000-02-29", 5.6), ("2000-03-31", 5.7), ("2000-04-30", 5.8), ("2000-05-31", 5.9), ("2000-06-30", 6.0), ("2000-07-31", 6.1), ("2000-08-31", 6.0), ("2000-09-30", 5.9), ("2000-10-31", 5.8), ("2000-11-30", 5.7), ("2000-12-31", 5.6), ("2001-01-31", 5.5), ("2001-02-28", 5.4), ("2001-03-31", 5.3)],
        "UNRATE": [("2000-01-31", 4.0), ("2000-02-29", 4.1), ("2000-03-31", 4.0), ("2000-04-30", 4.0), ("2000-05-31", 4.1), ("2000-06-30", 4.0), ("2000-07-31", 4.0), ("2000-08-31", 4.1), ("2000-09-30", 4.0), ("2000-10-31", 4.1), ("2000-11-30", 4.2), ("2000-12-31", 4.2), ("2001-01-31", 4.3), ("2001-02-28", 4.3), ("2001-03-31", 4.3)],
        "CPIAUCSL": [("2000-01-31", 168.8), ("2000-02-29", 169.8), ("2000-03-31", 171.2), ("2000-04-30", 171.3), ("2000-05-31", 171.5), ("2000-06-30", 172.4), ("2000-07-31", 172.8), ("2000-08-31", 172.8), ("2000-09-30", 173.7), ("2000-10-31", 174.0), ("2000-11-30", 174.1), ("2000-12-31", 174.0), ("2001-01-31", 175.1), ("2001-02-28", 175.8), ("2001-03-31", 176.2)],
    }
    auctions_rows = [
        ("2000-01-15", "Bill", 60_000_000_000.0),
        ("2000-02-15", "Note", 40_000_000_000.0),
        ("2000-04-15", "Bill", 70_000_000_000.0),
        ("2000-05-15", "Bond", 30_000_000_000.0),
        ("2000-07-15", "Bill", 50_000_000_000.0),
        ("2000-08-15", "Note", 50_000_000_000.0),
    ]

    def fake_download_current_z1_zip(raw_dir: Path, manifest_path: Path, *, timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "z1" / "z1_csv_files.zip"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(z1_zip.read_bytes())
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps({"runs": []}) + "\n", encoding="utf-8")
        return target

    def fake_download_fred_csv(series_id: str, raw_dir: Path, manifest_path: Path, *, timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "fred" / f"{series_id}.csv"
        _write_fred_csv(target, header=series_id, rows=fred_data[series_id])
        return target

    def fake_download_fiscaldata_auctions_csv(raw_dir: Path, manifest_path: Path, *, start_date: str = "2000-01-01", timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        target = raw_dir / "fiscaldata" / "auctions_query.csv"
        _write_auctions_csv(target, auctions_rows)
        return target

    monkeypatch.setattr(build_panel, "_download_current_z1_zip", fake_download_current_z1_zip)
    monkeypatch.setattr(build_panel, "_download_fred_csv", fake_download_fred_csv)
    monkeypatch.setattr(build_panel, "_download_fiscaldata_auctions_csv", fake_download_fiscaldata_auctions_csv)
    monkeypatch.setattr(build_panel, "build_cache_reuse_provenance", lambda reuse_mode=None: {"reuse_mode": "discover", "reused_artifacts": [], "fresh_downloads": []})
    monkeypatch.setattr(build_panel, "_load_canonical_tdc_series", lambda **kwargs: _fake_canonical_tdc_frame())

    result = build_panel.build_public_quarterly_panel(base_dir=tmp_path)
    sample_summary = json.loads(result.sample_construction_summary_path.read_text(encoding="utf-8"))

    fedfunds_summary = next(item for item in sample_summary["headline_sample_truncation"]["columns"] if item["column"] == "fedfunds")
    lag_fedfunds_summary = next(item for item in sample_summary["headline_sample_truncation"]["columns"] if item["column"] == "lag_fedfunds")
    assert fedfunds_summary["first_available_quarter"] == "2000Q1"
    assert lag_fedfunds_summary["first_available_quarter"] == "2000Q2"


def test_reused_tdc_series_accepts_legacy_alias(tmp_path: Path) -> None:
    reused_path = tmp_path / "reused.csv"
    reused_path.write_text("quarter,tdc_qoq\n2000Q1,1.0\n", encoding="utf-8")
    payload = {"reused_artifacts": [{"materialized_path": str(reused_path)}]}
    frame = build_panel._load_reused_tdc_series(payload)
    assert frame is not None
    assert list(frame.columns) == ["quarter", "tdc_bank_only_qoq"]


def test_load_canonical_tdc_series_csv_accepts_tdcest_export(tmp_path: Path) -> None:
    path = tmp_path / "tdc_estimates.csv"
    path.write_text(
        (
            "date,tdc_base_bank_only_ru_flow,tdc_base_broad_depository_np_cu_ru_flow\n"
            "2000-03-31,1000.0,2000.0\n"
            "2000-06-30,3000.0,4000.0\n"
        ),
        encoding="utf-8",
    )
    frame = build_panel._load_canonical_tdc_series_csv(path)
    assert frame is not None
    assert frame["quarter"].tolist() == ["2000Q1", "2000Q2"]
    assert frame["tdc_bank_only_qoq"].tolist() == [1.0, 3.0]
    assert frame["tdc_broad_depository_qoq"].tolist() == [2.0, 4.0]


def test_build_public_quarterly_panel_from_offline_raw_fixture(tmp_path: Path, monkeypatch) -> None:
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "offline_raw_fixture"
    monkeypatch.setattr(build_panel, "build_cache_reuse_provenance", lambda reuse_mode=None: {"reuse_mode": "rebuild", "reused_artifacts": [], "fresh_downloads": []})

    result = build_panel.build_public_quarterly_panel(base_dir=tmp_path, fixture_root=fixture_root, reuse_mode="rebuild")

    assert result.panel_path.exists()
    manifest = json.loads(result.raw_download_manifest_path.read_text(encoding="utf-8"))
    assert len(manifest["runs"]) == 9
    assert all(entry["params"]["mode"] == "raw_fixture" for entry in manifest["runs"])


def test_read_z1_levels_normalizes_live_multitable_zip_layout(tmp_path: Path) -> None:
    z1_zip = tmp_path / "fixtures" / "z1_multitable.zip"
    _write_z1_multitable_zip(z1_zip)

    frame = build_panel._read_z1_levels(z1_zip, build_panel.Z1_SERIES)

    assert "quarter" in frame.columns
    assert "date" not in frame.columns
    assert frame["quarter"].tolist() == ["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"]
    assert frame["domestic_nonfinancial_mmf_level"].notna().all()
    assert frame["domestic_nonfinancial_repo_level"].notna().all()
