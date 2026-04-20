from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd
import requests

from tdcpass.pipeline import build_panel


def _write_z1_zip(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "date": ["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"],
            "FL763127005": [60.0, 66.0, 72.0, 75.0, 79.0],
            "FL753127005": [15.0, 16.0, 18.0, 19.0, 20.0],
            "FL743127003": [4.0, 4.5, 5.0, 5.5, 6.0],
            "FL764110005": [15.0, 16.0, 18.0, 17.0, 19.0],
            "FL763130005": [25.0, 28.0, 31.0, 35.0, 35.0],
            "FL753130005": [9.0, 10.0, 11.0, 12.0, 13.0],
            "FL743130003": [2.0, 2.5, 3.0, 3.5, 4.0],
            "FL763123005": [10.0, 12.0, 16.0, 18.0, 21.0],
            "FL763128000": [5.0, 6.0, 7.0, 8.0, 9.0],
            "FL763122605": [12.0, 13.0, 14.0, 15.0, 15.0],
            "FL763129205": [33.0, 35.0, 35.0, 34.0, 34.0],
            "FL764116005": [100.0, 110.0, 150.0, 145.0, 155.0],
            "FL764016005": [80.0, 82.0, 90.0, 89.0, 93.0],
            "FL764016205": [10.0, 11.0, 14.0, 13.0, 15.0],
            "LM763061100": [700.0, 710.0, 730.0, 740.0, 760.0],
            "LM763061705": [2400.0, 2410.0, 2430.0, 2460.0, 2480.0],
            "LM763062005": [120.0, 122.0, 123.0, 125.0, 126.0],
            "LM763063005": [300.0, 303.0, 306.0, 310.0, 315.0],
            "FL762150005": [50.0, 52.0, 55.0, 54.0, 56.0],
            "FL764122005": [90.0, 93.0, 99.0, 101.0, 104.0],
            "FL763169305": [20.0, 22.0, 27.0, 28.0, 30.0],
            "FL763194735": [10.0, 11.0, 13.0, 12.0, 14.0],
            "LM153061105": [500.0, 520.0, 560.0, 570.0, 590.0],
            "FL633061110": [80.0, 85.0, 95.0, 94.0, 93.0],
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
            "FL763127005": [60.0, 66.0, 72.0, 75.0, 79.0],
            "FL753127005": [15.0, 16.0, 18.0, 19.0, 20.0],
            "FL743127003": [4.0, 4.5, 5.0, 5.5, 6.0],
            "FL764110005": [15.0, 16.0, 18.0, 17.0, 19.0],
            "FL763130005": [25.0, 28.0, 31.0, 35.0, 35.0],
            "FL753130005": [9.0, 10.0, 11.0, 12.0, 13.0],
            "FL743130003": [2.0, 2.5, 3.0, 3.5, 4.0],
            "FL763123005": [10.0, 12.0, 16.0, 18.0, 21.0],
            "FL763128000": [5.0, 6.0, 7.0, 8.0, 9.0],
            "FL763122605": [12.0, 13.0, 14.0, 15.0, 15.0],
            "FL763129205": [33.0, 35.0, 35.0, 34.0, 34.0],
            "FL764116005": [100.0, 110.0, 150.0, 145.0, 155.0],
            "FL764016005": [80.0, 82.0, 90.0, 89.0, 93.0],
            "FL764016205": [10.0, 11.0, 14.0, 13.0, 15.0],
            "LM763061100": [700.0, 710.0, 730.0, 740.0, 760.0],
            "LM763061705": [2400.0, 2410.0, 2430.0, 2460.0, 2480.0],
            "LM763062005": [120.0, 122.0, 123.0, 125.0, 126.0],
            "LM763063005": [300.0, 303.0, 306.0, 310.0, 315.0],
            "FL762150005": [50.0, 52.0, 55.0, 54.0, 56.0],
            "FL764122005": [90.0, 93.0, 99.0, 101.0, 104.0],
            "FL763169305": [20.0, 22.0, 27.0, 28.0, 30.0],
            "FL763194735": [10.0, 11.0, 13.0, 12.0, 14.0],
            "LM153061105": [500.0, 520.0, 560.0, 570.0, 590.0],
            "FL633061110": [80.0, 85.0, 95.0, 94.0, 93.0],
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
            all_sectors[
                [
                    "date",
                    "FL763127005",
                    "FL753127005",
                    "FL743127003",
                    "FL763123005",
                    "FL763128000",
                    "FL763122605",
                    "FL763129205",
                ]
            ].to_csv(index=False),
        )
        archive.writestr(
            "csv/l202.csv",
            all_sectors[
                [
                    "date",
                    "FL764110005",
                    "FL764116005",
                    "FL764016005",
                    "FL764016205",
                ]
            ].to_csv(index=False),
        )
        archive.writestr(
            "csv/l204.csv",
            all_sectors[["date", "FL763130005", "FL753130005", "FL743130003"]].to_csv(index=False),
        )
        archive.writestr("csv/l207.csv", all_sectors[["date", "FL762150005"]].to_csv(index=False))
        archive.writestr(
            "csv/l111.csv",
            all_sectors[
                [
                    "date",
                    "LM763061100",
                    "LM763061705",
                    "LM763062005",
                    "LM763063005",
                    "FL764122005",
                    "FL763169305",
                    "FL763194735",
                ]
            ].to_csv(index=False),
        )
        archive.writestr(
            "csv/l210.csv",
            all_sectors[
                [
                    "date",
                    "LM153061105",
                    "FL633061110",
                ]
            ].to_csv(index=False),
        )
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
            "tdc_domestic_bank_only_qoq": [None, 1.5, 3.5, 1.5, 2.5],
            "tdc_no_remit_bank_only_qoq": [None, 1.8, 3.8, 1.8, 2.8],
            "tdc_credit_union_sensitive_qoq": [None, 2.2, 4.2, 2.2, 3.2],
            "tdc_tier2_bank_only_qoq": [None, 1.2, 3.2, 1.2, 2.2],
            "tdc_tier3_bank_only_qoq": [None, 1.0, 3.0, 1.0, 2.0],
            "tdc_tier3_broad_depository_qoq": [None, 1.1, 3.1, 1.1, 2.1],
            "tdc_bank_receipt_historical_overlay_qoq": [None, None, 0.4, 0.5, 0.6],
            "tdc_row_mrv_nondefault_pilot_qoq": [None, 0.0, 0.2, 0.0, 0.3],
        }
    )


def _fake_canonical_tdc_result() -> build_panel.CanonicalTdcSeriesResult:
    return build_panel.CanonicalTdcSeriesResult(
        frame=_fake_canonical_tdc_frame(),
        source_path=None,
        source_kind="test_fixture",
    )


def _fake_imported_accounting_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "quarter": ["2000Q3", "2000Q4", "2001Q1"],
            "accounting_deposit_substitution_qoq": [-1.0, -2.0, -3.0],
            "accounting_bank_balance_sheet_qoq": [2.0, 3.0, 4.0],
            "accounting_public_liquidity_qoq": [0.5, 0.25, -0.5],
            "accounting_external_flow_qoq": [1.5, -0.25, 0.5],
        }
    )


def _fake_imported_accounting_result() -> build_panel.ImportedAccountingSeriesResult:
    return build_panel.ImportedAccountingSeriesResult(
        frame=_fake_imported_accounting_frame(),
        source_kind="fixture_accounting_bundle",
    )


def test_regime_aware_historical_cash_backfill_only_changes_pre_transaction_history() -> None:
    frame = pd.DataFrame(
        {
            "quarter": ["2002Q3", "2002Q4"],
            "tdc_bank_only_qoq": [pd.NA, 5.0],
            "tdc_bank_only_extended_1990_qoq": [4.0, 6.0],
            "tdc_broad_depository_qoq": [pd.NA, 7.0],
            "tdc_broad_depository_extended_1990_qoq": [8.0, 9.0],
            "tdc_treasury_operating_cash_qoq": [3.0, 10.0],
            "federal_govt_cash_balance_proxy_qoq": [11.0, 1.0],
        }
    )

    out = build_panel._apply_regime_aware_historical_cash_backfill(frame)

    assert abs(float(out.loc[0, "tdc_bank_only_regime_aware_extended_1990_qoq"]) - (-4.0)) < 1e-12
    assert abs(float(out.loc[0, "tdc_bank_only_qoq"]) - (-4.0)) < 1e-12
    assert abs(float(out.loc[1, "tdc_bank_only_qoq"]) - 5.0) < 1e-12
    assert abs(float(out.loc[0, "tdc_broad_depository_regime_aware_extended_1990_qoq"]) - 0.0) < 1e-12
    assert abs(float(out.loc[0, "tdc_broad_depository_qoq"]) - 0.0) < 1e-12
    assert abs(float(out.loc[1, "tdc_broad_depository_qoq"]) - 7.0) < 1e-12


def _strict_series(values: list[float]) -> pd.Series:
    return pd.Series(values, index=pd.Index(["2000Q1", "2000Q2", "2000Q3", "2000Q4", "2001Q1"], dtype="object"))


def _fake_strict_transaction_series() -> dict[str, tuple[pd.Series, str]]:
    return {
        "strict_loan_source_qoq": (_strict_series([5.0, 6.0, 7.0, 8.0, 9.0]), "BOGZ1FU764023005Q"),
        "strict_loan_mortgages_qoq": (_strict_series([1.0, 1.5, 2.0, 2.5, 3.0]), "BOGZ1FU763065005Q"),
        "strict_loan_consumer_credit_qoq": (_strict_series([0.5, 0.75, 1.0, 1.25, 1.5]), "BOGZ1FU763066000Q"),
        "strict_loan_di_loans_nec_qoq": (_strict_series([3.0, 3.25, 3.5, 3.75, 4.0]), "BOGZ1FU763068005Q"),
        "strict_di_loans_nec_systemwide_liability_total_qoq": (_strict_series([3.0, 3.25, 3.5, 3.75, 4.0]), "BOGZ1FU793068005Q"),
        "strict_di_loans_nec_households_nonprofits_qoq": (_strict_series([0.4, 0.45, 0.5, 0.55, 0.6]), "BOGZ1FU153168005Q"),
        "strict_di_loans_nec_nonfinancial_corporate_qoq": (_strict_series([0.8, 0.9, 1.0, 1.1, 1.2]), "BOGZ1FU103168005Q"),
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq": (_strict_series([0.6, 0.65, 0.7, 0.75, 0.8]), "BOGZ1FU113168005Q"),
        "strict_di_loans_nec_state_local_qoq": (_strict_series([0.3, 0.35, 0.4, 0.45, 0.5]), "BOGZ1FU213168003Q"),
        "strict_di_loans_nec_domestic_financial_qoq": (_strict_series([0.7, 0.75, 0.8, 0.85, 0.9]), "BOGZ1FU793168005Q"),
        "strict_di_loans_nec_rest_of_world_qoq": (_strict_series([0.2, 0.15, 0.1, 0.05, 0.0]), "BOGZ1FU263168005Q"),
        "strict_loan_other_advances_qoq": (_strict_series([0.5, 0.5, 0.5, 0.5, 0.5]), "BOGZ1FU763069005Q"),
        "strict_non_treasury_agency_gse_qoq": (_strict_series([0.2, 0.3, 0.4, 0.5, 0.6]), "BOGZ1FU763061705Q"),
        "strict_non_treasury_municipal_qoq": (_strict_series([0.1, 0.1, 0.2, 0.2, 0.3]), "BOGZ1FU763062005Q"),
        "strict_non_treasury_corporate_foreign_bonds_qoq": (_strict_series([0.4, 0.5, 0.6, 0.7, 0.8]), "BOGZ1FU763063005Q"),
        "strict_funding_fedfunds_repo_qoq": (_strict_series([0.3, 0.35, 0.4, 0.45, 0.5]), "BOGZ1FU762150005Q"),
        "strict_funding_debt_securities_qoq": (_strict_series([0.2, 0.25, 0.3, 0.35, 0.4]), "BOGZ1FU764122005Q"),
        "strict_funding_fhlb_advances_qoq": (_strict_series([0.1, 0.15, 0.2, 0.25, 0.3]), "BOGZ1FU763169305Q"),
        "broad_strict_loan_foreign_offices_qoq": (_strict_series([0.25, 0.3, 0.35, 0.4, 0.45]), "BOGZ1FU754023005Q"),
        "broad_strict_loan_affiliated_areas_qoq": (_strict_series([0.05, 0.06, 0.07, 0.08, 0.09]), "BOGZ1FU744023003Q"),
    }


def _write_accounting_bundle_csv(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "series_id": [
                "accounting_deposit_substitution_qoq",
                "accounting_bank_balance_sheet_qoq",
                "accounting_public_liquidity_qoq",
                "accounting_external_flow_qoq",
                "accounting_deposit_substitution_qoq",
                "accounting_bank_balance_sheet_qoq",
                "accounting_public_liquidity_qoq",
                "accounting_external_flow_qoq",
            ],
            "period_end": [
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-12-31",
                "2000-12-31",
                "2000-12-31",
                "2000-12-31",
            ],
            "available_at": [
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-12-31",
                "2000-12-31",
                "2000-12-31",
                "2000-12-31",
            ],
            "units": [
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
            ],
            "value": [-1.0, 2.0, 0.5, 1.5, -2.0, 3.0, 0.25, -0.25],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_large_accounting_bundle_csv(path: Path) -> None:
    frame = pd.DataFrame(
        {
            "series_id": [
                "accounting_deposit_substitution_qoq",
                "accounting_bank_balance_sheet_qoq",
                "accounting_public_liquidity_qoq",
                "accounting_external_flow_qoq",
            ],
            "period_end": [
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
            ],
            "available_at": [
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
                "2000-09-30",
            ],
            "units": [
                "usd_billions",
                "usd_billions",
                "usd_billions",
                "usd_billions",
            ],
            "value": [-10000.0, 20000.0, 5000.0, 15000.0],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def test_build_public_quarterly_panel_writes_contract_columns(tmp_path: Path, monkeypatch) -> None:
    z1_zip = tmp_path / "fixtures" / "z1.zip"
    _write_z1_zip(z1_zip)

    fred_data = {
        "BOGZ1FU713061103Q": [("2000-03-31", 1000.0), ("2000-06-30", 1200.0), ("2000-09-30", 1500.0), ("2000-12-31", 1600.0), ("2001-03-31", 1700.0)],
        "BOGZ1FU763061100Q": [("2000-03-31", 2000.0), ("2000-06-30", 2100.0), ("2000-09-30", 2200.0), ("2000-12-31", 2300.0), ("2001-03-31", 2400.0)],
        "BOGZ1FU753061103Q": [("2000-03-31", 400.0), ("2000-06-30", 450.0), ("2000-09-30", 500.0), ("2000-12-31", 550.0), ("2001-03-31", 600.0)],
        "BOGZ1FU743061103Q": [("2000-03-31", 100.0), ("2000-06-30", 120.0), ("2000-09-30", 140.0), ("2000-12-31", 160.0), ("2001-03-31", 180.0)],
        "BOGZ1FU263061105Q": [("2000-03-31", 900.0), ("2000-06-30", 950.0), ("2000-09-30", 1000.0), ("2000-12-31", 1050.0), ("2001-03-31", 1100.0)],
        "BOGZ1FU313024000Q": [("2000-03-31", 500.0), ("2000-06-30", 600.0), ("2000-09-30", 700.0), ("2000-12-31", 650.0), ("2001-03-31", 750.0)],
        "WTREGEN": [("2000-03-31", 1000.0), ("2000-06-30", 1020.0), ("2000-09-30", 980.0), ("2000-12-31", 990.0), ("2001-03-31", 1010.0)],
        "WRESBAL": [("2000-03-31", 2000.0), ("2000-06-30", 2010.0), ("2000-09-30", 1980.0), ("2000-12-31", 1995.0), ("2001-03-31", 2025.0)],
        "RRPONTSYD": [("2000-03-31", 1000.0), ("2000-06-30", 900.0), ("2000-09-30", 850.0), ("2000-12-31", 820.0), ("2001-03-31", 800.0)],
        "CURRNS": [("2000-03-31", 500.0), ("2000-06-30", 520.0), ("2000-09-30", 510.0), ("2000-12-31", 495.0), ("2001-03-31", 490.0)],
        "H8B3094NCBA": [("2000-03-31", 1500.0), ("2000-06-30", 1600.0), ("2000-09-30", 1900.0), ("2000-12-31", 1800.0), ("2001-03-31", 1950.0)],
        "BORROW": [("2000-03-31", 500.0), ("2000-06-30", 550.0), ("2000-09-30", 650.0), ("2000-12-31", 600.0), ("2001-03-31", 700.0)],
        "TOTBKCR": [("2000-03-31", 3000.0), ("2000-06-30", 3030.0), ("2000-09-30", 3060.0), ("2000-12-31", 3090.0), ("2001-03-31", 3125.0)],
        "TNMACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 420.0), ("2000-12-31", 435.0), ("2001-03-31", 440.0)],
        "TOTCINSA": [("2000-03-31", 200.0), ("2000-06-30", 205.0), ("2000-09-30", 210.0), ("2000-12-31", 215.0), ("2001-03-31", 220.0)],
        "CLDACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 30.0), ("2000-12-31", 32.0), ("2001-03-31", 34.0)],
        "SMPACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 40.0), ("2000-12-31", 41.0), ("2001-03-31", 42.0)],
        "SNFACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 90.0), ("2000-12-31", 95.0), ("2001-03-31", 98.0)],
        "CLSACBW027SBOG": [("2000-03-31", 80.0), ("2000-06-30", 82.0), ("2000-09-30", 84.0), ("2000-12-31", 86.0), ("2001-03-31", 88.0)],
        "CCLACBW027SBOG": [("2000-03-31", 20.0), ("2000-06-30", 21.0), ("2000-09-30", 22.0), ("2000-12-31", 23.0), ("2001-03-31", 24.0)],
        "CARACBW027SBOG": [("2000-03-31", "."), ("2000-06-30", "."), ("2000-09-30", 10.0), ("2000-12-31", 11.0), ("2001-03-31", 12.0)],
        "OCLACBW027SBOG": [("2000-03-31", 30.0), ("2000-06-30", 31.0), ("2000-09-30", 32.0), ("2000-12-31", 33.0), ("2001-03-31", 34.0)],
        "RHEACBW027SBOG": [("2000-03-31", 50.0), ("2000-06-30", 49.0), ("2000-09-30", 48.0), ("2000-12-31", 47.0), ("2001-03-31", 46.0)],
        "CRLACBW027SBOG": [("2000-03-31", 60.0), ("2000-06-30", 61.0), ("2000-09-30", 62.0), ("2000-12-31", 63.0), ("2001-03-31", 64.0)],
        "LCBACBW027SBOG": [("2000-03-31", 12.0), ("2000-06-30", 13.0), ("2000-09-30", 14.0), ("2000-12-31", 15.0), ("2001-03-31", 16.0)],
        "LNFACBW027SBOG": [("2000-03-31", 40.0), ("2000-06-30", 42.0), ("2000-09-30", 44.0), ("2000-12-31", 43.0), ("2001-03-31", 45.0)],
        "BOGZ1FL763067003Q": [("2000-03-31", 4000.0), ("2000-06-30", 4500.0), ("2000-09-30", 5000.0), ("2000-12-31", 5500.0), ("2001-03-31", 6000.0)],
        "CORBLACBS": [("2000-03-31", 2.0), ("2000-06-30", 2.0), ("2000-09-30", 2.0), ("2000-12-31", 2.0), ("2001-03-31", 2.0)],
        "CORCACBS": [("2000-03-31", 4.0), ("2000-06-30", 4.0), ("2000-09-30", 4.0), ("2000-12-31", 4.0), ("2001-03-31", 4.0)],
        "CORCCACBS": [("2000-03-31", 8.0), ("2000-06-30", 8.0), ("2000-09-30", 8.0), ("2000-12-31", 8.0), ("2001-03-31", 8.0)],
        "COROCLACBS": [("2000-03-31", 3.0), ("2000-06-30", 3.0), ("2000-09-30", 3.0), ("2000-12-31", 3.0), ("2001-03-31", 3.0)],
        "CORSFRMACBS": [("2000-03-31", 1.0), ("2000-06-30", 1.0), ("2000-09-30", 1.0), ("2000-12-31", 1.0), ("2001-03-31", 1.0)],
        "FEDFUNDS": [("2000-01-31", 5.5), ("2000-02-29", 5.6), ("2000-03-31", 5.7), ("2000-04-30", 5.8), ("2000-05-31", 5.9), ("2000-06-30", 6.0), ("2000-07-31", 6.1), ("2000-08-31", 6.0), ("2000-09-30", 5.9), ("2000-10-31", 5.8), ("2000-11-30", 5.7), ("2000-12-31", 5.6), ("2001-01-31", 5.5), ("2001-02-28", 5.4), ("2001-03-31", 5.3)],
        "UNRATE": [("2000-01-31", 4.0), ("2000-02-29", 4.1), ("2000-03-31", 4.0), ("2000-04-30", 4.0), ("2000-05-31", 4.1), ("2000-06-30", 4.0), ("2000-07-31", 4.0), ("2000-08-31", 4.1), ("2000-09-30", 4.0), ("2000-10-31", 4.1), ("2000-11-30", 4.2), ("2000-12-31", 4.2), ("2001-01-31", 4.3), ("2001-02-28", 4.3), ("2001-03-31", 4.3)],
        "CPIAUCSL": [("2000-01-31", 168.8), ("2000-02-29", 169.8), ("2000-03-31", 171.2), ("2000-04-30", 171.3), ("2000-05-31", 171.5), ("2000-06-30", 172.4), ("2000-07-31", 172.8), ("2000-08-31", 172.8), ("2000-09-30", 173.7), ("2000-10-31", 174.0), ("2000-11-30", 174.1), ("2000-12-31", 174.0), ("2001-01-31", 175.1), ("2001-02-28", 175.8), ("2001-03-31", 176.2)],
        "RESPPLLOPNWW": [("2000-01-05", -10.0), ("2000-02-02", 20.0), ("2000-03-01", 30.0), ("2000-04-05", 40.0), ("2000-05-03", -20.0), ("2000-06-07", 10.0), ("2000-07-05", 5.0), ("2000-08-02", 15.0), ("2000-09-06", 25.0), ("2000-10-04", 30.0), ("2000-11-01", 20.0), ("2000-12-06", 10.0), ("2001-01-03", 50.0), ("2001-02-07", 40.0), ("2001-03-07", 30.0)],
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
    monkeypatch.setattr(build_panel, "_load_canonical_tdc_series", lambda **kwargs: _fake_canonical_tdc_result())
    monkeypatch.setattr(build_panel, "_load_imported_accounting_series", lambda **kwargs: _fake_imported_accounting_result())
    strict_series = _fake_strict_transaction_series()

    def fake_load_quarterly_candidate_fred_series(candidate_ids, **kwargs):
        candidate_key = tuple(candidate_ids)
        for key, ids in build_panel.STRICT_Z1_TRANSACTION_SERIES.items():
            if candidate_key == tuple(ids):
                return strict_series[key]
        series_id = candidate_ids[0]
        source = pd.Series(
            [value for _, value in fred_data[series_id]],
            index=pd.Index(
                [pd.Period(date, freq="Q").strftime("%YQ%q") for date, _ in fred_data[series_id]],
                dtype="object",
            ),
            dtype="float64",
        )
        return source / build_panel.STRICT_Z1_TRANSACTION_DIVISOR, series_id

    monkeypatch.setattr(
        build_panel,
        "_load_quarterly_candidate_fred_series",
        fake_load_quarterly_candidate_fred_series,
    )

    result = build_panel.build_public_quarterly_panel(base_dir=tmp_path)
    frame = pd.read_csv(result.panel_path)

    assert result.panel_path.exists()
    assert result.raw_download_manifest_path.exists()
    assert result.reused_artifacts_path.exists()
    assert result.proxy_unit_audit_path.exists()
    assert result.sample_construction_summary_path.exists()
    assert result.accounting_source_kind == "fixture_accounting_bundle"
    assert result.strict_source_kind == build_panel.STRICT_SOURCE_KIND
    assert set(build_panel._required_panel_columns()).issubset(frame.columns)
    assert frame["quarter"].iloc[0] == "2000Q3"
    assert "tdc_domestic_bank_only_qoq" in frame.columns
    assert "tdc_us_chartered_bank_only_qoq" in frame.columns
    assert "tdc_no_foreign_bank_sectors_qoq" in frame.columns
    assert "tdc_no_toc_bank_only_qoq" in frame.columns
    assert "tdc_no_toc_no_row_bank_only_qoq" in frame.columns
    assert "tdc_core_deposit_proximate_bank_only_qoq" in frame.columns
    assert "tdc_toc_row_support_bundle_qoq" in frame.columns
    assert "deposits_only_bank_qoq" in frame.columns
    assert "broad_bank_deposits_qoq" in frame.columns
    assert "tdc_no_remit_bank_only_qoq" in frame.columns
    assert "tdc_credit_union_sensitive_qoq" in frame.columns
    assert "tdc_tier2_bank_only_qoq" in frame.columns
    assert "tdc_tier3_bank_only_qoq" in frame.columns
    assert "tdc_tier3_broad_depository_qoq" in frame.columns
    assert "tdc_bank_receipt_historical_overlay_qoq" in frame.columns
    assert "tdc_row_mrv_nondefault_pilot_qoq" in frame.columns
    assert frame.loc[frame["quarter"] == "2000Q3", "bank_credit_private_qoq"].isna().all()
    assert (frame["other_component_qoq"] == frame["total_deposits_bank_qoq"] - frame["tdc_bank_only_qoq"]).all()
    assert (
        (
            frame["deposits_only_bank_qoq"]
            - (frame["checkable_deposits_bank_qoq"] + frame["time_savings_deposits_bank_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["other_component_us_chartered_bank_only_qoq"]
            - (frame["total_deposits_bank_qoq"] - frame["tdc_us_chartered_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["broad_bank_deposits_qoq"]
            - (
                frame["checkable_deposits_bank_qoq"]
                + frame["time_savings_deposits_bank_qoq"]
                + frame["checkable_deposits_foreign_offices_qoq"]
                + frame["time_savings_deposits_foreign_offices_qoq"]
                + frame["checkable_deposits_affiliated_areas_qoq"]
                + frame["time_savings_deposits_affiliated_areas_qoq"]
            )
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["other_component_no_foreign_bank_sectors_qoq"]
            - (frame["total_deposits_bank_qoq"] - frame["tdc_no_foreign_bank_sectors_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["other_component_no_toc_bank_only_qoq"]
            - (frame["total_deposits_bank_qoq"] - frame["tdc_no_toc_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["other_component_no_toc_no_row_bank_only_qoq"]
            - (frame["total_deposits_bank_qoq"] - frame["tdc_no_toc_no_row_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["tdc_core_deposit_proximate_bank_only_qoq"] - frame["tdc_no_toc_no_row_bank_only_qoq"]
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["tdc_toc_row_support_bundle_qoq"]
            - (frame["tdc_bank_only_qoq"] - frame["tdc_core_deposit_proximate_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["other_component_core_deposit_proximate_bank_only_qoq"]
            - (frame["total_deposits_bank_qoq"] - frame["tdc_core_deposit_proximate_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert (
        (
            frame["broad_bank_other_component_qoq"]
            - (frame["broad_bank_deposits_qoq"] - frame["tdc_bank_only_qoq"])
        ).abs()
        < 1e-12
    ).all()
    assert frame["bill_share"].between(0.0, 1.0).all()
    assert (frame["reserve_drain_pressure"] == -frame["lag_reserves_qoq"]).all()
    assert frame["quarter_index"].tolist() == list(range(len(frame)))
    assert frame["quarter"].is_monotonic_increasing
    assert frame["quarter"].is_unique
    assert "checkable_deposits_bank_qoq" in frame.columns
    assert "checkable_deposits_foreign_offices_qoq" in frame.columns
    assert "checkable_deposits_affiliated_areas_qoq" in frame.columns
    assert "interbank_transactions_bank_qoq" in frame.columns
    assert "time_savings_deposits_bank_qoq" in frame.columns
    assert "time_savings_deposits_foreign_offices_qoq" in frame.columns
    assert "time_savings_deposits_affiliated_areas_qoq" in frame.columns
    assert "checkable_federal_govt_bank_qoq" in frame.columns
    assert "federal_govt_checkable_total_qoq" in frame.columns
    assert "federal_govt_time_savings_total_qoq" in frame.columns
    assert "federal_govt_cash_balance_proxy_qoq" in frame.columns
    assert "checkable_state_local_bank_qoq" in frame.columns
    assert "checkable_rest_of_world_bank_qoq" in frame.columns
    assert "checkable_private_domestic_bank_qoq" in frame.columns
    assert "interbank_transactions_foreign_banks_liability_qoq" in frame.columns
    assert "interbank_transactions_foreign_banks_asset_qoq" in frame.columns
    assert "deposits_at_foreign_banks_asset_qoq" in frame.columns
    assert "treasury_securities_bank_qoq" in frame.columns
    assert "agency_gse_backed_securities_bank_qoq" in frame.columns
    assert "municipal_securities_bank_qoq" in frame.columns
    assert "corporate_foreign_bonds_bank_qoq" in frame.columns
    assert "fedfunds_repo_liabilities_bank_qoq" in frame.columns
    assert "commercial_bank_borrowings_qoq" in frame.columns
    assert "fed_borrowings_depository_institutions_qoq" in frame.columns
    assert "debt_securities_bank_liability_qoq" in frame.columns
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in frame.columns
    assert "holding_company_parent_funding_bank_qoq" in frame.columns
    assert "accounting_deposit_substitution_qoq" in frame.columns
    assert "accounting_bank_balance_sheet_qoq" in frame.columns
    assert "accounting_public_liquidity_qoq" in frame.columns
    assert "accounting_external_flow_qoq" in frame.columns
    assert "accounting_identity_total_qoq" in frame.columns
    assert "accounting_identity_gap_qoq" in frame.columns
    assert "strict_loan_source_qoq" in frame.columns
    assert "strict_loan_mortgages_qoq" in frame.columns
    assert "strict_loan_consumer_credit_qoq" in frame.columns
    assert "strict_loan_di_loans_nec_qoq" in frame.columns
    assert "strict_di_loans_nec_households_nonprofits_qoq" in frame.columns
    assert "strict_di_loans_nec_nonfinancial_corporate_qoq" in frame.columns
    assert "strict_di_loans_nec_nonfinancial_noncorporate_qoq" in frame.columns
    assert "strict_di_loans_nec_state_local_qoq" in frame.columns
    assert "strict_di_loans_nec_domestic_financial_qoq" in frame.columns
    assert "strict_di_loans_nec_rest_of_world_qoq" in frame.columns
    assert "strict_di_loans_nec_systemwide_liability_total_qoq" in frame.columns
    assert "strict_di_loans_nec_systemwide_borrower_total_qoq" in frame.columns
    assert "strict_di_loans_nec_systemwide_borrower_gap_qoq" in frame.columns
    assert "strict_loan_other_advances_qoq" in frame.columns
    assert "strict_non_treasury_agency_gse_qoq" in frame.columns
    assert "strict_non_treasury_municipal_qoq" in frame.columns
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in frame.columns
    assert "strict_non_treasury_securities_qoq" in frame.columns
    assert "strict_identifiable_total_qoq" in frame.columns
    assert "strict_identifiable_gap_qoq" in frame.columns
    assert "strict_funding_fedfunds_repo_qoq" in frame.columns
    assert "strict_funding_debt_securities_qoq" in frame.columns
    assert "strict_funding_fhlb_advances_qoq" in frame.columns
    assert "strict_funding_offset_total_qoq" in frame.columns
    assert "strict_identifiable_net_after_funding_qoq" in frame.columns
    assert "strict_gap_after_funding_qoq" in frame.columns
    assert "broad_strict_loan_foreign_offices_qoq" in frame.columns
    assert "broad_strict_loan_affiliated_areas_qoq" in frame.columns
    assert "broad_strict_loan_source_qoq" in frame.columns
    assert "broad_strict_gap_qoq" in frame.columns
    assert "lag_checkable_deposits_bank_qoq" in frame.columns
    assert "lag_checkable_deposits_foreign_offices_qoq" in frame.columns
    assert "lag_checkable_deposits_affiliated_areas_qoq" in frame.columns
    assert "lag_interbank_transactions_bank_qoq" in frame.columns
    assert "lag_time_savings_deposits_bank_qoq" in frame.columns
    assert "lag_time_savings_deposits_foreign_offices_qoq" in frame.columns
    assert "lag_time_savings_deposits_affiliated_areas_qoq" in frame.columns
    assert "lag_checkable_federal_govt_bank_qoq" in frame.columns
    assert "lag_federal_govt_checkable_total_qoq" in frame.columns
    assert "lag_federal_govt_time_savings_total_qoq" in frame.columns
    assert "lag_federal_govt_cash_balance_proxy_qoq" in frame.columns
    assert "lag_checkable_state_local_bank_qoq" in frame.columns
    assert "lag_checkable_rest_of_world_bank_qoq" in frame.columns
    assert "lag_checkable_private_domestic_bank_qoq" in frame.columns
    assert "lag_interbank_transactions_foreign_banks_liability_qoq" in frame.columns
    assert "lag_interbank_transactions_foreign_banks_asset_qoq" in frame.columns
    assert "lag_deposits_at_foreign_banks_asset_qoq" in frame.columns
    assert "lag_treasury_securities_bank_qoq" in frame.columns
    assert "lag_agency_gse_backed_securities_bank_qoq" in frame.columns
    assert "lag_municipal_securities_bank_qoq" in frame.columns
    assert "lag_corporate_foreign_bonds_bank_qoq" in frame.columns
    assert "lag_fedfunds_repo_liabilities_bank_qoq" in frame.columns
    assert "lag_commercial_bank_borrowings_qoq" in frame.columns
    assert "lag_fed_borrowings_depository_institutions_qoq" in frame.columns
    assert "lag_debt_securities_bank_liability_qoq" in frame.columns
    assert "lag_fhlb_advances_sallie_mae_loans_bank_qoq" in frame.columns
    assert "lag_holding_company_parent_funding_bank_qoq" in frame.columns
    assert "lag_strict_loan_source_qoq" in frame.columns
    assert "lag_strict_loan_mortgages_qoq" in frame.columns
    assert "lag_strict_loan_consumer_credit_qoq" in frame.columns
    assert "lag_strict_loan_di_loans_nec_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_households_nonprofits_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_nonfinancial_corporate_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_nonfinancial_noncorporate_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_state_local_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_domestic_financial_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_rest_of_world_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_systemwide_liability_total_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_systemwide_borrower_total_qoq" in frame.columns
    assert "lag_strict_di_loans_nec_systemwide_borrower_gap_qoq" in frame.columns
    assert "lag_strict_loan_other_advances_qoq" in frame.columns
    assert "lag_strict_non_treasury_agency_gse_qoq" in frame.columns
    assert "lag_strict_non_treasury_municipal_qoq" in frame.columns
    assert "lag_strict_non_treasury_corporate_foreign_bonds_qoq" in frame.columns
    assert "lag_strict_non_treasury_securities_qoq" in frame.columns
    assert "lag_strict_identifiable_total_qoq" in frame.columns
    assert "lag_strict_identifiable_gap_qoq" in frame.columns
    assert "lag_strict_funding_fedfunds_repo_qoq" in frame.columns
    assert "lag_strict_funding_debt_securities_qoq" in frame.columns
    assert "lag_strict_funding_fhlb_advances_qoq" in frame.columns
    assert "lag_strict_funding_offset_total_qoq" in frame.columns
    assert "lag_strict_identifiable_net_after_funding_qoq" in frame.columns
    assert "lag_strict_gap_after_funding_qoq" in frame.columns
    assert "lag_tdc_core_deposit_proximate_bank_only_qoq" in frame.columns
    assert "lag_tdc_toc_row_support_bundle_qoq" in frame.columns
    assert "lag_broad_strict_loan_foreign_offices_qoq" in frame.columns
    assert "lag_broad_strict_loan_affiliated_areas_qoq" in frame.columns
    assert "lag_broad_strict_loan_source_qoq" in frame.columns
    assert "lag_broad_strict_gap_qoq" in frame.columns
    assert "commercial_industrial_loans_qoq" in frame.columns
    assert "construction_land_development_loans_qoq" in frame.columns
    assert "cre_multifamily_loans_qoq" in frame.columns
    assert "cre_nonfarm_nonresidential_loans_qoq" in frame.columns
    assert "consumer_loans_qoq" in frame.columns
    assert "credit_card_revolving_loans_qoq" in frame.columns
    assert "auto_loans_qoq" in frame.columns
    assert "other_consumer_loans_qoq" in frame.columns
    assert "heloc_loans_qoq" in frame.columns
    assert "closed_end_residential_loans_qoq" in frame.columns
    assert "loans_to_commercial_banks_qoq" in frame.columns
    assert "loans_to_nondepository_financial_institutions_qoq" in frame.columns
    assert "loans_for_purchasing_or_carrying_securities_qoq" in frame.columns
    assert "commercial_industrial_loans_ex_chargeoffs_qoq" in frame.columns
    assert "consumer_loans_ex_chargeoffs_qoq" in frame.columns
    assert "credit_card_revolving_loans_ex_chargeoffs_qoq" in frame.columns
    assert "other_consumer_loans_ex_chargeoffs_qoq" in frame.columns
    assert "closed_end_residential_loans_ex_chargeoffs_qoq" in frame.columns
    assert "lag_commercial_industrial_loans_qoq" in frame.columns
    assert "lag_construction_land_development_loans_qoq" in frame.columns
    assert "lag_cre_multifamily_loans_qoq" in frame.columns
    assert "lag_cre_nonfarm_nonresidential_loans_qoq" in frame.columns
    assert "lag_consumer_loans_qoq" in frame.columns
    assert "lag_credit_card_revolving_loans_qoq" in frame.columns
    assert "lag_auto_loans_qoq" in frame.columns
    assert "lag_other_consumer_loans_qoq" in frame.columns
    assert "lag_heloc_loans_qoq" in frame.columns
    assert "lag_closed_end_residential_loans_qoq" in frame.columns
    assert "lag_loans_to_commercial_banks_qoq" in frame.columns
    assert "lag_loans_to_nondepository_financial_institutions_qoq" in frame.columns
    assert "lag_loans_for_purchasing_or_carrying_securities_qoq" in frame.columns
    assert "lag_commercial_industrial_loans_ex_chargeoffs_qoq" in frame.columns
    assert "lag_consumer_loans_ex_chargeoffs_qoq" in frame.columns
    assert "lag_credit_card_revolving_loans_ex_chargeoffs_qoq" in frame.columns
    assert "lag_other_consumer_loans_ex_chargeoffs_qoq" in frame.columns
    assert "lag_closed_end_residential_loans_ex_chargeoffs_qoq" in frame.columns
    assert "on_rrp_reallocation_qoq" in frame.columns
    assert "household_treasury_securities_reallocation_qoq" in frame.columns
    assert "mmf_treasury_bills_reallocation_qoq" in frame.columns
    assert "currency_reallocation_qoq" in frame.columns
    assert "lag_on_rrp_reallocation_qoq" in frame.columns
    assert "lag_household_treasury_securities_reallocation_qoq" in frame.columns
    assert "lag_mmf_treasury_bills_reallocation_qoq" in frame.columns
    assert "lag_currency_reallocation_qoq" in frame.columns
    row_2000q3 = frame.loc[frame["quarter"] == "2000Q3"].iloc[0]
    assert abs(row_2000q3["commercial_industrial_loans_ex_chargeoffs_qoq"] - 6.025) < 1e-12
    assert abs(row_2000q3["consumer_loans_ex_chargeoffs_qoq"] - 2.82) < 1e-12
    assert abs(row_2000q3["credit_card_revolving_loans_ex_chargeoffs_qoq"] - 1.42) < 1e-12
    assert abs(row_2000q3["other_consumer_loans_ex_chargeoffs_qoq"] - 1.2325) < 1e-12
    assert abs(row_2000q3["closed_end_residential_loans_ex_chargeoffs_qoq"] - 1.1525) < 1e-12
    assert abs(row_2000q3["loans_to_commercial_banks_qoq"] - 1.0) < 1e-12
    assert abs(row_2000q3["loans_to_nondepository_financial_institutions_qoq"] - 2.0) < 1e-12
    assert abs(row_2000q3["loans_for_purchasing_or_carrying_securities_qoq"] - 0.5) < 1e-12
    assert abs(row_2000q3["treasury_securities_bank_qoq"] - 0.02) < 1e-12
    assert abs(row_2000q3["agency_gse_backed_securities_bank_qoq"] - 0.02) < 1e-12
    assert abs(row_2000q3["municipal_securities_bank_qoq"] - 0.001) < 1e-12
    assert abs(row_2000q3["corporate_foreign_bonds_bank_qoq"] - 0.003) < 1e-12
    assert abs(row_2000q3["interbank_transactions_foreign_banks_liability_qoq"] - 0.04) < 1e-12
    assert abs(row_2000q3["interbank_transactions_foreign_banks_asset_qoq"] - 0.008) < 1e-12
    assert abs(row_2000q3["deposits_at_foreign_banks_asset_qoq"] - 0.003) < 1e-12
    assert abs(row_2000q3["fedfunds_repo_liabilities_bank_qoq"] - 0.003) < 1e-12
    assert abs(row_2000q3["commercial_bank_borrowings_qoq"] - 0.3) < 1e-12
    assert abs(row_2000q3["fed_borrowings_depository_institutions_qoq"] - 0.1) < 1e-12
    assert abs(row_2000q3["debt_securities_bank_liability_qoq"] - 0.006) < 1e-12
    assert abs(row_2000q3["fhlb_advances_sallie_mae_loans_bank_qoq"] - 0.005) < 1e-12
    assert abs(row_2000q3["holding_company_parent_funding_bank_qoq"] - 0.002) < 1e-12
    assert abs(row_2000q3["accounting_identity_total_qoq"] - 3.0) < 1e-12
    assert abs(row_2000q3["accounting_identity_gap_qoq"] - (row_2000q3["other_component_qoq"] - 3.0)) < 1e-12
    assert abs(row_2000q3["strict_loan_source_qoq"] - 7.0) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_households_nonprofits_qoq"] - 0.5) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_nonfinancial_corporate_qoq"] - 1.0) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_nonfinancial_noncorporate_qoq"] - 0.7) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_state_local_qoq"] - 0.4) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_domestic_financial_qoq"] - 0.8) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_rest_of_world_qoq"] - 0.1) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_systemwide_liability_total_qoq"] - 3.5) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_systemwide_borrower_total_qoq"] - 3.5) < 1e-12
    assert abs(row_2000q3["strict_di_loans_nec_systemwide_borrower_gap_qoq"] - 0.0) < 1e-12
    assert abs(row_2000q3["strict_non_treasury_securities_qoq"] - 1.2) < 1e-12
    assert abs(row_2000q3["strict_identifiable_total_qoq"] - 8.2) < 1e-12
    assert abs(row_2000q3["strict_identifiable_gap_qoq"] - (row_2000q3["other_component_qoq"] - 8.2)) < 1e-12
    assert abs(row_2000q3["strict_funding_fedfunds_repo_qoq"] - 0.4) < 1e-12
    assert abs(row_2000q3["strict_funding_debt_securities_qoq"] - 0.3) < 1e-12
    assert abs(row_2000q3["strict_funding_fhlb_advances_qoq"] - 0.2) < 1e-12
    assert abs(row_2000q3["strict_funding_offset_total_qoq"] - 0.9) < 1e-12
    assert abs(row_2000q3["strict_identifiable_net_after_funding_qoq"] - 7.3) < 1e-12
    assert abs(row_2000q3["strict_gap_after_funding_qoq"] - (row_2000q3["other_component_qoq"] - 7.3)) < 1e-12
    assert abs(row_2000q3["checkable_deposits_foreign_offices_qoq"] - 0.002) < 1e-12
    assert abs(row_2000q3["checkable_deposits_affiliated_areas_qoq"] - 0.0005) < 1e-12
    assert abs(row_2000q3["time_savings_deposits_foreign_offices_qoq"] - 0.001) < 1e-12
    assert abs(row_2000q3["time_savings_deposits_affiliated_areas_qoq"] - 0.0005) < 1e-12
    assert abs(row_2000q3["federal_govt_checkable_total_qoq"] - 0.002) < 1e-12
    assert abs(row_2000q3["federal_govt_time_savings_total_qoq"] - 0.001) < 1e-12
    assert abs(row_2000q3["federal_govt_cash_balance_proxy_qoq"] - 0.003) < 1e-12
    assert abs(row_2000q3["broad_bank_deposits_qoq"] - 0.013) < 1e-12
    assert abs(
        row_2000q3["other_component_no_foreign_bank_sectors_qoq"]
        - (row_2000q3["total_deposits_bank_qoq"] - row_2000q3["tdc_no_foreign_bank_sectors_qoq"])
    ) < 1e-12
    assert abs(
        row_2000q3["other_component_no_toc_bank_only_qoq"]
        - (row_2000q3["total_deposits_bank_qoq"] - row_2000q3["tdc_no_toc_bank_only_qoq"])
    ) < 1e-12
    assert abs(
        row_2000q3["other_component_no_toc_no_row_bank_only_qoq"]
        - (row_2000q3["total_deposits_bank_qoq"] - row_2000q3["tdc_no_toc_no_row_bank_only_qoq"])
    ) < 1e-12
    assert abs(
        row_2000q3["tdc_core_deposit_proximate_bank_only_qoq"] - row_2000q3["tdc_no_toc_no_row_bank_only_qoq"]
    ) < 1e-12
    assert abs(
        row_2000q3["tdc_toc_row_support_bundle_qoq"]
        - (row_2000q3["tdc_bank_only_qoq"] - row_2000q3["tdc_core_deposit_proximate_bank_only_qoq"])
    ) < 1e-12
    assert abs(
        row_2000q3["other_component_core_deposit_proximate_bank_only_qoq"]
        - (row_2000q3["total_deposits_bank_qoq"] - row_2000q3["tdc_core_deposit_proximate_bank_only_qoq"])
    ) < 1e-12
    assert abs(
        row_2000q3["broad_bank_other_component_qoq"]
        - (row_2000q3["broad_bank_deposits_qoq"] - row_2000q3["tdc_bank_only_qoq"])
    ) < 1e-12
    assert abs(row_2000q3["broad_strict_loan_foreign_offices_qoq"] - 0.35) < 1e-12
    assert abs(row_2000q3["broad_strict_loan_affiliated_areas_qoq"] - 0.07) < 1e-12
    assert abs(row_2000q3["broad_strict_loan_source_qoq"] - 7.42) < 1e-12
    assert abs(row_2000q3["broad_strict_gap_qoq"] - (row_2000q3["broad_bank_other_component_qoq"] - 7.42)) < 1e-12
    assert abs(row_2000q3["on_rrp_reallocation_qoq"] - 0.05) < 1e-12
    assert abs(row_2000q3["household_treasury_securities_reallocation_qoq"] + 0.04) < 1e-12
    assert abs(row_2000q3["mmf_treasury_bills_reallocation_qoq"] + 0.01) < 1e-12
    assert abs(row_2000q3["currency_reallocation_qoq"] - 10.0) < 1e-12
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
    construction_coverage = next(item for item in sample_summary["extended_column_coverage"] if item["column"] == "construction_land_development_loans_qoq")
    assert construction_coverage["headline_sample_missing_obs"] > 0


def test_load_imported_accounting_series_from_fixture_bundle(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixtures"
    _write_accounting_bundle_csv(fixture_root / "accounting" / "standardized_series.csv")

    result = build_panel._load_imported_accounting_series(root=tmp_path, fixture_root=fixture_root)

    assert result is not None
    assert result.source_kind == "fixture_accounting_bundle"
    assert result.frame["quarter"].tolist() == ["2000Q3", "2000Q4"]
    row = result.frame.loc[result.frame["quarter"] == "2000Q3"].iloc[0]
    assert abs(row["accounting_deposit_substitution_qoq"] + 1.0) < 1e-12
    assert abs(row["accounting_bank_balance_sheet_qoq"] - 2.0) < 1e-12
    assert abs(row["accounting_public_liquidity_qoq"] - 0.5) < 1e-12
    assert abs(row["accounting_external_flow_qoq"] - 1.5) < 1e-12


def test_load_imported_accounting_series_rescales_large_bundle_values(tmp_path: Path) -> None:
    fixture_root = tmp_path / "fixtures"
    _write_large_accounting_bundle_csv(fixture_root / "accounting" / "standardized_series.csv")

    result = build_panel._load_imported_accounting_series(root=tmp_path, fixture_root=fixture_root)

    assert result is not None
    row = result.frame.loc[result.frame["quarter"] == "2000Q3"].iloc[0]
    assert abs(row["accounting_deposit_substitution_qoq"] + 10.0) < 1e-12
    assert abs(row["accounting_bank_balance_sheet_qoq"] - 20.0) < 1e-12
    assert abs(row["accounting_public_liquidity_qoq"] - 5.0) < 1e-12
    assert abs(row["accounting_external_flow_qoq"] - 15.0) < 1e-12


def test_build_public_quarterly_panel_preserves_macro_history_when_tga_starts_later(tmp_path: Path, monkeypatch) -> None:
    z1_zip = tmp_path / "fixtures" / "z1.zip"
    _write_z1_zip(z1_zip)

    fred_data = {
        "BOGZ1FU713061103Q": [("2002-03-31", 1000.0), ("2002-06-30", 1200.0), ("2002-09-30", 1500.0)],
        "BOGZ1FU763061100Q": [("2002-03-31", 2000.0), ("2002-06-30", 2100.0), ("2002-09-30", 2200.0)],
        "BOGZ1FU753061103Q": [("2002-03-31", 400.0), ("2002-06-30", 450.0), ("2002-09-30", 500.0)],
        "BOGZ1FU743061103Q": [("2002-03-31", 100.0), ("2002-06-30", 120.0), ("2002-09-30", 140.0)],
        "BOGZ1FU263061105Q": [("2002-03-31", 900.0), ("2002-06-30", 950.0), ("2002-09-30", 1000.0)],
        "BOGZ1FU313024000Q": [("2002-03-31", 500.0), ("2002-06-30", 600.0), ("2002-09-30", 700.0)],
        "WTREGEN": [("2002-03-31", 1000.0), ("2002-06-30", 1020.0), ("2002-09-30", 980.0)],
        "WRESBAL": [("2002-03-31", 2000.0), ("2002-06-30", 2010.0), ("2002-09-30", 1980.0)],
        "RRPONTSYD": [("2002-03-31", 1000.0), ("2002-06-30", 950.0), ("2002-09-30", 900.0)],
        "CURRNS": [("2002-03-31", 500.0), ("2002-06-30", 520.0), ("2002-09-30", 510.0)],
        "H8B3094NCBA": [("2002-03-31", 1500.0), ("2002-06-30", 1600.0), ("2002-09-30", 1900.0)],
        "BORROW": [("2002-03-31", 500.0), ("2002-06-30", 550.0), ("2002-09-30", 650.0)],
        "TOTBKCR": [("2002-03-31", 3000.0), ("2002-06-30", 3030.0), ("2002-09-30", 3060.0)],
        "TNMACBW027SBOG": [("2002-03-31", 420.0), ("2002-06-30", 435.0), ("2002-09-30", 440.0)],
        "TOTCINSA": [("2002-03-31", 210.0), ("2002-06-30", 212.0), ("2002-09-30", 214.0)],
        "CLDACBW027SBOG": [("2002-03-31", 30.0), ("2002-06-30", 31.0), ("2002-09-30", 32.0)],
        "SMPACBW027SBOG": [("2002-03-31", 40.0), ("2002-06-30", 41.0), ("2002-09-30", 42.0)],
        "SNFACBW027SBOG": [("2002-03-31", 90.0), ("2002-06-30", 92.0), ("2002-09-30", 94.0)],
        "CLSACBW027SBOG": [("2002-03-31", 80.0), ("2002-06-30", 82.0), ("2002-09-30", 84.0)],
        "CCLACBW027SBOG": [("2002-03-31", 20.0), ("2002-06-30", 21.0), ("2002-09-30", 22.0)],
        "CARACBW027SBOG": [("2002-03-31", 10.0), ("2002-06-30", 11.0), ("2002-09-30", 12.0)],
        "OCLACBW027SBOG": [("2002-03-31", 30.0), ("2002-06-30", 31.0), ("2002-09-30", 32.0)],
        "RHEACBW027SBOG": [("2002-03-31", 50.0), ("2002-06-30", 49.0), ("2002-09-30", 48.0)],
        "CRLACBW027SBOG": [("2002-03-31", 60.0), ("2002-06-30", 61.0), ("2002-09-30", 62.0)],
        "LCBACBW027SBOG": [("2002-03-31", 12.0), ("2002-06-30", 13.0), ("2002-09-30", 14.0)],
        "LNFACBW027SBOG": [("2002-03-31", 40.0), ("2002-06-30", 42.0), ("2002-09-30", 44.0)],
        "BOGZ1FL763067003Q": [("2002-03-31", 4000.0), ("2002-06-30", 4500.0), ("2002-09-30", 5000.0)],
        "CORBLACBS": [("2002-03-31", 2.0), ("2002-06-30", 2.0), ("2002-09-30", 2.0)],
        "CORCACBS": [("2002-03-31", 4.0), ("2002-06-30", 4.0), ("2002-09-30", 4.0)],
        "CORCCACBS": [("2002-03-31", 8.0), ("2002-06-30", 8.0), ("2002-09-30", 8.0)],
        "COROCLACBS": [("2002-03-31", 3.0), ("2002-06-30", 3.0), ("2002-09-30", 3.0)],
        "CORSFRMACBS": [("2002-03-31", 1.0), ("2002-06-30", 1.0), ("2002-09-30", 1.0)],
        "FEDFUNDS": [("2000-01-31", 5.5), ("2000-02-29", 5.6), ("2000-03-31", 5.7), ("2000-04-30", 5.8), ("2000-05-31", 5.9), ("2000-06-30", 6.0), ("2000-07-31", 6.1), ("2000-08-31", 6.0), ("2000-09-30", 5.9), ("2000-10-31", 5.8), ("2000-11-30", 5.7), ("2000-12-31", 5.6), ("2001-01-31", 5.5), ("2001-02-28", 5.4), ("2001-03-31", 5.3)],
        "UNRATE": [("2000-01-31", 4.0), ("2000-02-29", 4.1), ("2000-03-31", 4.0), ("2000-04-30", 4.0), ("2000-05-31", 4.1), ("2000-06-30", 4.0), ("2000-07-31", 4.0), ("2000-08-31", 4.1), ("2000-09-30", 4.0), ("2000-10-31", 4.1), ("2000-11-30", 4.2), ("2000-12-31", 4.2), ("2001-01-31", 4.3), ("2001-02-28", 4.3), ("2001-03-31", 4.3)],
        "CPIAUCSL": [("2000-01-31", 168.8), ("2000-02-29", 169.8), ("2000-03-31", 171.2), ("2000-04-30", 171.3), ("2000-05-31", 171.5), ("2000-06-30", 172.4), ("2000-07-31", 172.8), ("2000-08-31", 172.8), ("2000-09-30", 173.7), ("2000-10-31", 174.0), ("2000-11-30", 174.1), ("2000-12-31", 174.0), ("2001-01-31", 175.1), ("2001-02-28", 175.8), ("2001-03-31", 176.2)],
        "RESPPLLOPNWW": [("2002-01-02", 20.0), ("2002-02-06", 30.0), ("2002-03-06", 40.0), ("2002-04-03", 10.0), ("2002-05-01", 0.0), ("2002-06-05", 20.0), ("2002-07-03", 15.0), ("2002-08-07", 25.0), ("2002-09-04", 35.0)],
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
    monkeypatch.setattr(build_panel, "_load_canonical_tdc_series", lambda **kwargs: _fake_canonical_tdc_result())
    strict_series = _fake_strict_transaction_series()

    def fake_load_quarterly_candidate_fred_series(candidate_ids, **kwargs):
        candidate_key = tuple(candidate_ids)
        for key, ids in build_panel.STRICT_Z1_TRANSACTION_SERIES.items():
            if candidate_key == tuple(ids):
                return strict_series[key]
        series_id = candidate_ids[0]
        source = pd.Series(
            [value for _, value in fred_data[series_id]],
            index=pd.Index(
                [pd.Period(date, freq="Q").strftime("%YQ%q") for date, _ in fred_data[series_id]],
                dtype="object",
            ),
            dtype="float64",
        )
        return source / build_panel.STRICT_Z1_TRANSACTION_DIVISOR, series_id

    monkeypatch.setattr(
        build_panel,
        "_load_quarterly_candidate_fred_series",
        fake_load_quarterly_candidate_fred_series,
    )

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
    result = build_panel._load_reused_tdc_series(payload)
    assert result is not None
    frame = result.frame
    assert result.source_kind == "reused_artifact"
    assert result.source_path == reused_path
    assert frame["quarter"].tolist() == ["2000Q1"]
    assert frame["tdc_bank_only_qoq"].tolist() == [1.0]
    assert "tdc_broad_depository_qoq" in frame.columns
    assert "tdc_domestic_bank_only_qoq" in frame.columns
    assert "tdc_no_remit_bank_only_qoq" in frame.columns
    assert "tdc_credit_union_sensitive_qoq" in frame.columns
    assert "tdc_tier2_bank_only_qoq" in frame.columns
    assert "tdc_tier3_bank_only_qoq" in frame.columns
    assert "tdc_tier3_broad_depository_qoq" in frame.columns
    assert "tdc_bank_receipt_historical_overlay_qoq" in frame.columns
    assert "tdc_row_mrv_nondefault_pilot_qoq" in frame.columns
    assert frame["tdc_broad_depository_qoq"].isna().all()
    assert frame["tdc_domestic_bank_only_qoq"].isna().all()
    assert frame["tdc_no_remit_bank_only_qoq"].isna().all()
    assert frame["tdc_credit_union_sensitive_qoq"].isna().all()
    assert frame["tdc_tier2_bank_only_qoq"].isna().all()
    assert frame["tdc_tier3_bank_only_qoq"].isna().all()
    assert frame["tdc_tier3_broad_depository_qoq"].isna().all()
    assert frame["tdc_bank_receipt_historical_overlay_qoq"].isna().all()
    assert frame["tdc_row_mrv_nondefault_pilot_qoq"].isna().all()


def test_load_canonical_tdc_series_csv_accepts_tdcest_export(tmp_path: Path) -> None:
    path = tmp_path / "tdc_estimates.csv"
    path.write_text(
        (
            "date,tdc_base_bank_only_ru_flow,tdc_base_broad_depository_np_cu_ru_flow,tdc_domestic_bank_only_ru_flow,tdc_no_remit_bank_only,tdc_credit_union_aggregate_sensitivity,tdc_tier2_interest_corrected_bank_only_ru_flow,tdc_tier3_fiscal_corrected_bank_only_ru_flow,tdc_tier3_fiscal_corrected_broad_depository_np_cu_ru_flow\n"
            "2000-03-31,1000.0,2000.0,1100.0,1200.0,1300.0,1400.0,1500.0,1600.0\n"
            "2000-06-30,3000.0,4000.0,3100.0,3200.0,3300.0,3400.0,3500.0,3600.0\n"
        ),
        encoding="utf-8",
    )
    frame = build_panel._load_canonical_tdc_series_csv(path)
    assert frame is not None
    assert frame["quarter"].tolist() == ["2000Q1", "2000Q2"]
    assert frame["tdc_bank_only_qoq"].tolist() == [1.0, 3.0]
    assert frame["tdc_broad_depository_qoq"].tolist() == [2.0, 4.0]
    assert frame["tdc_domestic_bank_only_qoq"].tolist() == [1.1, 3.1]
    assert frame["tdc_no_remit_bank_only_qoq"].tolist() == [1.2, 3.2]
    assert frame["tdc_credit_union_sensitive_qoq"].tolist() == [1.3, 3.3]
    assert frame["tdc_tier2_bank_only_qoq"].tolist() == [1.4, 3.4]
    assert frame["tdc_tier3_bank_only_qoq"].tolist() == [1.5, 3.5]
    assert frame["tdc_tier3_broad_depository_qoq"].tolist() == [1.6, 3.6]


def test_load_canonical_tdc_series_csv_backfills_optional_variants_when_absent(tmp_path: Path) -> None:
    path = tmp_path / "tdc_estimates.csv"
    path.write_text(
        (
            "date,tdc_base_bank_only_ru_flow\n"
            "2000-03-31,1000.0\n"
            "2000-06-30,3000.0\n"
        ),
        encoding="utf-8",
    )
    frame = build_panel._load_canonical_tdc_series_csv(path)
    assert frame is not None
    assert frame["tdc_bank_only_qoq"].tolist() == [1.0, 3.0]
    assert frame["tdc_domestic_bank_only_qoq"].isna().all()
    assert frame["tdc_no_remit_bank_only_qoq"].isna().all()
    assert frame["tdc_credit_union_sensitive_qoq"].isna().all()
    assert frame["tdc_tier2_bank_only_qoq"].isna().all()
    assert frame["tdc_tier3_bank_only_qoq"].isna().all()
    assert frame["tdc_tier3_broad_depository_qoq"].isna().all()


def test_load_tdcest_downstream_series_csv_extracts_overlay_and_mrv(tmp_path: Path) -> None:
    path = tmp_path / "tdc_downstream_deposit_effect_series_panel.csv"
    path.write_text(
        (
            "date,series_key,value_millions\n"
            "2024-12-31,tdc_tier3_bank_only_plus_historical_bank_receipt_candidate,2000.0\n"
            "2025-03-31,row_mrv_primary_nondefault_pilot_series,500.0\n"
        ),
        encoding="utf-8",
    )
    frame = build_panel._load_tdcest_downstream_series_csv(path)
    assert frame is not None
    assert frame["quarter"].tolist() == ["2024Q4", "2025Q1"]
    assert float(frame.loc[frame["quarter"] == "2024Q4", "tdc_bank_receipt_historical_overlay_qoq"].iloc[0]) == 2.0
    assert pd.isna(frame.loc[frame["quarter"] == "2025Q1", "tdc_bank_receipt_historical_overlay_qoq"].iloc[0])
    assert pd.isna(frame.loc[frame["quarter"] == "2024Q4", "tdc_row_mrv_nondefault_pilot_qoq"].iloc[0])
    assert float(frame.loc[frame["quarter"] == "2025Q1", "tdc_row_mrv_nondefault_pilot_qoq"].iloc[0]) == 0.5


def test_merge_tdcest_downstream_overlay_fills_placeholder_columns() -> None:
    frame = pd.DataFrame(
        {
            "quarter": ["2024Q4", "2025Q1"],
            "tdc_bank_only_qoq": [1.0, 2.0],
            "tdc_bank_receipt_historical_overlay_qoq": [pd.NA, pd.NA],
            "tdc_row_mrv_nondefault_pilot_qoq": [pd.NA, pd.NA],
        }
    )
    supplement = pd.DataFrame(
        {
            "quarter": ["2024Q4", "2025Q1"],
            "tdc_bank_receipt_historical_overlay_qoq": [103.07105627851948, pd.NA],
            "tdc_row_mrv_nondefault_pilot_qoq": [0.6210987081222047, 0.6680884373253256],
        }
    )

    out = build_panel._merge_tdcest_downstream_overlay(frame, supplement)

    row_2024_q4 = out.loc[out["quarter"] == "2024Q4"].iloc[0]
    row_2025_q1 = out.loc[out["quarter"] == "2025Q1"].iloc[0]
    assert abs(float(row_2024_q4["tdc_bank_receipt_historical_overlay_qoq"]) - 103.07105627851948) < 1e-12
    assert pd.isna(row_2025_q1["tdc_bank_receipt_historical_overlay_qoq"])
    assert abs(float(row_2024_q4["tdc_row_mrv_nondefault_pilot_qoq"]) - 0.6210987081222047) < 1e-12
    assert abs(float(row_2025_q1["tdc_row_mrv_nondefault_pilot_qoq"]) - 0.6680884373253256) < 1e-12


def test_build_public_quarterly_panel_from_offline_raw_fixture(tmp_path: Path, monkeypatch) -> None:
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "offline_raw_fixture"
    monkeypatch.setattr(build_panel, "build_cache_reuse_provenance", lambda reuse_mode=None: {"reuse_mode": "rebuild", "reused_artifacts": [], "fresh_downloads": []})

    result = build_panel.build_public_quarterly_panel(base_dir=tmp_path, fixture_root=fixture_root, reuse_mode="rebuild")

    assert result.panel_path.exists()
    manifest = json.loads(result.raw_download_manifest_path.read_text(encoding="utf-8"))
    assert len(manifest["runs"]) == 58
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
    assert frame["checkable_deposits_bank_level"].notna().all()
    assert frame["checkable_deposits_foreign_offices_level"].notna().all()
    assert frame["checkable_deposits_affiliated_areas_level"].notna().all()
    assert frame["interbank_transactions_bank_level"].notna().all()
    assert frame["time_savings_deposits_bank_level"].notna().all()
    assert frame["time_savings_deposits_foreign_offices_level"].notna().all()
    assert frame["time_savings_deposits_affiliated_areas_level"].notna().all()
    assert frame["checkable_federal_govt_bank_level"].notna().all()
    assert frame["checkable_state_local_bank_level"].notna().all()
    assert frame["checkable_rest_of_world_bank_level"].notna().all()
    assert frame["checkable_private_domestic_bank_level"].notna().all()
    assert frame["interbank_transactions_foreign_banks_liability_level"].notna().all()
    assert frame["interbank_transactions_foreign_banks_asset_level"].notna().all()
    assert frame["deposits_at_foreign_banks_asset_level"].notna().all()
    assert frame["treasury_securities_bank_level"].notna().all()
    assert frame["agency_gse_backed_securities_bank_level"].notna().all()
    assert frame["municipal_securities_bank_level"].notna().all()
    assert frame["corporate_foreign_bonds_bank_level"].notna().all()
    assert frame["fedfunds_repo_liabilities_bank_level"].notna().all()
    assert frame["debt_securities_bank_liability_level"].notna().all()
    assert frame["fhlb_advances_sallie_mae_loans_bank_level"].notna().all()
    assert frame["holding_company_parent_funding_bank_level"].notna().all()
    assert frame["household_treasury_securities_level"].notna().all()
    assert frame["mmf_treasury_bills_level"].notna().all()


def test_download_fred_csv_falls_back_to_fredgraph_on_api_failure(tmp_path: Path, monkeypatch) -> None:
    raw_dir = tmp_path / "raw"
    manifest_path = raw_dir / "raw_download_manifest.json"
    api_calls: list[str] = []
    download_calls: list[str] = []

    def fake_fetch_fred_observations(series_id: str, *, api_key: str | None = None, out_path: Path | None = None, observation_start: str | None = None) -> pd.DataFrame:
        api_calls.append(series_id)
        raise requests.HTTPError("500 Server Error")

    def fake_download_file(url: str, destination: Path, *, timeout: int = build_panel.DEFAULT_TIMEOUT) -> Path:
        download_calls.append(url)
        _write_fred_csv(
            destination,
            header="TESTSERIES",
            rows=[("2000-03-31", 1.0), ("2000-06-30", 2.0)],
        )
        return destination

    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setattr(build_panel, "fetch_fred_observations", fake_fetch_fred_observations)
    monkeypatch.setattr(build_panel, "download_file", fake_download_file)

    output_path = build_panel._download_fred_csv("TESTSERIES", raw_dir, manifest_path)

    assert output_path.exists()
    assert api_calls == ["TESTSERIES"]
    assert download_calls == [f"{build_panel.FRED_GRAPH_URL}?id=TESTSERIES"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["runs"][-1]["source_key"] == "fred_graph"
    assert manifest["runs"][-1]["params"] == {"id": "TESTSERIES"}
