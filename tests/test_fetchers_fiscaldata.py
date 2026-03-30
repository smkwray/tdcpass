from __future__ import annotations

from pathlib import Path

import pandas as pd

from tdcpass.data.fetchers import fiscaldata


def test_fetch_fiscaldata_endpoint_resolves_relative_next_link(tmp_path: Path, monkeypatch) -> None:
    calls: list[tuple[str, dict[str, str] | None, int]] = []

    def fake_get_json(url: str, params: dict[str, str] | None = None, *, timeout: int = fiscaldata.DEFAULT_TIMEOUT) -> dict[str, object]:
        calls.append((url, params, timeout))
        if len(calls) == 1:
            return {
                "data": [{"record_date": "2024-01-01", "value": "1"}],
                "links": {"next": "&page%5Bnumber%5D=2&page%5Bsize%5D=2"},
            }
        return {
            "data": [{"record_date": "2024-01-02", "value": "2"}],
            "links": {"next": None},
        }

    monkeypatch.setattr(fiscaldata, "get_json", fake_get_json)

    out_path = tmp_path / "fiscaldata.csv"
    frame = fiscaldata.fetch_fiscaldata_endpoint(
        "v1/accounting/od/test_endpoint",
        out_path=out_path,
        page_size=2,
        extra_params={"filter": "record_date:gte:2024-01-01"},
    )

    assert list(frame["value"]) == ["1", "2"]
    assert out_path.exists()
    assert pd.read_csv(out_path)["value"].astype(str).tolist() == ["1", "2"]
    assert calls[0][0] == "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/test_endpoint"
    assert calls[0][1] == {"page[size]": "2", "filter": "record_date:gte:2024-01-01"}
    assert calls[1][0].startswith(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/test_endpoint?page%5Bsize%5D=2&filter=record_date%3Agte%3A2024-01-01"
    )
    assert "page%5Bnumber%5D=2" in calls[1][0]
    assert calls[1][1] == {}
