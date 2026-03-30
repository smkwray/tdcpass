from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tdcpass.core.manifest import sha256_file
from tdcpass.data.fetchers.frb_h41 import fetch_frb_h41_raw
from tdcpass.data.fetchers.frb_h8 import fetch_frb_h8_raw
from tdcpass.data.fetchers.frb_z1 import fetch_frb_z1_raw


class _FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


def _patch_requests(monkeypatch: Any, payload: bytes, calls: list[dict[str, Any]]) -> None:
    def _fake_get(url: str, params: dict[str, Any], timeout: int) -> _FakeResponse:
        calls.append({"url": url, "params": params, "timeout": timeout})
        return _FakeResponse(payload)

    monkeypatch.setattr("tdcpass.data.fetchers.frb_common.requests.get", _fake_get)


def _read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_frb_adapters_write_raw_file_and_manifest_with_defaults(monkeypatch: Any, tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []
    payload = b"official release bytes"
    _patch_requests(monkeypatch, payload, calls)

    z1_path = tmp_path / "z1.csv"
    h8_path = tmp_path / "h8.csv"
    h41_path = tmp_path / "h41.csv"
    manifest_path = tmp_path / "raw_downloads.json"

    fetch_frb_z1_raw(z1_path, manifest_path=manifest_path)
    fetch_frb_h8_raw(h8_path, manifest_path=manifest_path)
    fetch_frb_h41_raw(h41_path, manifest_path=manifest_path)

    assert z1_path.read_bytes() == payload
    assert h8_path.read_bytes() == payload
    assert h41_path.read_bytes() == payload

    assert len(calls) == 3
    for call in calls:
        assert call["url"] == "https://www.federalreserve.gov/datadownload/Output.aspx"
        assert call["params"]["filetype"] == "csv"

    assert calls[0]["params"]["rel"] == "Z1"
    assert calls[1]["params"]["rel"] == "H8"
    assert calls[2]["params"]["rel"] == "H41"

    manifest = _read_manifest(manifest_path)
    assert "runs" in manifest
    assert len(manifest["runs"]) == 3
    assert [item["source_key"] for item in manifest["runs"]] == ["frb_z1", "frb_h8", "frb_h41"]
    assert manifest["runs"][0]["file_sha256"] == sha256_file(z1_path)
    assert manifest["runs"][1]["file_sha256"] == sha256_file(h8_path)
    assert manifest["runs"][2]["file_sha256"] == sha256_file(h41_path)
    assert all(item["downloaded_at_utc"] for item in manifest["runs"])


def test_frb_adapters_forward_custom_url_and_params(monkeypatch: Any, tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []
    _patch_requests(monkeypatch, b"custom bytes", calls)

    destination = tmp_path / "custom.csv"
    manifest = tmp_path / "manifest.json"
    custom_url = "https://www.federalreserve.gov/datadownload/Output.aspx"
    custom_params = {"rel": "H8", "series": "ABC123", "filetype": "csv"}

    fetch_frb_h8_raw(
        destination,
        source_url=custom_url,
        params=custom_params,
        manifest_path=manifest,
        timeout=9,
    )

    assert destination.read_bytes() == b"custom bytes"
    assert calls == [{"url": custom_url, "params": custom_params, "timeout": 9}]

    payload = _read_manifest(manifest)
    assert len(payload["runs"]) == 1
    run = payload["runs"][0]
    assert run["source_key"] == "frb_h8"
    assert run["source_url"] == custom_url
    assert run["params"] == custom_params
    assert run["file_path"] == str(destination)
