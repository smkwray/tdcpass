from __future__ import annotations

import json
from pathlib import Path

import pytest

from tdcpass.data import sibling_cache


def _write_cache_config(root: Path) -> None:
    config = root / "config"
    config.mkdir(parents=True, exist_ok=True)
    config.joinpath("cache_shortcuts.yml").write_text(
        """
siblings:
  tdcest:
    search_roots:
      - $REPO_ROOT/../tdcest
    artifacts:
      - key: tdc_quarterly_main
        globs:
          - data/**/*.csv
        name_keywords:
          - tdc
          - bank
          - quarter
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_sibling_repo(root: Path) -> Path:
    sibling_root = root.parent / "tdcest"
    data_dir = sibling_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    valid = data_dir / "bank_tdc_quarter.csv"
    valid.write_text("bank_id,tdc_qoq\n1,2\n", encoding="utf-8")

    invalid = data_dir / "bad_bank_tdc_quarter.csv"
    invalid.write_text("wrong_col\n1\n", encoding="utf-8")

    manifest = {
        "schema_version": 1,
        "pipeline": "quarterly_panel",
        "files_written": [valid.name, invalid.name],
        "schema": {"required_columns": ["bank_id", "tdc_qoq"]},
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return sibling_root


@pytest.fixture()
def temp_repo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = tmp_path / "tdcpass"
    root.mkdir()
    _write_cache_config(root)
    _write_sibling_repo(root)
    (root / "output").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sibling_cache, "repo_root", lambda: root)
    return root


def test_discover_validates_candidates_and_separates_reuse_from_fresh(temp_repo: Path) -> None:
    out_path = sibling_cache.discover_and_write_manifest(reuse_mode="discover")
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload["reuse_mode"] == "discover"
    assert payload["reused_artifacts"] == []
    assert payload["fresh_downloads"] == []

    artifact = payload["siblings"]["tdcest"]["artifacts"][0]
    assert artifact["status"] == "available"
    assert artifact["selected"]["validation"]["validated"] is True
    candidates = artifact["candidates"]
    assert len(candidates) == 2
    rejected = next(item for item in candidates if item["path"].endswith("bad_bank_tdc_quarter.csv"))
    assert rejected["validated"] is False
    assert "missing_required_columns" in rejected["validation"]["reasons"]
    assert rejected["validation"]["missing_columns"] == ["bank_id", "tdc_qoq"]


@pytest.mark.parametrize("reuse_mode, expected_symlink", [("copy", False), ("symlink", True)])
def test_copy_and_symlink_materialize_selected_candidate(temp_repo: Path, reuse_mode: str, expected_symlink: bool) -> None:
    materialize_root = temp_repo / "data" / "cache" / "reuse"
    payload = sibling_cache.build_cache_reuse_provenance(
        reuse_mode=reuse_mode,
        materialize_root=materialize_root,
    )

    assert payload["reuse_mode"] == reuse_mode
    assert len(payload["reused_artifacts"]) == 1
    assert payload["fresh_downloads"] == []

    reused = payload["reused_artifacts"][0]
    materialized_path = Path(reused["materialized_path"])
    assert materialized_path.exists()
    assert materialized_path.is_symlink() is expected_symlink
    assert materialized_path.read_text(encoding="utf-8") == "bank_id,tdc_qoq\n1,2\n"
    assert reused["validation"]["validated"] is True


def test_rebuild_marks_artifact_as_fresh_download(temp_repo: Path) -> None:
    payload = sibling_cache.build_cache_reuse_provenance(reuse_mode="rebuild")

    assert payload["reuse_mode"] == "rebuild"
    assert payload["reused_artifacts"] == []
    assert len(payload["fresh_downloads"]) == 1
    assert payload["fresh_downloads"][0]["reason"] == "explicit_rebuild"

    artifact = payload["siblings"]["tdcest"]["artifacts"][0]
    assert artifact["status"] == "fresh_download_required"
    assert artifact["selected"]["validation"]["validated"] is True
