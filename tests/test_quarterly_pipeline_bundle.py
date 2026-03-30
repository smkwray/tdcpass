from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from tdcpass.pipeline.quarterly import run_quarterly_pipeline
from tdcpass.reports.site_export import contract_artifacts, contract_paths, load_output_contract


def _write_csv(path: Path, columns: list[str], row: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row], columns=columns).to_csv(path, index=False)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _stub_value(column: str) -> object:
    if column == "quarter":
        return "2000Q1"
    if column in {"metric", "notes", "model_name", "spec_name", "treatment_variant", "outcome", "regime"}:
        return "stub"
    if column in {"horizon", "n"}:
        return 1
    return 0.0


def _write_bundle_source_artifacts(root: Path, contract: dict[str, Any]) -> None:
    skip = {
        "output/manifests/raw_downloads.json",
        "output/manifests/reused_artifacts.json",
        "output/manifests/pipeline_run.json",
        "site/data/overview.json",
    }

    for artifact in contract_artifacts(contract):
        rel = artifact["path"]
        if rel in skip:
            continue

        if artifact.get("kind") == "csv":
            columns = [str(item) for item in artifact.get("required_columns", [])]
            row = [_stub_value(column) for column in columns]
            if rel == "data/derived/quarterly_panel.csv":
                row = ["2000Q1"] + [0.0] * (len(columns) - 1)
            _write_csv(root / rel, columns, row)
            continue

        if artifact.get("kind") == "json":
            required = [str(item) for item in artifact.get("required_keys", [])]
            payload = {key: [] for key in required}
            _write_json(root / rel, payload)


def test_quarterly_pipeline_bundle_contract_from_skeleton_source(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    contract_path = repo_root / "config" / "output_contract.yml"
    contract = load_output_contract(contract_path)
    paths = contract_paths(contract)
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"
    _write_bundle_source_artifacts(source_root, contract)

    raw_download_runs = [{"source": "skeleton", "mode": "offline"}]
    reused_artifacts = [
        {"materialized_path": str(source_root / "data" / "derived" / "quarterly_panel.csv")}
    ]

    result = run_quarterly_pipeline(
        base_dir=dest_root,
        source_root=source_root,
        contract_path=contract_path,
        raw_download_runs=raw_download_runs,
        reused_artifacts=reused_artifacts,
        overview_payload={
            "headline_metrics": {"share_other_negative": 0.0},
            "sample": {"frequency": "quarterly", "rows": 1, "source_root": str(source_root)},
            "main_findings": ["skeleton integration test"],
            "caveats": ["stubbed offline source bundle"],
            "evidence_tiers": {"direct_data": ["quarter"]},
            "artifacts": ["site/data/accounting_summary.csv"],
        },
    )

    for rel in paths:
        assert (dest_root / rel).exists()

    raw_payload = json.loads((dest_root / "output" / "manifests" / "raw_downloads.json").read_text(encoding="utf-8"))
    assert raw_payload["runs"] == raw_download_runs

    reused_payload = json.loads((dest_root / "output" / "manifests" / "reused_artifacts.json").read_text(encoding="utf-8"))
    assert reused_payload["artifacts"] == reused_artifacts

    pipeline_payload = json.loads((dest_root / "output" / "manifests" / "pipeline_run.json").read_text(encoding="utf-8"))
    assert pipeline_payload["command"] == "pipeline run"
    assert pipeline_payload["extra"]["mode"] == "mirror"
    assert pipeline_payload["extra"]["contract_path"] == str(contract_path)
    assert pipeline_payload["extra"]["source_root"] == str(source_root)

    expected_outputs = {
        str(dest_root / rel): rel.name
        for rel in paths
        if rel.as_posix() != "output/manifests/pipeline_run.json"
    }
    actual_outputs = {entry["path"]: Path(entry["path"]).name for entry in pipeline_payload["outputs"]}
    assert expected_outputs == actual_outputs
    assert all(entry["exists"] for entry in pipeline_payload["outputs"])
    assert all(entry["sha256"] for entry in pipeline_payload["outputs"])

    generated = {
        "output/manifests/raw_downloads.json",
        "output/manifests/reused_artifacts.json",
        "output/manifests/pipeline_run.json",
        "site/data/overview.json",
    }
    expected_materialized_count = len([item for item in paths if item.as_posix() not in generated])
    assert result["materialized_count"] == str(expected_materialized_count)

    for artifact in contract_artifacts(contract):
        rel = Path(artifact["path"])
        artifact_path = dest_root / rel
        if artifact["kind"] == "csv":
            frame = pd.read_csv(artifact_path)
            required = [str(item) for item in artifact.get("required_columns", [])]
            assert set(required).issubset(frame.columns)
        elif artifact["kind"] == "json":
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            for key in artifact.get("required_keys", []):
                assert key in payload

    for path in paths:
        if not path.as_posix().startswith("site/data/") or path.name == "overview.json":
            continue
        output_rel = next(item for item in paths if item.as_posix().startswith("output/") and item.name == path.name)
        assert (dest_root / output_rel).read_text(encoding="utf-8") == (dest_root / path).read_text(encoding="utf-8")

    overview = json.loads((dest_root / "site" / "data" / "overview.json").read_text(encoding="utf-8"))
    assert overview["headline_metrics"]["share_other_negative"] == 0.0
    assert overview["sample"]["source_root"] == str(source_root)
    assert overview["main_findings"] == ["skeleton integration test"]
    assert overview["evidence_tiers"]["direct_data"] == ["quarter"]
