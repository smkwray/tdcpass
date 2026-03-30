from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from tdcpass.core.manifest import write_manifest
from tdcpass.core.yaml_utils import load_yaml


def load_output_contract(path: Path) -> dict[str, Any]:
    return load_yaml(path)


def contract_artifacts(contract: Mapping[str, Any], *, kind: str | None = None) -> list[dict[str, Any]]:
    artifacts = list(contract.get("artifacts", []))
    if kind is None:
        return artifacts
    return [item for item in artifacts if item.get("kind") == kind]


def contract_paths(contract: Mapping[str, Any], *, prefix: str | None = None) -> list[Path]:
    paths = [Path(item["path"]) for item in contract_artifacts(contract)]
    if prefix is None:
        return paths
    return [path for path in paths if path.as_posix().startswith(prefix)]


def output_artifact_paths(contract: Mapping[str, Any]) -> list[Path]:
    return [
        path
        for path in contract_paths(contract, prefix="output/")
        if not path.as_posix().startswith("output/manifests/")
    ]


def site_mirror_paths(contract: Mapping[str, Any]) -> list[Path]:
    return [path for path in contract_paths(contract, prefix="site/data/") if path.name != "overview.json"]


def overview_artifact_path(contract: Mapping[str, Any]) -> Path:
    for path in contract_paths(contract, prefix="site/data/"):
        if path.name == "overview.json":
            return path
    raise KeyError("site/data/overview.json is missing from the output contract")


def export_frame(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def write_json_payload(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2) + "\n", encoding="utf-8")
    return path


def write_overview_json(
    path: Path,
    *,
    headline_metrics: dict,
    sample: dict,
    main_findings: list[str],
    caveats: list[str],
    evidence_tiers: dict,
    artifacts: list[str],
) -> Path:
    payload = {
        "headline_metrics": headline_metrics,
        "sample": sample,
        "main_findings": main_findings,
        "caveats": caveats,
        "evidence_tiers": evidence_tiers,
        "artifacts": artifacts,
    }
    return write_json_payload(path, payload)


def _copy_file(source: Path, destination: Path) -> Path:
    if source.resolve() == destination.resolve():
        if not source.exists():
            raise FileNotFoundError(f"Missing source artifact: {source}")
        return destination
    if not source.exists():
        raise FileNotFoundError(f"Missing source artifact: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination


def mirror_contract_artifacts(*, source_root: Path, dest_root: Path, contract: Mapping[str, Any]) -> list[Path]:
    written: list[Path] = []
    output_sources = {path.name: path for path in output_artifact_paths(contract)}

    for path in contract_paths(contract):
        rel = path.as_posix()
        if rel.startswith("output/") or rel.startswith("site/data/"):
            continue
        source = source_root / path
        target = dest_root / path
        written.append(_copy_file(source, target))

    for path in output_artifact_paths(contract):
        source = source_root / path
        target = dest_root / path
        written.append(_copy_file(source, target))

    for site_path in site_mirror_paths(contract):
        output_rel = output_sources.get(site_path.name)
        if output_rel is None:
            raise KeyError(f"No mirrored output artifact found for {site_path}")
        source = source_root / output_rel
        target = dest_root / site_path
        written.append(_copy_file(source, target))

    return written


def write_pipeline_manifests(
    root: Path,
    *,
    command: str,
    outputs: Iterable[Path],
    raw_download_runs: Iterable[Mapping[str, Any]] | None = None,
    reused_artifacts: Iterable[Mapping[str, Any]] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    raw_downloads_path = write_json_payload(root / "output" / "manifests" / "raw_downloads.json", {"runs": list(raw_download_runs or [])})
    reused_artifacts_path = write_json_payload(
        root / "output" / "manifests" / "reused_artifacts.json",
        {"artifacts": list(reused_artifacts or [])},
    )
    pipeline_run_path = write_manifest(
        root / "output" / "manifests" / "pipeline_run.json",
        command=command,
        outputs=outputs,
        extra=extra,
    )
    return {
        "raw_downloads": raw_downloads_path,
        "reused_artifacts": reused_artifacts_path,
        "pipeline_run": pipeline_run_path,
    }
