from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping

import pandas as pd

from tdcpass.core.manifest import sha256_file, write_manifest
from tdcpass.core.paths import repo_root
from tdcpass.core.yaml_utils import load_yaml

REUSE_MODES = {"discover", "rebuild", "copy", "symlink"}


@dataclass(frozen=True)
class ArtifactMatch:
    sibling: str
    artifact_key: str
    path: Path
    score: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_reuse_mode(reuse_mode: str | None) -> str:
    mode = (reuse_mode or os.getenv("TDCPASS_CACHE_REUSE_MODE") or "discover").strip().lower()
    if mode not in REUSE_MODES:
        raise ValueError(f"Unsupported reuse mode: {reuse_mode!r}")
    return mode


def _expand_root(raw: str, *, root: Path) -> Path | None:
    expanded = raw.replace("$REPO_ROOT", str(root))
    expanded = os.path.expandvars(expanded)
    expanded = os.path.expanduser(expanded)
    path = Path(expanded)
    return path if path.exists() else None


def _load_cache_config(root: Path) -> Mapping[str, Any]:
    return load_yaml(root / "config" / "cache_shortcuts.yml") or {}


def _candidate_columns(path: Path) -> List[str]:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        frame = pd.read_csv(path, nrows=0)
        return [str(col) for col in frame.columns]
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return [str(key) for key in payload.keys()]
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            return [str(key) for key in payload[0].keys()]
        return []
    if suffix == ".parquet":
        frame = pd.read_parquet(path)
        return [str(col) for col in frame.columns]
    return []


def _find_manifest_sidecar(path: Path) -> Path | None:
    candidates = [
        path.with_name("manifest.json"),
        path.with_suffix(path.suffix + ".manifest.json"),
        path.with_suffix(".manifest.json"),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    for parent in path.parents:
        candidate = parent / "manifest.json"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _extract_required_columns(artifact: Mapping[str, Any], manifest_payload: Mapping[str, Any] | None) -> List[str]:
    fields: List[str] = []
    for source in (artifact, manifest_payload or {}):
        for key in ("required_columns", "columns"):
            value = source.get(key) if isinstance(source, Mapping) else None
            if isinstance(value, list):
                fields.extend(str(item) for item in value if isinstance(item, (str, int, float)))
        schema = source.get("schema") if isinstance(source, Mapping) else None
        if isinstance(schema, Mapping):
            for key in ("required_columns", "columns"):
                value = schema.get(key)
                if isinstance(value, list):
                    fields.extend(str(item) for item in value if isinstance(item, (str, int, float)))
        config = source.get("config") if isinstance(source, Mapping) else None
        if isinstance(config, Mapping):
            for key in ("required_columns", "columns"):
                value = config.get(key)
                if isinstance(value, list):
                    fields.extend(str(item) for item in value if isinstance(item, (str, int, float)))
    seen: dict[str, None] = {}
    for item in fields:
        seen.setdefault(item, None)
    return list(seen.keys())


def _manifest_metadata(path: Path) -> dict[str, Any]:
    manifest_path = _find_manifest_sidecar(path)
    if manifest_path is None:
        return {}
    payload = _load_json(manifest_path)
    if payload is None:
        return {}
    metadata: dict[str, Any] = {"path": str(manifest_path)}
    for key in ("schema_version", "pipeline", "build_timestamp", "source", "claims_label", "files_written"):
        if key in payload:
            metadata[key] = payload[key]
    return metadata


def _manifest_mentions_candidate(path: Path, manifest_payload: Mapping[str, Any] | None) -> bool:
    if not manifest_payload:
        return False
    name = path.name
    relative = str(path)
    files_written = manifest_payload.get("files_written")
    if isinstance(files_written, list) and any(str(item) == name for item in files_written):
        return True
    outputs = manifest_payload.get("outputs")
    if isinstance(outputs, list):
        for item in outputs:
            if not isinstance(item, Mapping):
                continue
            output_path = item.get("path")
            if output_path is None:
                continue
            output_path = str(output_path)
            if output_path == name or output_path == relative or Path(output_path).name == name:
                return True
    return False


def _validate_candidate(path: Path, artifact: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "is_file": path.is_file(),
        "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
        "sha256": sha256_file(path) if path.exists() and path.is_file() else None,
        "validated": False,
        "reasons": [],
        "columns": [],
        "required_columns": [],
        "missing_columns": [],
        "manifest": {},
    }
    if not payload["exists"] or not payload["is_file"]:
        payload["reasons"].append("not_a_file")
        return payload
    if payload["size_bytes"] == 0:
        payload["reasons"].append("empty_file")
        return payload

    manifest_path = _find_manifest_sidecar(path)
    manifest_payload = _load_json(manifest_path) if manifest_path else None
    if manifest_path is not None:
        payload["manifest"] = _manifest_metadata(path)
        payload["manifest"]["matched_candidate"] = _manifest_mentions_candidate(path, manifest_payload)
    required_columns = _extract_required_columns(artifact, manifest_payload)
    if required_columns:
        payload["required_columns"] = required_columns
    try:
        columns = _candidate_columns(path)
        payload["columns"] = columns
    except Exception as exc:  # pragma: no cover - exercised only when file backends are unavailable
        payload["reasons"].append(f"schema_unreadable:{type(exc).__name__}")
        return payload

    if required_columns:
        missing = [column for column in required_columns if column not in columns]
        payload["missing_columns"] = missing
        if missing:
            payload["reasons"].append("missing_required_columns")
            return payload

    if manifest_payload is not None and not payload["manifest"].get("matched_candidate", False):
        payload["reasons"].append("manifest_does_not_mention_candidate")
        return payload

    payload["validated"] = True
    return payload


def _discover_candidates(root: Path, cfg: Mapping[str, Any]) -> Dict[str, List[ArtifactMatch]]:
    results: Dict[str, List[ArtifactMatch]] = {}
    for sibling, payload in cfg.get("siblings", {}).items():
        matches: List[ArtifactMatch] = []
        roots = [_expand_root(item, root=root) for item in payload.get("search_roots", [])]
        roots = [item for item in roots if item is not None]

        for sibling_root in roots:
            for artifact in payload.get("artifacts", []):
                keywords = [k.lower() for k in artifact.get("name_keywords", [])]
                for pattern in artifact.get("globs", []):
                    for candidate in sibling_root.glob(pattern):
                        if not candidate.is_file():
                            continue
                        hay = str(candidate).lower()
                        score = sum(1 for k in keywords if k in hay)
                        if score > 0:
                            matches.append(
                                ArtifactMatch(
                                    sibling=sibling,
                                    artifact_key=artifact["key"],
                                    path=candidate,
                                    score=score,
                                )
                            )

        matches.sort(key=lambda item: (-item.score, str(item.path)))
        results[sibling] = matches[:20]
    return results


def discover_sibling_artifacts() -> Dict[str, List[ArtifactMatch]]:
    root = repo_root()
    cfg = _load_cache_config(root)
    return _discover_candidates(root, cfg)


def _materialize_candidate(candidate: Path, *, reuse_mode: str, dest_root: Path, sibling: str, artifact_key: str) -> Path:
    target_dir = dest_root / sibling / artifact_key
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / candidate.name
    if target.exists() or target.is_symlink():
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    if reuse_mode == "copy":
        shutil.copy2(candidate, target)
    elif reuse_mode == "symlink":
        os.symlink(candidate, target)
    else:  # pragma: no cover - guarded by caller
        raise ValueError(f"Cannot materialize with mode={reuse_mode!r}")
    return target


def build_cache_reuse_provenance(*, reuse_mode: str | None = None, materialize_root: Path | None = None) -> dict[str, Any]:
    root = repo_root()
    cfg = _load_cache_config(root)
    mode = _normalize_reuse_mode(reuse_mode)
    discovered = _discover_candidates(root, cfg)
    timestamp = _utc_now()
    dest_root = materialize_root or (root / "data" / "cache" / "sibling_reuse")

    siblings_payload: dict[str, Any] = {}
    reused_artifacts: list[dict[str, Any]] = []
    fresh_downloads: list[dict[str, Any]] = []

    for sibling, matches in discovered.items():
        sibling_cfg = cfg.get("siblings", {}).get(sibling, {})
        artifacts_cfg = {artifact["key"]: artifact for artifact in sibling_cfg.get("artifacts", [])}
        per_artifact: dict[str, Any] = {}
        selected_by_key: dict[str, dict[str, Any]] = {}
        for match in matches:
            artifact_cfg = artifacts_cfg.get(match.artifact_key, {})
            validation = _validate_candidate(match.path, artifact_cfg)
            candidate_record = {
                "sibling": match.sibling,
                "artifact_key": match.artifact_key,
                "path": str(match.path),
                "score": match.score,
                "validated": validation["validated"],
                "validation": validation,
            }
            per_artifact.setdefault(match.artifact_key, {"artifact_key": match.artifact_key, "candidates": []})
            per_artifact[match.artifact_key]["candidates"].append(candidate_record)
            if validation["validated"] and match.artifact_key not in selected_by_key:
                selected_by_key[match.artifact_key] = candidate_record

        for artifact_key in artifacts_cfg:
            entry = per_artifact.setdefault(artifact_key, {"artifact_key": artifact_key, "candidates": []})
            selected = selected_by_key.get(artifact_key)
            entry["selected"] = selected
            entry["reuse_mode"] = mode
            if mode == "rebuild":
                fresh_downloads.append(
                    {
                        "sibling": sibling,
                        "artifact_key": artifact_key,
                        "status": "fresh_download",
                        "reason": "explicit_rebuild",
                        "validated_candidates": len(entry["candidates"]),
                    }
                )
                entry["status"] = "fresh_download_required"
                continue
            if selected is None:
                fresh_downloads.append(
                    {
                        "sibling": sibling,
                        "artifact_key": artifact_key,
                        "status": "fresh_download",
                        "reason": "no_valid_sibling_candidate",
                        "validated_candidates": len(entry["candidates"]),
                    }
                )
                entry["status"] = "fresh_download_required"
                continue

            entry["status"] = "reused" if mode in {"copy", "symlink"} else "discover_only"
            entry["selected_validation"] = selected["validation"]
            entry["selected"] = selected
            if mode in {"copy", "symlink"}:
                materialized_path = _materialize_candidate(
                    Path(selected["path"]),
                    reuse_mode=mode,
                    dest_root=dest_root,
                    sibling=sibling,
                    artifact_key=artifact_key,
                )
                reused_artifacts.append(
                    {
                        "sibling": sibling,
                        "artifact_key": artifact_key,
                        "reuse_mode": mode,
                        "source_path": selected["path"],
                        "materialized_path": str(materialized_path),
                        "size_bytes": selected["validation"]["size_bytes"],
                        "sha256": selected["validation"]["sha256"],
                        "discovered_at_utc": timestamp,
                        "validation": selected["validation"],
                    }
                )
                entry["materialized_path"] = str(materialized_path)
                entry["status"] = "reused"
            else:
                entry["status"] = "available"

        siblings_payload[sibling] = {
            "sibling": sibling,
            "artifacts": list(per_artifact.values()),
        }

    payload = {
        "generated_at_utc": timestamp,
        "reuse_mode": mode,
        "destination_root": str(dest_root),
        "siblings": siblings_payload,
        "reused_artifacts": reused_artifacts,
        "fresh_downloads": fresh_downloads,
    }
    return payload


def discover_and_write_manifest(reuse_mode: str | None = None, materialize_root: Path | None = None) -> Path:
    root = repo_root()
    payload = build_cache_reuse_provenance(reuse_mode=reuse_mode, materialize_root=materialize_root)
    out_path = root / "output" / "cache_discovery.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_manifest(
        root / "output" / "cache_discovery_manifest.json",
        command=f"cache-reuse:{payload['reuse_mode']}",
        outputs=[out_path],
        extra={
            "reuse_mode": payload["reuse_mode"],
            "reused_artifacts": len(payload["reused_artifacts"]),
            "fresh_downloads": len(payload["fresh_downloads"]),
            "destination_root": payload["destination_root"],
        },
    )
    return out_path
