from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from tdcpass.core.manifest import sha256_file


def _git_commit(cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _git_toplevel(cwd: Path) -> Path | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(cwd),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    text = result.stdout.strip()
    return Path(text) if text else None


def _relative_locator(path: Path, *, repo_root: Path) -> str:
    return os.path.relpath(path, start=repo_root)


def _upstream_locator(*, source_path: Path, source_repo_root: Path | None, repo_root: Path) -> str:
    if source_repo_root is not None and source_path.is_relative_to(source_repo_root):
        return f"{source_repo_root.name}:{source_path.relative_to(source_repo_root).as_posix()}"
    return _relative_locator(source_path, repo_root=repo_root)


def _combined_sha256(entries: Mapping[str, str]) -> str:
    digest = hashlib.sha256()
    for key, value in sorted(entries.items()):
        digest.update(key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _config_hash_payload(repo_root: Path) -> dict[str, Any]:
    config_paths = [
        repo_root / "config" / "shock_specs.yml",
        repo_root / "config" / "lp_specs.yml",
        repo_root / "config" / "regime_specs.yml",
        repo_root / "config" / "output_contract.yml",
    ]
    file_hashes = {
        _relative_locator(path, repo_root=repo_root): sha256_file(path)
        for path in config_paths
    }
    return {
        "files": file_hashes,
        "combined_sha256": _combined_sha256(file_hashes),
    }


def _upstream_input_payload(
    *,
    source_path: Path | None,
    source_kind: str,
    repo_root: Path,
) -> dict[str, Any]:
    if source_path is None:
        return {
            "source_kind": source_kind,
            "source_locator": None,
            "sha256": None,
            "source_repo_locator": None,
            "source_repo_commit": None,
        }
    source_repo_root = _git_toplevel(source_path.parent if source_path.is_file() else source_path)
    return {
        "source_kind": source_kind,
        "source_locator": _upstream_locator(source_path=source_path, source_repo_root=source_repo_root, repo_root=repo_root),
        "sha256": sha256_file(source_path) if source_path.exists() and source_path.is_file() else None,
        "source_repo_locator": None if source_repo_root is None else source_repo_root.name,
        "source_repo_commit": None if source_repo_root is None else _git_commit(source_repo_root),
    }


def build_headline_treatment_fingerprint(
    *,
    shock_spec: Mapping[str, Any],
    shocked: pd.DataFrame,
    repo_root: Path,
    canonical_tdc_source_path: Path | None = None,
    canonical_tdc_source_kind: str = "unknown",
) -> dict[str, Any]:
    shock_column = str(shock_spec.get("standardized_column", "tdc_residual_z"))
    usable = shocked.dropna(subset=[shock_column]).copy() if shock_column in shocked.columns else shocked.iloc[0:0].copy()
    return {
        "treatment_freeze_status": str(shock_spec.get("freeze_status", "frozen")),
        "model_name": str(shock_spec.get("model_name", "")),
        "target": str(shock_spec.get("target", "")),
        "method": str(shock_spec.get("method", "")),
        "predictors": [str(item) for item in shock_spec.get("predictors", [])],
        "ridge_alpha": None if shock_spec.get("ridge_alpha") is None else float(shock_spec.get("ridge_alpha")),
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
        "max_train_obs": None if shock_spec.get("max_train_obs") is None else int(shock_spec.get("max_train_obs")),
        "standardized_column": shock_column,
        "residual_column": str(shock_spec.get("residual_column", "tdc_residual")),
        "fitted_column": str(shock_spec.get("fitted_column", "tdc_fitted")),
        "train_start_obs_column": str(shock_spec.get("train_start_obs_column", "train_start_obs")),
        "usable_sample": {
            "rows": int(len(usable)),
            "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
            "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
        },
        "git_commit": _git_commit(repo_root),
        "config_hashes": _config_hash_payload(repo_root),
        "upstream_input": _upstream_input_payload(
            source_path=canonical_tdc_source_path,
            source_kind=canonical_tdc_source_kind,
            repo_root=repo_root,
        ),
    }


def validate_headline_treatment_fingerprint(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
    repo_root: Path | None = None,
) -> list[str]:
    failures: list[str] = []
    expected_pairs = {
        "treatment_freeze_status": str(shock_spec.get("freeze_status", "frozen")),
        "model_name": str(shock_spec.get("model_name", "")),
        "target": str(shock_spec.get("target", "")),
        "method": str(shock_spec.get("method", "")),
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
        "max_train_obs": None if shock_spec.get("max_train_obs") is None else int(shock_spec.get("max_train_obs")),
        "standardized_column": str(shock_spec.get("standardized_column", "tdc_residual_z")),
        "residual_column": str(shock_spec.get("residual_column", "tdc_residual")),
        "fitted_column": str(shock_spec.get("fitted_column", "tdc_fitted")),
        "train_start_obs_column": str(shock_spec.get("train_start_obs_column", "train_start_obs")),
        "ridge_alpha": None if shock_spec.get("ridge_alpha") is None else float(shock_spec.get("ridge_alpha")),
    }
    for key, expected in expected_pairs.items():
        observed = fingerprint.get(key)
        if observed != expected:
            failures.append(f"Fingerprint mismatch for {key}: expected {expected!r}, observed {observed!r}.")
    expected_predictors = [str(item) for item in shock_spec.get("predictors", [])]
    observed_predictors = [str(item) for item in fingerprint.get("predictors", [])]
    if observed_predictors != expected_predictors:
        failures.append("Fingerprint mismatch for predictors.")
    usable_sample = fingerprint.get("usable_sample", {})
    if not isinstance(usable_sample, Mapping) or "rows" not in usable_sample:
        failures.append("Fingerprint is missing usable_sample metadata.")
    config_hashes = fingerprint.get("config_hashes")
    if not isinstance(config_hashes, Mapping):
        failures.append("Fingerprint is missing config_hashes metadata.")
    else:
        file_hashes = config_hashes.get("files")
        combined_sha256 = config_hashes.get("combined_sha256")
        if not isinstance(file_hashes, Mapping) or not file_hashes:
            failures.append("Fingerprint config_hashes is missing per-file hashes.")
        if not isinstance(combined_sha256, str) or not combined_sha256:
            failures.append("Fingerprint config_hashes is missing combined_sha256.")
    upstream_input = fingerprint.get("upstream_input")
    if not isinstance(upstream_input, Mapping):
        failures.append("Fingerprint is missing upstream_input metadata.")
    else:
        if "source_kind" not in upstream_input:
            failures.append("Fingerprint upstream_input is missing source_kind.")
        if "sha256" not in upstream_input:
            failures.append("Fingerprint upstream_input is missing sha256.")
    stored_commit = fingerprint.get("git_commit")
    if not isinstance(stored_commit, str) or not stored_commit:
        failures.append("Fingerprint is missing git_commit metadata.")
    if repo_root is not None:
        current_commit = _git_commit(repo_root)
        if current_commit is None:
            failures.append("Could not resolve the current repo git commit for fingerprint validation.")
        elif stored_commit != current_commit:
            failures.append(
                f"Fingerprint git_commit mismatch: expected current repo commit {current_commit!r}, observed {stored_commit!r}."
            )
        current_config_hashes = _config_hash_payload(repo_root)
        if config_hashes != current_config_hashes:
            failures.append("Fingerprint config_hashes do not match the current repo config state.")
        if isinstance(upstream_input, Mapping):
            locator = upstream_input.get("source_locator")
            stored_sha256 = upstream_input.get("sha256")
            if isinstance(locator, str) and locator and isinstance(stored_sha256, str) and stored_sha256:
                candidate: Path
                if ":" in locator:
                    repo_name, relpath = locator.split(":", 1)
                    candidate = (repo_root.parent / repo_name / relpath).resolve()
                else:
                    candidate = (repo_root / locator).resolve()
                if candidate.exists() and candidate.is_file():
                    observed_sha256 = sha256_file(candidate)
                    if observed_sha256 != stored_sha256:
                        failures.append(
                            "Fingerprint upstream_input sha256 does not match the currently reachable source file."
                        )
    return failures
