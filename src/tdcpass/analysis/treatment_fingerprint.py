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


def _sha256_bytes(payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(payload)
    return digest.hexdigest()


def _config_hash_payload(repo_root: Path) -> dict[str, Any]:
    config_paths = [repo_root / relpath for relpath in _config_relpaths()]
    file_hashes = {
        _relative_locator(path, repo_root=repo_root): sha256_file(path)
        for path in config_paths
    }
    return {
        "files": file_hashes,
        "combined_sha256": _combined_sha256(file_hashes),
    }


def _config_relpaths() -> list[Path]:
    return [
        Path("config") / "shock_specs.yml",
        Path("config") / "lp_specs.yml",
        Path("config") / "regime_specs.yml",
        Path("config") / "output_contract.yml",
    ]


def _git_commit_exists(repo_root: Path, commit: str) -> bool:
    try:
        subprocess.run(
            ["git", "rev-parse", "--verify", f"{commit}^{{commit}}"],
            cwd=str(repo_root),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return True


def _git_blob_bytes(repo_root: Path, *, commit: str, relpath: Path) -> bytes | None:
    try:
        result = subprocess.run(
            ["git", "show", f"{commit}:{relpath.as_posix()}"],
            cwd=str(repo_root),
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout


def _stored_analysis_source_commit(fingerprint: Mapping[str, Any]) -> str | None:
    value = fingerprint.get("analysis_source_commit")
    if isinstance(value, str) and value:
        return value
    legacy = fingerprint.get("git_commit")
    if isinstance(legacy, str) and legacy:
        return legacy
    return None


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
        "analysis_source_commit": _git_commit(repo_root),
        "config_hashes": _config_hash_payload(repo_root),
        "upstream_input": _upstream_input_payload(
            source_path=canonical_tdc_source_path,
            source_kind=canonical_tdc_source_kind,
            repo_root=repo_root,
        ),
    }


def _spec_validation_failures(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
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
    stored_commit = _stored_analysis_source_commit(fingerprint)
    if stored_commit is None:
        failures.append("Fingerprint is missing analysis_source_commit metadata.")
    return failures


def build_headline_treatment_fingerprint_validation_summary(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    failures = _spec_validation_failures(fingerprint, shock_spec=shock_spec)
    config_hashes = fingerprint.get("config_hashes")
    upstream_input = fingerprint.get("upstream_input")
    stored_commit = _stored_analysis_source_commit(fingerprint)
    commit_status = "passed"
    if stored_commit is None:
        commit_status = "missing"
    elif not _git_commit_exists(repo_root, stored_commit):
        failures.append(f"Fingerprint analysis_source_commit {stored_commit!r} is not available in this repo.")
        commit_status = "unresolved"

    current_config_hashes = _config_hash_payload(repo_root)
    config_status = "passed"
    if not isinstance(config_hashes, Mapping):
        config_status = "missing"
    elif config_hashes != current_config_hashes:
        failures.append("Fingerprint config_hashes do not match the current repo config state.")
        config_status = "failed"

    upstream_status = "missing"
    upstream_rechecked = False
    upstream_candidate = None
    if isinstance(upstream_input, Mapping):
        locator = upstream_input.get("source_locator")
        stored_sha256 = upstream_input.get("sha256")
        stored_source_repo_locator = upstream_input.get("source_repo_locator")
        stored_source_repo_commit = upstream_input.get("source_repo_commit")
        if isinstance(locator, str) and locator and isinstance(stored_sha256, str) and stored_sha256:
            if (
                isinstance(stored_source_repo_locator, str)
                and stored_source_repo_locator
                and isinstance(stored_source_repo_commit, str)
                and stored_source_repo_commit
                and ":" in locator
            ):
                _, relpath = locator.split(":", 1)
                candidate_repo = (repo_root.parent / stored_source_repo_locator).resolve()
                upstream_candidate = f"{candidate_repo}:{relpath}@{stored_source_repo_commit}"
                blob = (
                    _git_blob_bytes(candidate_repo, commit=stored_source_repo_commit, relpath=Path(relpath))
                    if _git_commit_exists(candidate_repo, stored_source_repo_commit)
                    else None
                )
                if blob is None:
                    upstream_status = "skipped_unreachable"
                else:
                    upstream_rechecked = True
                    observed_sha256 = _sha256_bytes(blob)
                    if observed_sha256 != stored_sha256:
                        failures.append(
                            "Fingerprint upstream_input sha256 does not match the recorded upstream source commit."
                        )
                        upstream_status = "failed"
                    else:
                        upstream_status = "passed"
            else:
                candidate: Path
                if ":" in locator:
                    repo_name, relpath = locator.split(":", 1)
                    candidate = (repo_root.parent / repo_name / relpath).resolve()
                else:
                    candidate = (repo_root / locator).resolve()
                upstream_candidate = str(candidate)
                if candidate.exists() and candidate.is_file():
                    upstream_rechecked = True
                    observed_sha256 = sha256_file(candidate)
                    if observed_sha256 != stored_sha256:
                        failures.append(
                            "Fingerprint upstream_input sha256 does not match the currently reachable source file."
                        )
                        upstream_status = "failed"
                    else:
                        upstream_status = "passed"
                else:
                    upstream_status = "skipped_unreachable"
        else:
            upstream_status = "skipped_missing_locator_or_sha"

    return {
        "status": "passed" if not failures else "failed",
        "failures": failures,
        "repo_root": str(repo_root),
        "analysis_source_commit_check": {
            "status": commit_status,
            "stored_analysis_source_commit": stored_commit,
        },
        "config_hashes_check": {
            "status": config_status,
            "stored_combined_sha256": None if not isinstance(config_hashes, Mapping) else config_hashes.get("combined_sha256"),
            "current_combined_sha256": current_config_hashes.get("combined_sha256"),
        },
        "upstream_input_check": {
            "status": upstream_status,
            "rechecked_source_sha256": upstream_rechecked,
            "candidate_path": upstream_candidate,
            "stored_locator": None if not isinstance(upstream_input, Mapping) else upstream_input.get("source_locator"),
            "stored_sha256": None if not isinstance(upstream_input, Mapping) else upstream_input.get("sha256"),
            "stored_source_repo_locator": None if not isinstance(upstream_input, Mapping) else upstream_input.get("source_repo_locator"),
            "stored_source_repo_commit": None if not isinstance(upstream_input, Mapping) else upstream_input.get("source_repo_commit"),
        },
        "spec_metadata_check": {
            "status": "passed" if not _spec_validation_failures(fingerprint, shock_spec=shock_spec) else "failed",
        },
    }


def validate_headline_treatment_fingerprint_recorded_state(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
    repo_root: Path,
) -> list[str]:
    return list(
        build_headline_treatment_fingerprint_validation_summary(
            fingerprint,
            shock_spec=shock_spec,
            repo_root=repo_root,
        ).get("failures", [])
    )


def validate_headline_treatment_fingerprint(
    fingerprint: Mapping[str, Any],
    *,
    shock_spec: Mapping[str, Any],
    repo_root: Path | None = None,
) -> list[str]:
    failures = _spec_validation_failures(fingerprint, shock_spec=shock_spec)
    stored_commit = _stored_analysis_source_commit(fingerprint)
    if repo_root is not None:
        current_commit = _git_commit(repo_root)
        if stored_commit is None:
            return failures
        if current_commit is None:
            failures.append("Could not resolve the current repo git commit for fingerprint validation.")
        elif stored_commit != current_commit:
            failures.append(
                f"Fingerprint analysis_source_commit mismatch: expected current repo commit {current_commit!r}, observed {stored_commit!r}."
            )
        current_config_hashes = _config_hash_payload(repo_root)
        config_hashes = fingerprint.get("config_hashes")
        if config_hashes != current_config_hashes:
            failures.append("Fingerprint config_hashes do not match the current repo config state.")
        upstream_input = fingerprint.get("upstream_input")
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
