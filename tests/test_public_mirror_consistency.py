from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

from tdcpass.analysis.treatment_fingerprint import validate_headline_treatment_fingerprint_recorded_state
from tdcpass.core.yaml_utils import load_yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_exact_identity(df: pd.DataFrame, *, group_columns: list[str]) -> None:
    for keys, group in df.groupby(group_columns, dropna=False):
        rows = {str(row["outcome"]): row for _, row in group.iterrows()}
        direct_outcome = "tdc_bank_only_qoq"
        if "target" in group.columns:
            direct_outcome = str(group["target"].iloc[0])
        assert {direct_outcome, "total_deposits_bank_qoq", "other_component_qoq"}.issubset(rows), keys
        total = float(rows["total_deposits_bank_qoq"]["beta"])
        tdc = float(rows[direct_outcome]["beta"])
        other = float(rows["other_component_qoq"]["beta"])
        assert math.isclose(total - tdc, other, rel_tol=0.0, abs_tol=1e-10), keys


def test_committed_site_data_mirror_is_self_consistent() -> None:
    root = repo_root()
    shock_specs = load_yaml(root / "config" / "shock_specs.yml")["shocks"]

    fingerprint = _load_json(root / "site" / "data" / "headline_treatment_fingerprint.json")
    provenance = _load_json(root / "site" / "data" / "provenance_validation_summary.json")
    direct_identification = _load_json(root / "site" / "data" / "direct_identification_summary.json")
    readiness = _load_json(root / "site" / "data" / "result_readiness_summary.json")
    pass_through = _load_json(root / "site" / "data" / "pass_through_summary.json")

    assert validate_headline_treatment_fingerprint_recorded_state(
        fingerprint,
        shock_spec=shock_specs["unexpected_tdc_default"],
        repo_root=root,
    ) == []
    assert provenance["status"] == "passed"
    assert provenance["failures"] == []
    assert provenance["analysis_source_commit_check"]["status"] == "passed"
    assert provenance["config_hashes_check"]["status"] == "passed"

    assert direct_identification["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert direct_identification["answer_ready"] is False
    assert readiness["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert readiness["status"] == "provisional"
    assert pass_through["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert pass_through["estimation_path"]["measurement_variant_artifact"] == "identity_measurement_ladder.csv"
    assert "consistent with crowd-out" in pass_through["headline_answer"]

    for summary in [direct_identification, readiness, pass_through]:
        horizons = summary["ratio_reporting_gate"]["horizons"]
        assert all(horizon["allowed"] is False for horizon in horizons.values())

    identity_baseline = pd.read_csv(root / "site" / "data" / "lp_irf_identity_baseline.csv")
    assert identity_baseline["decomposition_mode"].eq("exact_identity_baseline").all()
    _assert_exact_identity(identity_baseline, group_columns=["horizon"])

    measurement_ladder = pd.read_csv(root / "site" / "data" / "identity_measurement_ladder.csv")
    assert measurement_ladder["decomposition_mode"].eq("exact_identity_baseline").all()
    _assert_exact_identity(measurement_ladder, group_columns=["treatment_variant", "horizon"])
