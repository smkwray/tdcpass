from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tdcpass.analysis.treatment_fingerprint import build_headline_treatment_fingerprint
from tdcpass.cli import build_parser, main


def _write_csv(path: Path, header: list[str], row: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if len(row) == 1 and isinstance(row[0], (list, tuple)):
        row = list(row[0])
    path.write_text(
        ",".join(header) + "\n" + ",".join(str(item) for item in row) + "\n",
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _valid_fingerprint_payload() -> dict[str, object]:
    repo_root = Path(__file__).resolve().parents[1]
    return build_headline_treatment_fingerprint(
        shock_spec={
            "freeze_status": "frozen",
            "model_name": "unexpected_tdc_default",
            "target": "tdc_bank_only_qoq",
            "method": "rolling_window_ridge",
            "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
            "ridge_alpha": 125.0,
            "min_train_obs": 24,
            "max_train_obs": 40,
            "standardized_column": "tdc_residual_z",
            "residual_column": "tdc_residual",
            "fitted_column": "tdc_fitted",
            "train_start_obs_column": "train_start_obs",
        },
        shocked=pd.DataFrame({"quarter": ["2010Q1"], "tdc_residual_z": [0.1]}),
        repo_root=repo_root,
    )


def test_pipeline_run_command_is_wired(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"

    panel_header = [
        "quarter",
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "other_component_qoq",
        "bank_credit_private_qoq",
        "cb_nonts_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
        "bill_share",
        "bank_absorption_share",
        "reserve_drain_pressure",
        "quarter_index",
        "slr_tight",
        "tga_qoq",
        "reserves_qoq",
        "fedfunds",
        "unemployment",
        "inflation",
        "lag_tdc_bank_only_qoq",
        "lag_total_deposits_bank_qoq",
        "lag_bank_credit_private_qoq",
        "lag_tga_qoq",
        "lag_reserves_qoq",
        "lag_bill_share",
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    _write_csv(source_root / "data" / "derived" / "quarterly_panel.csv", panel_header, ["2000Q1"] + [1] * (len(panel_header) - 1))

    for rel in [
        "output/accounting/accounting_summary.csv",
        "output/accounting/quarters_tdc_exceeds_total.csv",
        "output/shocks/unexpected_tdc.csv",
        "output/models/lp_irf.csv",
        "output/models/lp_irf_identity_baseline.csv",
        "output/models/identity_measurement_ladder.csv",
        "output/models/lp_irf_regimes.csv",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/period_sensitivity.csv",
        "output/models/period_sensitivity_summary.json",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/headline_treatment_fingerprint.json",
        "output/models/provenance_validation_summary.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/sample_construction_summary.json",
    ]:
        if rel.endswith("accounting_summary.csv"):
            _write_csv(source_root / rel, ["metric", "value", "notes"], [["share_other_negative", 0.0, "stub"]])
        elif rel.endswith("quarters_tdc_exceeds_total.csv"):
            _write_csv(source_root / rel, ["quarter", "tdc_bank_only_qoq", "total_deposits_bank_qoq", "other_component_qoq"], [["2000Q1", 1.0, 2.0, 1.0]])
        elif rel.endswith("unexpected_tdc.csv"):
            _write_csv(
                source_root / rel,
                [
                    "quarter",
                    "tdc_bank_only_qoq",
                    "tdc_fitted",
                    "tdc_residual",
                    "tdc_residual_z",
                    "model_name",
                    "train_start_obs",
                    "train_condition_number",
                    "train_target_sd",
                    "train_resid_sd",
                    "fitted_to_target_scale_ratio",
                    "fitted_to_train_target_sd_ratio",
                    "shock_flag",
                ],
                [["2000Q1", 1.0, 0.8, 0.2, 0.2, "stub", 1, 10.0, 0.5, 0.2, 0.8, 1.6, ""]],
            )
        elif rel.endswith("lp_irf.csv"):
            _write_csv(source_root / rel, ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("lp_irf_identity_baseline.csv"):
            _write_csv(source_root / rel, ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name", "decomposition_mode", "outcome_construction", "inference_method"], [["total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_baseline", "exact_identity_baseline", "estimated_common_design", "bootstrap"]])
        elif rel.endswith("identity_measurement_ladder.csv"):
            _write_csv(
                source_root / rel,
                [
                    "treatment_variant",
                    "treatment_role",
                    "treatment_family",
                    "target",
                    "outcome",
                    "horizon",
                    "beta",
                    "se",
                    "lower95",
                    "upper95",
                    "n",
                    "spec_name",
                    "shock_column",
                    "decomposition_mode",
                    "outcome_construction",
                    "inference_method",
                ],
                [["domestic_bank_only", "exploratory", "measurement", "tdc_domestic_bank_only_qoq", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_measurement_ladder", "tdc_domestic_bank_only_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"]],
            )
        elif rel.endswith("lp_irf_regimes.csv"):
            _write_csv(source_root / rel, ["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["reserve_drain_high", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("tdc_sensitivity_ladder.csv"):
            _write_csv(source_root / rel, ["treatment_variant", "treatment_role", "treatment_family", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["tdc_bank_only_qoq", "core", "headline", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("control_set_sensitivity.csv"):
            _write_csv(source_root / rel, ["control_variant", "control_role", "control_columns", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["headline_lagged_macro", "headline", "lag_fedfunds|lag_unemployment|lag_inflation", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "control_sensitivity"]])
        elif rel.endswith("shock_sample_sensitivity.csv"):
            _write_csv(source_root / rel, ["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["all_usable_shocks", "headline", "all_usable_shocks", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "sample_sensitivity"]])
        elif rel.endswith("period_sensitivity.csv"):
            _write_csv(source_root / rel, ["period_variant", "period_role", "start_quarter", "end_quarter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["all_usable", "headline", "2009Q1", "2025Q4", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "period_sensitivity"]])
        elif rel.endswith("period_sensitivity_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "materialized",
                    "headline_question": "stub",
                    "estimation_path": {"role": "secondary_period_sensitivity_surface"},
                    "periods": [],
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("total_minus_other_contrast.csv"):
            _write_csv(source_root / rel, ["scope", "variant", "role", "horizon", "beta_total", "beta_other", "beta_implied", "beta_direct", "gap_implied_minus_direct", "abs_gap", "n_total", "n_other", "n_direct", "sample_mismatch_flag"], [["baseline", "baseline", "headline", 0, 0.1, 0.0, 0.1, 0.1, 0.0, 0.0, 1, 1, 1, False]])
        elif rel.endswith("structural_proxy_evidence.csv"):
            _write_csv(source_root / rel, ["scope", "context", "horizon", "other_outcome", "other_beta", "other_se", "other_lower95", "other_upper95", "other_ci_excludes_zero", "proxy_outcome", "proxy_beta", "proxy_se", "proxy_lower95", "proxy_upper95", "proxy_ci_excludes_zero", "other_sign", "proxy_sign", "sign_alignment", "evidence_label", "proxy_share_of_other_beta"], [["baseline", "baseline", 0, "other_component_qoq", 1.0, 0.1, 0.8, 1.2, True, "bank_credit_private_qoq", 0.2, 0.1, 0.0, 0.4, False, "positive", "positive", "same_sign", "other_without_proxy_confirmation", 0.2]])
        elif rel.endswith("structural_proxy_evidence_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "weak",
                    "headline_question": "stub",
                    "key_horizons": {"h0": {"interpretation": "proxy_evidence_weak"}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("proxy_coverage_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "mixed",
                    "headline_question": "stub",
                    "covered_channel_families": [],
                    "major_uncovered_channel_families": [],
                    "history_limits": [],
                    "key_horizons": {"h0": {"coverage_label": "proxy_bundle_weak"}},
                    "published_regime_contexts": [],
                    "release_caveat": "stub",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("proxy_unit_audit.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "ok",
                    "source_series": [],
                    "derived_proxies": [],
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("headline_treatment_fingerprint.json"):
            _write_json(
                source_root / rel,
                {
                    "treatment_freeze_status": "frozen",
                    "model_name": "unexpected_tdc_default",
                    "target": "tdc_bank_only_qoq",
                    "method": "rolling_window_ridge",
                    "predictors": ["lag_tdc_bank_only_qoq"],
                    "min_train_obs": 24,
                    "max_train_obs": 40,
                    "usable_sample": {"rows": 1},
                    "analysis_source_commit": "stub",
                    "config_hashes": {"files": {"config/shock_specs.yml": "stub"}, "combined_sha256": "stub"},
                    "upstream_input": {
                        "source_kind": "tdcest_processed_csv",
                        "source_locator": None,
                        "sha256": None,
                        "source_repo_locator": None,
                        "source_repo_commit": None,
                    },
                },
            )
        elif rel.endswith("provenance_validation_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "passed",
                    "failures": [],
                    "analysis_source_commit_check": {"status": "passed"},
                    "config_hashes_check": {"status": "passed"},
                    "upstream_input_check": {"status": "skipped_missing_locator_or_sha"},
                    "spec_metadata_check": {"status": "passed"},
                },
            )
        elif rel.endswith("pass_through_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "not_ready",
                    "headline_question": "stub",
                    "headline_answer": "stub",
                    "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
                    "sample_policy": {"headline_sample_variant": "all_usable_shocks"},
                    "baseline_horizons": {},
                    "core_treatment_variants": [],
                    "measurement_treatment_variants": [],
                    "shock_design_treatment_variants": [],
                    "core_control_variants": [],
                    "shock_sample_variants": [],
                    "structural_proxy_context": {},
                    "proxy_coverage_context": {},
                    "published_regime_contexts": [],
                    "readiness_reasons": ["stub"],
                    "readiness_warnings": [],
                },
            )
        elif rel.endswith("sample_construction_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "full_panel": {"rows": 1},
                    "headline_sample": {"rows": 1},
                    "usable_shock_sample": {"rows": 0},
                    "shock_definition": {"shock_column": "tdc_residual_z"},
                    "headline_sample_truncation": {"dropped_rows_from_full_panel": 0},
                    "extended_column_coverage": [],
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("direct_identification_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "not_ready",
                    "headline_question": "stub",
                    "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
                    "shock_definition": {"shock_column": "tdc_residual_z"},
                    "horizon_evidence": {},
                    "first_stage_checks": {"tdc_ci_excludes_zero_at_h0_or_h4": False},
                    "sample_fragility": {},
                    "answer_ready": False,
                    "reasons": ["stub"],
                    "warnings": [],
                    "answer_ready_when": ["stub"],
                },
            )
        elif rel.endswith("result_readiness_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "not_ready",
                    "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
                    "headline_assessment": "stub",
                    "reasons": ["stub"],
                    "warnings": [],
                    "diagnostics": {"shock_usable_obs": 1},
                    "key_estimates": {},
                    "answer_ready_when": ["stub"],
                },
            )
        elif rel.endswith("shock_diagnostics_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "estimand_interpretation": {"shock_scale": "stub"},
                    "sample_comparison": {"overlap_observations": 1},
                    "impact_response_comparison": {},
                    "treatment_variant_comparisons": [],
                    "shock_quality": {},
                    "largest_disagreement_quarters": [],
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("regime_diagnostics_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "informative_regime_count": 1,
                    "stable_regime_count": 1,
                    "regimes": [],
                    "takeaways": ["stub"],
                },
            )

    parser = build_parser()
    parsed = parser.parse_args(["pipeline", "run", "--root", str(dest_root), "--source-root", str(source_root), "--contract", str(repo_root / "config" / "output_contract.yml")])
    assert parsed.command == "pipeline"
    assert parsed.pipeline_command == "run"

    exit_code = main(["pipeline", "run", "--root", str(dest_root), "--source-root", str(source_root), "--contract", str(repo_root / "config" / "output_contract.yml")])
    assert exit_code == 0
    assert json.loads((dest_root / "output" / "manifests" / "pipeline_run.json").read_text(encoding="utf-8"))["command"] == "pipeline run"


def test_pipeline_closeout_command_is_wired() -> None:
    parser = build_parser()
    parsed = parser.parse_args(["pipeline", "closeout", "--root", "/tmp/demo"])
    assert parsed.command == "pipeline"
    assert parsed.pipeline_command == "closeout"


def test_pipeline_closeout_reads_existing_artifacts(tmp_path: Path, capsys) -> None:
    root = tmp_path / "closeout-root"
    _write_json(
        root / "output" / "models" / "backend_closeout_summary.json",
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "stub",
        },
    )
    (root / "output" / "reports").mkdir(parents=True, exist_ok=True)
    (root / "output" / "reports" / "backend_closeout.md").write_text("# closeout\n", encoding="utf-8")
    _write_json(root / "output" / "models" / "backend_evidence_packet_summary.json", {"status": "not_ready"})
    _write_json(root / "output" / "models" / "backend_decision_bundle_summary.json", {"status": "not_ready"})
    _write_csv(
        root / "output" / "models" / "lp_irf_identity_baseline.csv",
        [
            "outcome",
            "horizon",
            "beta",
            "se",
            "lower95",
            "upper95",
            "n",
            "spec_name",
            "decomposition_mode",
            "outcome_construction",
            "inference_method",
        ],
        [["tdc_bank_only_qoq", 0, 1.0, 0.1, 0.8, 1.2, 10, "identity_baseline", "exact_identity_baseline", "estimated", "bootstrap"]],
    )
    _write_json(
        root / "output" / "models" / "headline_treatment_fingerprint.json",
        {
            **_valid_fingerprint_payload(),
            "usable_sample": {"rows": 10, "start_quarter": "2010Q1", "end_quarter": "2012Q2"},
        },
    )
    _write_json(
        root / "output" / "models" / "direct_identification_summary.json",
        {
            "status": "provisional",
            "headline_question": "stub",
            "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
            "shock_definition": {"shock_column": "tdc_residual_z"},
            "horizon_evidence": {},
            "first_stage_checks": {},
            "sample_fragility": {},
            "answer_ready": False,
            "reasons": [],
            "warnings": [],
            "answer_ready_when": [],
        },
    )
    _write_json(
        root / "output" / "models" / "result_readiness_summary.json",
        {
            "status": "provisional",
            "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
            "headline_assessment": "stub",
            "reasons": [],
            "warnings": [],
            "diagnostics": {},
            "key_estimates": {},
            "answer_ready_when": [],
        },
    )

    exit_code = main(["pipeline", "closeout", "--root", str(root)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["recommended_action"] == "stop_and_package"
    assert payload["closeout_summary_path"].endswith("backend_closeout_summary.json")
    assert payload["closeout_failures"] == []


def test_pipeline_closeout_fails_on_fingerprint_mismatch(tmp_path: Path, capsys) -> None:
    root = tmp_path / "closeout-root"
    _write_json(
        root / "output" / "models" / "backend_closeout_summary.json",
        {
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "headline_question": "stub",
        },
    )
    (root / "output" / "reports").mkdir(parents=True, exist_ok=True)
    (root / "output" / "reports" / "backend_closeout.md").write_text("# closeout\n", encoding="utf-8")
    _write_json(root / "output" / "models" / "backend_evidence_packet_summary.json", {"status": "not_ready"})
    _write_json(root / "output" / "models" / "backend_decision_bundle_summary.json", {"status": "not_ready"})
    _write_csv(
        root / "output" / "models" / "lp_irf_identity_baseline.csv",
        [
            "outcome",
            "horizon",
            "beta",
            "se",
            "lower95",
            "upper95",
            "n",
            "spec_name",
            "decomposition_mode",
            "outcome_construction",
            "inference_method",
        ],
        [["tdc_bank_only_qoq", 0, 1.0, 0.1, 0.8, 1.2, 10, "identity_baseline", "exact_identity_baseline", "estimated_common_design", "bootstrap"]],
    )
    _write_json(
        root / "output" / "models" / "headline_treatment_fingerprint.json",
        {
            **_valid_fingerprint_payload(),
            "model_name": "wrong_model_name",
            "usable_sample": {"rows": 10},
        },
    )
    _write_json(
        root / "output" / "models" / "direct_identification_summary.json",
        {
            "status": "provisional",
            "headline_question": "stub",
            "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
            "shock_definition": {"shock_column": "tdc_residual_z"},
            "horizon_evidence": {},
            "first_stage_checks": {},
            "sample_fragility": {},
            "answer_ready": False,
            "reasons": [],
            "warnings": [],
            "answer_ready_when": [],
        },
    )
    _write_json(
        root / "output" / "models" / "result_readiness_summary.json",
        {
            "status": "provisional",
            "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
            "headline_assessment": "stub",
            "reasons": [],
            "warnings": [],
            "diagnostics": {},
            "key_estimates": {},
            "answer_ready_when": [],
        },
    )

    exit_code = main(["pipeline", "closeout", "--root", str(root)])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert any("Fingerprint mismatch" in item for item in payload["closeout_failures"])


def test_demo_command_still_exists() -> None:
    parser = build_parser()
    parsed = parser.parse_args(["demo"])
    assert parsed.command == "demo"


def test_pipeline_run_supports_offline_raw_fixture(tmp_path: Path) -> None:
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "offline_raw_fixture"
    dest_root = tmp_path / "offline-dest"

    exit_code = main(["pipeline", "run", "--root", str(dest_root), "--raw-fixture-root", str(fixture_root), "--reuse-mode", "rebuild"])

    assert exit_code == 0
    assert (dest_root / "data" / "derived" / "quarterly_panel.csv").exists()
    assert (dest_root / "output" / "models" / "sample_construction_summary.json").exists()
    assert (dest_root / "output" / "models" / "lp_irf_identity_baseline.csv").exists()
    assert (dest_root / "output" / "models" / "identity_measurement_ladder.csv").exists()
    assert (dest_root / "output" / "models" / "headline_treatment_fingerprint.json").exists()
    assert (dest_root / "output" / "models" / "provenance_validation_summary.json").exists()
    assert (dest_root / "output" / "models" / "published_state_proxy_comparator_summary.json").exists()
    assert (dest_root / "output" / "models" / "published_state_proxy_vs_baseline_summary.json").exists()
    assert (dest_root / "output" / "models" / "backend_decision_bundle_summary.json").exists()
    assert (dest_root / "output" / "models" / "backend_evidence_packet_summary.json").exists()
    assert (dest_root / "output" / "models" / "backend_closeout_summary.json").exists()
    assert (dest_root / "output" / "reports" / "published_state_proxy_comparator.md").exists()
    assert (dest_root / "output" / "reports" / "published_state_proxy_vs_baseline.md").exists()
    assert (dest_root / "output" / "reports" / "backend_decision_bundle.md").exists()
    assert (dest_root / "output" / "reports" / "backend_evidence_packet.md").exists()
    assert (dest_root / "output" / "reports" / "backend_closeout.md").exists()
    assert (dest_root / "output" / "manifests" / "pipeline_run.json").exists()

    sample_summary = json.loads((dest_root / "output" / "models" / "sample_construction_summary.json").read_text(encoding="utf-8"))
    shock_diagnostics = json.loads((dest_root / "output" / "models" / "shock_diagnostics_summary.json").read_text(encoding="utf-8"))
    direct_identification = json.loads((dest_root / "output" / "models" / "direct_identification_summary.json").read_text(encoding="utf-8"))
    readiness = json.loads((dest_root / "output" / "models" / "result_readiness_summary.json").read_text(encoding="utf-8"))
    pass_through = json.loads((dest_root / "output" / "models" / "pass_through_summary.json").read_text(encoding="utf-8"))

    assert sample_summary["treatment_freeze_status"] == "frozen"
    assert shock_diagnostics["treatment_freeze_status"] == "frozen"
    assert direct_identification["treatment_freeze_status"] == "frozen"
    assert readiness["treatment_freeze_status"] == "frozen"
    assert pass_through["treatment_freeze_status"] == "frozen"
