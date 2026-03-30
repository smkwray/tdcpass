from __future__ import annotations

import json
from pathlib import Path

from tdcpass.cli import build_parser, main


def _write_csv(path: Path, header: list[str], row: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ",".join(header) + "\n" + ",".join(str(item) for item in row) + "\n",
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


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
        "output/models/lp_irf_regimes.csv",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
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
                    "shock_flag",
                ],
                [["2000Q1", 1.0, 0.8, 0.2, 0.2, "stub", 1, 10.0, 0.5, 0.2, 0.8, ""]],
            )
        elif rel.endswith("lp_irf.csv"):
            _write_csv(source_root / rel, ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("lp_irf_regimes.csv"):
            _write_csv(source_root / rel, ["regime", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["reserve_drain_high", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("tdc_sensitivity_ladder.csv"):
            _write_csv(source_root / rel, ["treatment_variant", "treatment_role", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["tdc_bank_only_qoq", "core", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "baseline"]])
        elif rel.endswith("control_set_sensitivity.csv"):
            _write_csv(source_root / rel, ["control_variant", "control_role", "control_columns", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["headline_lagged_macro", "headline", "lag_fedfunds|lag_unemployment|lag_inflation", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "control_sensitivity"]])
        elif rel.endswith("shock_sample_sensitivity.csv"):
            _write_csv(source_root / rel, ["sample_variant", "sample_role", "sample_filter", "outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"], [["all_usable_shocks", "headline", "all_usable_shocks", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "sample_sensitivity"]])
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
        elif rel.endswith("pass_through_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "not_ready",
                    "headline_question": "stub",
                    "headline_answer": "stub",
                    "sample_policy": {"headline_sample_variant": "all_usable_shocks"},
                    "baseline_horizons": {},
                    "core_treatment_variants": [],
                    "core_control_variants": [],
                    "shock_sample_variants": [],
                    "structural_proxy_context": {},
                    "proxy_coverage_context": {},
                    "published_regime_contexts": [],
                    "readiness_reasons": ["stub"],
                    "readiness_warnings": [],
                },
            )
        elif rel.endswith("direct_identification_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "not_ready",
                    "headline_question": "stub",
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


def test_demo_command_still_exists() -> None:
    parser = build_parser()
    parsed = parser.parse_args(["demo"])
    assert parsed.command == "demo"
