from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tdcpass.core.yaml_utils import load_yaml
from tdcpass.analysis.accounting import AccountingSummary
from tdcpass.pipeline.quarterly import _default_overview_payload
from tdcpass.pipeline.quarterly import run_quarterly_pipeline
from tdcpass.reports.site_export import contract_paths, load_output_contract


def _write_csv(path: Path, columns: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_quarterly_pipeline_materializes_contract_bundle(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    contract = load_output_contract(repo_root / "config" / "output_contract.yml")

    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"
    dest_root.mkdir(parents=True, exist_ok=True)

    panel_columns = [
        "quarter",
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "other_component_qoq",
        "tdc_domestic_bank_only_qoq",
        "tdc_no_remit_bank_only_qoq",
        "tdc_credit_union_sensitive_qoq",
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
    _write_csv(source_root / "data" / "derived" / "quarterly_panel.csv", panel_columns, [["2000Q1"] + [1] * (len(panel_columns) - 1)])

    csv_artifacts = {
        "output/accounting/accounting_summary.csv",
        "output/accounting/quarters_tdc_exceeds_total.csv",
        "output/shocks/unexpected_tdc.csv",
        "output/models/lp_irf.csv",
        "output/models/lp_irf_identity_baseline.csv",
        "output/models/lp_irf_regimes.csv",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/period_sensitivity.csv",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/proxy_unit_audit.json",
        "output/models/sample_construction_summary.json",
    }
    json_artifacts = {
        "output/models/regime_diagnostics_summary.json",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/headline_treatment_fingerprint.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/period_sensitivity_summary.json",
    }
    for rel in csv_artifacts:
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
            _write_csv(
                source_root / rel,
                ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name", "decomposition_mode", "outcome_construction", "inference_method"],
                [["total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_baseline", "exact_identity_baseline", "estimated", "shared_bootstrap"]],
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
        elif rel.endswith("total_minus_other_contrast.csv"):
            _write_csv(source_root / rel, ["scope", "variant", "role", "horizon", "beta_total", "beta_other", "beta_implied", "beta_direct", "gap_implied_minus_direct", "abs_gap", "n_total", "n_other", "n_direct", "sample_mismatch_flag"], [["baseline", "baseline", "headline", 0, 0.1, 0.0, 0.1, 0.1, 0.0, 0.0, 1, 1, 1, False]])
        elif rel.endswith("structural_proxy_evidence.csv"):
            _write_csv(source_root / rel, ["scope", "context", "horizon", "other_outcome", "other_beta", "other_se", "other_lower95", "other_upper95", "other_ci_excludes_zero", "proxy_outcome", "proxy_beta", "proxy_se", "proxy_lower95", "proxy_upper95", "proxy_ci_excludes_zero", "other_sign", "proxy_sign", "sign_alignment", "evidence_label", "proxy_share_of_other_beta"], [["baseline", "baseline", 0, "other_component_qoq", 0.1, 0.01, 0.0, 0.2, False, "bank_credit_private_qoq", 0.1, 0.01, 0.0, 0.2, False, "positive", "positive", "same_sign", "ambiguous", 1.0]])
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
    for rel in json_artifacts:
        if rel.endswith("regime_diagnostics_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "informative_regime_count": 1,
                    "stable_regime_count": 1,
                    "regimes": [],
                    "takeaways": ["stub"],
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
        elif rel.endswith("headline_treatment_fingerprint.json"):
            _write_json(
                source_root / rel,
                {
                    "treatment_freeze_status": "frozen",
                    "model_name": "unexpected_tdc_default",
                    "target": "tdc_bank_only_qoq",
                    "method": "rolling_window_ridge",
                    "predictors": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                    "min_train_obs": 24,
                    "max_train_obs": 40,
                    "usable_sample": {"start_quarter": "2009Q1", "end_quarter": "2025Q4", "observations": 68},
                    "git_commit": "stub",
                },
            )
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
        elif rel.endswith("period_sensitivity_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "materialized",
                    "headline_question": "stub",
                    "periods": [],
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        else:
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

    result = run_quarterly_pipeline(
        base_dir=dest_root,
        source_root=source_root,
        overview_payload={
            "headline_metrics": {"share_other_negative": 0.0},
            "sample": {"frequency": "quarterly", "rows": 1},
            "main_findings": ["stub"],
            "caveats": ["stub"],
            "evidence_tiers": {"direct_data": ["tdc_bank_only_qoq"]},
            "artifacts": ["site/data/accounting_summary.csv"],
        },
    )

    for rel in contract_paths(contract):
        assert (dest_root / rel).exists(), rel

    overview = json.loads((dest_root / "site" / "data" / "overview.json").read_text(encoding="utf-8"))
    assert overview["headline_metrics"]["share_other_negative"] == 0.0
    assert overview["sample"]["frequency"] == "quarterly"
    assert overview["evidence_tiers"]["direct_data"] == ["tdc_bank_only_qoq"]
    assert (dest_root / "output" / "manifests" / "pipeline_run.json").exists()
    assert (dest_root / "output" / "manifests" / "raw_downloads.json").exists()
    assert (dest_root / "output" / "manifests" / "reused_artifacts.json").exists()
    assert result["pipeline_run_path"].endswith("output/manifests/pipeline_run.json")

    pipeline_run = json.loads((dest_root / "output" / "manifests" / "pipeline_run.json").read_text(encoding="utf-8"))
    assert pipeline_run["command"] == "pipeline run"
    assert pipeline_run["outputs"]


def test_contract_loader_reads_frozen_layout() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    payload = load_yaml(repo_root / "config" / "output_contract.yml")
    assert "site/data/overview.json" in {item["path"] for item in payload["artifacts"]}


def test_default_overview_payload_stays_methods_preview() -> None:
    panel = pd.DataFrame(
        {
            "quarter": ["1954Q4", "1955Q1", "1955Q2"],
            "bill_share": [0.7, 0.8, 0.9],
        }
    )
    shocked = pd.DataFrame(
        {
            "quarter": ["1954Q4", "1960Q4", "2025Q4"],
            "tdc_residual_z": [None, 0.1, -0.1],
        }
    )
    accounting = AccountingSummary(
        mean_tdc=0.1,
        mean_total_deposits=1.0,
        mean_other_component=0.9,
        share_other_negative=0.2,
        correlation_tdc_total=0.0,
        correlation_tdc_other=0.0,
    )

    payload = _default_overview_payload(
        panel=panel,
        shocked=shocked,
        accounting_summary=accounting,
        readiness={"status": "not_ready"},
        root=Path("/tmp/tdcpass"),
    )

    assert "methods-and-reproducibility preview centered on the frozen rolling 40-quarter ridge unexpected-TDC shock" in payload["main_findings"][1]
    assert "1960Q4" in payload["main_findings"][2]
    assert "`not_ready`" in payload["main_findings"][2]
    assert "deposit-response readout" in payload["caveats"][0]
