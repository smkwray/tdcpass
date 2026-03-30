from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from tdcpass.analysis.accounting import (
    build_accounting_summary,
    build_quarters_tdc_exceeds_total,
    compute_other_component,
    summary_to_frame,
)
from tdcpass.analysis.direct_identification import (
    build_direct_identification_summary,
    build_total_minus_other_contrast,
)
from tdcpass.analysis.local_projections import run_lp_from_specs
from tdcpass.analysis.pass_through_summary import build_pass_through_summary
from tdcpass.analysis.proxy_coverage import build_proxy_coverage_summary
from tdcpass.analysis.regime_diagnostics import build_regime_diagnostics_summary
from tdcpass.analysis.result_readiness import build_result_readiness_summary
from tdcpass.analysis.shock_diagnostics import build_shock_diagnostics_summary
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.analysis.structural_proxy_evidence import build_structural_proxy_evidence
from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.pipeline.build_panel import build_public_quarterly_panel, load_panel
from tdcpass.reports.site_export import (
    contract_paths,
    export_frame,
    load_output_contract,
    mirror_contract_artifacts,
    overview_artifact_path,
    write_json_payload,
    write_overview_json,
    write_pipeline_manifests,
)

GENERATED_CONTRACT_PATHS = {
    "output/manifests/raw_downloads.json",
    "output/manifests/reused_artifacts.json",
    "output/manifests/pipeline_run.json",
    "site/data/overview.json",
}


def _config_path(name: str) -> Path:
    return repo_root() / "config" / name


def _default_overview_payload(*, panel: Any, accounting_summary: Any, root: Path) -> dict[str, Any]:
    share_other_negative = float(accounting_summary.share_other_negative)
    return {
        "headline_metrics": {
            "share_other_negative": share_other_negative,
            "mean_tdc": float(accounting_summary.mean_tdc),
            "mean_total_deposits": float(accounting_summary.mean_total_deposits),
            "median_bill_share": float(panel["bill_share"].median()),
        },
        "sample": {
            "frequency": "quarterly",
            "rows": int(len(panel)),
            "start_quarter": str(panel["quarter"].iloc[0]),
            "end_quarter": str(panel["quarter"].iloc[-1]),
            "source_root": str(root),
        },
        "main_findings": [
            "Quarterly public-data bundle materialized from direct official-source rebuild with optional sibling-cache reuse.",
            "This public prototype is a methods-and-reproducibility preview, not a settled pass-through-versus-crowd-out result.",
            f"{share_other_negative:.1%} of quarters show `other_component_qoq < 0` in the headline sample.",
            "The expanded structural proxy bundle is useful for partial mechanism cross-checks, but it does not yet close the non-TDC residual at key horizons.",
        ],
        "caveats": [
            "Current release wording is gated by readiness diagnostics: the live bundle should be read as `not_ready` for a clean headline causal claim until the response separation improves.",
            "The bill_share regime is a quarterly issue-date share of Treasury bill auction offering amounts from FiscalData; it is a regime overlay, not standalone mechanism proof.",
            "Structural proxies are partial cross-checks on the residual, not exhaustive counterpart accounting or standalone mechanism proof.",
        ],
        "evidence_tiers": {
            "direct_data": [
                "tdc_bank_only_qoq",
                "total_deposits_bank_qoq",
                "bill_share",
                "fedfunds",
                "unemployment",
                "inflation",
            ],
            "transparent_transformations": [
                "other_component_qoq",
                "bank_credit_private_qoq",
                "cb_nonts_qoq",
                "foreign_nonts_qoq",
                "domestic_nonfinancial_mmf_reallocation_qoq",
                "domestic_nonfinancial_repo_reallocation_qoq",
                "bank_absorption_share",
                "reserve_drain_pressure",
                "quarter_index",
                "slr_tight",
            ],
            "model_based_estimates": [
                "tdc_fitted",
                "tdc_residual",
                "tdc_residual_z",
                "lp_irf",
                "lp_irf_regimes",
                "tdc_sensitivity_ladder",
                "control_set_sensitivity",
                "shock_sample_sensitivity",
                "total_minus_other_contrast",
                "structural_proxy_evidence",
                "proxy_coverage_summary",
                "proxy_unit_audit",
            ],
            "inferred_counterfactuals": [
                "pass_through_h0_h8",
                "crowd_out_h0_h8",
                "regime_split_pass_through",
                "direct_identification_summary",
                "pass_through_summary",
            ],
        },
        "artifacts": [
            "site/data/accounting_summary.csv",
            "site/data/unexpected_tdc.csv",
            "site/data/lp_irf.csv",
            "site/data/regime_diagnostics_summary.json",
            "site/data/control_set_sensitivity.csv",
            "site/data/shock_sample_sensitivity.csv",
            "site/data/total_minus_other_contrast.csv",
            "site/data/structural_proxy_evidence.csv",
            "site/data/structural_proxy_evidence_summary.json",
            "site/data/proxy_coverage_summary.json",
            "site/data/proxy_unit_audit.json",
            "site/data/shock_diagnostics_summary.json",
            "site/data/result_readiness_summary.json",
            "site/data/direct_identification_summary.json",
            "site/data/pass_through_summary.json",
        ],
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_columns(contract: Mapping[str, Any], artifact_path: str) -> list[str]:
    for artifact in contract.get("artifacts", []):
        if artifact.get("path") == artifact_path:
            return [str(item) for item in artifact.get("required_columns", [])]
    raise KeyError(f"Artifact not found in contract: {artifact_path}")


def _apply_shock_spec(df: Any, spec: Mapping[str, Any]) -> Any:
    return expanding_window_residual(
        df,
        target=str(spec["target"]),
        predictors=[str(item) for item in spec["predictors"]],
        min_train_obs=int(spec["min_train_obs"]),
        standardize=bool(spec.get("standardize_residual", True)),
        model_name=str(spec.get("model_name", "unexpected_tdc_default")),
        fitted_column=str(spec.get("fitted_column", "tdc_fitted")),
        residual_column=str(spec.get("residual_column", "tdc_residual")),
        standardized_column=str(spec.get("standardized_column", "tdc_residual_z")),
        train_start_obs_column=str(spec.get("train_start_obs_column", "train_start_obs")),
        model_name_column=str(spec.get("model_name_column", "model_name")),
        condition_number_column=str(spec.get("condition_number_column", "train_condition_number")),
        target_sd_column=str(spec.get("target_sd_column", "train_target_sd")),
        residual_sd_column=str(spec.get("residual_sd_column", "train_resid_sd")),
        scale_ratio_column=str(spec.get("scale_ratio_column", "fitted_to_target_scale_ratio")),
        flag_column=str(spec.get("flag_column", "shock_flag")),
        max_condition_number=float(spec["max_condition_number"]) if spec.get("max_condition_number") is not None else None,
        max_scale_ratio=float(spec["max_scale_ratio"]) if spec.get("max_scale_ratio") is not None else None,
    )


def _write_sample_construction_summary(
    path: Path,
    *,
    shocked: Any,
    shock_spec: Mapping[str, Any],
) -> Path:
    payload = _load_json(path)
    shock_column = str(shock_spec.get("standardized_column", "tdc_residual_z"))
    usable = shocked.dropna(subset=[shock_column]).copy() if shock_column in shocked.columns else shocked.iloc[0:0].copy()
    payload["usable_shock_sample"] = {
        "rows": int(len(usable)),
        "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
        "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
    }
    payload["shock_definition"] = {
        "shock_column": shock_column,
        "target": str(shock_spec.get("target", "")),
        "model_name": str(shock_spec.get("model_name", "")),
        "predictors": [str(item) for item in shock_spec.get("predictors", [])],
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
    }
    payload["takeaways"] = list(payload.get("takeaways", [])) + [
        "Usable shock counts are reported separately from headline panel rows because the expanding-window burn-in is part of the treatment definition."
    ]
    return write_json_payload(path, payload)


def _materialize_real_outputs(
    root: Path,
    contract: Mapping[str, Any],
    *,
    reuse_mode: str | None = None,
    raw_fixture_root: Path | None = None,
) -> dict[str, str]:
    build_result = build_public_quarterly_panel(root, reuse_mode=reuse_mode, fixture_root=raw_fixture_root)
    panel = compute_other_component(load_panel(build_result.panel_path))

    accounting_summary = build_accounting_summary(panel)
    accounting_summary_path = root / "output" / "accounting" / "accounting_summary.csv"
    export_frame(summary_to_frame(accounting_summary), accounting_summary_path)

    quarters_exceeds_path = root / "output" / "accounting" / "quarters_tdc_exceeds_total.csv"
    export_frame(build_quarters_tdc_exceeds_total(panel), quarters_exceeds_path)

    all_shock_specs = load_yaml(_config_path("shock_specs.yml"))["shocks"]
    baseline_shock_spec = all_shock_specs["unexpected_tdc_default"]
    shocked = _apply_shock_spec(panel, baseline_shock_spec)
    for shock_name, shock_spec in all_shock_specs.items():
        if shock_name == "unexpected_tdc_default":
            continue
        shocked = _apply_shock_spec(shocked, shock_spec)

    shocks_path = root / "output" / "shocks" / "unexpected_tdc.csv"
    export_frame(shocked[_artifact_columns(contract, "output/shocks/unexpected_tdc.csv")], shocks_path)
    sample_construction_summary_path = _write_sample_construction_summary(
        build_result.sample_construction_summary_path,
        shocked=shocked,
        shock_spec=baseline_shock_spec,
    )

    lp_specs = load_yaml(_config_path("lp_specs.yml"))
    regime_specs = load_yaml(_config_path("regime_specs.yml"))
    lp_outputs = run_lp_from_specs(
        shocked,
        lp_specs=lp_specs,
        regime_specs=regime_specs,
    )
    lp_irf_path = root / "output" / "models" / "lp_irf.csv"
    export_frame(lp_outputs["lp_irf"], lp_irf_path)
    lp_irf_regimes_path = root / "output" / "models" / "lp_irf_regimes.csv"
    export_frame(lp_outputs["lp_irf_regimes"], lp_irf_regimes_path)
    regime_diagnostics_path = root / "output" / "models" / "regime_diagnostics_summary.json"
    regime_diagnostics = build_regime_diagnostics_summary(
        panel=shocked,
        regime_specs=regime_specs,
        selected_regime_columns={str(col) for col in lp_specs["specs"]["regimes"].get("regime_columns", [])},
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        shock_column=str(lp_specs["specs"]["regimes"]["shock_column"]),
        controls=[str(col) for col in lp_specs["specs"]["regimes"].get("controls", [])],
    )
    write_json_payload(
        regime_diagnostics_path,
        regime_diagnostics,
    )
    sensitivity_path = root / "output" / "models" / "tdc_sensitivity_ladder.csv"
    export_frame(lp_outputs["tdc_sensitivity_ladder"], sensitivity_path)
    control_set_sensitivity_path = root / "output" / "models" / "control_set_sensitivity.csv"
    export_frame(lp_outputs["control_set_sensitivity"], control_set_sensitivity_path)
    sample_sensitivity_path = root / "output" / "models" / "shock_sample_sensitivity.csv"
    export_frame(lp_outputs["shock_sample_sensitivity"], sample_sensitivity_path)
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_outputs["lp_irf"],
        sensitivity=lp_outputs["tdc_sensitivity_ladder"],
        control_sensitivity=lp_outputs["control_set_sensitivity"],
        sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
    )
    contrast_path = root / "output" / "models" / "total_minus_other_contrast.csv"
    export_frame(contrast, contrast_path)
    structural_proxy_frame, structural_proxy_summary = build_structural_proxy_evidence(
        lp_irf=lp_outputs["lp_irf"],
    )
    structural_proxy_path = root / "output" / "models" / "structural_proxy_evidence.csv"
    export_frame(structural_proxy_frame, structural_proxy_path)
    structural_proxy_summary_path = root / "output" / "models" / "structural_proxy_evidence_summary.json"
    write_json_payload(structural_proxy_summary_path, structural_proxy_summary)
    proxy_coverage_summary_path = root / "output" / "models" / "proxy_coverage_summary.json"
    proxy_coverage_summary = build_proxy_coverage_summary(
        lp_irf=lp_outputs["lp_irf"],
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        regime_diagnostics=regime_diagnostics,
        regime_specs=regime_specs,
        proxy_unit_audit=_load_json(build_result.proxy_unit_audit_path),
    )
    write_json_payload(proxy_coverage_summary_path, proxy_coverage_summary)
    shock_diagnostics_path = root / "output" / "models" / "shock_diagnostics_summary.json"
    write_json_payload(
        shock_diagnostics_path,
        build_shock_diagnostics_summary(
            shocks=shocked,
            sensitivity=lp_outputs["tdc_sensitivity_ladder"],
            baseline_shock_spec=dict(baseline_shock_spec),
            shock_specs=dict(all_shock_specs),
        ),
    )
    direct_identification_path = root / "output" / "models" / "direct_identification_summary.json"
    direct_identification = build_direct_identification_summary(
        lp_irf=lp_outputs["lp_irf"],
        contrast=contrast,
        sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
        shock_metadata=dict(baseline_shock_spec),
    )
    write_json_payload(
        direct_identification_path,
        direct_identification,
    )
    result_readiness_path = root / "output" / "models" / "result_readiness_summary.json"
    readiness_payload = build_result_readiness_summary(
        accounting_summary=accounting_summary,
        shocks=shocked,
        lp_irf=lp_outputs["lp_irf"],
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        sensitivity=lp_outputs["tdc_sensitivity_ladder"],
        control_sensitivity=lp_outputs["control_set_sensitivity"],
        sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
        regime_diagnostics=regime_diagnostics,
        direct_identification=direct_identification,
        contrast=contrast,
        structural_proxy_evidence=structural_proxy_summary,
        proxy_coverage_summary=proxy_coverage_summary,
        headline_shock_metadata=dict(baseline_shock_spec),
    )
    write_json_payload(
        result_readiness_path,
        readiness_payload,
    )
    pass_through_summary_path = root / "output" / "models" / "pass_through_summary.json"
    write_json_payload(
        pass_through_summary_path,
        build_pass_through_summary(
            lp_irf=lp_outputs["lp_irf"],
            sensitivity=lp_outputs["tdc_sensitivity_ladder"],
            control_sensitivity=lp_outputs["control_set_sensitivity"],
            sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
            contrast=contrast,
            lp_irf_regimes=lp_outputs["lp_irf_regimes"],
            readiness=readiness_payload,
            regime_diagnostics=regime_diagnostics,
            regime_specs=regime_specs,
            structural_proxy_evidence=structural_proxy_summary,
            proxy_coverage_summary=proxy_coverage_summary,
        ),
    )

    mirror_contract_artifacts(source_root=root, dest_root=root, contract=contract)
    overview_path = root / overview_artifact_path(contract)
    write_overview_json(
        overview_path,
        **_default_overview_payload(panel=panel, accounting_summary=accounting_summary, root=root),
    )

    raw_download_runs = _load_json(build_result.raw_download_manifest_path).get("runs", [])
    reused_payload = _load_json(build_result.reused_artifacts_path)
    manifest_paths = write_pipeline_manifests(
        root,
        command="pipeline run",
        outputs=[
            build_result.panel_path,
            build_result.proxy_unit_audit_path,
            sample_construction_summary_path,
            accounting_summary_path,
            quarters_exceeds_path,
            shocks_path,
            lp_irf_path,
            lp_irf_regimes_path,
            regime_diagnostics_path,
            sensitivity_path,
            control_set_sensitivity_path,
            sample_sensitivity_path,
            contrast_path,
            structural_proxy_path,
            structural_proxy_summary_path,
            proxy_coverage_summary_path,
            shock_diagnostics_path,
            direct_identification_path,
            result_readiness_path,
            pass_through_summary_path,
            overview_path,
        ],
        raw_download_runs=raw_download_runs,
        reused_artifacts=reused_payload.get("reused_artifacts", []),
        extra={
            "contract_path": str(_config_path("output_contract.yml")),
            "mode": "real",
            "raw_fixture_root": None if raw_fixture_root is None else str(raw_fixture_root),
        },
    )

    return {
        "panel_path": str(build_result.panel_path),
        "proxy_unit_audit_path": str(build_result.proxy_unit_audit_path),
        "sample_construction_summary_path": str(sample_construction_summary_path),
        "accounting_summary_path": str(accounting_summary_path),
        "quarters_tdc_exceeds_total_path": str(quarters_exceeds_path),
        "shocks_path": str(shocks_path),
        "lp_irf_path": str(lp_irf_path),
        "lp_irf_regimes_path": str(lp_irf_regimes_path),
        "regime_diagnostics_path": str(regime_diagnostics_path),
        "sensitivity_path": str(sensitivity_path),
        "control_set_sensitivity_path": str(control_set_sensitivity_path),
        "sample_sensitivity_path": str(sample_sensitivity_path),
        "contrast_path": str(contrast_path),
        "structural_proxy_path": str(structural_proxy_path),
        "structural_proxy_summary_path": str(structural_proxy_summary_path),
        "proxy_coverage_summary_path": str(proxy_coverage_summary_path),
        "shock_diagnostics_path": str(shock_diagnostics_path),
        "direct_identification_path": str(direct_identification_path),
        "result_readiness_path": str(result_readiness_path),
        "pass_through_summary_path": str(pass_through_summary_path),
        "overview_path": str(overview_path),
        "raw_downloads_path": str(manifest_paths["raw_downloads"]),
        "reused_artifacts_path": str(manifest_paths["reused_artifacts"]),
        "pipeline_run_path": str(manifest_paths["pipeline_run"]),
    }


def _materialize_from_source_bundle(
    *,
    root: Path,
    source_root: Path,
    contract: Mapping[str, Any],
    contract_path: Path,
    overview_payload: Mapping[str, Any] | None,
    raw_download_runs: list[Mapping[str, Any]] | None,
    reused_artifacts: list[Mapping[str, Any]] | None,
) -> dict[str, str]:
    materialized_paths = mirror_contract_artifacts(source_root=source_root, dest_root=root, contract=contract)

    overview_path = overview_artifact_path(contract)
    overview = dict(
        overview_payload
        or {
            "headline_metrics": {},
            "sample": {"frequency": "quarterly", "rows": None, "source_root": str(source_root), "layout": "contract_skeleton"},
            "main_findings": ["Quarterly export skeleton materialized from the frozen contract."],
            "caveats": ["This command only freezes the export layout; substantive analysis is wired later."],
            "evidence_tiers": {"contract_skeleton": ["site/data/overview.json"]},
            "artifacts": [path.as_posix() for path in contract_paths(contract, prefix="site/data/") if path.name != "overview.json"],
        }
    )
    write_overview_json(root / overview_path, **overview)

    manifest_outputs = [
        root / "output" / "manifests" / "raw_downloads.json",
        root / "output" / "manifests" / "reused_artifacts.json",
    ]
    data_outputs = [
        root / path
        for path in contract_paths(contract)
        if path.as_posix() not in GENERATED_CONTRACT_PATHS
    ]
    manifest_paths = write_pipeline_manifests(
        root,
        command="pipeline run",
        outputs=[*data_outputs, *manifest_outputs, root / overview_path],
        raw_download_runs=raw_download_runs,
        reused_artifacts=reused_artifacts,
        extra={"contract_path": str(contract_path), "source_root": str(source_root), "mode": "mirror"},
    )

    return {
        "contract_path": str(contract_path),
        "source_root": str(source_root),
        "overview_path": str(root / overview_path),
        "materialized_count": str(len(materialized_paths)),
        "raw_downloads_path": str(manifest_paths["raw_downloads"]),
        "reused_artifacts_path": str(manifest_paths["reused_artifacts"]),
        "pipeline_run_path": str(manifest_paths["pipeline_run"]),
    }


def run_quarterly_pipeline(
    base_dir: Path | None = None,
    *,
    source_root: Path | None = None,
    contract_path: Path | None = None,
    overview_payload: Mapping[str, Any] | None = None,
    raw_download_runs: list[Mapping[str, Any]] | None = None,
    reused_artifacts: list[Mapping[str, Any]] | None = None,
    reuse_mode: str | None = None,
    raw_fixture_root: Path | None = None,
) -> dict[str, str]:
    root = base_dir or repo_root()
    ensure_repo_dirs(root)

    contract_path = contract_path or repo_root() / "config" / "output_contract.yml"
    contract = load_output_contract(contract_path)

    if source_root is not None and source_root.resolve() != root.resolve():
        return _materialize_from_source_bundle(
            root=root,
            source_root=source_root,
            contract=contract,
            contract_path=contract_path,
            overview_payload=overview_payload,
            raw_download_runs=raw_download_runs,
            reused_artifacts=reused_artifacts,
        )

    return _materialize_real_outputs(root, contract, reuse_mode=reuse_mode, raw_fixture_root=raw_fixture_root)
