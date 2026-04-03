from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from tdcpass.analysis.accounting import (
    build_accounting_summary,
    build_quarters_tdc_exceeds_total,
    compute_other_component,
    summary_to_frame,
)
from tdcpass.analysis.backend_decision_bundle import build_backend_decision_bundle
from tdcpass.analysis.backend_closeout_summary import build_backend_closeout_summary
from tdcpass.analysis.backend_evidence_packet import build_backend_evidence_packet
from tdcpass.analysis.counterpart_channel_scorecard import build_counterpart_channel_scorecard
from tdcpass.analysis.direct_identification import (
    build_direct_identification_summary,
    build_total_minus_other_contrast,
)
from tdcpass.analysis.factor_control_diagnostics import build_factor_control_diagnostics_summary
from tdcpass.analysis.identity_baseline import build_identity_baseline_irf, build_identity_variant_ladder
from tdcpass.analysis.local_projections import run_local_projections, run_lp_from_specs
from tdcpass.analysis.pass_through_summary import build_pass_through_summary
from tdcpass.analysis.period_sensitivity import build_period_sensitivity_summary
from tdcpass.analysis.published_state_proxy_comparator import build_published_state_proxy_comparator
from tdcpass.analysis.published_state_proxy_vs_baseline import build_published_state_proxy_vs_baseline_summary
from tdcpass.analysis.proxy_factor_diagnostics import build_proxy_factor_diagnostics
from tdcpass.analysis.proxy_coverage import build_proxy_coverage_summary
from tdcpass.analysis.research_dashboard import build_research_dashboard_summary
from tdcpass.analysis.regime_diagnostics import build_regime_diagnostics_summary
from tdcpass.analysis.result_readiness import build_result_readiness_summary
from tdcpass.analysis.shock_diagnostics import build_shock_diagnostics_summary
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.analysis.smoothed_local_projections import (
    build_smoothed_lp_diagnostics_summary,
    build_smoothed_lp_irf,
)
from tdcpass.analysis.state_proxy_factor_diagnostics import build_state_proxy_factor_diagnostics
from tdcpass.analysis.structural_proxy_evidence import build_structural_proxy_evidence
from tdcpass.analysis.treatment_fingerprint import (
    build_headline_treatment_fingerprint,
    build_headline_treatment_fingerprint_validation_summary,
)
from tdcpass.core.paths import ensure_repo_dirs, repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.pipeline.build_panel import build_public_quarterly_panel, load_panel
from tdcpass.pipeline.call_report_components import build_call_report_deposit_components
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
from tdcpass.reports.published_state_proxy_report import write_published_state_proxy_report
from tdcpass.reports.published_state_proxy_vs_baseline_report import write_published_state_proxy_vs_baseline_report
from tdcpass.reports.research_dashboard_report import write_research_dashboard_report
from tdcpass.reports.backend_decision_bundle_report import write_backend_decision_bundle_report
from tdcpass.reports.backend_closeout_report import write_backend_closeout_report
from tdcpass.reports.backend_evidence_packet_report import write_backend_evidence_packet_report

GENERATED_CONTRACT_PATHS = {
    "output/manifests/raw_downloads.json",
    "output/manifests/reused_artifacts.json",
    "output/manifests/pipeline_run.json",
    "site/data/overview.json",
}


def _config_path(name: str) -> Path:
    return repo_root() / "config" / name


def _default_overview_payload(
    *,
    panel: Any,
    shocked: Any,
    accounting_summary: Any,
    readiness: Mapping[str, Any],
    counterpart_channel_scorecard: Mapping[str, Any] | None = None,
    root: Path,
) -> dict[str, Any]:
    share_other_negative = float(accounting_summary.share_other_negative)
    usable_shock_rows = 0
    usable_shock_start = None
    usable_shock_end = None
    if "tdc_residual_z" in shocked.columns:
        usable = shocked.dropna(subset=["tdc_residual_z"]).copy()
        usable_shock_rows = int(len(usable))
        if usable_shock_rows:
            usable_shock_start = str(usable["quarter"].iloc[0])
            usable_shock_end = str(usable["quarter"].iloc[-1])
    readiness_status = str(readiness.get("status", "not_ready"))
    counterpart_key_horizons = (
        {} if counterpart_channel_scorecard is None else dict(counterpart_channel_scorecard.get("key_horizons", {}))
    )
    h0_counterpart = dict(counterpart_key_horizons.get("h0", {}))
    h4_counterpart = dict(counterpart_key_horizons.get("h4", {}))
    h8_counterpart = dict(counterpart_key_horizons.get("h8", {}))
    counterpart_findings: list[str] = []
    if counterpart_channel_scorecard is not None:
        if (
            not h0_counterpart.get("decisive_positive_core_creator_channels", [])
            and not list(counterpart_channel_scorecard.get("creator_channel_outcomes_present", []))
        ):
            counterpart_findings.append(
                "Counterpart channels do not show a decisive positive core creator-lending offset on impact; the current broad creator surface does not explain the negative non-TDC residual at h0."
            )
        external_horizons = [h0_counterpart, h4_counterpart, h8_counterpart]
        if any(
            "foreign_nonts_qoq"
            in list(dict(horizon_payload.get("external_channel_block", {})).get("decisive_positive_external_channels", []) or [])
            for horizon_payload in external_horizons
        ):
            counterpart_findings.append(
                "The currently materialized external counterpart preview most clearly shows foreign nontransaction pressure; creator, domestic escape, and funding lanes should still be read as partial diagnostics rather than settled mechanism evidence."
            )
        counterpart_findings.append(
            "See counterpart_channel_scorecard.json for the creator, escape, external, and funding blocks rather than relying on bank_credit_private_qoq alone."
        )
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
        },
        "main_findings": [
            "Quarterly public-data bundle materialized from direct official-source rebuild with optional sibling-cache reuse.",
            "This release is a methods-and-reproducibility preview centered on the frozen rolling 40-quarter ridge unexpected-TDC shock.",
            (
                f"The repaired baseline shock passes its treatment-quality gate on {usable_shock_rows} usable quarters "
                f"from {usable_shock_start} to {usable_shock_end}, but the current release status remains `{readiness_status}`."
            ),
            "The exact identity-preserving baseline is now the primary decomposition path; the older outcome-specific LP contrast remains a secondary robustness check only.",
            "The public surface keeps the exact identity baseline as the headline path and treats counterpart or deposit side reads as secondary diagnostics rather than settled mechanism evidence.",
            "Period sensitivity remains on the public surface because medium-horizon persistence differs across the post-GFC early, pre-COVID, and COVID/post-COVID windows.",
            f"{share_other_negative:.1%} of quarters show `other_component_qoq < 0` in the headline sample.",
            *counterpart_findings,
        ],
        "caveats": [
            "Current release wording is gated by readiness diagnostics: the live bundle should be read as an exploratory deposit-response readout, not a clean headline causal decomposition, until total and non-TDC responses separate more clearly.",
            "When the exact identity baseline and the older approximate dynamic LP path disagree, interpretation should default to the exact baseline and treat the older path as a robustness diagnostic rather than the headline read.",
            "Headline pass-through and crowd-out ratios are out of scope in the current release until the repo has a dimensionally coherent first-stage gate for raw-unit treatment responses.",
            "bill_share is a quarterly issue-date share of Treasury bill auction offering amounts from FiscalData; it remains an exploratory sensitivity input, not a live regime export or standalone mechanism proof.",
            "bill_share-linked shock variants are retained only as exploratory stress tests because they preserve the impact-stage sign pattern but change medium-horizon persistence enough that they should not be promoted into the headline treatment family.",
            "Structural proxies are partial cross-checks on the residual, not exhaustive counterpart accounting or standalone mechanism proof.",
        ],
        "evidence_tiers": {
            "direct_data": [
                "tdc_bank_only_qoq",
                "total_deposits_bank_qoq",
                "checkable_deposits_bank_qoq",
                "interbank_transactions_bank_qoq",
                "time_savings_deposits_bank_qoq",
                "checkable_federal_govt_bank_qoq",
                "checkable_state_local_bank_qoq",
                "checkable_rest_of_world_bank_qoq",
                "checkable_private_domestic_bank_qoq",
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
                "reserve_drain_pressure",
                "quarter_index",
            ],
            "model_based_estimates": [
                "tdc_fitted",
                "tdc_residual",
                "tdc_residual_z",
                "lp_irf",
                "lp_irf_identity_baseline",
                "identity_measurement_ladder",
                "lp_irf_regimes",
                "tdc_sensitivity_ladder",
                "control_set_sensitivity",
                "shock_sample_sensitivity",
                "period_sensitivity",
                "total_minus_other_contrast",
                "structural_proxy_evidence",
                "proxy_coverage_summary",
                "proxy_unit_audit",
                "call_report_deposit_components",
                "headline_treatment_fingerprint",
                "provenance_validation_summary",
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
            "site/data/lp_irf_identity_baseline.csv",
            "site/data/identity_measurement_ladder.csv",
            "site/data/regime_diagnostics_summary.json",
            "site/data/control_set_sensitivity.csv",
            "site/data/shock_sample_sensitivity.csv",
            "site/data/period_sensitivity.csv",
            "site/data/period_sensitivity_summary.json",
            "site/data/total_minus_other_contrast.csv",
            "site/data/structural_proxy_evidence.csv",
            "site/data/structural_proxy_evidence_summary.json",
            "site/data/proxy_coverage_summary.json",
            "site/data/proxy_unit_audit.json",
            "site/data/headline_treatment_fingerprint.json",
            "site/data/provenance_validation_summary.json",
            "site/data/shock_diagnostics_summary.json",
            "site/data/result_readiness_summary.json",
            "site/data/direct_identification_summary.json",
            "site/data/pass_through_summary.json",
            "site/data/deposit_type_side_read.csv",
            "site/data/counterpart_channel_scorecard.json",
        ],
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_columns(contract: Mapping[str, Any], artifact_path: str) -> list[str]:
    for artifact in contract.get("artifacts", []):
        if artifact.get("path") == artifact_path:
            return [str(item) for item in artifact.get("required_columns", [])]
    raise KeyError(f"Artifact not found in contract: {artifact_path}")


def _treatment_freeze_status(shock_spec: Mapping[str, Any]) -> str:
    return str(shock_spec.get("freeze_status", "frozen"))


def _treatment_candidates(shock_specs: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for name, spec in shock_specs.items():
        if not isinstance(spec, Mapping):
            continue
        if str(spec.get("candidate_role", "")) != "repair_candidate":
            continue
        candidates.append(
            {
                "name": str(name),
                "model_name": str(spec.get("model_name", "")),
                "shock_column": str(spec.get("standardized_column", "")),
                "raw_shock_column": str(spec.get("residual_column", "")),
                "target": str(spec.get("target", "")),
                "method": str(spec.get("method", "expanding_window_ols")),
                "min_train_obs": int(spec.get("min_train_obs", 0)),
                "max_train_obs": None if spec.get("max_train_obs") is None else int(spec.get("max_train_obs")),
                "predictors": [str(item) for item in spec.get("predictors", [])],
            }
        )
    return candidates


def _usable_shock_sample_window(shocked: Any, shock_spec: Mapping[str, Any]) -> dict[str, Any]:
    shock_column = str(shock_spec.get("standardized_column", "tdc_residual_z"))
    usable = shocked.dropna(subset=[shock_column]).copy() if shock_column in shocked.columns else shocked.iloc[0:0].copy()
    return {
        "shock_column": shock_column,
        "rows": int(len(usable)),
        "start_quarter": None if usable.empty else str(usable["quarter"].iloc[0]),
        "end_quarter": None if usable.empty else str(usable["quarter"].iloc[-1]),
    }


def _apply_shock_spec(df: Any, spec: Mapping[str, Any]) -> Any:
    method = str(spec.get("method", "expanding_window_ols"))
    if method not in {
        "expanding_window_ols",
        "rolling_window_ols",
        "expanding_window_ridge",
        "rolling_window_ridge",
    }:
        raise ValueError(f"Unsupported shock method: {method}")
    return expanding_window_residual(
        df,
        target=str(spec["target"]),
        predictors=[str(item) for item in spec["predictors"]],
        min_train_obs=int(spec["min_train_obs"]),
        max_train_obs=None if spec.get("max_train_obs") is None else int(spec["max_train_obs"]),
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
        train_target_scale_ratio_column=str(spec.get("train_target_scale_ratio_column", "fitted_to_train_target_sd_ratio")),
        flag_column=str(spec.get("flag_column", "shock_flag")),
        max_condition_number=float(spec["max_condition_number"]) if spec.get("max_condition_number") is not None else None,
        max_scale_ratio=float(spec["max_scale_ratio"]) if spec.get("max_scale_ratio") is not None else None,
        ridge_alpha=float(spec["ridge_alpha"]) if "ridge_alpha" in spec and spec.get("ridge_alpha") is not None else None,
    )


def _write_sample_construction_summary(
    path: Path,
    *,
    shocked: Any,
    shock_spec: Mapping[str, Any],
    shock_specs: Mapping[str, Any],
) -> Path:
    payload = _load_json(path)
    payload["usable_shock_sample"] = _usable_shock_sample_window(shocked, shock_spec)
    payload["shock_definition"] = {
        "shock_column": str(shock_spec.get("standardized_column", "tdc_residual_z")),
        "target": str(shock_spec.get("target", "")),
        "model_name": str(shock_spec.get("model_name", "")),
        "predictors": [str(item) for item in shock_spec.get("predictors", [])],
        "min_train_obs": int(shock_spec.get("min_train_obs", 0)),
    }
    payload["treatment_freeze_status"] = _treatment_freeze_status(shock_spec)
    payload["treatment_candidates"] = _treatment_candidates(shock_specs)
    payload["candidate_usable_shock_samples"] = [
        {
            **candidate,
            **_usable_shock_sample_window(shocked, shock_specs[candidate["name"]]),
        }
        for candidate in payload["treatment_candidates"]
    ]
    payload["takeaways"] = list(payload.get("takeaways", [])) + [
        "Usable shock counts are reported separately from headline panel rows because treatment-model burn-in is part of the frozen shock definition."
    ]
    return write_json_payload(path, payload)


def _build_identity_measurement_ladder(
    shocked: Any,
    *,
    lp_specs: Mapping[str, Any],
    shock_specs: Mapping[str, Any],
) -> Any:
    sensitivity_spec = lp_specs["specs"]["sensitivity"]
    shock_variants = sensitivity_spec.get("shock_variants", {})
    shock_specs_by_column = {
        str(spec.get("standardized_column", "")): dict(spec)
        for spec in shock_specs.values()
        if isinstance(spec, Mapping) and spec.get("standardized_column")
    }
    variants: list[dict[str, Any]] = []
    for variant_name, variant_spec in shock_variants.items():
        if not isinstance(variant_spec, Mapping):
            continue
        if str(variant_spec.get("treatment_family", "")) != "measurement":
            continue
        shock_column = str(variant_spec.get("shock_column", ""))
        resolved_spec = shock_specs_by_column.get(shock_column)
        if resolved_spec is None:
            continue
        variants.append(
            {
                "treatment_variant": str(variant_name),
                "treatment_role": str(variant_spec.get("treatment_role", "")),
                "treatment_family": str(variant_spec.get("treatment_family", "")),
                "shock_column": shock_column,
                "target": str(resolved_spec.get("target", "")),
                "controls": [str(item) for item in resolved_spec.get("predictors", [])],
            }
        )

    baseline_lp_spec = lp_specs["specs"]["baseline"]
    return build_identity_variant_ladder(
        shocked,
        variants=variants,
        total_outcome_col="total_deposits_bank_qoq",
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="identity_measurement_ladder",
    )


def _build_identity_treatment_sensitivity(
    shocked: Any,
    *,
    lp_specs: Mapping[str, Any],
    shock_specs: Mapping[str, Any],
) -> Any:
    sensitivity_spec = lp_specs["specs"]["sensitivity"]
    shock_variants = sensitivity_spec.get("shock_variants", {})
    shock_specs_by_column = {
        str(spec.get("standardized_column", "")): dict(spec)
        for spec in shock_specs.values()
        if isinstance(spec, Mapping) and spec.get("standardized_column")
    }
    variants: list[dict[str, Any]] = []
    for variant_name, variant_spec in shock_variants.items():
        if not isinstance(variant_spec, Mapping):
            continue
        shock_column = str(variant_spec.get("shock_column", ""))
        resolved_spec = shock_specs_by_column.get(shock_column)
        if resolved_spec is None:
            continue
        variants.append(
            {
                "treatment_variant": str(variant_name),
                "treatment_role": str(variant_spec.get("treatment_role", "")),
                "treatment_family": str(variant_spec.get("treatment_family", "")),
                "shock_column": shock_column,
                "target": str(resolved_spec.get("target", "")),
                "controls": [str(item) for item in resolved_spec.get("predictors", [])],
            }
        )
    return build_identity_variant_ladder(
        shocked,
        variants=variants,
        total_outcome_col="total_deposits_bank_qoq",
        horizons=[int(h) for h in sensitivity_spec.get("horizons", [])],
        cumulative=bool(sensitivity_spec.get("cumulative", True)),
        spec_name="identity_treatment_sensitivity",
    )


def _build_identity_control_sensitivity(
    shocked: Any,
    *,
    lp_specs: Mapping[str, Any],
    baseline_shock_spec: Mapping[str, Any],
) -> Any:
    control_spec = lp_specs["specs"]["control_sensitivity"]
    frames = []
    for variant_name, variant_spec in control_spec.get("control_variants", {}).items():
        if not isinstance(variant_spec, Mapping):
            continue
        controls = [str(col) for col in variant_spec.get("controls", [])]
        frame = build_identity_baseline_irf(
            shocked,
            shock_col=str(control_spec.get("shock_column", "tdc_residual_z")),
            tdc_outcome_col=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
            total_outcome_col="total_deposits_bank_qoq",
            controls=controls,
            horizons=[int(h) for h in control_spec.get("horizons", [])],
            cumulative=bool(control_spec.get("cumulative", True)),
            spec_name="identity_control_sensitivity",
            nested_shock_spec=dict(baseline_shock_spec),
        )
        if frame.empty:
            continue
        frame.insert(0, "control_columns", "|".join(controls))
        frame.insert(0, "control_role", str(variant_spec.get("control_role", "")))
        frame.insert(0, "control_variant", str(variant_name))
        frames.append(frame)
    if not frames:
        return pd.DataFrame(
            columns=[
                "control_variant",
                "control_role",
                "control_columns",
                "outcome",
                "horizon",
                "beta",
                "se",
                "lower95",
                "upper95",
                "n",
                "spec_name",
                "shock_column",
                "shock_scale",
                "response_type",
                "decomposition_mode",
                "outcome_construction",
                "inference_method",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _build_identity_sample_sensitivity(
    shocked: Any,
    *,
    lp_specs: Mapping[str, Any],
    baseline_shock_spec: Mapping[str, Any],
) -> Any:
    sample_spec = lp_specs["specs"]["sample_sensitivity"]
    frames = []
    for variant_name, variant_spec in sample_spec.get("sample_variants", {}).items():
        if not isinstance(variant_spec, Mapping):
            continue
        flag_column = str(variant_spec.get("flag_column", "shock_flag"))
        exclude_flagged = bool(variant_spec.get("exclude_flagged_shocks", False))
        sample_mask = pd.Series(True, index=shocked.index, dtype=bool)
        sample_filters: list[str] = []
        if exclude_flagged:
            if flag_column not in shocked.columns:
                raise KeyError(f"Missing sample_sensitivity flag column: {flag_column}")
            sample_mask = shocked[flag_column].fillna("").astype(str).eq("")
            sample_filters.append(f"{flag_column}==''")
        max_value_column = variant_spec.get("max_value_column")
        if max_value_column is not None:
            max_value_column = str(max_value_column)
            if max_value_column not in shocked.columns:
                raise KeyError(f"Missing sample_sensitivity max_value_column: {max_value_column}")
            max_value = float(variant_spec["max_value"])
            sample_mask = sample_mask & shocked[max_value_column].le(max_value)
            sample_filters.append(f"{max_value_column}<={max_value}")
        sample_filter = "all_usable_shocks" if not sample_filters else " & ".join(sample_filters)
        frame = build_identity_baseline_irf(
            shocked.loc[sample_mask].copy(),
            shock_col=str(sample_spec.get("shock_column", "tdc_residual_z")),
            tdc_outcome_col=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
            total_outcome_col="total_deposits_bank_qoq",
            controls=[str(col) for col in sample_spec.get("controls", [])],
            horizons=[int(h) for h in sample_spec.get("horizons", [])],
            cumulative=bool(sample_spec.get("cumulative", True)),
            spec_name="identity_sample_sensitivity",
            nested_shock_spec=dict(baseline_shock_spec),
        )
        if frame.empty:
            continue
        frame.insert(0, "sample_filter", sample_filter)
        frame.insert(0, "sample_role", str(variant_spec.get("sample_role", "")))
        frame.insert(0, "sample_variant", str(variant_name))
        frames.append(frame)
    if not frames:
        return pd.DataFrame(
            columns=[
                "sample_variant",
                "sample_role",
                "sample_filter",
                "outcome",
                "horizon",
                "beta",
                "se",
                "lower95",
                "upper95",
                "n",
                "spec_name",
                "shock_column",
                "shock_scale",
                "response_type",
                "decomposition_mode",
                "outcome_construction",
                "inference_method",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _score_snapshot(frame: Any, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = frame[(frame["outcome"] == outcome) & (frame["horizon"] == horizon)]
    if sample.empty:
        return None
    row = sample.iloc[0]
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _build_deposit_component_scorecard(
    *,
    identity_lp_irf: Any,
    lp_irf: Any,
    structural_proxy_summary: Mapping[str, Any],
    proxy_coverage_summary: Mapping[str, Any],
) -> dict[str, Any]:
    component_outcomes = [
        "checkable_deposits_bank_qoq",
        "interbank_transactions_bank_qoq",
        "time_savings_deposits_bank_qoq",
        "checkable_federal_govt_bank_qoq",
        "checkable_state_local_bank_qoq",
        "checkable_rest_of_world_bank_qoq",
        "checkable_private_domestic_bank_qoq",
    ]
    creator_outcomes = [
        "commercial_industrial_loans_qoq",
        "construction_land_development_loans_qoq",
        "cre_multifamily_loans_qoq",
        "cre_nonfarm_nonresidential_loans_qoq",
        "consumer_loans_qoq",
        "credit_card_revolving_loans_qoq",
        "auto_loans_qoq",
        "other_consumer_loans_qoq",
        "heloc_loans_qoq",
        "closed_end_residential_loans_qoq",
    ]
    horizons = (0, 4, 8)
    payload_horizons: dict[str, Any] = {}
    observed_components: set[str] = set()
    observed_creators: set[str] = set()
    for horizon in horizons:
        key = f"h{horizon}"
        component_payload = {
            outcome: snapshot
            for outcome in component_outcomes
            if (snapshot := _score_snapshot(lp_irf, outcome=outcome, horizon=horizon)) is not None
        }
        creator_payload = {
            outcome: snapshot
            for outcome in creator_outcomes
            if (snapshot := _score_snapshot(lp_irf, outcome=outcome, horizon=horizon)) is not None
        }
        observed_components.update(component_payload.keys())
        observed_creators.update(creator_payload.keys())
        payload_horizons[key] = {
            "exact_identity_baseline": {
                "tdc": _score_snapshot(identity_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon),
                "total": _score_snapshot(identity_lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon),
                "other": _score_snapshot(identity_lp_irf, outcome="other_component_qoq", horizon=horizon),
            },
            "z1_deposit_component_lp_responses": component_payload,
            "creator_lending_channel_lp_responses": creator_payload,
            "proxy_bundle_coverage": {
                "structural_proxy_context": dict((structural_proxy_summary.get("key_horizons") or {}).get(key, {})),
                "coverage_context": dict((proxy_coverage_summary.get("key_horizons") or {}).get(key, {})),
            },
            "major_uncovered_channel_families": list(proxy_coverage_summary.get("major_uncovered_channel_families", [])),
        }
    return {
        "status": "available",
        "headline_question": "As a secondary side read, which observable deposit types move alongside the exact non-TDC deposit response?",
        "estimation_path": {
            "primary_decomposition_mode": "exact_identity_baseline",
            "primary_artifact": "lp_irf_identity_baseline.csv",
            "component_artifact": "lp_irf.csv",
            "proxy_artifacts": [
                "structural_proxy_evidence_summary.json",
                "proxy_coverage_summary.json",
            ],
        },
        "component_outcomes_present": sorted(observed_components),
        "creator_channel_outcomes_present": sorted(observed_creators),
        "horizons": payload_horizons,
        "takeaways": [
            "This scorecard is a secondary side read that pairs the exact-baseline TDC/total/other decomposition with observed deposit-type LP responses, first-wave creator-lending channels, and the current proxy uncovered remainder."
        ],
    }


def _build_deposit_type_side_read(lp_irf: pd.DataFrame) -> pd.DataFrame:
    outcomes = [
        "checkable_deposits_bank_qoq",
        "interbank_transactions_bank_qoq",
        "time_savings_deposits_bank_qoq",
        "checkable_federal_govt_bank_qoq",
        "checkable_state_local_bank_qoq",
        "checkable_rest_of_world_bank_qoq",
        "checkable_private_domestic_bank_qoq",
    ]
    horizon_labels = {0: "impact", 4: "medium_run", 8: "longer_run"}
    display_labels = {
        "checkable_deposits_bank_qoq": "Checkable deposits",
        "interbank_transactions_bank_qoq": "Interbank transactions",
        "time_savings_deposits_bank_qoq": "Time and savings deposits",
        "checkable_federal_govt_bank_qoq": "Checkable: federal government",
        "checkable_state_local_bank_qoq": "Checkable: state and local government",
        "checkable_rest_of_world_bank_qoq": "Checkable: rest of world",
        "checkable_private_domestic_bank_qoq": "Checkable: private domestic",
    }
    sample = lp_irf[
        lp_irf["outcome"].isin(outcomes) & lp_irf["horizon"].isin([0, 4, 8])
    ].copy()
    if sample.empty:
        return pd.DataFrame(
            columns=[
                "outcome",
                "display_name",
                "horizon",
                "horizon_label",
                "beta",
                "se",
                "lower95",
                "upper95",
                "n",
                "ci_excludes_zero",
                "sign_label",
                "interpretation_note",
            ]
        )
    sample["display_name"] = sample["outcome"].map(display_labels)
    sample["horizon_label"] = sample["horizon"].map(horizon_labels)
    sample["ci_excludes_zero"] = (sample["lower95"] > 0.0) | (sample["upper95"] < 0.0)
    sample["sign_label"] = sample["beta"].apply(lambda value: "positive" if float(value) > 0.0 else "negative")
    sample["interpretation_note"] = (
        "Secondary observed deposit-type side read; not a clean decomposition of the non-TDC residual."
    )
    return sample[
        [
            "outcome",
            "display_name",
            "horizon",
            "horizon_label",
            "beta",
            "se",
            "lower95",
            "upper95",
            "n",
            "ci_excludes_zero",
            "sign_label",
            "interpretation_note",
        ]
    ].reset_index(drop=True)


def _materialize_real_outputs(
    root: Path,
    contract: Mapping[str, Any],
    *,
    reuse_mode: str | None = None,
    raw_fixture_root: Path | None = None,
) -> dict[str, str]:
    build_result = build_public_quarterly_panel(root, reuse_mode=reuse_mode, fixture_root=raw_fixture_root)
    panel = compute_other_component(load_panel(build_result.panel_path))
    call_report_components, call_report_summary = build_call_report_deposit_components(
        root=root,
        fixture_root=raw_fixture_root,
    )
    call_report_components_path = root / "data" / "derived" / "call_report_deposit_components.csv"
    export_frame(call_report_components, call_report_components_path)
    call_report_summary_path = root / "output" / "models" / "call_report_deposit_components_summary.json"
    write_json_payload(call_report_summary_path, call_report_summary)

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
        shock_specs=all_shock_specs,
    )

    lp_specs = load_yaml(_config_path("lp_specs.yml"))
    regime_specs = load_yaml(_config_path("regime_specs.yml"))
    baseline_lp_spec = lp_specs["specs"]["baseline"]
    lp_outputs = run_lp_from_specs(
        shocked,
        lp_specs=lp_specs,
        regime_specs=regime_specs,
    )
    identity_baseline = build_identity_baseline_irf(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        tdc_outcome_col=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
        total_outcome_col="total_deposits_bank_qoq",
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="identity_baseline",
        nested_shock_spec=dict(baseline_shock_spec),
    )
    raw_tdc_lp = run_local_projections(
        shocked,
        shock_col=str(baseline_shock_spec.get("residual_column", "tdc_residual")),
        outcome_cols=[str(baseline_shock_spec.get("target", "tdc_bank_only_qoq"))],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[0, 4, 8],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="baseline_raw_tdc",
    )
    lp_irf_path = root / "output" / "models" / "lp_irf.csv"
    export_frame(lp_outputs["lp_irf"], lp_irf_path)
    lp_irf_identity_baseline_path = root / "output" / "models" / "lp_irf_identity_baseline.csv"
    export_frame(identity_baseline, lp_irf_identity_baseline_path)
    identity_measurement_ladder = _build_identity_measurement_ladder(
        shocked,
        lp_specs=lp_specs,
        shock_specs=all_shock_specs,
    )
    identity_treatment_sensitivity = _build_identity_treatment_sensitivity(
        shocked,
        lp_specs=lp_specs,
        shock_specs=all_shock_specs,
    )
    identity_control_sensitivity = _build_identity_control_sensitivity(
        shocked,
        lp_specs=lp_specs,
        baseline_shock_spec=baseline_shock_spec,
    )
    identity_sample_sensitivity = _build_identity_sample_sensitivity(
        shocked,
        lp_specs=lp_specs,
        baseline_shock_spec=baseline_shock_spec,
    )
    identity_measurement_ladder_path = root / "output" / "models" / "identity_measurement_ladder.csv"
    export_frame(identity_measurement_ladder, identity_measurement_ladder_path)
    identity_treatment_sensitivity_path = root / "output" / "models" / "identity_treatment_sensitivity.csv"
    export_frame(identity_treatment_sensitivity, identity_treatment_sensitivity_path)
    identity_control_sensitivity_path = root / "output" / "models" / "identity_control_sensitivity.csv"
    export_frame(identity_control_sensitivity, identity_control_sensitivity_path)
    identity_sample_sensitivity_path = root / "output" / "models" / "identity_sample_sensitivity.csv"
    export_frame(identity_sample_sensitivity, identity_sample_sensitivity_path)
    treatment_fingerprint_path = root / "output" / "models" / "headline_treatment_fingerprint.json"
    fingerprint_payload = build_headline_treatment_fingerprint(
        shock_spec=dict(baseline_shock_spec),
        shocked=shocked,
        repo_root=repo_root(),
        canonical_tdc_source_path=build_result.canonical_tdc_source_path,
        canonical_tdc_source_kind=build_result.canonical_tdc_source_kind,
    )
    write_json_payload(
        treatment_fingerprint_path,
        fingerprint_payload,
    )
    provenance_validation_summary_path = root / "output" / "models" / "provenance_validation_summary.json"
    provenance_validation_payload = build_headline_treatment_fingerprint_validation_summary(
        fingerprint_payload,
        shock_spec=dict(baseline_shock_spec),
        repo_root=repo_root(),
    )
    write_json_payload(
        provenance_validation_summary_path,
        provenance_validation_payload,
    )
    smooth_lp_spec = lp_specs["specs"].get("smooth_lp", {})
    smoothed_lp_irf = build_smoothed_lp_irf(
        lp_outputs["lp_irf"],
        method=str(smooth_lp_spec.get("method", "gaussian_kernel")),
        bandwidth=float(smooth_lp_spec.get("bandwidth", 1.0)),
        degree=int(smooth_lp_spec.get("degree", 3)),
        ridge_alpha=float(smooth_lp_spec.get("ridge_alpha", 1.0)),
        min_horizons=int(smooth_lp_spec.get("min_horizons", 4)),
    )
    smoothed_lp_irf_path = root / "output" / "models" / "lp_irf_smoothed.csv"
    export_frame(smoothed_lp_irf, smoothed_lp_irf_path)
    smoothed_lp_diagnostics_path = root / "output" / "models" / "smoothed_lp_diagnostics_summary.json"
    write_json_payload(
        smoothed_lp_diagnostics_path,
        build_smoothed_lp_diagnostics_summary(smoothed_lp_irf),
    )
    lp_irf_regimes_path = root / "output" / "models" / "lp_irf_regimes.csv"
    export_frame(lp_outputs["lp_irf_regimes"], lp_irf_regimes_path)
    lp_irf_state_dependence_path = root / "output" / "models" / "lp_irf_state_dependence.csv"
    export_frame(lp_outputs["lp_irf_state_dependence"], lp_irf_state_dependence_path)
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
    factor_control_sensitivity_path = root / "output" / "models" / "factor_control_sensitivity.csv"
    export_frame(lp_outputs["factor_control_sensitivity"], factor_control_sensitivity_path)
    factor_control_diagnostics_path = root / "output" / "models" / "factor_control_diagnostics_summary.json"
    write_json_payload(
        factor_control_diagnostics_path,
        build_factor_control_diagnostics_summary(
            control_sensitivity=lp_outputs["control_set_sensitivity"],
            factor_control_sensitivity=lp_outputs["factor_control_sensitivity"],
        ),
    )
    sample_sensitivity_path = root / "output" / "models" / "shock_sample_sensitivity.csv"
    export_frame(lp_outputs["shock_sample_sensitivity"], sample_sensitivity_path)
    period_sensitivity_path = root / "output" / "models" / "period_sensitivity.csv"
    export_frame(lp_outputs["period_sensitivity"], period_sensitivity_path)
    period_sensitivity_summary_path = root / "output" / "models" / "period_sensitivity_summary.json"
    write_json_payload(
        period_sensitivity_summary_path,
        build_period_sensitivity_summary(lp_outputs["period_sensitivity"]),
    )
    contrast = build_total_minus_other_contrast(
        lp_irf=lp_outputs["lp_irf"],
        identity_lp_irf=identity_baseline,
        sensitivity=identity_treatment_sensitivity
        if not identity_treatment_sensitivity.empty
        else lp_outputs["tdc_sensitivity_ladder"],
        control_sensitivity=identity_control_sensitivity
        if not identity_control_sensitivity.empty
        else lp_outputs["control_set_sensitivity"],
        sample_sensitivity=identity_sample_sensitivity
        if not identity_sample_sensitivity.empty
        else lp_outputs["shock_sample_sensitivity"],
        identity_check_mode=(
            "approximate_with_outcome_specific_lags"
            if bool(baseline_lp_spec.get("include_lagged_outcome", False))
            else "exact_accounting_identity"
        ),
    )
    contrast_path = root / "output" / "models" / "total_minus_other_contrast.csv"
    export_frame(contrast, contrast_path)
    structural_proxy_frame, structural_proxy_summary = build_structural_proxy_evidence(
        lp_irf=lp_outputs["lp_irf"],
        identity_lp_irf=identity_baseline,
    )
    structural_proxy_path = root / "output" / "models" / "structural_proxy_evidence.csv"
    export_frame(structural_proxy_frame, structural_proxy_path)
    structural_proxy_summary_path = root / "output" / "models" / "structural_proxy_evidence_summary.json"
    write_json_payload(structural_proxy_summary_path, structural_proxy_summary)
    proxy_factor_frame, proxy_factor_summary = build_proxy_factor_diagnostics(
        lp_irf=lp_outputs["lp_irf"],
    )
    proxy_factor_path = root / "output" / "models" / "proxy_factor_diagnostics.csv"
    export_frame(proxy_factor_frame, proxy_factor_path)
    proxy_factor_summary_path = root / "output" / "models" / "proxy_factor_diagnostics_summary.json"
    write_json_payload(proxy_factor_summary_path, proxy_factor_summary)
    state_proxy_factor_frame, state_proxy_factor_summary = build_state_proxy_factor_diagnostics(
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        regime_diagnostics=regime_diagnostics,
    )
    state_proxy_factor_path = root / "output" / "models" / "state_proxy_factor_diagnostics.csv"
    export_frame(state_proxy_factor_frame, state_proxy_factor_path)
    state_proxy_factor_summary_path = root / "output" / "models" / "state_proxy_factor_diagnostics_summary.json"
    write_json_payload(state_proxy_factor_summary_path, state_proxy_factor_summary)
    published_state_proxy_comparator = build_published_state_proxy_comparator(
        state_proxy_factor_summary=state_proxy_factor_summary,
    )
    published_state_proxy_comparator_path = root / "output" / "models" / "published_state_proxy_comparator_summary.json"
    write_json_payload(
        published_state_proxy_comparator_path,
        published_state_proxy_comparator,
    )
    published_state_proxy_vs_baseline = build_published_state_proxy_vs_baseline_summary(
        lp_irf=lp_outputs["lp_irf"],
        published_state_proxy_comparator=published_state_proxy_comparator,
    )
    published_state_proxy_vs_baseline_path = root / "output" / "models" / "published_state_proxy_vs_baseline_summary.json"
    write_json_payload(
        published_state_proxy_vs_baseline_path,
        published_state_proxy_vs_baseline,
    )
    proxy_coverage_summary_path = root / "output" / "models" / "proxy_coverage_summary.json"
    proxy_coverage_summary = build_proxy_coverage_summary(
        lp_irf=lp_outputs["lp_irf"],
        identity_lp_irf=identity_baseline,
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        regime_diagnostics=regime_diagnostics,
        regime_specs=regime_specs,
        proxy_unit_audit=_load_json(build_result.proxy_unit_audit_path),
    )
    write_json_payload(proxy_coverage_summary_path, proxy_coverage_summary)
    counterpart_channel_scorecard = build_counterpart_channel_scorecard(
        identity_lp_irf=identity_baseline,
        lp_irf=lp_outputs["lp_irf"],
        proxy_coverage_summary=proxy_coverage_summary,
    )
    shock_diagnostics_path = root / "output" / "models" / "shock_diagnostics_summary.json"
    shock_diagnostics_payload = build_shock_diagnostics_summary(
        shocks=shocked,
        sensitivity=lp_outputs["tdc_sensitivity_ladder"],
        baseline_shock_spec=dict(baseline_shock_spec),
        shock_specs=dict(all_shock_specs),
        lp_controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=bool(baseline_lp_spec.get("include_lagged_outcome", False)),
    )
    write_json_payload(
        shock_diagnostics_path,
        shock_diagnostics_payload,
    )
    direct_identification_path = root / "output" / "models" / "direct_identification_summary.json"
    direct_identification = build_direct_identification_summary(
        lp_irf=lp_outputs["lp_irf"],
        identity_lp_irf=identity_baseline,
        contrast=contrast,
        sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
        identity_sample_sensitivity=identity_sample_sensitivity,
        shock_metadata=dict(baseline_shock_spec),
        shock_specs=dict(all_shock_specs),
        shocks=shocked,
        raw_tdc_lp=raw_tdc_lp,
        shock_column=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
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
        identity_lp_irf=identity_baseline,
        lp_irf_regimes=lp_outputs["lp_irf_regimes"],
        sensitivity=lp_outputs["tdc_sensitivity_ladder"],
        identity_sensitivity=identity_treatment_sensitivity,
        control_sensitivity=lp_outputs["control_set_sensitivity"],
        identity_control_sensitivity=identity_control_sensitivity,
        sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
        identity_sample_sensitivity=identity_sample_sensitivity,
        regime_diagnostics=regime_diagnostics,
        direct_identification=direct_identification,
        contrast=contrast,
        structural_proxy_evidence=structural_proxy_summary,
        proxy_coverage_summary=proxy_coverage_summary,
        counterpart_channel_scorecard=counterpart_channel_scorecard,
        shock_diagnostics=shock_diagnostics_payload,
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
            identity_lp_irf=identity_baseline,
            identity_measurement_ladder=identity_measurement_ladder,
            sensitivity=lp_outputs["tdc_sensitivity_ladder"],
            identity_sensitivity=identity_treatment_sensitivity,
            control_sensitivity=lp_outputs["control_set_sensitivity"],
            identity_control_sensitivity=identity_control_sensitivity,
            sample_sensitivity=lp_outputs["shock_sample_sensitivity"],
            identity_sample_sensitivity=identity_sample_sensitivity,
            contrast=contrast,
            lp_irf_regimes=lp_outputs["lp_irf_regimes"],
            readiness=readiness_payload,
            regime_diagnostics=regime_diagnostics,
            regime_specs=regime_specs,
            structural_proxy_evidence=structural_proxy_summary,
            proxy_coverage_summary=proxy_coverage_summary,
            counterpart_channel_scorecard=counterpart_channel_scorecard,
        ),
    )
    research_dashboard_path = root / "output" / "models" / "research_dashboard_summary.json"
    research_dashboard_payload = build_research_dashboard_summary(
        readiness=readiness_payload,
        direct_identification=direct_identification,
        shock_diagnostics=shock_diagnostics_payload,
        lp_irf=lp_outputs["lp_irf"],
        smoothed_lp_diagnostics=_load_json(smoothed_lp_diagnostics_path),
        lp_irf_state_dependence=lp_outputs["lp_irf_state_dependence"],
        factor_control_sensitivity=lp_outputs["factor_control_sensitivity"],
        factor_control_diagnostics=_load_json(factor_control_diagnostics_path),
        proxy_factor_summary=proxy_factor_summary,
        state_proxy_factor_summary=state_proxy_factor_summary,
    )
    write_json_payload(
        research_dashboard_path,
        research_dashboard_payload,
    )
    research_dashboard_report_path = root / "output" / "reports" / "research_dashboard.md"
    write_research_dashboard_report(research_dashboard_report_path, research_dashboard_payload)
    published_state_proxy_report_path = root / "output" / "reports" / "published_state_proxy_comparator.md"
    write_published_state_proxy_report(
        published_state_proxy_report_path,
        published_state_proxy_comparator,
    )
    published_state_proxy_vs_baseline_report_path = root / "output" / "reports" / "published_state_proxy_vs_baseline.md"
    write_published_state_proxy_vs_baseline_report(
        published_state_proxy_vs_baseline_report_path,
        published_state_proxy_vs_baseline,
    )
    backend_decision_bundle = build_backend_decision_bundle(
        readiness=readiness_payload,
        direct_identification=direct_identification,
        shock_diagnostics=shock_diagnostics_payload,
        smoothed_lp_diagnostics=_load_json(smoothed_lp_diagnostics_path),
        factor_control_diagnostics=_load_json(factor_control_diagnostics_path),
        proxy_factor_summary=proxy_factor_summary,
        state_proxy_factor_summary=state_proxy_factor_summary,
        published_state_proxy_comparator=published_state_proxy_comparator,
        published_state_proxy_vs_baseline=published_state_proxy_vs_baseline,
    )
    backend_decision_bundle_path = root / "output" / "models" / "backend_decision_bundle_summary.json"
    write_json_payload(backend_decision_bundle_path, backend_decision_bundle)
    backend_decision_bundle_report_path = root / "output" / "reports" / "backend_decision_bundle.md"
    write_backend_decision_bundle_report(
        backend_decision_bundle_report_path,
        backend_decision_bundle,
    )
    backend_evidence_packet = build_backend_evidence_packet(
        root=root,
        backend_decision_bundle=backend_decision_bundle,
        research_dashboard_path=research_dashboard_path,
        research_dashboard_report_path=research_dashboard_report_path,
        published_state_proxy_comparator_path=published_state_proxy_comparator_path,
        published_state_proxy_report_path=published_state_proxy_report_path,
        published_state_proxy_vs_baseline_path=published_state_proxy_vs_baseline_path,
        published_state_proxy_vs_baseline_report_path=published_state_proxy_vs_baseline_report_path,
        backend_decision_bundle_path=backend_decision_bundle_path,
        backend_decision_bundle_report_path=backend_decision_bundle_report_path,
    )
    backend_evidence_packet_path = root / "output" / "models" / "backend_evidence_packet_summary.json"
    write_json_payload(backend_evidence_packet_path, backend_evidence_packet)
    backend_evidence_packet_report_path = root / "output" / "reports" / "backend_evidence_packet.md"
    write_backend_evidence_packet_report(
        backend_evidence_packet_report_path,
        backend_evidence_packet,
    )
    backend_closeout_summary = build_backend_closeout_summary(
        decision_bundle=backend_decision_bundle,
        evidence_packet=backend_evidence_packet,
    )
    backend_closeout_summary_path = root / "output" / "models" / "backend_closeout_summary.json"
    write_json_payload(backend_closeout_summary_path, backend_closeout_summary)
    backend_closeout_report_path = root / "output" / "reports" / "backend_closeout.md"
    write_backend_closeout_report(
        backend_closeout_report_path,
        backend_closeout_summary,
    )
    deposit_component_scorecard = _build_deposit_component_scorecard(
        identity_lp_irf=identity_baseline,
        lp_irf=lp_outputs["lp_irf"],
        structural_proxy_summary=structural_proxy_summary,
        proxy_coverage_summary=proxy_coverage_summary,
    )
    deposit_component_scorecard_path = root / "output" / "models" / "deposit_component_scorecard.json"
    write_json_payload(deposit_component_scorecard_path, deposit_component_scorecard)
    deposit_type_side_read = _build_deposit_type_side_read(lp_outputs["lp_irf"])
    deposit_type_side_read_path = root / "output" / "models" / "deposit_type_side_read.csv"
    export_frame(deposit_type_side_read, deposit_type_side_read_path)
    counterpart_channel_scorecard_path = root / "output" / "models" / "counterpart_channel_scorecard.json"
    write_json_payload(counterpart_channel_scorecard_path, counterpart_channel_scorecard)
    if provenance_validation_payload["status"] != "passed":
        raise RuntimeError(
            "Refusing to mirror public artifacts because headline treatment provenance validation failed."
        )

    mirror_contract_artifacts(source_root=root, dest_root=root, contract=contract)
    overview_path = root / overview_artifact_path(contract)
    write_overview_json(
        overview_path,
        **_default_overview_payload(
            panel=panel,
            shocked=shocked,
            accounting_summary=accounting_summary,
            readiness=readiness_payload,
            counterpart_channel_scorecard=counterpart_channel_scorecard,
            root=root,
        ),
    )
    raw_download_runs = _load_json(build_result.raw_download_manifest_path).get("runs", [])
    reused_payload = _load_json(build_result.reused_artifacts_path)
    manifest_paths = write_pipeline_manifests(
        root,
        command="pipeline run",
        outputs=[
            build_result.panel_path,
            call_report_components_path,
            build_result.proxy_unit_audit_path,
            sample_construction_summary_path,
            accounting_summary_path,
            quarters_exceeds_path,
            shocks_path,
            lp_irf_path,
            lp_irf_identity_baseline_path,
            identity_measurement_ladder_path,
            identity_treatment_sensitivity_path,
            identity_control_sensitivity_path,
            identity_sample_sensitivity_path,
            smoothed_lp_irf_path,
            smoothed_lp_diagnostics_path,
            lp_irf_regimes_path,
            lp_irf_state_dependence_path,
            regime_diagnostics_path,
            sensitivity_path,
            control_set_sensitivity_path,
            factor_control_sensitivity_path,
            factor_control_diagnostics_path,
            sample_sensitivity_path,
            period_sensitivity_path,
            period_sensitivity_summary_path,
            contrast_path,
            structural_proxy_path,
            structural_proxy_summary_path,
            proxy_factor_path,
            proxy_factor_summary_path,
            state_proxy_factor_path,
            state_proxy_factor_summary_path,
            published_state_proxy_comparator_path,
            published_state_proxy_vs_baseline_path,
            backend_decision_bundle_path,
            backend_evidence_packet_path,
            backend_closeout_summary_path,
            proxy_coverage_summary_path,
            call_report_summary_path,
            treatment_fingerprint_path,
            provenance_validation_summary_path,
            shock_diagnostics_path,
            direct_identification_path,
            result_readiness_path,
            pass_through_summary_path,
            research_dashboard_path,
            research_dashboard_report_path,
            published_state_proxy_report_path,
            published_state_proxy_vs_baseline_report_path,
            backend_decision_bundle_report_path,
            backend_evidence_packet_report_path,
            backend_closeout_report_path,
            deposit_component_scorecard_path,
            deposit_type_side_read_path,
            counterpart_channel_scorecard_path,
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
        "call_report_components_path": str(call_report_components_path),
        "proxy_unit_audit_path": str(build_result.proxy_unit_audit_path),
        "sample_construction_summary_path": str(sample_construction_summary_path),
        "accounting_summary_path": str(accounting_summary_path),
        "quarters_tdc_exceeds_total_path": str(quarters_exceeds_path),
        "shocks_path": str(shocks_path),
        "lp_irf_path": str(lp_irf_path),
        "lp_irf_identity_baseline_path": str(lp_irf_identity_baseline_path),
        "identity_measurement_ladder_path": str(identity_measurement_ladder_path),
        "identity_treatment_sensitivity_path": str(identity_treatment_sensitivity_path),
        "identity_control_sensitivity_path": str(identity_control_sensitivity_path),
        "identity_sample_sensitivity_path": str(identity_sample_sensitivity_path),
        "smoothed_lp_irf_path": str(smoothed_lp_irf_path),
        "smoothed_lp_diagnostics_path": str(smoothed_lp_diagnostics_path),
        "lp_irf_regimes_path": str(lp_irf_regimes_path),
        "lp_irf_state_dependence_path": str(lp_irf_state_dependence_path),
        "regime_diagnostics_path": str(regime_diagnostics_path),
        "sensitivity_path": str(sensitivity_path),
        "control_set_sensitivity_path": str(control_set_sensitivity_path),
        "factor_control_sensitivity_path": str(factor_control_sensitivity_path),
        "factor_control_diagnostics_path": str(factor_control_diagnostics_path),
        "sample_sensitivity_path": str(sample_sensitivity_path),
        "period_sensitivity_path": str(period_sensitivity_path),
        "period_sensitivity_summary_path": str(period_sensitivity_summary_path),
        "contrast_path": str(contrast_path),
        "structural_proxy_path": str(structural_proxy_path),
        "structural_proxy_summary_path": str(structural_proxy_summary_path),
        "proxy_factor_path": str(proxy_factor_path),
        "proxy_factor_summary_path": str(proxy_factor_summary_path),
        "state_proxy_factor_path": str(state_proxy_factor_path),
        "state_proxy_factor_summary_path": str(state_proxy_factor_summary_path),
        "published_state_proxy_comparator_path": str(published_state_proxy_comparator_path),
        "published_state_proxy_vs_baseline_path": str(published_state_proxy_vs_baseline_path),
        "backend_decision_bundle_path": str(backend_decision_bundle_path),
        "backend_evidence_packet_path": str(backend_evidence_packet_path),
        "backend_closeout_summary_path": str(backend_closeout_summary_path),
        "proxy_coverage_summary_path": str(proxy_coverage_summary_path),
        "call_report_summary_path": str(call_report_summary_path),
        "headline_treatment_fingerprint_path": str(treatment_fingerprint_path),
        "provenance_validation_summary_path": str(provenance_validation_summary_path),
        "shock_diagnostics_path": str(shock_diagnostics_path),
        "direct_identification_path": str(direct_identification_path),
        "result_readiness_path": str(result_readiness_path),
        "pass_through_summary_path": str(pass_through_summary_path),
        "research_dashboard_path": str(research_dashboard_path),
        "research_dashboard_report_path": str(research_dashboard_report_path),
        "published_state_proxy_report_path": str(published_state_proxy_report_path),
        "published_state_proxy_vs_baseline_report_path": str(published_state_proxy_vs_baseline_report_path),
        "backend_decision_bundle_report_path": str(backend_decision_bundle_report_path),
        "backend_evidence_packet_report_path": str(backend_evidence_packet_report_path),
        "backend_closeout_report_path": str(backend_closeout_report_path),
        "deposit_component_scorecard_path": str(deposit_component_scorecard_path),
        "deposit_type_side_read_path": str(deposit_type_side_read_path),
        "counterpart_channel_scorecard_path": str(counterpart_channel_scorecard_path),
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
            "sample": {"frequency": "quarterly", "rows": None, "layout": "contract_skeleton"},
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
