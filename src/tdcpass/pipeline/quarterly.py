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
from tdcpass.analysis.accounting_identity import (
    build_accounting_identity_alignment_frame,
    build_accounting_identity_summary,
    slice_accounting_identity_lp_irf,
)
from tdcpass.analysis.backend_decision_bundle import build_backend_decision_bundle
from tdcpass.analysis.backend_closeout_summary import build_backend_closeout_summary
from tdcpass.analysis.backend_evidence_packet import build_backend_evidence_packet
from tdcpass.analysis.big_picture_synthesis import build_big_picture_synthesis_summary
from tdcpass.analysis.broad_scope_system import build_broad_scope_system_summary
from tdcpass.analysis.counterpart_channel_scorecard import build_counterpart_channel_scorecard
from tdcpass.analysis.core_treatment_promotion import build_core_treatment_promotion_summary
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
from tdcpass.analysis.rest_of_world_treasury_audit import build_rest_of_world_treasury_audit_summary
from tdcpass.analysis.research_dashboard import build_research_dashboard_summary
from tdcpass.analysis.regime_diagnostics import build_regime_diagnostics_summary
from tdcpass.analysis.result_readiness import build_result_readiness_summary
from tdcpass.analysis.scope_alignment import build_scope_alignment_summary
from tdcpass.analysis.shock_diagnostics import build_shock_diagnostics_summary
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.analysis.split_treatment_architecture import build_split_treatment_architecture_summary
from tdcpass.analysis.smoothed_local_projections import (
    build_smoothed_lp_diagnostics_summary,
    build_smoothed_lp_irf,
)
from tdcpass.analysis.state_proxy_factor_diagnostics import build_state_proxy_factor_diagnostics
from tdcpass.analysis.strict_identifiable import (
    build_strict_funding_offset_alignment_frame,
    build_strict_identifiable_alignment_frame,
    build_strict_identifiable_followup_summary,
    build_strict_identifiable_summary,
    slice_strict_identifiable_lp_irf,
)
from tdcpass.analysis.strict_missing_channel_summary import build_strict_missing_channel_summary
from tdcpass.analysis.strict_di_bucket_bridge_summary import build_strict_di_bucket_bridge_summary
from tdcpass.analysis.strict_di_bucket_role_summary import build_strict_di_bucket_role_summary
from tdcpass.analysis.strict_private_borrower_bridge_summary import build_strict_private_borrower_bridge_summary
from tdcpass.analysis.strict_nonfinancial_corporate_bridge_summary import build_strict_nonfinancial_corporate_bridge_summary
from tdcpass.analysis.strict_private_offset_residual_summary import build_strict_private_offset_residual_summary
from tdcpass.analysis.strict_corporate_bridge_secondary_comparison_summary import (
    build_strict_corporate_bridge_secondary_comparison_summary,
)
from tdcpass.analysis.strict_component_framework_summary import build_strict_component_framework_summary
from tdcpass.analysis.strict_additional_creator_candidate_summary import (
    build_strict_additional_creator_candidate_summary,
)
from tdcpass.analysis.strict_di_loans_nec_measurement_audit_summary import (
    build_strict_di_loans_nec_measurement_audit_summary,
)
from tdcpass.analysis.strict_results_closeout_summary import (
    build_strict_results_closeout_summary,
)
from tdcpass.analysis.strict_direct_core_component_summary import build_strict_direct_core_component_summary
from tdcpass.analysis.strict_direct_core_horizon_stability_summary import (
    build_strict_direct_core_horizon_stability_summary,
)
from tdcpass.analysis.strict_release_framing_summary import build_strict_release_framing_summary
from tdcpass.analysis.strict_loan_core_redesign_summary import build_strict_loan_core_redesign_summary
from tdcpass.analysis.strict_redesign_summary import build_strict_redesign_summary
from tdcpass.analysis.strict_shock_composition import build_strict_shock_composition_summary
from tdcpass.analysis.strict_sign_mismatch_audit import build_strict_sign_mismatch_audit_summary
from tdcpass.analysis.strict_top_gap_quarter_audit import build_strict_top_gap_quarter_audit_summary
from tdcpass.analysis.strict_top_gap_anomaly import build_strict_top_gap_anomaly_summary
from tdcpass.analysis.strict_top_gap_anomaly_component_split import (
    build_strict_top_gap_anomaly_component_split_summary,
)
from tdcpass.analysis.strict_top_gap_anomaly_di_loans_split import (
    build_strict_top_gap_anomaly_di_loans_split_summary,
)
from tdcpass.analysis.strict_top_gap_anomaly_backdrop import (
    build_strict_top_gap_anomaly_backdrop_summary,
)
from tdcpass.analysis.strict_top_gap_quarter_direction import build_strict_top_gap_quarter_direction_summary
from tdcpass.analysis.strict_top_gap_inversion import build_strict_top_gap_inversion_summary
from tdcpass.analysis.structural_proxy_evidence import build_structural_proxy_evidence
from tdcpass.analysis.treatment_fingerprint import (
    build_headline_treatment_fingerprint,
    build_headline_treatment_fingerprint_validation_summary,
)
from tdcpass.analysis.tdc_treatment_audit import build_tdc_treatment_audit_summary
from tdcpass.analysis.tdcest_broad_object_comparison_summary import (
    build_tdcest_broad_object_comparison_summary,
)
from tdcpass.analysis.tdcest_broad_treatment_sensitivity_summary import (
    build_tdcest_broad_treatment_sensitivity_summary,
)
from tdcpass.analysis.tdcest_ladder_integration_summary import build_tdcest_ladder_integration_summary
from tdcpass.analysis.treatment_object_comparison import build_treatment_object_comparison_summary
from tdcpass.analysis.toc_row_bundle_audit import build_toc_row_bundle_audit_summary
from tdcpass.analysis.toc_row_excluded_interpretation import build_toc_row_excluded_interpretation_summary
from tdcpass.analysis.toc_row_incidence_audit_summary import build_toc_row_incidence_audit_summary
from tdcpass.analysis.toc_row_liability_incidence_raw_summary import (
    build_toc_row_liability_incidence_raw_summary,
)
from tdcpass.analysis.toc_validated_share_candidate_summary import (
    build_toc_validated_share_candidate_summary,
)
from tdcpass.analysis.toc_row_path_split import build_toc_row_path_split_summary
from tdcpass.analysis.treasury_cash_regime_audit_summary import (
    build_treasury_cash_regime_audit_summary,
)
from tdcpass.analysis.historical_cash_term_reestimation_summary import (
    build_historical_cash_term_reestimation_summary,
)
from tdcpass.analysis.treasury_operating_cash_audit import build_treasury_operating_cash_audit_summary
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


def _should_refuse_public_mirror(*, root: Path, provenance_validation_payload: Mapping[str, Any]) -> bool:
    if provenance_validation_payload.get("status") == "passed":
        return False
    try:
        return root.resolve() == repo_root().resolve()
    except FileNotFoundError:
        return root == repo_root()


def _default_overview_payload(
    *,
    panel: Any,
    shocked: Any,
    accounting_summary: Any,
    readiness: Mapping[str, Any],
    counterpart_channel_scorecard: Mapping[str, Any] | None = None,
    scope_alignment_summary: Mapping[str, Any] | None = None,
    strict_identifiable_followup_summary: Mapping[str, Any] | None = None,
    tdc_treatment_audit_summary: Mapping[str, Any] | None = None,
    treasury_operating_cash_audit_summary: Mapping[str, Any] | None = None,
    rest_of_world_treasury_audit_summary: Mapping[str, Any] | None = None,
    toc_row_path_split_summary: Mapping[str, Any] | None = None,
    toc_row_excluded_interpretation_summary: Mapping[str, Any] | None = None,
    strict_missing_channel_summary: Mapping[str, Any] | None = None,
    strict_sign_mismatch_audit_summary: Mapping[str, Any] | None = None,
    strict_shock_composition_summary: Mapping[str, Any] | None = None,
    strict_top_gap_quarter_audit_summary: Mapping[str, Any] | None = None,
    strict_top_gap_quarter_direction_summary: Mapping[str, Any] | None = None,
    strict_top_gap_inversion_summary: Mapping[str, Any] | None = None,
    strict_top_gap_anomaly_summary: Mapping[str, Any] | None = None,
    strict_top_gap_anomaly_component_split_summary: Mapping[str, Any] | None = None,
    strict_top_gap_anomaly_di_loans_split_summary: Mapping[str, Any] | None = None,
    strict_top_gap_anomaly_backdrop_summary: Mapping[str, Any] | None = None,
    big_picture_synthesis_summary: Mapping[str, Any] | None = None,
    treatment_object_comparison_summary: Mapping[str, Any] | None = None,
    split_treatment_architecture_summary: Mapping[str, Any] | None = None,
    core_treatment_promotion_summary: Mapping[str, Any] | None = None,
    strict_redesign_summary: Mapping[str, Any] | None = None,
    strict_loan_core_redesign_summary: Mapping[str, Any] | None = None,
    strict_di_bucket_role_summary: Mapping[str, Any] | None = None,
    strict_di_bucket_bridge_summary: Mapping[str, Any] | None = None,
    strict_private_borrower_bridge_summary: Mapping[str, Any] | None = None,
    strict_nonfinancial_corporate_bridge_summary: Mapping[str, Any] | None = None,
    strict_private_offset_residual_summary: Mapping[str, Any] | None = None,
    strict_corporate_bridge_secondary_comparison_summary: Mapping[str, Any] | None = None,
    strict_component_framework_summary: Mapping[str, Any] | None = None,
    toc_row_incidence_audit_summary: Mapping[str, Any] | None = None,
    toc_row_liability_incidence_raw_summary: Mapping[str, Any] | None = None,
    toc_validated_share_candidate_summary: Mapping[str, Any] | None = None,
    strict_release_framing_summary: Mapping[str, Any] | None = None,
    strict_direct_core_component_summary: Mapping[str, Any] | None = None,
    strict_direct_core_horizon_stability_summary: Mapping[str, Any] | None = None,
    strict_additional_creator_candidate_summary: Mapping[str, Any] | None = None,
    strict_di_loans_nec_measurement_audit_summary: Mapping[str, Any] | None = None,
    strict_results_closeout_summary: Mapping[str, Any] | None = None,
    broad_scope_system_summary: Mapping[str, Any] | None = None,
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
    scope_findings: list[str] = []
    scope_caveats: list[str] = []
    if scope_alignment_summary is not None and str(scope_alignment_summary.get("status", "not_available")) == "available":
        total_concept = dict(scope_alignment_summary.get("deposit_concepts", {})).get(
            "total_deposits_including_interbank", {}
        )
        h0_payload = dict(total_concept.get("key_horizons", {})).get("h0", {})
        variants = dict(h0_payload.get("variants", {}))
        domestic_delta = (
            variants.get("domestic_bank_only", {})
            .get("differences_vs_baseline_beta", {})
            .get("residual_response")
        )
        us_chartered_delta = (
            variants.get("us_chartered_bank_only", {})
            .get("differences_vs_baseline_beta", {})
            .get("residual_response")
        )
        if domestic_delta is not None and us_chartered_delta is not None:
            scope_findings.append(
                "Scope-alignment diagnostics show that the headline residual is not perfectly scope-clean: at h0, removing only the rest-of-world term makes it about "
                f"{float(domestic_delta):.2f} less negative, while the true U.S.-chartered bank-leg match makes it about {float(us_chartered_delta):.2f} less negative."
            )
            scope_findings.append(
                "So scope mismatch is real but partial, and the no-ROW sensitivity should not be read as equivalent to the matched-bank-leg treatment."
            )
            scope_findings.append(
                "Current release policy keeps `total_deposits_bank_qoq` as the headline outcome for now and uses the U.S.-chartered bank-leg match as the standard scope-check comparison."
            )
            scope_caveats.append(
                "The upstream `domestic_bank_only` variant is a no-ROW sensitivity, not a true U.S.-chartered bank-leg match; release-facing scope comparisons should treat those as distinct objects."
            )
    strict_gap_findings: list[str] = []
    if (
        strict_identifiable_followup_summary is not None
        and str(strict_identifiable_followup_summary.get("status", "not_available")) == "available"
    ):
        h0_scope_gap = (
            strict_identifiable_followup_summary.get("scope_check_gap_assessment", {})
            .get("key_horizons", {})
            .get("h0", {})
        )
        h0_variants = dict(h0_scope_gap.get("variant_gap_assessments", {}))
        us_chartered_remaining_share = (
            h0_variants.get("us_chartered_bank_only", {}).get("remaining_share_of_baseline_strict_gap")
        )
        us_chartered_relief_share = (
            h0_variants.get("us_chartered_bank_only", {}).get("relief_share_of_baseline_strict_gap")
        )
        if us_chartered_remaining_share is not None and us_chartered_relief_share is not None:
            strict_gap_findings.append(
                "The matched-bank-leg scope check only relieves a small share of the strict direct-count gap: at h0, "
                f"it removes about {float(us_chartered_relief_share):.2f} of the baseline strict gap and leaves about "
                f"{float(us_chartered_remaining_share):.2f} still in place when the direct-count strict total is held fixed."
            )
            strict_gap_findings.append(
                "So the remaining strict-gap story still looks dominated by missing direct-count channels and/or remaining treatment misspecification, not by the bank-leg scope mismatch alone."
            )
    broad_scope_findings: list[str] = []
    if broad_scope_system_summary is not None and str(broad_scope_system_summary.get("status", "not_available")) == "available":
        broad_h0 = dict(
            broad_scope_system_summary.get("broad_matched_system", {}).get("key_horizons", {}).get("h0", {})
        )
        broad_gap_share = broad_h0.get("broad_strict_gap_share_of_residual")
        if broad_gap_share is not None:
            broad_scope_findings.append(
                "The broad matched-scope system still leaves a large strict gap: at h0, about "
                f"{float(broad_gap_share):.2f} of the broad non-TDC residual remains in the broad strict gap."
            )
        audit_h0 = dict(broad_scope_system_summary.get("tdc_component_audit", {}).get("key_horizons", {}).get("h0", {}))
        no_row_shift = (
            audit_h0.get("variant_removal_diagnostics", {})
            .get("domestic_bank_only", {})
            .get("residual_shift_vs_baseline_beta")
        )
        no_foreign_shift = (
            audit_h0.get("variant_removal_diagnostics", {})
            .get("no_foreign_bank_sectors", {})
            .get("residual_shift_vs_baseline_beta")
        )
        if no_row_shift is not None and no_foreign_shift is not None:
            broad_scope_findings.append(
                "The treatment-side audit now separates ROW from foreign bank sectors: at h0, removing only ROW shifts the headline residual by about "
                f"{float(no_row_shift):.2f}, while removing only foreign bank-sector Treasury legs shifts it by about {float(no_foreign_shift):.2f}."
            )
    tdc_treatment_findings: list[str] = []
    if tdc_treatment_audit_summary is not None and str(tdc_treatment_audit_summary.get("status", "not_available")) == "available":
        audit_h0 = dict(tdc_treatment_audit_summary.get("key_horizons", {}).get("h0", {}))
        dominant_component = audit_h0.get("dominant_signed_component")
        no_toc_shift = (
            audit_h0.get("variant_removal_diagnostics", {})
            .get("no_toc_bank_only", {})
            .get("residual_shift_vs_baseline_beta")
        )
        no_row_shift = (
            audit_h0.get("variant_removal_diagnostics", {})
            .get("domestic_bank_only", {})
            .get("residual_shift_vs_baseline_beta")
        )
        no_foreign_shift = (
            audit_h0.get("variant_removal_diagnostics", {})
            .get("no_foreign_bank_sectors", {})
            .get("residual_shift_vs_baseline_beta")
        )
        if dominant_component is not None:
            tdc_treatment_findings.append(
                f"The direct TDC treatment audit now shows which building block moves most with the baseline shock: at h0, the largest signed component is `{dominant_component}`."
            )
        if str(tdc_treatment_audit_summary.get("construction_alignment", {}).get("status", "not_available")) == "available":
            tdc_treatment_findings.append(
                "Quarter-level construction alignment is exact for the audited TDC legs, so the remaining treatment-side issue is dynamic interpretation rather than arithmetic mismatch."
            )
        if no_toc_shift is not None and no_row_shift is not None:
            tdc_treatment_findings.append(
                "The TDC treatment audit now compares Treasury operating cash and ROW on the same removal basis: at h0, removing Treasury operating cash shifts the residual by about "
                f"{float(no_toc_shift):.2f}, while removing only ROW shifts it by about {float(no_row_shift):.2f}."
            )
        if no_row_shift is not None and no_foreign_shift is not None:
            tdc_treatment_findings.append(
                "The TDC treatment audit also separates direct component movement from removal tests: at h0, removing only ROW shifts the residual by about "
                f"{float(no_row_shift):.2f}, while removing only foreign bank-sector Treasury legs shifts it by about {float(no_foreign_shift):.2f}."
            )
    treasury_operating_cash_findings: list[str] = []
    if (
        treasury_operating_cash_audit_summary is not None
        and str(treasury_operating_cash_audit_summary.get("status", "not_available")) == "available"
    ):
        quarterly_alignment = dict(treasury_operating_cash_audit_summary.get("quarterly_alignment", {}))
        h0_toc = dict(treasury_operating_cash_audit_summary.get("key_horizons", {}).get("h0", {}))
        corr = quarterly_alignment.get("contemporaneous_corr_tga_vs_toc")
        slope = dict(quarterly_alignment.get("ols_tga_on_toc", {})).get("slope")
        toc_response = dict(h0_toc.get("treasury_operating_cash_response", {}) or {})
        tga_response = dict(h0_toc.get("tga_response", {}) or {})
        reserves_response = dict(h0_toc.get("reserves_response", {}) or {})
        if corr is not None and slope is not None:
            treasury_operating_cash_findings.append(
                "The Treasury-operating-cash leg tracks TGA closely quarter by quarter: correlation ≈ "
                f"{float(corr):.2f}, TGA-on-TOC slope ≈ {float(slope):.2f}."
            )
        if toc_response and tga_response and reserves_response:
            treasury_operating_cash_findings.append(
                "At h0, the Treasury-operating-cash audit points to a genuine cash-release pattern rather than a sign bug: "
                f"TOC ≈ {float(toc_response['beta']):.2f}, TGA ≈ {float(tga_response['beta']):.2f}, reserves ≈ {float(reserves_response['beta']):.2f}."
            )
    rest_of_world_findings: list[str] = []
    if (
        rest_of_world_treasury_audit_summary is not None
        and str(rest_of_world_treasury_audit_summary.get("status", "not_available")) == "available"
    ):
        quarterly_alignment = dict(rest_of_world_treasury_audit_summary.get("quarterly_alignment", {}))
        counterparts = dict(quarterly_alignment.get("counterparts", {}))
        foreign_nonts_corr = dict(counterparts.get("foreign_nonts_qoq", {})).get("contemporaneous_corr")
        row_deposits_corr = dict(counterparts.get("checkable_rest_of_world_bank_qoq", {})).get("contemporaneous_corr")
        h0_row = dict(rest_of_world_treasury_audit_summary.get("key_horizons", {}).get("h0", {}))
        row_response = dict(h0_row.get("rest_of_world_treasury_response", {}) or {})
        foreign_nonts_response = dict(h0_row.get("foreign_nonts_response", {}) or {})
        foreign_asset_response = dict(h0_row.get("interbank_transactions_foreign_banks_asset_response", {}) or {})
        if foreign_nonts_corr is not None and row_deposits_corr is not None:
            rest_of_world_findings.append(
                "The ROW Treasury leg is not a simple same-quarter deposit-liability counterpart: "
                f"corr with foreign NONTS ≈ {float(foreign_nonts_corr):.2f}, corr with checkable ROW deposits ≈ {float(row_deposits_corr):.2f}."
            )
        if row_response and foreign_nonts_response and foreign_asset_response:
            rest_of_world_findings.append(
                "At h0, the ROW Treasury-leg audit looks more like a broader external-support channel than a direct deposit-liability match: "
                f"ROW ≈ {float(row_response['beta']):.2f}, foreign NONTS ≈ {float(foreign_nonts_response['beta']):.2f}, "
                f"foreign-bank interbank assets ≈ {float(foreign_asset_response['beta']):.2f}."
            )
    toc_row_path_split_findings: list[str] = []
    if toc_row_path_split_summary is not None and str(toc_row_path_split_summary.get("status", "not_available")) == "available":
        quarterly_split = dict(toc_row_path_split_summary.get("quarterly_split", {}))
        quarterly_corrs = dict(quarterly_split.get("bundle_contemporaneous_corr", {}))
        broad_corr = quarterly_corrs.get("broad_support_path")
        direct_corr = quarterly_corrs.get("direct_deposit_path")
        quarterly_preferred = quarterly_split.get("preferred_quarterly_path")
        h0_split = dict(toc_row_path_split_summary.get("key_horizons", {}).get("h0", {}))
        h0_preferred = h0_split.get("preferred_horizon_path")
        h0_broad = dict(h0_split.get("broad_support_path_response", {}) or {}).get("beta")
        h0_direct = dict(h0_split.get("direct_deposit_path_response", {}) or {}).get("beta")
        if quarterly_preferred is not None and broad_corr is not None and direct_corr is not None:
            toc_row_path_split_findings.append(
                "Inside the combined TOC/ROW block, quarter-by-quarter co-movement looks more TGA-anchored than broad-support driven: "
                f"preferred quarterly path = `{quarterly_preferred}`, broad-support corr ≈ {float(broad_corr):.2f}, direct-deposit corr ≈ {float(direct_corr):.2f}."
            )
        if h0_preferred is not None and h0_broad is not None and h0_direct is not None:
            toc_row_path_split_findings.append(
                "Under the shock response, the TOC/ROW block leans the other way: "
                f"at h0 the preferred path is `{h0_preferred}` with broad-support ≈ {float(h0_broad):.2f} versus direct-deposit ≈ {float(h0_direct):.2f}."
            )
    toc_row_excluded_findings: list[str] = []
    if (
        toc_row_excluded_interpretation_summary is not None
        and str(toc_row_excluded_interpretation_summary.get("status", "not_available")) == "available"
    ):
        h0_exclusion = dict(toc_row_excluded_interpretation_summary.get("key_horizons", {}).get("h0", {}))
        baseline_h0 = dict(h0_exclusion.get("baseline", {}))
        excluded_h0 = dict(h0_exclusion.get("toc_row_excluded", {}))
        baseline_residual = dict(baseline_h0.get("residual_response", {}) or {}).get("beta")
        excluded_residual = dict(excluded_h0.get("residual_response", {}) or {}).get("beta")
        baseline_gap_share = baseline_h0.get("strict_gap_share_of_residual")
        excluded_gap_share = excluded_h0.get("strict_gap_share_of_residual")
        if baseline_residual is not None and excluded_residual is not None:
            toc_row_excluded_findings.append(
                "As a secondary comparison only, excluding TOC/ROW materially changes the h0 residual read: "
                f"baseline residual ≈ {float(baseline_residual):.2f}, TOC/ROW-excluded residual ≈ {float(excluded_residual):.2f}."
            )
        if baseline_gap_share is not None and excluded_gap_share is not None:
            toc_row_excluded_findings.append(
                "The TOC/ROW-excluded comparison also tests how much the direct-count strict-gap story changes: "
                f"at h0 the strict gap share moves from about {float(baseline_gap_share):.2f} to about {float(excluded_gap_share):.2f}."
            )
    strict_missing_channel_findings: list[str] = []
    if (
        strict_missing_channel_summary is not None
        and str(strict_missing_channel_summary.get("status", "not_available")) == "available"
    ):
        h0_missing = dict(strict_missing_channel_summary.get("key_horizons", {}).get("h0", {}))
        excluded_h0 = dict(h0_missing.get("toc_row_excluded", {}))
        excluded_direct_core = dict(excluded_h0.get("strict_headline_direct_core_response", {}) or {}).get("beta")
        excluded_loan = dict(excluded_h0.get("strict_loan_source_response", {}) or {}).get("beta")
        excluded_private_aug = dict(excluded_h0.get("strict_loan_core_plus_private_borrower_response", {}) or {}).get("beta")
        excluded_noncore = dict(excluded_h0.get("strict_loan_noncore_system_response", {}) or {}).get("beta")
        excluded_securities = dict(excluded_h0.get("strict_non_treasury_securities_response", {}) or {}).get("beta")
        excluded_net = dict(excluded_h0.get("strict_identifiable_net_after_funding_response", {}) or {}).get("beta")
        excluded_gap_after_funding_share = excluded_h0.get("strict_gap_after_funding_share_of_residual_abs")
        if (
            excluded_direct_core is not None
            and excluded_loan is not None
            and excluded_private_aug is not None
            and excluded_noncore is not None
            and excluded_securities is not None
            and excluded_net is not None
        ):
            strict_missing_channel_findings.append(
                "Returning to the strict lane after excluding TOC/ROW still leaves a weak direct-count verification read: "
                f"at h0 headline direct core ≈ {float(excluded_direct_core):.2f}, current broad loan source ≈ {float(excluded_loan):.2f}, "
                f"private-borrower-augmented core ≈ {float(excluded_private_aug):.2f}, noncore/system diagnostic ≈ {float(excluded_noncore):.2f}, "
                f"securities ≈ {float(excluded_securities):.2f}, funding-adjusted net ≈ {float(excluded_net):.2f}."
            )
        if excluded_gap_after_funding_share is not None:
            strict_missing_channel_findings.append(
                "Even after the funding-offset sensitivity is applied under the TOC/ROW-excluded comparison, "
                f"the remaining direct-count gap is still about {float(excluded_gap_after_funding_share):.2f} of the residual at h0."
            )
    strict_sign_mismatch_findings: list[str] = []
    if (
        strict_sign_mismatch_audit_summary is not None
        and str(strict_sign_mismatch_audit_summary.get("status", "not_available")) == "available"
    ):
        alignment = dict(strict_sign_mismatch_audit_summary.get("shock_alignment", {}))
        concentration = dict(strict_sign_mismatch_audit_summary.get("quarter_concentration", {}))
        driver_alignment = dict(strict_sign_mismatch_audit_summary.get("gap_driver_alignment", {}))
        component_alignment = dict(strict_sign_mismatch_audit_summary.get("component_alignment", {}))
        shock_corr = alignment.get("shock_corr")
        same_sign_share = alignment.get("same_sign_share")
        top5_share = concentration.get("top5_abs_gap_share")
        dominant_period = concentration.get("dominant_period_bucket")
        driver_corr = dict(driver_alignment.get("shock_gap_driver_correlations", {})).get(
            "baseline_minus_excluded_target_qoq"
        )
        direct_core_alignment = dict(component_alignment.get("strict_loan_core_min_qoq", {}))
        total_alignment = dict(component_alignment.get("strict_identifiable_total_qoq", {}))
        if shock_corr is not None and same_sign_share is not None:
            strict_sign_mismatch_findings.append(
                "The TOC/ROW-excluded shock is not just a rescaled baseline shock: "
                f"overlap correlation ≈ {float(shock_corr):.2f}, same-sign share ≈ {float(same_sign_share):.2f}."
            )
        if top5_share is not None and dominant_period is not None:
            strict_sign_mismatch_findings.append(
                "The shock rotation is concentrated in a limited set of quarters rather than spread evenly across the sample: "
                f"the top five shock-gap quarters explain about {float(top5_share):.2f} of absolute gap mass, with the largest share in `{str(dominant_period)}`."
            )
        if driver_corr is not None:
            strict_sign_mismatch_findings.append(
                "The shock-gap lines up strongly with the baseline-minus-excluded TOC/ROW target bundle itself: "
                f"quarter-level corr ≈ {float(driver_corr):.2f}."
            )
        if (
            direct_core_alignment.get("baseline_shock_corr") is not None
            and direct_core_alignment.get("toc_row_excluded_shock_corr") is not None
            and total_alignment.get("baseline_shock_corr") is not None
            and total_alignment.get("toc_row_excluded_shock_corr") is not None
        ):
            strict_sign_mismatch_findings.append(
                "Relative to baseline, the TOC/ROW-excluded shock rotates toward positive direct-count channels: "
                f"headline direct-core corr ≈ {float(direct_core_alignment['baseline_shock_corr']):.2f} to {float(direct_core_alignment['toc_row_excluded_shock_corr']):.2f}, "
                f"strict identifiable total corr ≈ {float(total_alignment['baseline_shock_corr']):.2f} to {float(total_alignment['toc_row_excluded_shock_corr']):.2f}."
            )
    strict_shock_composition_findings: list[str] = []
    if (
        strict_shock_composition_summary is not None
        and str(strict_shock_composition_summary.get("status", "not_available")) == "available"
    ):
        period_profiles = list(strict_shock_composition_summary.get("period_bucket_profiles", []))
        trim_diagnostics = dict(strict_shock_composition_summary.get("trim_diagnostics", {}))
        dominant_bucket = period_profiles[0]["period_bucket"] if period_profiles else None
        top5 = dict(trim_diagnostics.get("drop_top5_gap_quarters", {}))
        drop_covid = dict(trim_diagnostics.get("drop_covid_post", {}))
        if dominant_bucket is not None:
            strict_shock_composition_findings.append(
                "Quarter-composition diagnostics show the largest absolute shock-gap share sits in "
                f"`{str(dominant_bucket)}`."
            )
        if top5 and top5.get("shock_corr") is not None and top5.get("same_sign_share") is not None:
            strict_shock_composition_findings.append(
                "Dropping the five largest shock-gap quarters still leaves "
                f"corr ≈ {float(top5.get('shock_corr')):.2f}, same-sign share ≈ {float(top5.get('same_sign_share')):.2f}, "
                f"interpretation = `{str(top5.get('interpretation'))}`."
            )
        if drop_covid and drop_covid.get("shock_corr") is not None and drop_covid.get("same_sign_share") is not None:
            strict_shock_composition_findings.append(
                "Dropping the full `covid_post` bucket leaves "
                f"corr ≈ {float(drop_covid.get('shock_corr')):.2f}, same-sign share ≈ {float(drop_covid.get('same_sign_share')):.2f}, "
                f"interpretation = `{str(drop_covid.get('interpretation'))}`."
            )
    strict_top_gap_quarter_findings: list[str] = []
    if (
        strict_top_gap_quarter_audit_summary is not None
        and str(strict_top_gap_quarter_audit_summary.get("status", "not_available")) == "available"
    ):
        dominant_leg_summary = list(strict_top_gap_quarter_audit_summary.get("dominant_leg_summary", []))
        contribution_pattern_summary = list(strict_top_gap_quarter_audit_summary.get("contribution_pattern_summary", []))
        top_gap_quarters = list(strict_top_gap_quarter_audit_summary.get("top_gap_quarters", []))
        if dominant_leg_summary:
            lead = dominant_leg_summary[0]
            strict_top_gap_quarter_findings.append(
                "Within the top shock-gap quarters, the largest dominant-leg bucket is "
                f"`{str(lead.get('dominant_leg'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}."
            )
        if contribution_pattern_summary:
            lead = contribution_pattern_summary[0]
            strict_top_gap_quarter_findings.append(
                "Within the top shock-gap quarters, the leading TOC/ROW contribution pattern is "
                f"`{str(lead.get('contribution_pattern'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}."
            )
        if top_gap_quarters:
            lead = top_gap_quarters[0]
            strict_top_gap_quarter_findings.append(
                "The largest top-gap quarter is "
                f"`{str(lead.get('quarter'))}`, where the dominant leg is `{str(lead.get('dominant_leg'))}` "
                f"and the contribution pattern is `{str(lead.get('contribution_pattern'))}`."
            )
    strict_top_gap_quarter_direction_findings: list[str] = []
    if (
        strict_top_gap_quarter_direction_summary is not None
        and str(strict_top_gap_quarter_direction_summary.get("status", "not_available")) == "available"
    ):
        gap_bundle_alignment_summary = list(strict_top_gap_quarter_direction_summary.get("gap_bundle_alignment_summary", []))
        directional_driver_summary = list(strict_top_gap_quarter_direction_summary.get("directional_driver_summary", []))
        top_gap_quarters = list(strict_top_gap_quarter_direction_summary.get("top_gap_quarters", []))
        if gap_bundle_alignment_summary:
            lead = gap_bundle_alignment_summary[0]
            strict_top_gap_quarter_direction_findings.append(
                "Within the top shock-gap quarters, the leading gap-versus-bundle alignment is "
                f"`{str(lead.get('gap_alignment_to_bundle'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}."
            )
        if directional_driver_summary:
            lead = directional_driver_summary[0]
            strict_top_gap_quarter_direction_findings.append(
                "Within the top shock-gap quarters, the leading directional-driver bucket is "
                f"`{str(lead.get('directional_driver'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}."
            )
        if top_gap_quarters:
            lead = top_gap_quarters[0]
            strict_top_gap_quarter_direction_findings.append(
                "The largest top-gap quarter is "
                f"`{str(lead.get('quarter'))}`, where gap-versus-bundle alignment is `{str(lead.get('gap_alignment_to_bundle'))}` "
                f"and the directional driver is `{str(lead.get('directional_driver'))}`."
            )
    strict_top_gap_inversion_findings: list[str] = []
    if (
        strict_top_gap_inversion_summary is not None
        and str(strict_top_gap_inversion_summary.get("status", "not_available")) == "available"
    ):
        driver_summary = list(strict_top_gap_inversion_summary.get("directional_driver_context_summary", []))
        top_gap_quarters = list(strict_top_gap_inversion_summary.get("top_gap_quarters", []))
        if driver_summary:
            lead = driver_summary[0]
            strict_top_gap_inversion_findings.append(
                "Inside the top-gap inversion read, the leading driver bucket is "
                f"`{str(lead.get('directional_driver'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}; "
                f"weighted excluded residual ≈ {float(lead.get('weighted_mean_excluded_other_component_qoq') or 0.0):.2f} "
                f"versus weighted strict total ≈ {float(lead.get('weighted_mean_strict_identifiable_total_qoq') or 0.0):.2f}."
            )
            if lead.get("leading_residual_strict_pattern") is not None:
                strict_top_gap_inversion_findings.append(
                    "Within that leading inversion bucket, the largest residual-versus-strict pattern is "
                    f"`{str(lead.get('leading_residual_strict_pattern'))}` with abs-gap share ≈ "
                    f"{float(lead.get('leading_residual_strict_pattern_share') or 0.0):.2f}."
                )
        toc_driven = next(
            (row for row in top_gap_quarters if str(row.get("directional_driver")) == "toc_driven_gap_direction"),
            None,
        )
        if toc_driven is not None:
            strict_top_gap_inversion_findings.append(
                "The TOC-driven exception is "
                f"`{str(toc_driven.get('quarter'))}`: excluded residual ≈ {float(toc_driven.get('excluded_other_component_qoq') or 0.0):.2f}, "
                f"strict total ≈ {float(toc_driven.get('strict_identifiable_total_qoq') or 0.0):.2f}."
            )
    strict_top_gap_anomaly_findings: list[str] = []
    if (
        strict_top_gap_anomaly_summary is not None
        and str(strict_top_gap_anomaly_summary.get("status", "not_available")) == "available"
    ):
        anomaly_quarter = dict(strict_top_gap_anomaly_summary.get("anomaly_quarter", {}) or {})
        anomaly_vs_peer_deltas = dict(strict_top_gap_anomaly_summary.get("anomaly_vs_peer_deltas", {}) or {})
        peer_pattern_summary = list(strict_top_gap_anomaly_summary.get("peer_pattern_summary", []))
        if anomaly_quarter and anomaly_vs_peer_deltas:
            strict_top_gap_anomaly_findings.append(
                "The main within-bucket anomaly is "
                f"`{str(anomaly_quarter.get('quarter'))}`: excluded residual ≈ {float(anomaly_quarter.get('excluded_other_component_qoq') or 0.0):.2f}, "
                f"loan-source delta versus peers ≈ {float(anomaly_vs_peer_deltas.get('strict_loan_source_qoq') or 0.0):.2f}, "
                f"strict total ≈ {float(anomaly_quarter.get('strict_identifiable_total_qoq') or 0.0):.2f}, "
                f"and anomaly-minus-peer strict-total delta ≈ {float(anomaly_vs_peer_deltas.get('strict_identifiable_total_qoq') or 0.0):.2f}."
            )
        if peer_pattern_summary:
            lead = peer_pattern_summary[0]
            strict_top_gap_anomaly_findings.append(
                "Among same-bucket peers, the leading residual-versus-strict pattern is "
                f"`{str(lead.get('residual_strict_pattern'))}` with abs-gap share ≈ {float(lead.get('abs_gap_share') or 0.0):.2f}."
            )
    strict_top_gap_anomaly_component_split_findings: list[str] = []
    if (
        strict_top_gap_anomaly_component_split_summary is not None
        and str(strict_top_gap_anomaly_component_split_summary.get("status", "not_available")) == "available"
    ):
        loan_rows = list(strict_top_gap_anomaly_component_split_summary.get("loan_subcomponent_deltas", []))
        liquidity_rows = list(strict_top_gap_anomaly_component_split_summary.get("liquidity_external_deltas", []))
        if loan_rows:
            strict_top_gap_anomaly_component_split_findings.append(
                "Inside `2009Q4`, the largest detailed loan-subcomponent shortfall versus same-bucket peers is "
                f"`{str(loan_rows[0].get('label'))}` at ≈ {float(loan_rows[0].get('anomaly_minus_peer_delta') or 0.0):.2f}."
            )
        if liquidity_rows:
            strict_top_gap_anomaly_component_split_findings.append(
                "The same anomaly also shows weaker liquidity/external support, led by "
                f"`{str(liquidity_rows[0].get('label'))}` at ≈ {float(liquidity_rows[0].get('anomaly_minus_peer_delta') or 0.0):.2f}."
            )
    strict_top_gap_anomaly_di_loans_split_findings: list[str] = []
    if (
        strict_top_gap_anomaly_di_loans_split_summary is not None
        and str(strict_top_gap_anomaly_di_loans_split_summary.get("status", "not_available")) == "available"
    ):
        dominant = dict(strict_top_gap_anomaly_di_loans_split_summary.get("dominant_borrower_component") or {})
        borrower_gap = dict(strict_top_gap_anomaly_di_loans_split_summary.get("borrower_gap_row") or {})
        if dominant:
            strict_top_gap_anomaly_di_loans_split_findings.append(
                "Within the DI-loans-n.e.c. borrower-side split, the dominant peer delta is "
                f"`{str(dominant.get('label'))}` at ≈ {float(dominant.get('anomaly_minus_peer_delta') or 0.0):.2f}."
            )
        if borrower_gap:
            strict_top_gap_anomaly_di_loans_split_findings.append(
                "The DI-loans-n.e.c. borrower-gap delta is "
                f"≈ {float(borrower_gap.get('anomaly_minus_peer_delta') or 0.0):.2f}."
            )
    strict_top_gap_anomaly_backdrop_findings: list[str] = []
    if (
        strict_top_gap_anomaly_backdrop_summary is not None
        and str(strict_top_gap_anomaly_backdrop_summary.get("status", "not_available")) == "available"
    ):
        corporate = dict(strict_top_gap_anomaly_backdrop_summary.get("corporate_credit_row") or {})
        reserves = dict(strict_top_gap_anomaly_backdrop_summary.get("reserves_row") or {})
        foreign_nonts = dict(strict_top_gap_anomaly_backdrop_summary.get("foreign_nonts_row") or {})
        if corporate and reserves and foreign_nonts:
            strict_top_gap_anomaly_backdrop_findings.append(
                "Relative to same-bucket peers, `2009Q4` combines nonfinancial-corporate DI-loans weakness "
                f"(≈ {float(corporate.get('anomaly_minus_peer_delta') or 0.0):.2f}) with weaker reserves "
                f"(≈ {float(reserves.get('anomaly_minus_peer_delta') or 0.0):.2f}) and weaker foreign NONTS "
                f"(≈ {float(foreign_nonts.get('anomaly_minus_peer_delta') or 0.0):.2f})."
            )
    big_picture_synthesis_findings: list[str] = []
    if (
        big_picture_synthesis_summary is not None
        and str(big_picture_synthesis_summary.get("status", "not_available")) == "available"
    ):
        snapshot = dict(big_picture_synthesis_summary.get("h0_snapshot", {}))
        excluded_residual = snapshot.get("toc_row_excluded_residual_beta")
        excluded_total = snapshot.get("toc_row_excluded_strict_identifiable_total_beta")
        if excluded_residual is not None and excluded_total is not None:
            big_picture_synthesis_findings.append(
                "The main big-picture read is now explicit: scope mismatch is real but partial, TOC/ROW dominates the residual problem, and the independent lane still does not validate the remaining object "
                f"(TOC/ROW-excluded h0 residual ≈ {float(excluded_residual):.2f} versus strict identifiable total ≈ {float(excluded_total):.2f})."
            )
        interpretation = str(big_picture_synthesis_summary.get("interpretation", ""))
        if interpretation:
            big_picture_synthesis_findings.append(f"Current synthesis classification = `{interpretation}`.")
    treatment_object_comparison_findings: list[str] = []
    if (
        treatment_object_comparison_summary is not None
        and str(treatment_object_comparison_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(treatment_object_comparison_summary.get("recommendation", {}))
        if recommendation:
            treatment_object_comparison_findings.append(
                "Treatment-object comparison now says the next redesign branch should be "
                f"`{str(recommendation.get('recommended_next_branch', 'unknown'))}`, and that the repo should "
                f"{str(recommendation.get('headline_decision_now', 'keep the current headline provisional'))}."
            )
    split_treatment_architecture_findings: list[str] = []
    if (
        split_treatment_architecture_summary is not None
        and str(split_treatment_architecture_summary.get("status", "not_available")) == "available"
    ):
        h0_split = dict(split_treatment_architecture_summary.get("key_horizons", {}).get("h0", {}) or {})
        support_bundle_beta = h0_split.get("support_bundle_beta")
        core_residual = dict(h0_split.get("core_deposit_proximate_residual_response", {}) or {}).get("beta")
        if support_bundle_beta is not None and core_residual is not None:
            split_treatment_architecture_findings.append(
                "The explicit split treatment read is now available: "
                f"h0 TOC/ROW support bundle ≈ {float(support_bundle_beta):.2f}, "
                f"core residual ≈ {float(core_residual):.2f}."
            )
    core_treatment_promotion_findings: list[str] = []
    if (
        core_treatment_promotion_summary is not None
        and str(core_treatment_promotion_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(core_treatment_promotion_summary.get("promotion_recommendation", {}) or {})
        strict_validation = dict(core_treatment_promotion_summary.get("strict_validation_check", {}) or {})
        core_residual = strict_validation.get("h0_core_residual_beta")
        strict_total = strict_validation.get("h0_strict_identifiable_total_beta")
        if recommendation:
            core_treatment_promotion_findings.append(
                "Core-treatment promotion diagnostics now say to "
                f"`{str(recommendation.get('status', 'keep_interpretive_only'))}` and "
                f"{str(recommendation.get('current_release_role', 'keep the split architecture interpretive'))}."
            )
        if core_residual is not None and strict_total is not None:
            core_treatment_promotion_findings.append(
                "The promotion check still fails direct-count validation at h0: "
                f"core residual ≈ {float(core_residual):.2f}, strict total ≈ {float(strict_total):.2f}."
            )
    strict_redesign_findings: list[str] = []
    if strict_redesign_summary is not None and str(strict_redesign_summary.get("status", "not_available")) == "available":
        problem = dict(strict_redesign_summary.get("current_strict_problem_definition", {}) or {})
        failure_modes = dict(strict_redesign_summary.get("failure_modes", {}) or {})
        scope = dict(failure_modes.get("scope_mismatch_not_primary", {}) or {})
        loan_shape = dict(failure_modes.get("loan_bucket_shape", {}) or {})
        funding = dict(failure_modes.get("funding_offset_instability", {}) or {})
        remaining_share = scope.get("h0_remaining_share_of_baseline_strict_gap")
        core_residual = problem.get("h0_core_residual_beta")
        strict_total = problem.get("h0_toc_row_excluded_strict_identifiable_total_beta")
        dominant_loan = loan_shape.get("h0_dominant_loan_component")
        funding_share = funding.get("h0_funding_offset_share_of_identifiable_total_beta")
        if remaining_share is not None and core_residual is not None and strict_total is not None:
            strict_redesign_findings.append(
                "Strict-lane redesign now has a clear starting point: scope mismatch is no longer the main blocker, because about "
                f"{float(remaining_share):.2f} of the baseline strict gap still remains after the matched-bank-leg scope check while the h0 core residual is ≈ {float(core_residual):.2f} and the TOC/ROW-excluded direct-count total is ≈ {float(strict_total):.2f}."
            )
        if dominant_loan is not None:
            strict_redesign_findings.append(
                f"The next strict build should focus on loan-core classification rather than more treatment tweaks: the dominant h0 loan block is `{str(dominant_loan)}`."
            )
        if funding_share is not None:
            strict_redesign_findings.append(
                f"Funding offsets remain secondary but materially large at h0 (≈ {float(funding_share):.2f} of signed identifiable total), so more netting should wait until the loan core is redesigned."
            )
    strict_loan_core_redesign_findings: list[str] = []
    if (
        strict_loan_core_redesign_summary is not None
        and str(strict_loan_core_redesign_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_loan_core_redesign_summary.get("recommendation", {}) or {})
        h0_core = dict(strict_loan_core_redesign_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {})
        residual = dict(h0_core.get("core_residual_response", {}) or {}).get("beta")
        broad = dict(h0_core.get("current_broad_loan_source_response", {}) or {}).get("beta")
        direct = dict(h0_core.get("redesigned_direct_min_core_response", {}) or {}).get("beta")
        private_aug = dict(h0_core.get("private_borrower_augmented_core_response", {}) or {}).get("beta")
        noncore = dict(h0_core.get("noncore_system_diagnostic_response", {}) or {}).get("beta")
        if None not in (residual, broad, direct, private_aug, noncore):
            strict_loan_core_redesign_findings.append(
                "The strict loan-core redesign is now estimated under the core-deposit-proximate shock: "
                f"core residual ≈ {float(residual):.2f}, current broad loan source ≈ {float(broad):.2f}, "
                f"direct minimum core ≈ {float(direct):.2f}, private-borrower-augmented core ≈ {float(private_aug):.2f}, "
                f"noncore/system diagnostic ≈ {float(noncore):.2f}."
            )
        if recommendation:
            strict_loan_core_redesign_findings.append(
                "Current loan-core role design = "
                f"`{str(recommendation.get('release_headline_candidate', 'strict_loan_core_min_qoq'))}` as the headline direct core, "
                f"`{str(recommendation.get('standard_secondary_candidate', 'strict_loan_core_plus_private_borrower_qoq'))}` as the standard secondary comparison, "
                f"and `{str(recommendation.get('diagnostic_di_bucket', 'strict_loan_di_loans_nec_qoq'))}` kept diagnostic."
            )
    strict_di_bucket_role_findings: list[str] = []
    if (
        strict_di_bucket_role_summary is not None
        and str(strict_di_bucket_role_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_di_bucket_role_summary.get("recommendation", {}) or {})
        h0_bridge = dict(strict_di_bucket_role_summary.get("key_horizons", {}).get("h0", {}) or {})
        private_increment = h0_bridge.get("private_borrower_increment_beta")
        dominant = h0_bridge.get("dominant_borrower_component")
        if recommendation:
            strict_di_bucket_role_findings.append(
                "The broad DI-loans-n.e.c. bucket is now explicitly non-headline: "
                f"headline = `{str(recommendation.get('headline_direct_core', 'strict_loan_core_min_qoq'))}`, "
                f"standard secondary = `{str(recommendation.get('standard_secondary_comparison', 'strict_loan_core_plus_private_borrower_qoq'))}`, "
                f"diagnostic DI bucket = `{str(recommendation.get('diagnostic_di_bucket', 'strict_loan_di_loans_nec_qoq'))}`."
            )
        if private_increment is not None and dominant is not None:
            strict_di_bucket_role_findings.append(
                "At h0, the bounded private-borrower increment relative to the headline direct core is about "
                f"{float(private_increment):.2f}, with dominant borrower counterpart `{str(dominant)}` inside the cross-scope DI diagnostic."
            )
    strict_di_bucket_bridge_findings: list[str] = []
    if (
        strict_di_bucket_bridge_summary is not None
        and str(strict_di_bucket_bridge_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_di_bucket_bridge_summary.get("recommendation", {}) or {})
        h0_bridge = dict(
            strict_di_bucket_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
        )
        di_asset = dict(h0_bridge.get("di_asset_response", {}) or {}).get("beta")
        private_bridge = dict(h0_bridge.get("private_borrower_bridge_response", {}) or {}).get("beta")
        noncore_bridge = dict(h0_bridge.get("noncore_system_bridge_response", {}) or {}).get("beta")
        bridge_residual = h0_bridge.get("bridge_residual_beta")
        if None not in (di_asset, private_bridge, noncore_bridge, bridge_residual):
            strict_di_bucket_bridge_findings.append(
                "The DI bucket is now measured as an explicit bridge at h0 under the core-deposit-proximate shock: "
                f"DI asset ≈ {float(di_asset):.2f}, private bridge ≈ {float(private_bridge):.2f}, "
                f"noncore/system bridge ≈ {float(noncore_bridge):.2f}, bridge residual ≈ {float(bridge_residual):.2f}."
            )
        if recommendation:
            strict_di_bucket_bridge_findings.append(
                "Current DI-bucket bridge recommendation = keep the release role diagnostic-only and make the next heavy branch "
                f"`{str(recommendation.get('next_branch', 'build_counterpart_alignment_surface'))}`."
            )
    strict_private_borrower_bridge_findings: list[str] = []
    if (
        strict_private_borrower_bridge_summary is not None
        and str(strict_private_borrower_bridge_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_private_borrower_bridge_summary.get("recommendation", {}) or {})
        h0_private = dict(
            strict_private_borrower_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
        )
        private_total = dict(h0_private.get("private_bridge_response", {}) or {}).get("beta")
        households = dict(h0_private.get("households_nonprofits_response", {}) or {}).get("beta")
        corporate = dict(h0_private.get("nonfinancial_corporate_response", {}) or {}).get("beta")
        noncorporate = dict(h0_private.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
        if None not in (private_total, households, corporate, noncorporate):
            strict_private_borrower_bridge_findings.append(
                "The private-borrower bridge is now split explicitly at h0 under the core-deposit-proximate shock: "
                f"private total ≈ {float(private_total):.2f}, households/nonprofits ≈ {float(households):.2f}, "
                f"nonfinancial corporate ≈ {float(corporate):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
            )
        if recommendation:
            strict_private_borrower_bridge_findings.append(
                "Current private-bridge recommendation = "
                f"`{str(recommendation.get('next_branch', 'build_nonfinancial_corporate_bridge_surface'))}`."
            )
    strict_nonfinancial_corporate_bridge_findings: list[str] = []
    if (
        strict_nonfinancial_corporate_bridge_summary is not None
        and str(strict_nonfinancial_corporate_bridge_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_nonfinancial_corporate_bridge_summary.get("recommendation", {}) or {})
        h0_corp = dict(
            strict_nonfinancial_corporate_bridge_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
        )
        corporate = dict(h0_corp.get("nonfinancial_corporate_response", {}) or {}).get("beta")
        private_total = dict(h0_corp.get("private_bridge_response", {}) or {}).get("beta")
        households = dict(h0_corp.get("households_nonprofits_response", {}) or {}).get("beta")
        noncorporate = dict(h0_corp.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
        if None not in (corporate, private_total, households, noncorporate):
            strict_nonfinancial_corporate_bridge_findings.append(
                "The nonfinancial-corporate bridge is now explicit at h0 under the core-deposit-proximate shock: "
                f"nonfinancial corporate ≈ {float(corporate):.2f}, private total ≈ {float(private_total):.2f}, "
                f"households/nonprofits ≈ {float(households):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
            )
        if recommendation:
            strict_nonfinancial_corporate_bridge_findings.append(
                "Current nonfinancial-corporate bridge recommendation = "
                f"`{str(recommendation.get('next_branch', 'assess_household_and_nonfinancial_noncorporate_offset_residual'))}`."
            )
    strict_private_offset_residual_findings: list[str] = []
    if (
        strict_private_offset_residual_summary is not None
        and str(strict_private_offset_residual_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_private_offset_residual_summary.get("recommendation", {}) or {})
        h0_offset = dict(
            strict_private_offset_residual_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
        )
        offset_total = dict(h0_offset.get("private_offset_total_response", {}) or {}).get("beta")
        private_total = dict(h0_offset.get("private_bridge_response", {}) or {}).get("beta")
        households = dict(h0_offset.get("households_nonprofits_response", {}) or {}).get("beta")
        noncorporate = dict(h0_offset.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
        if None not in (offset_total, private_total, households, noncorporate):
            strict_private_offset_residual_findings.append(
                "The remaining private offset block is now explicit at h0 under the core-deposit-proximate shock: "
                f"offset total ≈ {float(offset_total):.2f}, private total ≈ {float(private_total):.2f}, "
                f"households/nonprofits ≈ {float(households):.2f}, nonfinancial noncorporate ≈ {float(noncorporate):.2f}."
            )
        if recommendation:
            strict_private_offset_residual_findings.append(
                "Current private-offset recommendation = "
                f"`{str(recommendation.get('next_branch', 'assess_corporate_bridge_secondary_comparison_role'))}`."
            )
    strict_corporate_bridge_secondary_findings: list[str] = []
    if (
        strict_corporate_bridge_secondary_comparison_summary is not None
        and str(strict_corporate_bridge_secondary_comparison_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_corporate_bridge_secondary_comparison_summary.get("recommendation", {}) or {})
        h0_secondary = dict(
            strict_corporate_bridge_secondary_comparison_summary.get("key_horizons", {})
            .get("h0", {})
            .get("core_deposit_proximate", {})
            or {}
        )
        residual = dict(h0_secondary.get("core_residual_response", {}) or {}).get("beta")
        private_bridge = dict(h0_secondary.get("core_plus_private_bridge_response", {}) or {}).get("beta")
        corporate_bridge = dict(h0_secondary.get("core_plus_nonfinancial_corporate_response", {}) or {}).get("beta")
        if None not in (residual, private_bridge, corporate_bridge):
            strict_corporate_bridge_secondary_findings.append(
                "The secondary strict comparison is now explicit at h0 under the core-deposit-proximate shock: "
                f"core residual ≈ {float(residual):.2f}, core + private bridge ≈ {float(private_bridge):.2f}, "
                f"core + nonfinancial corporate ≈ {float(corporate_bridge):.2f}."
            )
        if recommendation:
            strict_corporate_bridge_secondary_findings.append(
                "Current secondary-role recommendation = "
                f"`{str(recommendation.get('standard_secondary_candidate', 'strict_loan_core_plus_private_borrower_qoq'))}` as the standard secondary comparison."
            )
    strict_component_framework_findings: list[str] = []
    if (
        strict_component_framework_summary is not None
        and str(strict_component_framework_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_component_framework_summary.get("recommendation", {}) or {})
        h0_framework = dict(strict_component_framework_summary.get("h0_snapshot", {}) or {})
        support_bundle = h0_framework.get("toc_row_support_bundle_beta")
        core_residual = h0_framework.get("core_residual_beta")
        standard_secondary = h0_framework.get("standard_secondary_beta")
        narrowing = h0_framework.get("narrowing_diagnostic_beta")
        if None not in (support_bundle, core_residual, standard_secondary, narrowing):
            strict_component_framework_findings.append(
                "The strict deposit-component framework is now frozen for release framing: "
                f"h0 TOC/ROW support bundle ≈ {float(support_bundle):.2f}, core residual ≈ {float(core_residual):.2f}, "
                f"standard secondary ≈ {float(standard_secondary):.2f}, narrowing diagnostic ≈ {float(narrowing):.2f}."
            )
        if recommendation:
            strict_component_framework_findings.append(
                "Current framework recommendation = "
                f"`{str(recommendation.get('status', 'use_frozen_framework_and_run_toc_row_incidence_audit'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units'))}`."
            )
    toc_row_incidence_findings: list[str] = []
    if toc_row_incidence_audit_summary is not None and str(toc_row_incidence_audit_summary.get("status", "not_available")) == "available":
        h0_incidence = dict(toc_row_incidence_audit_summary.get("key_horizons", {}).get("h0", {}) or {})
        toc_leg = dict(h0_incidence.get("toc_leg", {}) or {})
        row_leg = dict(h0_incidence.get("row_leg", {}) or {})
        toc_deposit_share = toc_leg.get("in_scope_deposit_proxy_share_of_toc_beta")
        toc_reserve_share = toc_leg.get("reserve_capture_share_of_toc_beta")
        row_deposit_share = row_leg.get("in_scope_deposit_proxy_share_of_row_beta")
        row_external_share = row_leg.get("external_support_share_of_row_beta")
        if None not in (toc_deposit_share, toc_reserve_share, row_deposit_share, row_external_share):
            toc_row_incidence_findings.append(
                "The first TOC/ROW incidence audit says the strict problem is not symmetric across legs: "
                f"TOC h0 deposit-proxy share ≈ {float(toc_deposit_share):.2f} versus reserve share ≈ {float(toc_reserve_share):.2f}, "
                f"ROW deposit-proxy share ≈ {float(row_deposit_share):.2f} versus external-support share ≈ {float(row_external_share):.2f}."
            )
    toc_row_liability_incidence_raw_findings: list[str] = []
    if (
        toc_row_liability_incidence_raw_summary is not None
        and str(toc_row_liability_incidence_raw_summary.get("status", "not_available")) == "available"
    ):
        h0_raw = dict(toc_row_liability_incidence_raw_summary.get("key_horizons", {}).get("h0", {}) or {})
        toc_leg = dict(h0_raw.get("toc_leg", {}) or {})
        row_leg = dict(h0_raw.get("row_leg", {}) or {})
        toc_shares = dict(toc_leg.get("counterpart_share_of_leg_beta", {}) or {})
        row_shares = dict(row_leg.get("counterpart_share_of_leg_beta", {}) or {})
        toc_dep_only = toc_shares.get("deposits_only_bank_qoq")
        toc_reserves = toc_shares.get("reserves_qoq")
        row_row_checkable = row_shares.get("checkable_rest_of_world_bank_qoq")
        row_external = row_shares.get("foreign_nonts_qoq")
        if None not in (toc_dep_only, toc_reserves, row_row_checkable, row_external):
            toc_row_liability_incidence_raw_findings.append(
                "The raw-units TOC/ROW liability-incidence audit turns the strict gate into a direct comparison: "
                f"TOC h0 deposits-only share ≈ {float(toc_dep_only):.2f} versus reserves share ≈ {float(toc_reserves):.2f}, "
                f"ROW-checkable share ≈ {float(row_row_checkable):.2f} versus foreign-NONTS share ≈ {float(row_external):.2f}."
            )
        decision_gate = dict(toc_row_liability_incidence_raw_summary.get("classification", {}) or {}).get("decision_gate")
        if decision_gate:
            toc_row_liability_incidence_raw_findings.append(
                f"Current raw-incidence binary gate = `{str(decision_gate)}`."
            )
    toc_validated_share_candidate_findings: list[str] = []
    if (
        toc_validated_share_candidate_summary is not None
        and str(toc_validated_share_candidate_summary.get("status", "not_available")) == "available"
    ):
        h0_candidate = dict(toc_validated_share_candidate_summary.get("key_horizons", {}).get("h0", {}) or {})
        best_candidate = dict(h0_candidate.get("best_candidate", {}) or {})
        core_residual = h0_candidate.get("core_residual_beta")
        direct_core = h0_candidate.get("headline_direct_core_beta")
        implied_residual = best_candidate.get("implied_residual_beta")
        abs_gap = best_candidate.get("abs_gap_to_direct_core")
        if None not in (core_residual, direct_core, implied_residual, abs_gap):
            toc_validated_share_candidate_findings.append(
                "The narrow-TOC candidate gate says even the best candidate still moves the strict comparison the wrong way: "
                f"core residual ≈ {float(core_residual):.2f}, direct core ≈ {float(direct_core):.2f}, "
                f"best-candidate implied residual ≈ {float(implied_residual):.2f}, abs gap ≈ {float(abs_gap):.2f}."
            )
        candidate_decision = dict(toc_validated_share_candidate_summary.get("classification", {}) or {}).get("decision")
        if candidate_decision:
            toc_validated_share_candidate_findings.append(
                f"Current narrow-TOC candidate decision = `{str(candidate_decision)}`."
            )
    strict_release_framing_findings: list[str] = []
    if (
        strict_release_framing_summary is not None
        and str(strict_release_framing_summary.get("status", "not_available")) == "available"
    ):
        release_position = dict(strict_release_framing_summary.get("release_position", {}) or {})
        recommendation = dict(strict_release_framing_summary.get("recommendation", {}) or {})
        h0_release = dict(strict_release_framing_summary.get("h0_snapshot", {}) or {})
        support_bundle = h0_release.get("toc_row_support_bundle_beta")
        core_residual = h0_release.get("core_residual_beta")
        direct_core = h0_release.get("headline_direct_core_beta")
        if None not in (support_bundle, core_residual, direct_core):
            strict_release_framing_findings.append(
                "The release-facing strict rule is now frozen: "
                f"TOC/ROW support bundle ≈ {float(support_bundle):.2f}, "
                f"core residual ≈ {float(core_residual):.2f}, "
                f"headline direct core ≈ {float(direct_core):.2f}; "
                "TOC and ROW stay outside the strict object under current evidence."
            )
        if release_position:
            strict_release_framing_findings.append(
                "Release position = full TDC as `"
                f"{str(release_position.get('full_tdc_release_role', 'broad_treasury_attributed_object_only'))}`"
                " and strict object rule `"
                f"{str(release_position.get('strict_object_rule', 'exclude_toc_and_row_under_current_evidence'))}`."
            )
        if recommendation:
            strict_release_framing_findings.append(
                "Current release-framing recommendation = "
                f"`{str(recommendation.get('status', 'strict_release_framing_finalized'))}` with reopen rule "
                f"`{str(recommendation.get('reopen_rule', 'reopen_only_if_new_scope_and_timing_matched_incidence_evidence_appears'))}`."
            )
    strict_direct_core_component_findings: list[str] = []
    if (
        strict_direct_core_component_summary is not None
        and str(strict_direct_core_component_summary.get("status", "not_available")) == "available"
    ):
        recommendation = dict(strict_direct_core_component_summary.get("recommendation", {}) or {})
        h0_component = dict(strict_direct_core_component_summary.get("key_horizons", {}).get("h0", {}) or {})
        core_component = dict(h0_component.get("core_deposit_proximate", {}) or {})
        residual = dict(core_component.get("residual_response", {}) or {}).get("beta")
        mortgages = dict(core_component.get("mortgages_response", {}) or {}).get("beta")
        consumer = dict(core_component.get("consumer_credit_response", {}) or {}).get("beta")
        direct = dict(core_component.get("direct_core_response", {}) or {}).get("beta")
        if None not in (residual, mortgages, consumer, direct):
            strict_direct_core_component_findings.append(
                "The direct-core split now isolates the headline strict benchmark itself at h0 under the core-deposit-proximate shock: "
                f"residual ≈ {float(residual):.2f}, mortgages ≈ {float(mortgages):.2f}, "
                f"consumer credit ≈ {float(consumer):.2f}, bundled direct core ≈ {float(direct):.2f}."
            )
        if recommendation:
            strict_direct_core_component_findings.append(
                "Current direct-core component recommendation = "
                f"`{str(recommendation.get('status', 'keep_bundled_direct_core'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'keep_bundled_core_and_reassess_other_strict_creator_channels'))}`."
            )
    strict_direct_core_horizon_stability_findings: list[str] = []
    if (
        strict_direct_core_horizon_stability_summary is not None
        and str(strict_direct_core_horizon_stability_summary.get("status", "not_available")) == "available"
    ):
        winners = dict(strict_direct_core_horizon_stability_summary.get("horizon_winners", {}) or {})
        recommendation = dict(strict_direct_core_horizon_stability_summary.get("recommendation", {}) or {})
        if winners:
            strict_direct_core_horizon_stability_findings.append(
                "The direct-core winner is horizon-specific rather than universal: "
                f"h0 = `{str(winners.get('h0', 'not_available'))}`, "
                f"h4 = `{str(winners.get('h4', 'not_available'))}`, "
                f"h8 = `{str(winners.get('h8', 'not_available'))}`."
            )
        if recommendation:
            strict_direct_core_horizon_stability_findings.append(
                "Current horizon-stability recommendation = "
                f"`{str(recommendation.get('status', 'keep_current_direct_core'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'reassess_direct_core_release_role'))}`."
            )
    strict_additional_creator_candidate_findings: list[str] = []
    if (
        strict_additional_creator_candidate_summary is not None
        and str(strict_additional_creator_candidate_summary.get("status", "not_available")) == "available"
    ):
        classification = dict(strict_additional_creator_candidate_summary.get("classification", {}) or {})
        recommendation = dict(strict_additional_creator_candidate_summary.get("recommendation", {}) or {})
        if classification:
            strict_additional_creator_candidate_findings.append(
                "The additional creator-channel search now separates broad validation proxies from true extension candidates: "
                f"h0 best validation proxy = `{str(classification.get('h0_best_validation_proxy', 'not_available'))}`, "
                f"h0 best extension candidate = `{str(classification.get('h0_best_extension_candidate', 'not_available'))}`."
            )
        if recommendation:
            strict_additional_creator_candidate_findings.append(
                "Current additional creator-channel recommendation = "
                f"`{str(recommendation.get('status', 'no_additional_extension_candidate_supported'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'freeze_creator_search_and_only_reopen_if_new_same_scope_channel_appears'))}`."
            )
    strict_di_loans_nec_measurement_audit_findings: list[str] = []
    if (
        strict_di_loans_nec_measurement_audit_summary is not None
        and str(strict_di_loans_nec_measurement_audit_summary.get("status", "not_available")) == "available"
    ):
        classification = dict(strict_di_loans_nec_measurement_audit_summary.get("classification", {}) or {})
        recommendation = dict(strict_di_loans_nec_measurement_audit_summary.get("recommendation", {}) or {})
        if classification:
            strict_di_loans_nec_measurement_audit_findings.append(
                "The DI-loans-n.e.c. measurement audit now states the public-data limit directly: "
                f"same-scope transaction-subcomponent status = `{str(classification.get('same_scope_transaction_subcomponent_status', 'not_available'))}`, "
                f"h0 best cross-scope bridge = `{str(classification.get('h0_best_cross_scope_transaction_bridge', 'not_available'))}`, "
                f"h0 best same-scope proxy = `{str(classification.get('h0_best_same_scope_proxy', 'not_available'))}`."
            )
        if recommendation:
            strict_di_loans_nec_measurement_audit_findings.append(
                "Current DI-loans-n.e.c. measurement-audit recommendation = "
                f"`{str(recommendation.get('status', 'no_promotable_same_scope_transaction_subcomponent_supported'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'freeze_framework_and_move_to_writeup_if_no_new_public_transaction_split_appears'))}`."
            )
    strict_results_closeout_findings: list[str] = []
    if (
        strict_results_closeout_summary is not None
        and str(strict_results_closeout_summary.get("status", "not_available")) == "available"
    ):
        classification = dict(strict_results_closeout_summary.get("classification", {}) or {})
        recommendation = dict(strict_results_closeout_summary.get("recommendation", {}) or {})
        if classification:
            strict_results_closeout_findings.append(
                "The strict closeout summary now says the empirical expansion branch is effectively complete under current evidence: "
                f"branch state = `{str(classification.get('branch_state', 'not_available'))}`, "
                f"closeout readiness = `{str(classification.get('closeout_readiness', 'not_available'))}`."
            )
        if recommendation:
            strict_results_closeout_findings.append(
                "Current strict closeout recommendation = "
                f"`{str(recommendation.get('status', 'move_to_writeup_and_results_packaging'))}` with next branch "
                f"`{str(recommendation.get('next_branch', 'writeup_results_and_release_packaging'))}`."
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
            "A strict source-side lane now runs in parallel to the imported accounting reconstruction so the non-TDC residual can be compared against both direct identifiable transactions and a separate closure-oriented check.",
            "The public surface keeps the exact identity baseline as the headline path and treats counterpart or deposit side reads as secondary diagnostics rather than settled mechanism evidence.",
            "Period sensitivity remains on the public surface because medium-horizon persistence differs across the post-GFC early, pre-COVID, and COVID/post-COVID windows.",
            f"{share_other_negative:.1%} of quarters show `other_component_qoq < 0` in the headline sample.",
            *scope_findings,
            *strict_gap_findings,
            *tdc_treatment_findings,
            *treasury_operating_cash_findings,
            *rest_of_world_findings,
            *toc_row_path_split_findings,
            *toc_row_excluded_findings,
            *strict_missing_channel_findings,
            *strict_sign_mismatch_findings,
            *strict_shock_composition_findings,
            *strict_top_gap_quarter_findings,
            *strict_top_gap_quarter_direction_findings,
            *strict_top_gap_inversion_findings,
            *strict_top_gap_anomaly_findings,
            *strict_top_gap_anomaly_component_split_findings,
            *strict_top_gap_anomaly_di_loans_split_findings,
            *strict_top_gap_anomaly_backdrop_findings,
            *big_picture_synthesis_findings,
            *treatment_object_comparison_findings,
            *split_treatment_architecture_findings,
            *core_treatment_promotion_findings,
            *strict_redesign_findings,
            *strict_loan_core_redesign_findings,
            *strict_di_bucket_role_findings,
            *strict_di_bucket_bridge_findings,
            *strict_private_borrower_bridge_findings,
            *strict_nonfinancial_corporate_bridge_findings,
            *strict_private_offset_residual_findings,
            *strict_corporate_bridge_secondary_findings,
            *strict_component_framework_findings,
            *toc_row_incidence_findings,
            *toc_row_liability_incidence_raw_findings,
            *toc_validated_share_candidate_findings,
            *strict_release_framing_findings,
            *strict_direct_core_component_findings,
            *strict_direct_core_horizon_stability_findings,
            *strict_additional_creator_candidate_findings,
            *strict_di_loans_nec_measurement_audit_findings,
            *strict_results_closeout_findings,
            *broad_scope_findings,
            *counterpart_findings,
        ],
        "caveats": [
            "Current release wording is gated by readiness diagnostics: the live bundle should be read as an exploratory deposit-response readout, not a clean headline causal decomposition, until total and non-TDC responses separate more clearly.",
            "When the exact identity baseline and the older approximate dynamic LP path disagree, interpretation should default to the exact baseline and treat the older path as a robustness diagnostic rather than the headline read.",
            "Headline pass-through and crowd-out ratios are out of scope in the current release until the repo has a dimensionally coherent first-stage gate for raw-unit treatment responses.",
            "bill_share is a quarterly issue-date share of Treasury bill auction offering amounts from FiscalData; it remains an exploratory sensitivity input, not a live regime export or standalone mechanism proof.",
            "bill_share-linked shock variants are retained only as exploratory stress tests because they preserve the impact-stage sign pattern but change medium-horizon persistence enough that they should not be promoted into the headline treatment family.",
            "Structural proxies are partial cross-checks on the residual, not exhaustive counterpart accounting or standalone mechanism proof.",
            "The strict source-side lane is deliberately incomplete and gross; a large strict gap is evidence about what remains uncounted, not a license to backsolve a plug into the headline strict measure.",
            *scope_caveats,
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
                "strict_loan_source_qoq",
                "strict_non_treasury_securities_qoq",
                "strict_identifiable_total_qoq",
                "strict_identifiable_gap_qoq",
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
                "core_treatment_promotion_summary",
                "strict_redesign_summary",
                "strict_loan_core_redesign_summary",
                "strict_di_bucket_role_summary",
                "strict_di_bucket_bridge_summary",
                "strict_private_borrower_bridge_summary",
                "strict_nonfinancial_corporate_bridge_summary",
                "strict_private_offset_residual_summary",
                "strict_corporate_bridge_secondary_comparison_summary",
                "strict_component_framework_summary",
                "strict_direct_core_component_summary",
                "strict_direct_core_horizon_stability_summary",
                "strict_additional_creator_candidate_summary",
                "strict_release_framing_summary",
                "toc_row_incidence_audit_summary",
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
            "site/data/lp_irf_accounting_identity.csv",
            "site/data/lp_irf_strict_identifiable.csv",
            "site/data/accounting_identity_alignment.csv",
            "site/data/accounting_identity_summary.json",
            "site/data/strict_funding_offset_alignment.csv",
            "site/data/strict_identifiable_alignment.csv",
            "site/data/strict_identifiable_summary.json",
            "site/data/strict_identifiable_followup_summary.json",
            "site/data/strict_top_gap_inversion_summary.json",
            "site/data/strict_top_gap_anomaly_summary.json",
            "site/data/strict_top_gap_anomaly_component_split_summary.json",
            "site/data/strict_top_gap_anomaly_di_loans_split_summary.json",
            "site/data/strict_top_gap_anomaly_backdrop_summary.json",
            "site/data/big_picture_synthesis_summary.json",
            "site/data/treatment_object_comparison_summary.json",
            "site/data/split_treatment_architecture_summary.json",
            "site/data/core_treatment_promotion_summary.json",
            "site/data/strict_redesign_summary.json",
            "site/data/strict_loan_core_redesign_summary.json",
            "site/data/strict_di_bucket_role_summary.json",
            "site/data/strict_di_bucket_bridge_summary.json",
            "site/data/strict_private_borrower_bridge_summary.json",
            "site/data/strict_nonfinancial_corporate_bridge_summary.json",
            "site/data/strict_private_offset_residual_summary.json",
            "site/data/strict_corporate_bridge_secondary_comparison_summary.json",
            "site/data/strict_component_framework_summary.json",
            "site/data/strict_direct_core_component_summary.json",
            "site/data/strict_direct_core_horizon_stability_summary.json",
            "site/data/strict_additional_creator_candidate_summary.json",
            "site/data/strict_release_framing_summary.json",
            "site/data/toc_row_incidence_audit_summary.json",
            "site/data/toc_row_liability_incidence_raw_summary.json",
            "site/data/scope_alignment_summary.json",
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
        bootstrap_reps=int(sensitivity_spec.get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(sensitivity_spec.get("identity_bootstrap_block_length", 4)),
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
        bootstrap_reps=int(sensitivity_spec.get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(sensitivity_spec.get("identity_bootstrap_block_length", 4)),
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
            bootstrap_reps=int(control_spec.get("identity_bootstrap_reps", 40)),
            bootstrap_block_length=int(control_spec.get("identity_bootstrap_block_length", 4)),
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
            bootstrap_reps=int(sample_spec.get("identity_bootstrap_reps", 40)),
            bootstrap_block_length=int(sample_spec.get("identity_bootstrap_block_length", 4)),
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
    accounting_identity_lp_irf = slice_accounting_identity_lp_irf(lp_outputs["lp_irf"])
    accounting_identity_lp_irf_path = root / "output" / "models" / "lp_irf_accounting_identity.csv"
    export_frame(accounting_identity_lp_irf, accounting_identity_lp_irf_path)
    accounting_identity_alignment = build_accounting_identity_alignment_frame(accounting_identity_lp_irf)
    accounting_identity_alignment_path = root / "output" / "models" / "accounting_identity_alignment.csv"
    export_frame(accounting_identity_alignment, accounting_identity_alignment_path)
    accounting_identity_summary = build_accounting_identity_summary(
        lp_irf=accounting_identity_lp_irf,
        accounting_source_kind=build_result.accounting_source_kind,
    )
    accounting_identity_summary_path = root / "output" / "models" / "accounting_identity_summary.json"
    write_json_payload(accounting_identity_summary_path, accounting_identity_summary)
    strict_identifiable_lp_irf = slice_strict_identifiable_lp_irf(lp_outputs["lp_irf"])
    strict_identifiable_lp_irf_path = root / "output" / "models" / "lp_irf_strict_identifiable.csv"
    export_frame(strict_identifiable_lp_irf, strict_identifiable_lp_irf_path)
    strict_identifiable_alignment = build_strict_identifiable_alignment_frame(strict_identifiable_lp_irf)
    strict_identifiable_alignment_path = root / "output" / "models" / "strict_identifiable_alignment.csv"
    export_frame(strict_identifiable_alignment, strict_identifiable_alignment_path)
    strict_funding_offset_alignment = build_strict_funding_offset_alignment_frame(strict_identifiable_lp_irf)
    strict_funding_offset_alignment_path = root / "output" / "models" / "strict_funding_offset_alignment.csv"
    export_frame(strict_funding_offset_alignment, strict_funding_offset_alignment_path)
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
    strict_identifiable_summary = build_strict_identifiable_summary(
        lp_irf=strict_identifiable_lp_irf,
        strict_source_kind=build_result.strict_source_kind,
    )
    strict_identifiable_summary_path = root / "output" / "models" / "strict_identifiable_summary.json"
    write_json_payload(strict_identifiable_summary_path, strict_identifiable_summary)
    strict_identifiable_followup_summary = build_strict_identifiable_followup_summary(
        identity_baseline_lp_irf=identity_baseline,
        identity_measurement_ladder=identity_measurement_ladder,
        lp_irf=lp_outputs["lp_irf"],
        strict_source_kind=build_result.strict_source_kind,
    )
    strict_identifiable_followup_summary_path = root / "output" / "models" / "strict_identifiable_followup_summary.json"
    write_json_payload(strict_identifiable_followup_summary_path, strict_identifiable_followup_summary)
    scope_alignment_summary = build_scope_alignment_summary(
        shocked=shocked,
        lp_specs=lp_specs,
        shock_specs=all_shock_specs,
    )
    scope_alignment_summary_path = root / "output" / "models" / "scope_alignment_summary.json"
    write_json_payload(scope_alignment_summary_path, scope_alignment_summary)
    tdc_treatment_audit_summary = build_tdc_treatment_audit_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        shock_specs=dict(all_shock_specs),
        bootstrap_reps=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_block_length", 4)),
    )
    tdc_treatment_audit_summary_path = root / "output" / "models" / "tdc_treatment_audit_summary.json"
    write_json_payload(tdc_treatment_audit_summary_path, tdc_treatment_audit_summary)
    treasury_operating_cash_audit_summary = build_treasury_operating_cash_audit_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
    )
    treasury_operating_cash_audit_summary_path = (
        root / "output" / "models" / "treasury_operating_cash_audit_summary.json"
    )
    write_json_payload(
        treasury_operating_cash_audit_summary_path,
        treasury_operating_cash_audit_summary,
    )
    treasury_cash_regime_audit_summary = build_treasury_cash_regime_audit_summary(
        shocked=shocked,
    )
    treasury_cash_regime_audit_summary_path = root / "output" / "models" / "treasury_cash_regime_audit_summary.json"
    write_json_payload(
        treasury_cash_regime_audit_summary_path,
        treasury_cash_regime_audit_summary,
    )
    historical_cash_term_reestimation_summary = build_historical_cash_term_reestimation_summary(
        shocked=shocked,
        canonical_tdc_source_path=build_result.canonical_tdc_source_path,
        root=root,
    )
    historical_cash_term_reestimation_summary_path = (
        root / "output" / "models" / "historical_cash_term_reestimation_summary.json"
    )
    write_json_payload(
        historical_cash_term_reestimation_summary_path,
        historical_cash_term_reestimation_summary,
    )
    rest_of_world_treasury_audit_summary = build_rest_of_world_treasury_audit_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
    )
    rest_of_world_treasury_audit_summary_path = (
        root / "output" / "models" / "rest_of_world_treasury_audit_summary.json"
    )
    write_json_payload(
        rest_of_world_treasury_audit_summary_path,
        rest_of_world_treasury_audit_summary,
    )
    toc_row_bundle_audit_summary = build_toc_row_bundle_audit_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
    )
    toc_row_bundle_audit_summary_path = root / "output" / "models" / "toc_row_bundle_audit_summary.json"
    write_json_payload(
        toc_row_bundle_audit_summary_path,
        toc_row_bundle_audit_summary,
    )
    toc_row_path_split_summary = build_toc_row_path_split_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
    )
    toc_row_path_split_summary_path = root / "output" / "models" / "toc_row_path_split_summary.json"
    write_json_payload(
        toc_row_path_split_summary_path,
        toc_row_path_split_summary,
    )
    toc_row_excluded_interpretation_summary = build_toc_row_excluded_interpretation_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        excluded_shock_spec=dict(all_shock_specs["unexpected_tdc_no_toc_no_row_bank_only"]),
        bootstrap_reps=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_block_length", 4)),
    )
    toc_row_excluded_interpretation_summary_path = (
        root / "output" / "models" / "toc_row_excluded_interpretation_summary.json"
    )
    write_json_payload(
        toc_row_excluded_interpretation_summary_path,
        toc_row_excluded_interpretation_summary,
    )
    strict_missing_channel_summary = build_strict_missing_channel_summary(
        strict_lp_irf=strict_identifiable_lp_irf,
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        excluded_shock_spec=dict(all_shock_specs["unexpected_tdc_no_toc_no_row_bank_only"]),
    )
    strict_missing_channel_summary_path = root / "output" / "models" / "strict_missing_channel_summary.json"
    write_json_payload(
        strict_missing_channel_summary_path,
        strict_missing_channel_summary,
    )
    strict_sign_mismatch_audit_summary = build_strict_sign_mismatch_audit_summary(
        shocked=shocked,
        strict_missing_channel_summary=strict_missing_channel_summary,
    )
    strict_sign_mismatch_audit_summary_path = root / "output" / "models" / "strict_sign_mismatch_audit_summary.json"
    write_json_payload(
        strict_sign_mismatch_audit_summary_path,
        strict_sign_mismatch_audit_summary,
    )
    strict_shock_composition_summary = build_strict_shock_composition_summary(
        shocked=shocked,
    )
    strict_shock_composition_summary_path = root / "output" / "models" / "strict_shock_composition_summary.json"
    write_json_payload(
        strict_shock_composition_summary_path,
        strict_shock_composition_summary,
    )
    strict_top_gap_quarter_audit_summary = build_strict_top_gap_quarter_audit_summary(
        shocked=shocked,
    )
    strict_top_gap_quarter_audit_summary_path = (
        root / "output" / "models" / "strict_top_gap_quarter_audit_summary.json"
    )
    write_json_payload(
        strict_top_gap_quarter_audit_summary_path,
        strict_top_gap_quarter_audit_summary,
    )
    strict_top_gap_quarter_direction_summary = build_strict_top_gap_quarter_direction_summary(
        shocked=shocked,
    )
    strict_top_gap_quarter_direction_summary_path = (
        root / "output" / "models" / "strict_top_gap_quarter_direction_summary.json"
    )
    write_json_payload(
        strict_top_gap_quarter_direction_summary_path,
        strict_top_gap_quarter_direction_summary,
    )
    strict_top_gap_inversion_summary = build_strict_top_gap_inversion_summary(
        shocked=shocked,
    )
    strict_top_gap_inversion_summary_path = root / "output" / "models" / "strict_top_gap_inversion_summary.json"
    write_json_payload(
        strict_top_gap_inversion_summary_path,
        strict_top_gap_inversion_summary,
    )
    strict_top_gap_anomaly_summary = build_strict_top_gap_anomaly_summary(
        shocked=shocked,
    )
    strict_top_gap_anomaly_summary_path = root / "output" / "models" / "strict_top_gap_anomaly_summary.json"
    write_json_payload(
        strict_top_gap_anomaly_summary_path,
        strict_top_gap_anomaly_summary,
    )
    strict_top_gap_anomaly_component_split_summary = build_strict_top_gap_anomaly_component_split_summary(
        shocked=shocked,
        strict_top_gap_anomaly_summary=strict_top_gap_anomaly_summary,
    )
    strict_top_gap_anomaly_component_split_summary_path = (
        root / "output" / "models" / "strict_top_gap_anomaly_component_split_summary.json"
    )
    write_json_payload(
        strict_top_gap_anomaly_component_split_summary_path,
        strict_top_gap_anomaly_component_split_summary,
    )
    strict_top_gap_anomaly_di_loans_split_summary = build_strict_top_gap_anomaly_di_loans_split_summary(
        shocked=shocked,
        strict_top_gap_anomaly_summary=strict_top_gap_anomaly_summary,
    )
    strict_top_gap_anomaly_di_loans_split_summary_path = (
        root / "output" / "models" / "strict_top_gap_anomaly_di_loans_split_summary.json"
    )
    write_json_payload(
        strict_top_gap_anomaly_di_loans_split_summary_path,
        strict_top_gap_anomaly_di_loans_split_summary,
    )
    strict_top_gap_anomaly_backdrop_summary = build_strict_top_gap_anomaly_backdrop_summary(
        shocked=shocked,
        strict_top_gap_anomaly_summary=strict_top_gap_anomaly_summary,
    )
    strict_top_gap_anomaly_backdrop_summary_path = (
        root / "output" / "models" / "strict_top_gap_anomaly_backdrop_summary.json"
    )
    write_json_payload(
        strict_top_gap_anomaly_backdrop_summary_path,
        strict_top_gap_anomaly_backdrop_summary,
    )
    broad_scope_system_summary = build_broad_scope_system_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        scope_alignment_summary=scope_alignment_summary,
        strict_identifiable_followup_summary=strict_identifiable_followup_summary,
        tdc_treatment_audit_summary=tdc_treatment_audit_summary,
        bootstrap_reps=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(lp_specs["specs"]["sensitivity"].get("identity_bootstrap_block_length", 4)),
    )
    broad_scope_system_summary_path = root / "output" / "models" / "broad_scope_system_summary.json"
    write_json_payload(broad_scope_system_summary_path, broad_scope_system_summary)
    big_picture_synthesis_summary = build_big_picture_synthesis_summary(
        scope_alignment_summary=scope_alignment_summary,
        broad_scope_system_summary=broad_scope_system_summary,
        tdc_treatment_audit_summary=tdc_treatment_audit_summary,
        toc_row_excluded_interpretation_summary=toc_row_excluded_interpretation_summary,
        strict_missing_channel_summary=strict_missing_channel_summary,
        strict_sign_mismatch_audit_summary=strict_sign_mismatch_audit_summary,
        strict_top_gap_anomaly_backdrop_summary=strict_top_gap_anomaly_backdrop_summary,
    )
    big_picture_synthesis_summary_path = root / "output" / "models" / "big_picture_synthesis_summary.json"
    write_json_payload(
        big_picture_synthesis_summary_path,
        big_picture_synthesis_summary,
    )
    treatment_object_comparison_summary = build_treatment_object_comparison_summary(
        scope_alignment_summary=scope_alignment_summary,
        broad_scope_system_summary=broad_scope_system_summary,
        tdc_treatment_audit_summary=tdc_treatment_audit_summary,
        toc_row_excluded_interpretation_summary=toc_row_excluded_interpretation_summary,
        strict_missing_channel_summary=strict_missing_channel_summary,
        strict_sign_mismatch_audit_summary=strict_sign_mismatch_audit_summary,
    )
    treatment_object_comparison_summary_path = root / "output" / "models" / "treatment_object_comparison_summary.json"
    write_json_payload(
        treatment_object_comparison_summary_path,
        treatment_object_comparison_summary,
    )
    split_treatment_architecture_summary = build_split_treatment_architecture_summary(
        shocked=shocked,
        tdc_treatment_audit_summary=tdc_treatment_audit_summary,
        toc_row_path_split_summary=toc_row_path_split_summary,
        treatment_object_comparison_summary=treatment_object_comparison_summary,
    )
    split_treatment_architecture_summary_path = root / "output" / "models" / "split_treatment_architecture_summary.json"
    write_json_payload(
        split_treatment_architecture_summary_path,
        split_treatment_architecture_summary,
    )
    core_treatment_promotion_summary = build_core_treatment_promotion_summary(
        shocked=shocked,
        identity_treatment_sensitivity=identity_treatment_sensitivity,
        shock_specs=dict(all_shock_specs),
        split_treatment_architecture_summary=split_treatment_architecture_summary,
        strict_missing_channel_summary=strict_missing_channel_summary,
    )
    core_treatment_promotion_summary_path = root / "output" / "models" / "core_treatment_promotion_summary.json"
    write_json_payload(
        core_treatment_promotion_summary_path,
        core_treatment_promotion_summary,
    )
    strict_redesign_summary = build_strict_redesign_summary(
        strict_identifiable_followup_summary=strict_identifiable_followup_summary,
        strict_missing_channel_summary=strict_missing_channel_summary,
        split_treatment_architecture_summary=split_treatment_architecture_summary,
        core_treatment_promotion_summary=core_treatment_promotion_summary,
    )
    strict_redesign_summary_path = root / "output" / "models" / "strict_redesign_summary.json"
    write_json_payload(
        strict_redesign_summary_path,
        strict_redesign_summary,
    )
    strict_loan_core_redesign_summary = build_strict_loan_core_redesign_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_redesign_summary=strict_redesign_summary,
    )
    strict_loan_core_redesign_summary_path = root / "output" / "models" / "strict_loan_core_redesign_summary.json"
    write_json_payload(
        strict_loan_core_redesign_summary_path,
        strict_loan_core_redesign_summary,
    )
    strict_di_bucket_role_summary = build_strict_di_bucket_role_summary(
        strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
        strict_identifiable_followup_summary=strict_identifiable_followup_summary,
    )
    strict_di_bucket_role_summary_path = root / "output" / "models" / "strict_di_bucket_role_summary.json"
    write_json_payload(
        strict_di_bucket_role_summary_path,
        strict_di_bucket_role_summary,
    )
    strict_di_bucket_bridge_summary = build_strict_di_bucket_bridge_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_di_bucket_role_summary=strict_di_bucket_role_summary,
    )
    strict_di_bucket_bridge_summary_path = root / "output" / "models" / "strict_di_bucket_bridge_summary.json"
    write_json_payload(
        strict_di_bucket_bridge_summary_path,
        strict_di_bucket_bridge_summary,
    )
    strict_private_borrower_bridge_summary = build_strict_private_borrower_bridge_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_di_bucket_bridge_summary=strict_di_bucket_bridge_summary,
    )
    strict_private_borrower_bridge_summary_path = root / "output" / "models" / "strict_private_borrower_bridge_summary.json"
    write_json_payload(
        strict_private_borrower_bridge_summary_path,
        strict_private_borrower_bridge_summary,
    )
    strict_nonfinancial_corporate_bridge_summary = build_strict_nonfinancial_corporate_bridge_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_private_borrower_bridge_summary=strict_private_borrower_bridge_summary,
    )
    strict_nonfinancial_corporate_bridge_summary_path = root / "output" / "models" / "strict_nonfinancial_corporate_bridge_summary.json"
    write_json_payload(
        strict_nonfinancial_corporate_bridge_summary_path,
        strict_nonfinancial_corporate_bridge_summary,
    )
    strict_private_offset_residual_summary = build_strict_private_offset_residual_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_nonfinancial_corporate_bridge_summary=strict_nonfinancial_corporate_bridge_summary,
    )
    strict_private_offset_residual_summary_path = root / "output" / "models" / "strict_private_offset_residual_summary.json"
    write_json_payload(
        strict_private_offset_residual_summary_path,
        strict_private_offset_residual_summary,
    )
    strict_corporate_bridge_secondary_comparison_summary = build_strict_corporate_bridge_secondary_comparison_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_private_offset_residual_summary=strict_private_offset_residual_summary,
    )
    strict_corporate_bridge_secondary_comparison_summary_path = (
        root / "output" / "models" / "strict_corporate_bridge_secondary_comparison_summary.json"
    )
    write_json_payload(
        strict_corporate_bridge_secondary_comparison_summary_path,
        strict_corporate_bridge_secondary_comparison_summary,
    )
    toc_row_incidence_audit_summary = build_toc_row_incidence_audit_summary(
        treasury_operating_cash_audit_summary=treasury_operating_cash_audit_summary,
        rest_of_world_treasury_audit_summary=rest_of_world_treasury_audit_summary,
        toc_row_path_split_summary=toc_row_path_split_summary,
        split_treatment_architecture_summary=split_treatment_architecture_summary,
    )
    toc_row_incidence_audit_summary_path = root / "output" / "models" / "toc_row_incidence_audit_summary.json"
    write_json_payload(
        toc_row_incidence_audit_summary_path,
        toc_row_incidence_audit_summary,
    )
    toc_row_liability_incidence_raw_summary = build_toc_row_liability_incidence_raw_summary(
        shocked=shocked,
        baseline_lp_spec=baseline_lp_spec,
    )
    toc_row_liability_incidence_raw_summary_path = (
        root / "output" / "models" / "toc_row_liability_incidence_raw_summary.json"
    )
    write_json_payload(
        toc_row_liability_incidence_raw_summary_path,
        toc_row_liability_incidence_raw_summary,
    )
    preliminary_strict_component_framework_summary = build_strict_component_framework_summary(
        big_picture_synthesis_summary=big_picture_synthesis_summary,
        split_treatment_architecture_summary=split_treatment_architecture_summary,
        core_treatment_promotion_summary=core_treatment_promotion_summary,
        strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
        strict_corporate_bridge_secondary_comparison_summary=strict_corporate_bridge_secondary_comparison_summary,
        toc_row_incidence_audit_summary=toc_row_incidence_audit_summary,
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
    )
    toc_validated_share_candidate_summary = build_toc_validated_share_candidate_summary(
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
        strict_component_framework_summary=preliminary_strict_component_framework_summary,
    )
    toc_validated_share_candidate_summary_path = (
        root / "output" / "models" / "toc_validated_share_candidate_summary.json"
    )
    write_json_payload(
        toc_validated_share_candidate_summary_path,
        toc_validated_share_candidate_summary,
    )
    strict_component_framework_summary = build_strict_component_framework_summary(
        big_picture_synthesis_summary=big_picture_synthesis_summary,
        split_treatment_architecture_summary=split_treatment_architecture_summary,
        core_treatment_promotion_summary=core_treatment_promotion_summary,
        strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
        strict_corporate_bridge_secondary_comparison_summary=strict_corporate_bridge_secondary_comparison_summary,
        toc_row_incidence_audit_summary=toc_row_incidence_audit_summary,
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
        toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
    )
    preliminary_strict_release_framing_summary = build_strict_release_framing_summary(
        strict_component_framework_summary=strict_component_framework_summary,
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
        toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
    )
    strict_direct_core_component_summary = build_strict_direct_core_component_summary(
        shocked=shocked,
        baseline_lp_spec=dict(baseline_lp_spec),
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_release_framing_summary=preliminary_strict_release_framing_summary,
    )
    strict_direct_core_component_summary_path = root / "output" / "models" / "strict_direct_core_component_summary.json"
    write_json_payload(
        strict_direct_core_component_summary_path,
        strict_direct_core_component_summary,
    )
    strict_direct_core_horizon_stability_summary = build_strict_direct_core_horizon_stability_summary(
        strict_direct_core_component_summary=strict_direct_core_component_summary,
    )
    strict_direct_core_horizon_stability_summary_path = (
        root / "output" / "models" / "strict_direct_core_horizon_stability_summary.json"
    )
    write_json_payload(
        strict_direct_core_horizon_stability_summary_path,
        strict_direct_core_horizon_stability_summary,
    )
    strict_component_framework_summary = build_strict_component_framework_summary(
        big_picture_synthesis_summary=big_picture_synthesis_summary,
        split_treatment_architecture_summary=split_treatment_architecture_summary,
        core_treatment_promotion_summary=core_treatment_promotion_summary,
        strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
        strict_corporate_bridge_secondary_comparison_summary=strict_corporate_bridge_secondary_comparison_summary,
        toc_row_incidence_audit_summary=toc_row_incidence_audit_summary,
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
        toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
        strict_direct_core_horizon_stability_summary=strict_direct_core_horizon_stability_summary,
    )
    strict_component_framework_summary_path = root / "output" / "models" / "strict_component_framework_summary.json"
    write_json_payload(
        strict_component_framework_summary_path,
        strict_component_framework_summary,
    )
    strict_release_framing_summary = build_strict_release_framing_summary(
        strict_component_framework_summary=strict_component_framework_summary,
        toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
        toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
    )
    strict_release_framing_summary_path = root / "output" / "models" / "strict_release_framing_summary.json"
    write_json_payload(
        strict_release_framing_summary_path,
        strict_release_framing_summary,
    )
    strict_additional_creator_candidate_summary = build_strict_additional_creator_candidate_summary(
        shocked=shocked,
        baseline_lp_spec=dict(baseline_lp_spec),
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_release_framing_summary=strict_release_framing_summary,
        strict_direct_core_horizon_stability_summary=strict_direct_core_horizon_stability_summary,
    )
    strict_additional_creator_candidate_summary_path = (
        root / "output" / "models" / "strict_additional_creator_candidate_summary.json"
    )
    write_json_payload(
        strict_additional_creator_candidate_summary_path,
        strict_additional_creator_candidate_summary,
    )
    strict_di_loans_nec_measurement_audit_summary = build_strict_di_loans_nec_measurement_audit_summary(
        shocked=shocked,
        baseline_lp_spec=dict(baseline_lp_spec),
        baseline_shock_spec=dict(baseline_shock_spec),
        core_shock_spec=dict(all_shock_specs["unexpected_tdc_core_deposit_proximate_bank_only"]),
        strict_release_framing_summary=strict_release_framing_summary,
        strict_di_bucket_bridge_summary=strict_di_bucket_bridge_summary,
    )
    strict_di_loans_nec_measurement_audit_summary_path = (
        root / "output" / "models" / "strict_di_loans_nec_measurement_audit_summary.json"
    )
    write_json_payload(
        strict_di_loans_nec_measurement_audit_summary_path,
        strict_di_loans_nec_measurement_audit_summary,
    )
    strict_results_closeout_summary = build_strict_results_closeout_summary(
        strict_release_framing_summary=strict_release_framing_summary,
        strict_component_framework_summary=strict_component_framework_summary,
        strict_di_loans_nec_measurement_audit_summary=strict_di_loans_nec_measurement_audit_summary,
        strict_additional_creator_candidate_summary=strict_additional_creator_candidate_summary,
    )
    strict_results_closeout_summary_path = root / "output" / "models" / "strict_results_closeout_summary.json"
    write_json_payload(
        strict_results_closeout_summary_path,
        strict_results_closeout_summary,
    )
    tdcest_ladder_integration_summary = build_tdcest_ladder_integration_summary(shocked)
    tdcest_ladder_integration_summary_path = (
        root / "output" / "models" / "tdcest_ladder_integration_summary.json"
    )
    write_json_payload(
        tdcest_ladder_integration_summary_path,
        tdcest_ladder_integration_summary,
    )
    tdcest_broad_object_comparison_summary = build_tdcest_broad_object_comparison_summary(
        shocked,
        tdcest_ladder_integration_summary=tdcest_ladder_integration_summary,
    )
    tdcest_broad_object_comparison_summary_path = (
        root / "output" / "models" / "tdcest_broad_object_comparison_summary.json"
    )
    write_json_payload(
        tdcest_broad_object_comparison_summary_path,
        tdcest_broad_object_comparison_summary,
    )
    tdcest_broad_treatment_sensitivity_summary = build_tdcest_broad_treatment_sensitivity_summary(
        lp_outputs["tdc_sensitivity_ladder"],
        shocked=shocked,
    )
    tdcest_broad_treatment_sensitivity_summary_path = (
        root / "output" / "models" / "tdcest_broad_treatment_sensitivity_summary.json"
    )
    write_json_payload(
        tdcest_broad_treatment_sensitivity_summary_path,
        tdcest_broad_treatment_sensitivity_summary,
    )
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
            scope_alignment_summary=scope_alignment_summary,
            strict_identifiable_followup_summary=strict_identifiable_followup_summary,
            broad_scope_system_summary=broad_scope_system_summary,
            tdc_treatment_audit_summary=tdc_treatment_audit_summary,
            treasury_operating_cash_audit_summary=treasury_operating_cash_audit_summary,
            rest_of_world_treasury_audit_summary=rest_of_world_treasury_audit_summary,
            toc_row_path_split_summary=toc_row_path_split_summary,
            toc_row_excluded_interpretation_summary=toc_row_excluded_interpretation_summary,
            strict_missing_channel_summary=strict_missing_channel_summary,
            strict_sign_mismatch_audit_summary=strict_sign_mismatch_audit_summary,
            strict_shock_composition_summary=strict_shock_composition_summary,
            strict_top_gap_quarter_audit_summary=strict_top_gap_quarter_audit_summary,
            strict_top_gap_quarter_direction_summary=strict_top_gap_quarter_direction_summary,
            strict_top_gap_inversion_summary=strict_top_gap_inversion_summary,
            strict_top_gap_anomaly_summary=strict_top_gap_anomaly_summary,
            strict_top_gap_anomaly_component_split_summary=strict_top_gap_anomaly_component_split_summary,
            strict_top_gap_anomaly_di_loans_split_summary=strict_top_gap_anomaly_di_loans_split_summary,
            strict_top_gap_anomaly_backdrop_summary=strict_top_gap_anomaly_backdrop_summary,
            big_picture_synthesis_summary=big_picture_synthesis_summary,
            treatment_object_comparison_summary=treatment_object_comparison_summary,
            split_treatment_architecture_summary=split_treatment_architecture_summary,
            core_treatment_promotion_summary=core_treatment_promotion_summary,
            strict_redesign_summary=strict_redesign_summary,
            strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
            strict_di_bucket_role_summary=strict_di_bucket_role_summary,
            strict_di_bucket_bridge_summary=strict_di_bucket_bridge_summary,
            strict_private_borrower_bridge_summary=strict_private_borrower_bridge_summary,
            strict_nonfinancial_corporate_bridge_summary=strict_nonfinancial_corporate_bridge_summary,
            strict_private_offset_residual_summary=strict_private_offset_residual_summary,
            strict_corporate_bridge_secondary_comparison_summary=strict_corporate_bridge_secondary_comparison_summary,
            strict_component_framework_summary=strict_component_framework_summary,
            toc_row_incidence_audit_summary=toc_row_incidence_audit_summary,
            toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
            toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
            strict_release_framing_summary=strict_release_framing_summary,
            strict_direct_core_component_summary=strict_direct_core_component_summary,
            strict_direct_core_horizon_stability_summary=strict_direct_core_horizon_stability_summary,
            strict_additional_creator_candidate_summary=strict_additional_creator_candidate_summary,
            strict_di_loans_nec_measurement_audit_summary=strict_di_loans_nec_measurement_audit_summary,
            strict_results_closeout_summary=strict_results_closeout_summary,
            tdcest_ladder_integration_summary=tdcest_ladder_integration_summary,
            tdcest_broad_object_comparison_summary=tdcest_broad_object_comparison_summary,
            tdcest_broad_treatment_sensitivity_summary=tdcest_broad_treatment_sensitivity_summary,
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
    if _should_refuse_public_mirror(root=root, provenance_validation_payload=provenance_validation_payload):
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
            scope_alignment_summary=scope_alignment_summary,
            strict_identifiable_followup_summary=strict_identifiable_followup_summary,
            tdc_treatment_audit_summary=tdc_treatment_audit_summary,
            treasury_operating_cash_audit_summary=treasury_operating_cash_audit_summary,
            rest_of_world_treasury_audit_summary=rest_of_world_treasury_audit_summary,
            toc_row_path_split_summary=toc_row_path_split_summary,
            toc_row_excluded_interpretation_summary=toc_row_excluded_interpretation_summary,
            strict_missing_channel_summary=strict_missing_channel_summary,
            strict_sign_mismatch_audit_summary=strict_sign_mismatch_audit_summary,
            strict_shock_composition_summary=strict_shock_composition_summary,
            strict_top_gap_quarter_audit_summary=strict_top_gap_quarter_audit_summary,
            strict_top_gap_quarter_direction_summary=strict_top_gap_quarter_direction_summary,
            strict_top_gap_inversion_summary=strict_top_gap_inversion_summary,
            strict_top_gap_anomaly_summary=strict_top_gap_anomaly_summary,
            strict_top_gap_anomaly_component_split_summary=strict_top_gap_anomaly_component_split_summary,
            strict_top_gap_anomaly_di_loans_split_summary=strict_top_gap_anomaly_di_loans_split_summary,
            strict_top_gap_anomaly_backdrop_summary=strict_top_gap_anomaly_backdrop_summary,
            big_picture_synthesis_summary=big_picture_synthesis_summary,
            treatment_object_comparison_summary=treatment_object_comparison_summary,
            split_treatment_architecture_summary=split_treatment_architecture_summary,
            core_treatment_promotion_summary=core_treatment_promotion_summary,
            strict_redesign_summary=strict_redesign_summary,
            strict_loan_core_redesign_summary=strict_loan_core_redesign_summary,
            strict_di_bucket_role_summary=strict_di_bucket_role_summary,
            strict_di_bucket_bridge_summary=strict_di_bucket_bridge_summary,
            strict_private_borrower_bridge_summary=strict_private_borrower_bridge_summary,
            strict_nonfinancial_corporate_bridge_summary=strict_nonfinancial_corporate_bridge_summary,
            strict_private_offset_residual_summary=strict_private_offset_residual_summary,
            strict_corporate_bridge_secondary_comparison_summary=strict_corporate_bridge_secondary_comparison_summary,
            strict_component_framework_summary=strict_component_framework_summary,
            toc_row_incidence_audit_summary=toc_row_incidence_audit_summary,
            toc_row_liability_incidence_raw_summary=toc_row_liability_incidence_raw_summary,
            toc_validated_share_candidate_summary=toc_validated_share_candidate_summary,
            strict_release_framing_summary=strict_release_framing_summary,
            strict_direct_core_component_summary=strict_direct_core_component_summary,
            strict_direct_core_horizon_stability_summary=strict_direct_core_horizon_stability_summary,
            strict_additional_creator_candidate_summary=strict_additional_creator_candidate_summary,
            strict_di_loans_nec_measurement_audit_summary=strict_di_loans_nec_measurement_audit_summary,
            strict_results_closeout_summary=strict_results_closeout_summary,
            broad_scope_system_summary=broad_scope_system_summary,
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
            accounting_identity_lp_irf_path,
            strict_identifiable_lp_irf_path,
            strict_funding_offset_alignment_path,
            strict_identifiable_alignment_path,
            strict_identifiable_summary_path,
            strict_identifiable_followup_summary_path,
            strict_di_bucket_role_summary_path,
            strict_di_bucket_bridge_summary_path,
            strict_private_borrower_bridge_summary_path,
            strict_nonfinancial_corporate_bridge_summary_path,
            strict_private_offset_residual_summary_path,
            strict_corporate_bridge_secondary_comparison_summary_path,
            strict_component_framework_summary_path,
            strict_direct_core_component_summary_path,
            strict_direct_core_horizon_stability_summary_path,
            strict_additional_creator_candidate_summary_path,
            strict_di_loans_nec_measurement_audit_summary_path,
            strict_results_closeout_summary_path,
            strict_release_framing_summary_path,
            tdcest_ladder_integration_summary_path,
            tdcest_broad_object_comparison_summary_path,
            tdcest_broad_treatment_sensitivity_summary_path,
            toc_row_incidence_audit_summary_path,
            toc_row_liability_incidence_raw_summary_path,
            toc_validated_share_candidate_summary_path,
            broad_scope_system_summary_path,
            scope_alignment_summary_path,
            tdc_treatment_audit_summary_path,
            treasury_operating_cash_audit_summary_path,
            treasury_cash_regime_audit_summary_path,
            historical_cash_term_reestimation_summary_path,
            rest_of_world_treasury_audit_summary_path,
            toc_row_bundle_audit_summary_path,
            toc_row_path_split_summary_path,
            toc_row_excluded_interpretation_summary_path,
            strict_missing_channel_summary_path,
            strict_sign_mismatch_audit_summary_path,
            strict_shock_composition_summary_path,
            strict_top_gap_quarter_audit_summary_path,
            strict_top_gap_quarter_direction_summary_path,
            strict_top_gap_inversion_summary_path,
            strict_top_gap_anomaly_summary_path,
            strict_top_gap_anomaly_component_split_summary_path,
            strict_top_gap_anomaly_di_loans_split_summary_path,
            strict_top_gap_anomaly_backdrop_summary_path,
            big_picture_synthesis_summary_path,
            treatment_object_comparison_summary_path,
            split_treatment_architecture_summary_path,
            core_treatment_promotion_summary_path,
            strict_redesign_summary_path,
            strict_loan_core_redesign_summary_path,
            accounting_identity_alignment_path,
            accounting_identity_summary_path,
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
        "lp_irf_accounting_identity_path": str(accounting_identity_lp_irf_path),
        "lp_irf_strict_identifiable_path": str(strict_identifiable_lp_irf_path),
        "accounting_identity_alignment_path": str(accounting_identity_alignment_path),
        "accounting_identity_summary_path": str(accounting_identity_summary_path),
        "strict_funding_offset_alignment_path": str(strict_funding_offset_alignment_path),
        "strict_identifiable_alignment_path": str(strict_identifiable_alignment_path),
        "strict_identifiable_summary_path": str(strict_identifiable_summary_path),
        "strict_identifiable_followup_summary_path": str(strict_identifiable_followup_summary_path),
        "broad_scope_system_summary_path": str(broad_scope_system_summary_path),
        "scope_alignment_summary_path": str(scope_alignment_summary_path),
        "tdc_treatment_audit_summary_path": str(tdc_treatment_audit_summary_path),
        "treasury_operating_cash_audit_summary_path": str(treasury_operating_cash_audit_summary_path),
        "treasury_cash_regime_audit_summary_path": str(treasury_cash_regime_audit_summary_path),
        "historical_cash_term_reestimation_summary_path": str(historical_cash_term_reestimation_summary_path),
        "rest_of_world_treasury_audit_summary_path": str(rest_of_world_treasury_audit_summary_path),
        "toc_row_bundle_audit_summary_path": str(toc_row_bundle_audit_summary_path),
        "toc_row_path_split_summary_path": str(toc_row_path_split_summary_path),
        "toc_row_incidence_audit_summary_path": str(toc_row_incidence_audit_summary_path),
        "toc_row_liability_incidence_raw_summary_path": str(toc_row_liability_incidence_raw_summary_path),
        "toc_validated_share_candidate_summary_path": str(toc_validated_share_candidate_summary_path),
        "strict_release_framing_summary_path": str(strict_release_framing_summary_path),
        "strict_direct_core_component_summary_path": str(strict_direct_core_component_summary_path),
        "strict_direct_core_horizon_stability_summary_path": str(strict_direct_core_horizon_stability_summary_path),
        "strict_additional_creator_candidate_summary_path": str(strict_additional_creator_candidate_summary_path),
        "strict_di_loans_nec_measurement_audit_summary_path": str(strict_di_loans_nec_measurement_audit_summary_path),
        "strict_results_closeout_summary_path": str(strict_results_closeout_summary_path),
        "tdcest_broad_object_comparison_summary_path": str(tdcest_broad_object_comparison_summary_path),
        "tdcest_broad_treatment_sensitivity_summary_path": str(tdcest_broad_treatment_sensitivity_summary_path),
        "toc_row_excluded_interpretation_summary_path": str(toc_row_excluded_interpretation_summary_path),
        "strict_missing_channel_summary_path": str(strict_missing_channel_summary_path),
        "strict_sign_mismatch_audit_summary_path": str(strict_sign_mismatch_audit_summary_path),
        "strict_shock_composition_summary_path": str(strict_shock_composition_summary_path),
        "strict_top_gap_quarter_audit_summary_path": str(strict_top_gap_quarter_audit_summary_path),
        "strict_top_gap_quarter_direction_summary_path": str(strict_top_gap_quarter_direction_summary_path),
        "strict_top_gap_inversion_summary_path": str(strict_top_gap_inversion_summary_path),
        "strict_top_gap_anomaly_summary_path": str(strict_top_gap_anomaly_summary_path),
        "strict_top_gap_anomaly_component_split_summary_path": str(strict_top_gap_anomaly_component_split_summary_path),
        "strict_top_gap_anomaly_di_loans_split_summary_path": str(strict_top_gap_anomaly_di_loans_split_summary_path),
        "strict_top_gap_anomaly_backdrop_summary_path": str(strict_top_gap_anomaly_backdrop_summary_path),
        "big_picture_synthesis_summary_path": str(big_picture_synthesis_summary_path),
        "treatment_object_comparison_summary_path": str(treatment_object_comparison_summary_path),
        "split_treatment_architecture_summary_path": str(split_treatment_architecture_summary_path),
        "core_treatment_promotion_summary_path": str(core_treatment_promotion_summary_path),
        "strict_redesign_summary_path": str(strict_redesign_summary_path),
        "strict_loan_core_redesign_summary_path": str(strict_loan_core_redesign_summary_path),
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
