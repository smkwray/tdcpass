from __future__ import annotations

import json
from pathlib import Path

from tdcpass.core.yaml_utils import load_yaml


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_output_contract_has_required_artifacts() -> None:
    payload = load_yaml(repo_root() / "config" / "output_contract.yml")
    artifacts = payload.get("artifacts", [])
    paths = {row["path"] for row in artifacts}
    expected = {
        "data/derived/quarterly_panel.csv",
        "data/derived/call_report_deposit_components.csv",
        "output/accounting/accounting_summary.csv",
        "output/accounting/quarters_tdc_exceeds_total.csv",
        "output/shocks/unexpected_tdc.csv",
        "output/models/lp_irf.csv",
        "output/models/lp_irf_identity_baseline.csv",
        "output/models/lp_irf_accounting_identity.csv",
        "output/models/lp_irf_strict_identifiable.csv",
        "output/models/accounting_identity_alignment.csv",
        "output/models/accounting_identity_summary.json",
        "output/models/strict_funding_offset_alignment.csv",
        "output/models/strict_identifiable_alignment.csv",
        "output/models/strict_identifiable_summary.json",
        "output/models/strict_identifiable_followup_summary.json",
        "output/models/strict_missing_channel_summary.json",
        "output/models/scope_alignment_summary.json",
        "output/models/broad_scope_system_summary.json",
        "output/models/treasury_operating_cash_audit_summary.json",
        "output/models/treasury_cash_regime_audit_summary.json",
        "output/models/historical_cash_term_reestimation_summary.json",
        "output/models/rest_of_world_treasury_audit_summary.json",
        "output/models/toc_row_bundle_audit_summary.json",
        "output/models/toc_row_path_split_summary.json",
        "output/models/toc_row_excluded_interpretation_summary.json",
        "output/models/strict_missing_channel_summary.json",
        "output/models/strict_sign_mismatch_audit_summary.json",
        "output/models/strict_shock_composition_summary.json",
        "output/models/strict_top_gap_quarter_audit_summary.json",
        "output/models/strict_top_gap_quarter_direction_summary.json",
        "output/models/strict_top_gap_inversion_summary.json",
        "output/models/strict_top_gap_anomaly_summary.json",
        "output/models/big_picture_synthesis_summary.json",
        "output/models/treatment_object_comparison_summary.json",
        "output/models/split_treatment_architecture_summary.json",
        "output/models/core_treatment_promotion_summary.json",
        "output/models/strict_redesign_summary.json",
        "output/models/tdc_treatment_audit_summary.json",
        "output/models/identity_measurement_ladder.csv",
        "output/models/identity_treatment_sensitivity.csv",
        "output/models/identity_control_sensitivity.csv",
        "output/models/identity_sample_sensitivity.csv",
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
        "output/models/call_report_deposit_components_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/headline_treatment_fingerprint.json",
        "output/models/provenance_validation_summary.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/strict_di_bucket_bridge_summary.json",
        "output/models/strict_private_borrower_bridge_summary.json",
        "output/models/strict_nonfinancial_corporate_bridge_summary.json",
        "output/models/strict_private_offset_residual_summary.json",
        "output/models/strict_corporate_bridge_secondary_comparison_summary.json",
        "output/models/strict_component_framework_summary.json",
        "output/models/strict_direct_core_component_summary.json",
        "output/models/strict_direct_core_horizon_stability_summary.json",
        "output/models/strict_additional_creator_candidate_summary.json",
        "output/models/strict_di_loans_nec_measurement_audit_summary.json",
        "output/models/strict_results_closeout_summary.json",
        "output/models/tdcest_ladder_integration_summary.json",
        "output/models/tdcest_broad_object_comparison_summary.json",
        "output/models/tdcest_broad_treatment_sensitivity_summary.json",
        "output/models/strict_release_framing_summary.json",
        "output/models/toc_row_incidence_audit_summary.json",
        "output/models/toc_row_liability_incidence_raw_summary.json",
        "output/models/deposit_component_scorecard.json",
        "output/models/deposit_type_side_read.csv",
        "output/models/counterpart_channel_scorecard.json",
        "output/models/sample_construction_summary.json",
        "output/manifests/raw_downloads.json",
        "output/manifests/reused_artifacts.json",
        "output/manifests/pipeline_run.json",
        "site/data/overview.json",
        "site/data/accounting_summary.csv",
        "site/data/quarters_tdc_exceeds_total.csv",
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
            "site/data/scope_alignment_summary.json",
            "site/data/broad_scope_system_summary.json",
            "site/data/treasury_operating_cash_audit_summary.json",
            "site/data/treasury_cash_regime_audit_summary.json",
            "site/data/historical_cash_term_reestimation_summary.json",
            "site/data/rest_of_world_treasury_audit_summary.json",
            "site/data/toc_row_bundle_audit_summary.json",
            "site/data/toc_row_path_split_summary.json",
            "site/data/toc_row_excluded_interpretation_summary.json",
            "site/data/strict_missing_channel_summary.json",
            "site/data/strict_sign_mismatch_audit_summary.json",
            "site/data/strict_shock_composition_summary.json",
            "site/data/strict_top_gap_quarter_audit_summary.json",
            "site/data/strict_top_gap_quarter_direction_summary.json",
            "site/data/strict_top_gap_inversion_summary.json",
            "site/data/strict_top_gap_anomaly_summary.json",
            "site/data/big_picture_synthesis_summary.json",
            "site/data/treatment_object_comparison_summary.json",
            "site/data/split_treatment_architecture_summary.json",
            "site/data/core_treatment_promotion_summary.json",
            "site/data/tdc_treatment_audit_summary.json",
            "site/data/identity_measurement_ladder.csv",
            "site/data/lp_irf_regimes.csv",
            "site/data/regime_diagnostics_summary.json",
            "site/data/tdc_sensitivity_ladder.csv",
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
            "site/data/direct_identification_summary.json",
            "site/data/result_readiness_summary.json",
        "site/data/pass_through_summary.json",
        "site/data/strict_di_bucket_bridge_summary.json",
        "site/data/strict_private_borrower_bridge_summary.json",
        "site/data/strict_nonfinancial_corporate_bridge_summary.json",
        "site/data/strict_private_offset_residual_summary.json",
        "site/data/strict_corporate_bridge_secondary_comparison_summary.json",
        "site/data/strict_component_framework_summary.json",
        "site/data/strict_direct_core_component_summary.json",
        "site/data/strict_direct_core_horizon_stability_summary.json",
        "site/data/strict_additional_creator_candidate_summary.json",
        "site/data/strict_di_loans_nec_measurement_audit_summary.json",
        "site/data/strict_results_closeout_summary.json",
        "site/data/tdcest_ladder_integration_summary.json",
        "site/data/tdcest_broad_object_comparison_summary.json",
        "site/data/tdcest_broad_treatment_sensitivity_summary.json",
        "site/data/strict_release_framing_summary.json",
        "site/data/toc_row_incidence_audit_summary.json",
        "site/data/toc_row_liability_incidence_raw_summary.json",
        "site/data/deposit_type_side_read.csv",
            "site/data/counterpart_channel_scorecard.json",
            "site/data/sample_construction_summary.json",
        }
    assert expected.issubset(paths)


def test_contract_freezes_canonical_aliases_and_shock_column() -> None:
    payload = load_yaml(repo_root() / "config" / "output_contract.yml")
    assert payload["canonical_aliases"]["tdc_qoq"] == "tdc_bank_only_qoq"
    assert payload["canonical_aliases"]["total_deposits_qoq"] == "total_deposits_bank_qoq"
    assert payload["shock_column"] == "tdc_residual_z"
    panel_artifact = next(item for item in payload["artifacts"] if item["path"] == "data/derived/quarterly_panel.csv")
    assert "bank_credit_private_qoq" not in panel_artifact["headline_sample_columns"]
    assert "tdc_domestic_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_us_chartered_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_no_foreign_bank_sectors_qoq" in panel_artifact["required_columns"]
    assert "tdc_no_toc_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_no_toc_no_row_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_no_remit_bank_only_qoq" in panel_artifact["required_columns"]
    assert "tdc_credit_union_sensitive_qoq" in panel_artifact["required_columns"]
    assert "deposits_only_bank_qoq" in panel_artifact["required_columns"]
    assert "broad_bank_deposits_qoq" in panel_artifact["required_columns"]
    assert "checkable_deposits_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "checkable_deposits_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "time_savings_deposits_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "time_savings_deposits_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "accounting_deposit_substitution_qoq" in panel_artifact["required_columns"]
    assert "accounting_bank_balance_sheet_qoq" in panel_artifact["required_columns"]
    assert "accounting_public_liquidity_qoq" in panel_artifact["required_columns"]
    assert "accounting_external_flow_qoq" in panel_artifact["required_columns"]
    assert "accounting_identity_total_qoq" in panel_artifact["required_columns"]
    assert "accounting_identity_gap_qoq" in panel_artifact["required_columns"]
    assert "strict_loan_source_qoq" in panel_artifact["required_columns"]
    assert "strict_loan_mortgages_qoq" in panel_artifact["required_columns"]
    assert "strict_loan_consumer_credit_qoq" in panel_artifact["required_columns"]
    assert "strict_loan_di_loans_nec_qoq" in panel_artifact["required_columns"]
    assert "strict_di_loans_nec_households_nonprofits_qoq" in panel_artifact["required_columns"]
    assert "strict_di_loans_nec_nonfinancial_corporate_qoq" in panel_artifact["required_columns"]
    assert "strict_di_loans_nec_systemwide_liability_total_qoq" in panel_artifact["required_columns"]
    assert "strict_di_loans_nec_systemwide_borrower_total_qoq" in panel_artifact["required_columns"]
    assert "strict_di_loans_nec_systemwide_borrower_gap_qoq" in panel_artifact["required_columns"]
    assert "strict_loan_other_advances_qoq" in panel_artifact["required_columns"]
    assert "strict_non_treasury_agency_gse_qoq" in panel_artifact["required_columns"]
    assert "strict_non_treasury_municipal_qoq" in panel_artifact["required_columns"]
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in panel_artifact["required_columns"]
    assert "strict_non_treasury_securities_qoq" in panel_artifact["required_columns"]
    assert "strict_identifiable_total_qoq" in panel_artifact["required_columns"]
    assert "strict_identifiable_gap_qoq" in panel_artifact["required_columns"]
    assert "broad_strict_loan_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "broad_strict_loan_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "broad_strict_loan_source_qoq" in panel_artifact["required_columns"]
    assert "broad_strict_gap_qoq" in panel_artifact["required_columns"]
    assert "strict_funding_offset_total_qoq" in panel_artifact["required_columns"]
    assert "strict_identifiable_net_after_funding_qoq" in panel_artifact["required_columns"]
    assert "strict_gap_after_funding_qoq" in panel_artifact["required_columns"]
    assert "checkable_deposits_bank_qoq" in panel_artifact["required_columns"]
    assert "interbank_transactions_bank_qoq" in panel_artifact["required_columns"]
    assert "time_savings_deposits_bank_qoq" in panel_artifact["required_columns"]
    assert "checkable_federal_govt_bank_qoq" in panel_artifact["required_columns"]
    assert "checkable_state_local_bank_qoq" in panel_artifact["required_columns"]
    assert "checkable_rest_of_world_bank_qoq" in panel_artifact["required_columns"]
    assert "checkable_private_domestic_bank_qoq" in panel_artifact["required_columns"]
    assert "interbank_transactions_foreign_banks_liability_qoq" in panel_artifact["required_columns"]
    assert "interbank_transactions_foreign_banks_asset_qoq" in panel_artifact["required_columns"]
    assert "deposits_at_foreign_banks_asset_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_deposits_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_tdc_us_chartered_bank_only_qoq" in panel_artifact["required_columns"]
    assert "lag_tdc_no_foreign_bank_sectors_qoq" in panel_artifact["required_columns"]
    assert "lag_tdc_no_toc_bank_only_qoq" in panel_artifact["required_columns"]
    assert "lag_tdc_no_toc_no_row_bank_only_qoq" in panel_artifact["required_columns"]
    assert "lag_deposits_only_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_broad_bank_deposits_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_deposits_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_deposits_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "lag_interbank_transactions_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_time_savings_deposits_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_time_savings_deposits_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "lag_time_savings_deposits_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_federal_govt_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_state_local_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_rest_of_world_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_checkable_private_domestic_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_interbank_transactions_foreign_banks_liability_qoq" in panel_artifact["required_columns"]
    assert "lag_interbank_transactions_foreign_banks_asset_qoq" in panel_artifact["required_columns"]
    assert "lag_deposits_at_foreign_banks_asset_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_loan_source_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_loan_mortgages_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_loan_consumer_credit_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_loan_di_loans_nec_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_di_loans_nec_households_nonprofits_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_di_loans_nec_systemwide_liability_total_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_di_loans_nec_systemwide_borrower_total_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_di_loans_nec_systemwide_borrower_gap_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_loan_other_advances_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_non_treasury_agency_gse_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_non_treasury_municipal_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_non_treasury_corporate_foreign_bonds_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_non_treasury_securities_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_identifiable_total_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_identifiable_gap_qoq" in panel_artifact["required_columns"]
    assert "lag_broad_strict_loan_foreign_offices_qoq" in panel_artifact["required_columns"]
    assert "lag_broad_strict_loan_affiliated_areas_qoq" in panel_artifact["required_columns"]
    assert "lag_broad_strict_loan_source_qoq" in panel_artifact["required_columns"]
    assert "lag_broad_strict_gap_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_funding_offset_total_qoq" in panel_artifact["required_columns"]
    assert "lag_strict_identifiable_net_after_funding_qoq" in panel_artifact["required_columns"]
    assert "commercial_industrial_loans_qoq" in panel_artifact["required_columns"]
    assert "construction_land_development_loans_qoq" in panel_artifact["required_columns"]
    assert "cre_multifamily_loans_qoq" in panel_artifact["required_columns"]
    assert "cre_nonfarm_nonresidential_loans_qoq" in panel_artifact["required_columns"]
    assert "consumer_loans_qoq" in panel_artifact["required_columns"]
    assert "credit_card_revolving_loans_qoq" in panel_artifact["required_columns"]
    assert "auto_loans_qoq" in panel_artifact["required_columns"]
    assert "other_consumer_loans_qoq" in panel_artifact["required_columns"]
    assert "heloc_loans_qoq" in panel_artifact["required_columns"]
    assert "closed_end_residential_loans_qoq" in panel_artifact["required_columns"]
    assert "loans_to_commercial_banks_qoq" in panel_artifact["required_columns"]
    assert "loans_to_nondepository_financial_institutions_qoq" in panel_artifact["required_columns"]
    assert "loans_for_purchasing_or_carrying_securities_qoq" in panel_artifact["required_columns"]
    assert "treasury_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "agency_gse_backed_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "municipal_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "corporate_foreign_bonds_bank_qoq" in panel_artifact["required_columns"]
    assert "fedfunds_repo_liabilities_bank_qoq" in panel_artifact["required_columns"]
    assert "commercial_bank_borrowings_qoq" in panel_artifact["required_columns"]
    assert "fed_borrowings_depository_institutions_qoq" in panel_artifact["required_columns"]
    assert "debt_securities_bank_liability_qoq" in panel_artifact["required_columns"]
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in panel_artifact["required_columns"]
    assert "holding_company_parent_funding_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_commercial_industrial_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_construction_land_development_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_cre_multifamily_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_cre_nonfarm_nonresidential_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_consumer_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_credit_card_revolving_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_auto_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_other_consumer_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_heloc_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_closed_end_residential_loans_qoq" in panel_artifact["required_columns"]
    assert "lag_loans_to_commercial_banks_qoq" in panel_artifact["required_columns"]
    assert "lag_loans_to_nondepository_financial_institutions_qoq" in panel_artifact["required_columns"]
    assert "lag_loans_for_purchasing_or_carrying_securities_qoq" in panel_artifact["required_columns"]
    assert "lag_treasury_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_agency_gse_backed_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_municipal_securities_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_corporate_foreign_bonds_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_fedfunds_repo_liabilities_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_commercial_bank_borrowings_qoq" in panel_artifact["required_columns"]
    assert "lag_fed_borrowings_depository_institutions_qoq" in panel_artifact["required_columns"]
    assert "lag_debt_securities_bank_liability_qoq" in panel_artifact["required_columns"]
    assert "lag_fhlb_advances_sallie_mae_loans_bank_qoq" in panel_artifact["required_columns"]
    assert "lag_holding_company_parent_funding_bank_qoq" in panel_artifact["required_columns"]
    assert "on_rrp_reallocation_qoq" in panel_artifact["required_columns"]
    assert "household_treasury_securities_reallocation_qoq" in panel_artifact["required_columns"]
    assert "mmf_treasury_bills_reallocation_qoq" in panel_artifact["required_columns"]
    assert "currency_reallocation_qoq" in panel_artifact["required_columns"]
    assert "lag_on_rrp_reallocation_qoq" in panel_artifact["required_columns"]
    assert "lag_household_treasury_securities_reallocation_qoq" in panel_artifact["required_columns"]
    assert "lag_mmf_treasury_bills_reallocation_qoq" in panel_artifact["required_columns"]
    assert "lag_currency_reallocation_qoq" in panel_artifact["required_columns"]
    assert "other_component_no_foreign_bank_sectors_qoq" in panel_artifact["required_columns"]
    assert "other_component_no_toc_bank_only_qoq" in panel_artifact["required_columns"]
    assert "other_component_no_toc_no_row_bank_only_qoq" in panel_artifact["required_columns"]
    assert "broad_bank_other_component_qoq" in panel_artifact["required_columns"]

    audit_artifact = next(item for item in payload["artifacts"] if item["path"] == "output/models/tdc_treatment_audit_summary.json")
    assert "construction_alignment" in audit_artifact["required_keys"]
    site_audit_artifact = next(item for item in payload["artifacts"] if item["path"] == "site/data/tdc_treatment_audit_summary.json")
    assert "construction_alignment" in site_audit_artifact["required_keys"]
    toc_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/treasury_operating_cash_audit_summary.json"
    )
    assert "quarterly_alignment" in toc_audit_artifact["required_keys"]
    site_toc_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/treasury_operating_cash_audit_summary.json"
    )
    assert "quarterly_alignment" in site_toc_audit_artifact["required_keys"]
    row_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/rest_of_world_treasury_audit_summary.json"
    )
    assert "quarterly_alignment" in row_audit_artifact["required_keys"]
    site_row_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/rest_of_world_treasury_audit_summary.json"
    )
    assert "quarterly_alignment" in site_row_audit_artifact["required_keys"]
    toc_row_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/toc_row_bundle_audit_summary.json"
    )
    assert "quarterly_alignment" in toc_row_audit_artifact["required_keys"]
    site_toc_row_audit_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/toc_row_bundle_audit_summary.json"
    )
    assert "quarterly_alignment" in site_toc_row_audit_artifact["required_keys"]
    toc_row_path_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/toc_row_path_split_summary.json"
    )
    assert "quarterly_split" in toc_row_path_artifact["required_keys"]
    site_toc_row_path_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/toc_row_path_split_summary.json"
    )
    assert "quarterly_split" in site_toc_row_path_artifact["required_keys"]
    toc_row_excluded_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/toc_row_excluded_interpretation_summary.json"
    )
    assert "comparison_definition" in toc_row_excluded_artifact["required_keys"]
    site_toc_row_excluded_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/toc_row_excluded_interpretation_summary.json"
    )
    assert "comparison_definition" in site_toc_row_excluded_artifact["required_keys"]
    strict_missing_channel_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_missing_channel_summary.json"
    )
    assert "comparison_definition" in strict_missing_channel_artifact["required_keys"]
    strict_sign_mismatch_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_sign_mismatch_audit_summary.json"
    )
    assert "shock_alignment" in strict_sign_mismatch_artifact["required_keys"]
    assert "quarter_concentration" in strict_sign_mismatch_artifact["required_keys"]
    assert "gap_driver_alignment" in strict_sign_mismatch_artifact["required_keys"]
    strict_shock_composition_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_shock_composition_summary.json"
    )
    assert "trim_diagnostics" in strict_shock_composition_artifact["required_keys"]
    strict_top_gap_quarter_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_top_gap_quarter_audit_summary.json"
    )
    assert "dominant_leg_summary" in strict_top_gap_quarter_artifact["required_keys"]
    assert "contribution_pattern_summary" in strict_top_gap_quarter_artifact["required_keys"]
    strict_top_gap_quarter_direction_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_top_gap_quarter_direction_summary.json"
    )
    assert "gap_bundle_alignment_summary" in strict_top_gap_quarter_direction_artifact["required_keys"]
    assert "directional_driver_summary" in strict_top_gap_quarter_direction_artifact["required_keys"]
    strict_top_gap_inversion_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_top_gap_inversion_summary.json"
    )
    assert "directional_driver_context_summary" in strict_top_gap_inversion_artifact["required_keys"]
    assert "residual_strict_pattern_summary" in strict_top_gap_inversion_artifact["required_keys"]
    strict_top_gap_anomaly_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/strict_top_gap_anomaly_summary.json"
    )
    assert "weighted_peer_means" in strict_top_gap_anomaly_artifact["required_keys"]
    assert "anomaly_vs_peer_deltas" in strict_top_gap_anomaly_artifact["required_keys"]
    assert "ranked_anomaly_component_deltas" in strict_top_gap_anomaly_artifact["required_keys"]
    strict_top_gap_anomaly_component_split_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "output/models/strict_top_gap_anomaly_component_split_summary.json"
    )
    assert "loan_subcomponent_deltas" in strict_top_gap_anomaly_component_split_artifact["required_keys"]
    assert "liquidity_external_deltas" in strict_top_gap_anomaly_component_split_artifact["required_keys"]
    strict_top_gap_anomaly_di_loans_split_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "output/models/strict_top_gap_anomaly_di_loans_split_summary.json"
    )
    assert "di_loans_nec_component_deltas" in strict_top_gap_anomaly_di_loans_split_artifact["required_keys"]
    assert "dominant_borrower_component" in strict_top_gap_anomaly_di_loans_split_artifact["required_keys"]
    strict_top_gap_anomaly_backdrop_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "output/models/strict_top_gap_anomaly_backdrop_summary.json"
    )
    assert "corporate_credit_row" in strict_top_gap_anomaly_backdrop_artifact["required_keys"]
    assert "reserves_row" in strict_top_gap_anomaly_backdrop_artifact["required_keys"]
    big_picture_synthesis_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/big_picture_synthesis_summary.json"
    )
    assert "classification" in big_picture_synthesis_artifact["required_keys"]
    assert "h0_snapshot" in big_picture_synthesis_artifact["required_keys"]
    treatment_object_comparison_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/treatment_object_comparison_summary.json"
    )
    assert "candidate_objects" in treatment_object_comparison_artifact["required_keys"]
    assert "recommendation" in treatment_object_comparison_artifact["required_keys"]
    split_treatment_architecture_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "output/models/split_treatment_architecture_summary.json"
    )
    assert "series_definitions" in split_treatment_architecture_artifact["required_keys"]
    assert "architecture_recommendation" in split_treatment_architecture_artifact["required_keys"]
    site_strict_missing_channel_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_missing_channel_summary.json"
    )
    assert "comparison_definition" in site_strict_missing_channel_artifact["required_keys"]
    site_strict_sign_mismatch_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_sign_mismatch_audit_summary.json"
    )
    assert "shock_alignment" in site_strict_sign_mismatch_artifact["required_keys"]
    assert "quarter_concentration" in site_strict_sign_mismatch_artifact["required_keys"]
    assert "gap_driver_alignment" in site_strict_sign_mismatch_artifact["required_keys"]
    site_strict_shock_composition_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_shock_composition_summary.json"
    )
    assert "trim_diagnostics" in site_strict_shock_composition_artifact["required_keys"]
    site_strict_top_gap_quarter_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_top_gap_quarter_audit_summary.json"
    )
    assert "dominant_leg_summary" in site_strict_top_gap_quarter_artifact["required_keys"]
    assert "contribution_pattern_summary" in site_strict_top_gap_quarter_artifact["required_keys"]
    site_strict_top_gap_quarter_direction_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_top_gap_quarter_direction_summary.json"
    )
    assert "gap_bundle_alignment_summary" in site_strict_top_gap_quarter_direction_artifact["required_keys"]
    assert "directional_driver_summary" in site_strict_top_gap_quarter_direction_artifact["required_keys"]
    site_strict_top_gap_inversion_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_top_gap_inversion_summary.json"
    )
    assert "directional_driver_context_summary" in site_strict_top_gap_inversion_artifact["required_keys"]
    assert "residual_strict_pattern_summary" in site_strict_top_gap_inversion_artifact["required_keys"]
    site_strict_top_gap_anomaly_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/strict_top_gap_anomaly_summary.json"
    )
    assert "weighted_peer_means" in site_strict_top_gap_anomaly_artifact["required_keys"]
    assert "anomaly_vs_peer_deltas" in site_strict_top_gap_anomaly_artifact["required_keys"]
    assert "ranked_anomaly_component_deltas" in site_strict_top_gap_anomaly_artifact["required_keys"]
    site_strict_top_gap_anomaly_component_split_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "site/data/strict_top_gap_anomaly_component_split_summary.json"
    )
    assert "loan_subcomponent_deltas" in site_strict_top_gap_anomaly_component_split_artifact["required_keys"]
    assert "liquidity_external_deltas" in site_strict_top_gap_anomaly_component_split_artifact["required_keys"]
    site_strict_top_gap_anomaly_di_loans_split_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "site/data/strict_top_gap_anomaly_di_loans_split_summary.json"
    )
    assert "di_loans_nec_component_deltas" in site_strict_top_gap_anomaly_di_loans_split_artifact["required_keys"]
    assert "dominant_borrower_component" in site_strict_top_gap_anomaly_di_loans_split_artifact["required_keys"]
    site_strict_top_gap_anomaly_backdrop_artifact = next(
        item
        for item in payload["artifacts"]
        if item["path"] == "site/data/strict_top_gap_anomaly_backdrop_summary.json"
    )
    assert "corporate_credit_row" in site_strict_top_gap_anomaly_backdrop_artifact["required_keys"]
    assert "reserves_row" in site_strict_top_gap_anomaly_backdrop_artifact["required_keys"]
    site_big_picture_synthesis_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/big_picture_synthesis_summary.json"
    )
    assert "classification" in site_big_picture_synthesis_artifact["required_keys"]
    assert "supporting_case" in site_big_picture_synthesis_artifact["required_keys"]
    site_treatment_object_comparison_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/treatment_object_comparison_summary.json"
    )
    assert "candidate_objects" in site_treatment_object_comparison_artifact["required_keys"]
    assert "recommendation" in site_treatment_object_comparison_artifact["required_keys"]
    site_split_treatment_architecture_artifact = next(
        item for item in payload["artifacts"] if item["path"] == "site/data/split_treatment_architecture_summary.json"
    )
    assert "series_definitions" in site_split_treatment_architecture_artifact["required_keys"]
    assert "architecture_recommendation" in site_split_treatment_architecture_artifact["required_keys"]


def test_shock_and_lp_specs_use_canonical_names() -> None:
    shock_specs = load_yaml(repo_root() / "config" / "shock_specs.yml")
    lp_specs = load_yaml(repo_root() / "config" / "lp_specs.yml")

    default_shock = shock_specs["shocks"]["unexpected_tdc_default"]
    assert default_shock["target"] == "tdc_bank_only_qoq"
    assert default_shock["method"] == "rolling_window_ridge"
    assert default_shock["freeze_status"] == "frozen"
    assert default_shock["ridge_alpha"] == 125.0
    assert default_shock["max_train_obs"] == 40
    assert default_shock["quality_gate"]["min_usable_observations"] == 60
    assert default_shock["quality_gate"]["min_shock_target_correlation"] == 0.5
    assert default_shock["quality_gate"]["max_realized_scale_ratio_p95"] == 25.0
    assert default_shock["quality_gate"]["max_realized_scale_ratio_p99"] == 100.0
    assert default_shock["standardized_column"] == "tdc_residual_z"
    assert default_shock["fitted_column"] == "tdc_fitted"
    assert "lag_tdc_bank_only_qoq" in default_shock["predictors"]
    assert "lag_bill_share" not in default_shock["predictors"]
    assert "lag_total_deposits_bank_qoq" not in default_shock["predictors"]
    assert "lag_bank_credit_private_qoq" not in default_shock["predictors"]

    no_foreign = shock_specs["shocks"]["unexpected_tdc_no_foreign_bank_sectors"]
    assert no_foreign["target"] == "tdc_no_foreign_bank_sectors_qoq"
    assert no_foreign["standardized_column"] == "tdc_no_foreign_bank_sectors_residual_z"
    assert "lag_tdc_no_foreign_bank_sectors_qoq" in no_foreign["predictors"]
    no_toc_no_row = shock_specs["shocks"]["unexpected_tdc_no_toc_no_row_bank_only"]
    assert no_toc_no_row["target"] == "tdc_no_toc_no_row_bank_only_qoq"
    assert no_toc_no_row["standardized_column"] == "tdc_no_toc_no_row_bank_only_residual_z"
    assert "lag_tdc_no_toc_no_row_bank_only_qoq" in no_toc_no_row["predictors"]
    core_deposit_proximate = shock_specs["shocks"]["unexpected_tdc_core_deposit_proximate_bank_only"]
    assert core_deposit_proximate["target"] == "tdc_core_deposit_proximate_bank_only_qoq"
    assert core_deposit_proximate["standardized_column"] == "tdc_core_deposit_proximate_bank_only_residual_z"
    assert "lag_tdc_core_deposit_proximate_bank_only_qoq" in core_deposit_proximate["predictors"]

    baseline = lp_specs["specs"]["baseline"]
    assert baseline["shock_column"] == "tdc_residual_z"
    assert "tdc_bank_only_qoq" in baseline["outcomes"]
    assert "total_deposits_bank_qoq" in baseline["outcomes"]
    assert "checkable_deposits_bank_qoq" in baseline["outcomes"]
    assert "interbank_transactions_bank_qoq" in baseline["outcomes"]
    assert "time_savings_deposits_bank_qoq" in baseline["outcomes"]
    assert "checkable_federal_govt_bank_qoq" in baseline["outcomes"]
    assert "checkable_state_local_bank_qoq" in baseline["outcomes"]
    assert "checkable_rest_of_world_bank_qoq" in baseline["outcomes"]
    assert "checkable_private_domestic_bank_qoq" in baseline["outcomes"]
    assert "interbank_transactions_foreign_banks_liability_qoq" in baseline["outcomes"]
    assert "interbank_transactions_foreign_banks_asset_qoq" in baseline["outcomes"]
    assert "deposits_at_foreign_banks_asset_qoq" in baseline["outcomes"]
    assert "accounting_deposit_substitution_qoq" in baseline["outcomes"]
    assert "accounting_bank_balance_sheet_qoq" in baseline["outcomes"]
    assert "accounting_public_liquidity_qoq" in baseline["outcomes"]
    assert "accounting_external_flow_qoq" in baseline["outcomes"]
    assert "accounting_identity_total_qoq" in baseline["outcomes"]
    assert "accounting_identity_gap_qoq" in baseline["outcomes"]
    assert "strict_loan_source_qoq" in baseline["outcomes"]
    assert "strict_loan_mortgages_qoq" in baseline["outcomes"]
    assert "strict_loan_consumer_credit_qoq" in baseline["outcomes"]
    assert "strict_loan_di_loans_nec_qoq" in baseline["outcomes"]
    assert "strict_loan_other_advances_qoq" in baseline["outcomes"]
    assert "strict_non_treasury_agency_gse_qoq" in baseline["outcomes"]
    assert "strict_non_treasury_municipal_qoq" in baseline["outcomes"]
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in baseline["outcomes"]
    assert "strict_non_treasury_securities_qoq" in baseline["outcomes"]
    assert "strict_identifiable_total_qoq" in baseline["outcomes"]
    assert "strict_identifiable_gap_qoq" in baseline["outcomes"]
    assert "commercial_industrial_loans_qoq" in baseline["outcomes"]
    assert "construction_land_development_loans_qoq" in baseline["outcomes"]
    assert "cre_multifamily_loans_qoq" in baseline["outcomes"]
    assert "cre_nonfarm_nonresidential_loans_qoq" in baseline["outcomes"]
    assert "consumer_loans_qoq" in baseline["outcomes"]
    assert "credit_card_revolving_loans_qoq" in baseline["outcomes"]
    assert "auto_loans_qoq" in baseline["outcomes"]
    assert "other_consumer_loans_qoq" in baseline["outcomes"]
    assert "heloc_loans_qoq" in baseline["outcomes"]
    assert "closed_end_residential_loans_qoq" in baseline["outcomes"]
    assert "loans_to_commercial_banks_qoq" in baseline["outcomes"]
    assert "loans_to_nondepository_financial_institutions_qoq" in baseline["outcomes"]
    assert "loans_for_purchasing_or_carrying_securities_qoq" in baseline["outcomes"]
    assert "treasury_securities_bank_qoq" in baseline["outcomes"]
    assert "agency_gse_backed_securities_bank_qoq" in baseline["outcomes"]
    assert "municipal_securities_bank_qoq" in baseline["outcomes"]
    assert "corporate_foreign_bonds_bank_qoq" in baseline["outcomes"]
    assert "fedfunds_repo_liabilities_bank_qoq" in baseline["outcomes"]
    assert "commercial_bank_borrowings_qoq" in baseline["outcomes"]
    assert "fed_borrowings_depository_institutions_qoq" in baseline["outcomes"]
    assert "debt_securities_bank_liability_qoq" in baseline["outcomes"]
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in baseline["outcomes"]
    assert "holding_company_parent_funding_bank_qoq" in baseline["outcomes"]
    assert "tga_qoq" in baseline["outcomes"]
    assert "reserves_qoq" in baseline["outcomes"]
    assert "on_rrp_reallocation_qoq" in baseline["outcomes"]
    assert "household_treasury_securities_reallocation_qoq" in baseline["outcomes"]
    assert "mmf_treasury_bills_reallocation_qoq" in baseline["outcomes"]
    assert "currency_reallocation_qoq" in baseline["outcomes"]
    assert baseline["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert baseline["include_lagged_outcome"] is True
    assert lp_specs["specs"]["regimes"]["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["regimes"]["include_lagged_outcome"] is True
    assert "checkable_deposits_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "interbank_transactions_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "time_savings_deposits_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "checkable_federal_govt_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "checkable_state_local_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "checkable_rest_of_world_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "checkable_private_domestic_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "interbank_transactions_foreign_banks_liability_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "interbank_transactions_foreign_banks_asset_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "deposits_at_foreign_banks_asset_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_deposit_substitution_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_bank_balance_sheet_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_public_liquidity_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_external_flow_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_identity_total_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "accounting_identity_gap_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_loan_source_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_loan_mortgages_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_loan_consumer_credit_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_loan_di_loans_nec_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_loan_other_advances_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_non_treasury_agency_gse_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_non_treasury_municipal_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_non_treasury_securities_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_identifiable_total_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "strict_identifiable_gap_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "tga_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "reserves_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "on_rrp_reallocation_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "household_treasury_securities_reallocation_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "mmf_treasury_bills_reallocation_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "currency_reallocation_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "fedfunds_repo_liabilities_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "commercial_bank_borrowings_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "fed_borrowings_depository_institutions_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "debt_securities_bank_liability_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert "holding_company_parent_funding_bank_qoq" in lp_specs["specs"]["regimes"]["outcomes"]
    assert lp_specs["specs"]["regimes"]["regime_columns"] == [
        "reserve_drain_pressure",
    ]
    assert lp_specs["specs"]["sensitivity"]["controls"] == ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"]
    assert lp_specs["specs"]["sensitivity"]["include_lagged_outcome"] is True
    assert "checkable_deposits_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "interbank_transactions_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "time_savings_deposits_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "checkable_federal_govt_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "checkable_state_local_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "checkable_rest_of_world_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "checkable_private_domestic_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "interbank_transactions_foreign_banks_liability_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "interbank_transactions_foreign_banks_asset_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "deposits_at_foreign_banks_asset_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_deposit_substitution_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_bank_balance_sheet_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_public_liquidity_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_external_flow_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_identity_total_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "accounting_identity_gap_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_loan_source_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_loan_mortgages_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_loan_consumer_credit_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_loan_di_loans_nec_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_loan_other_advances_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_non_treasury_agency_gse_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_non_treasury_municipal_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_non_treasury_securities_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_identifiable_total_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "strict_identifiable_gap_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "tga_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "reserves_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "on_rrp_reallocation_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "household_treasury_securities_reallocation_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "mmf_treasury_bills_reallocation_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "currency_reallocation_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "fedfunds_repo_liabilities_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "commercial_bank_borrowings_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "fed_borrowings_depository_institutions_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "debt_securities_bank_liability_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "holding_company_parent_funding_bank_qoq" in lp_specs["specs"]["sensitivity"]["outcomes"]
    assert "checkable_deposits_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "interbank_transactions_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "time_savings_deposits_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "checkable_federal_govt_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "checkable_state_local_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "checkable_rest_of_world_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "checkable_private_domestic_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "interbank_transactions_foreign_banks_liability_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "interbank_transactions_foreign_banks_asset_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "deposits_at_foreign_banks_asset_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_deposit_substitution_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_bank_balance_sheet_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_public_liquidity_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_external_flow_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_identity_total_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "accounting_identity_gap_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_loan_source_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_loan_mortgages_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_loan_consumer_credit_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_loan_di_loans_nec_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_loan_other_advances_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_non_treasury_agency_gse_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_non_treasury_municipal_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_non_treasury_corporate_foreign_bonds_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_non_treasury_securities_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_identifiable_total_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "strict_identifiable_gap_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "tga_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "reserves_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "on_rrp_reallocation_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "household_treasury_securities_reallocation_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "mmf_treasury_bills_reallocation_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "currency_reallocation_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "fedfunds_repo_liabilities_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "commercial_bank_borrowings_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "fed_borrowings_depository_institutions_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "debt_securities_bank_liability_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "fhlb_advances_sallie_mae_loans_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert "holding_company_parent_funding_bank_qoq" in lp_specs["specs"]["state_dependence"]["outcomes"]
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["headline_lagged_macro"]["control_role"] == "headline"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_bill"]["control_role"] == "core"
    assert lp_specs["specs"]["control_sensitivity"]["control_variants"]["lagged_macro_plus_trend"]["control_role"] == "exploratory"
    macro_factor_spec = lp_specs["specs"]["factor_control_sensitivity"]["factor_variants"]["recursive_macro_factors2"]
    assert macro_factor_spec["factor_role"] == "core"
    assert macro_factor_spec["factor_count"] == 2
    assert macro_factor_spec["min_train_obs"] == 24
    assert macro_factor_spec["source_columns"] == [
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    plumbing_factor_spec = lp_specs["specs"]["factor_control_sensitivity"]["factor_variants"][
        "recursive_macro_plumbing_factors3"
    ]
    assert plumbing_factor_spec["factor_role"] == "exploratory"
    assert plumbing_factor_spec["factor_count"] == 3
    assert plumbing_factor_spec["min_train_obs"] == 40
    assert plumbing_factor_spec["source_columns"] == [
        "lag_tga_qoq",
        "lag_reserves_qoq",
        "lag_bill_share",
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    smooth_lp = lp_specs["specs"]["smooth_lp"]
    assert smooth_lp["method"] == "gaussian_kernel"
    assert smooth_lp["bandwidth"] == 1.0
    assert smooth_lp["min_horizons"] == 4
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["all_usable_shocks"]["sample_role"] == "headline"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_flagged_shocks"]["sample_role"] == "exploratory"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_severe_scale_tail"]["sample_role"] == "exploratory"
    assert lp_specs["specs"]["sample_sensitivity"]["sample_variants"]["drop_severe_scale_tail"]["max_value_column"] == "fitted_to_target_scale_ratio"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["all_usable"]["period_role"] == "headline"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["post_gfc_early"]["start_quarter"] == "2009Q1"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["post_gfc_early"]["end_quarter"] == "2014Q4"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["pre_covid"]["period_role"] == "core"
    assert lp_specs["specs"]["period_sensitivity"]["period_variants"]["covid_post"]["start_quarter"] == "2020Q1"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["baseline"]["treatment_role"] == "core"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["baseline"]["treatment_family"] == "headline"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_long_burnin"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_long_burnin"]["treatment_family"] == "shock_design"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_no_bill_share"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["bank_only_billshare_macro_rolling40"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_rolling40_ols"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_billshare_expanding"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["legacy_totaldep_long_burnin"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["broad_depository"]["treatment_role"] == "exploratory"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["broad_depository"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["domestic_bank_only"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["us_chartered_bank_only"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["core_deposit_proximate"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["no_remit_bank_only"]["treatment_family"] == "measurement"
    assert lp_specs["specs"]["sensitivity"]["shock_variants"]["credit_union_sensitive"]["treatment_family"] == "measurement"
    assert shock_specs["shocks"]["unexpected_tdc_domestic_bank_only"]["target"] == "tdc_domestic_bank_only_qoq"
    assert shock_specs["shocks"]["unexpected_tdc_us_chartered_bank_only"]["target"] == "tdc_us_chartered_bank_only_qoq"
    assert shock_specs["shocks"]["unexpected_tdc_no_remit_bank_only"]["target"] == "tdc_no_remit_bank_only_qoq"
    assert shock_specs["shocks"]["unexpected_tdc_credit_union_sensitive"]["target"] == "tdc_credit_union_sensitive_qoq"


def test_output_schema_mentions_full_bundle() -> None:
    text = (repo_root() / "docs" / "output_schema.md").read_text(encoding="utf-8")
    for needle in [
        "tdc_bank_only_qoq",
        "total_deposits_bank_qoq",
        "output/models/identity_measurement_ladder.csv",
        "output/models/lp_irf_regimes.csv",
        "output/models/lp_irf_accounting_identity.csv",
        "output/models/lp_irf_strict_identifiable.csv",
        "output/models/accounting_identity_alignment.csv",
        "output/models/accounting_identity_summary.json",
        "output/models/strict_identifiable_alignment.csv",
        "output/models/strict_funding_offset_alignment.csv",
        "output/models/strict_identifiable_summary.json",
        "output/models/strict_identifiable_followup_summary.json",
        "output/models/scope_alignment_summary.json",
        "output/models/tdc_treatment_audit_summary.json",
        "output/models/regime_diagnostics_summary.json",
        "output/models/tdc_sensitivity_ladder.csv",
        "output/models/control_set_sensitivity.csv",
        "output/models/shock_sample_sensitivity.csv",
        "output/models/period_sensitivity.csv",
        "output/models/period_sensitivity_summary.json",
        "output/models/provenance_validation_summary.json",
        "output/models/total_minus_other_contrast.csv",
        "output/models/structural_proxy_evidence.csv",
        "output/models/structural_proxy_evidence_summary.json",
        "output/models/proxy_coverage_summary.json",
        "output/models/proxy_unit_audit.json",
        "output/models/shock_diagnostics_summary.json",
        "output/models/direct_identification_summary.json",
        "output/models/result_readiness_summary.json",
        "output/models/pass_through_summary.json",
        "output/models/sample_construction_summary.json",
        "proxy_coverage_context",
        "treatment_role",
        "control_role",
        "output/manifests/raw_downloads.json",
        "site/data/identity_measurement_ladder.csv",
        "site/data/lp_irf_accounting_identity.csv",
        "site/data/lp_irf_strict_identifiable.csv",
        "site/data/accounting_identity_alignment.csv",
        "site/data/accounting_identity_summary.json",
        "site/data/strict_identifiable_alignment.csv",
        "site/data/strict_funding_offset_alignment.csv",
        "site/data/strict_identifiable_summary.json",
        "site/data/strict_identifiable_followup_summary.json",
        "site/data/strict_missing_channel_summary.json",
        "site/data/strict_sign_mismatch_audit_summary.json",
        "site/data/strict_shock_composition_summary.json",
        "site/data/big_picture_synthesis_summary.json",
        "site/data/split_treatment_architecture_summary.json",
        "site/data/strict_redesign_summary.json",
        "site/data/tdc_treatment_audit_summary.json",
        "site/data/tdc_sensitivity_ladder.csv",
        "site/data/control_set_sensitivity.csv",
        "site/data/shock_sample_sensitivity.csv",
        "site/data/period_sensitivity.csv",
        "site/data/period_sensitivity_summary.json",
        "site/data/provenance_validation_summary.json",
        "site/data/total_minus_other_contrast.csv",
        "site/data/structural_proxy_evidence.csv",
        "site/data/structural_proxy_evidence_summary.json",
        "site/data/proxy_coverage_summary.json",
        "site/data/proxy_unit_audit.json",
        "site/data/regime_diagnostics_summary.json",
        "site/data/shock_diagnostics_summary.json",
        "site/data/direct_identification_summary.json",
        "site/data/result_readiness_summary.json",
        "site/data/pass_through_summary.json",
        "site/data/strict_di_bucket_bridge_summary.json",
        "site/data/sample_construction_summary.json",
        "site/data/core_treatment_promotion_summary.json",
        "strict_redesign_context",
        "strict_sign_mismatch_audit_context",
        "strict_shock_composition_context",
        "strict_top_gap_quarter_audit_context",
        "strict_top_gap_quarter_direction_context",
        "strict_top_gap_inversion_context",
        "strict_top_gap_anomaly_context",
        "strict_top_gap_anomaly_component_split_context",
        "strict_top_gap_anomaly_di_loans_split_context",
        "strict_top_gap_anomaly_backdrop_context",
        "big_picture_synthesis_context",
        "treatment_object_comparison_context",
        "split_treatment_architecture_context",
        "core_treatment_promotion_context",
        "strict_di_bucket_role_context",
        "strict_di_bucket_bridge_context",
        "strict_private_borrower_bridge_context",
        "strict_nonfinancial_corporate_bridge_context",
        "strict_private_offset_residual_context",
        "strict_corporate_bridge_secondary_comparison_context",
        "strict_component_framework_context",
        "strict_direct_core_component_context",
        "strict_direct_core_horizon_stability_context",
        "strict_additional_creator_candidate_context",
        "tdcest_ladder_integration_context",
        "tdcest_broad_object_comparison_context",
        "tdcest_broad_treatment_sensitivity_context",
        "strict_release_framing_context",
        "output/models/strict_redesign_summary.json",
        "output/models/strict_di_bucket_role_summary.json",
        "output/models/strict_di_bucket_bridge_summary.json",
        "output/models/strict_private_borrower_bridge_summary.json",
        "output/models/strict_nonfinancial_corporate_bridge_summary.json",
        "output/models/strict_private_offset_residual_summary.json",
        "output/models/strict_corporate_bridge_secondary_comparison_summary.json",
        "output/models/strict_component_framework_summary.json",
        "output/models/strict_direct_core_component_summary.json",
        "output/models/strict_direct_core_horizon_stability_summary.json",
        "output/models/strict_additional_creator_candidate_summary.json",
        "output/models/strict_di_loans_nec_measurement_audit_summary.json",
        "output/models/strict_results_closeout_summary.json",
        "output/models/tdcest_ladder_integration_summary.json",
        "output/models/tdcest_broad_object_comparison_summary.json",
        "output/models/tdcest_broad_treatment_sensitivity_summary.json",
        "output/models/strict_release_framing_summary.json",
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
        "site/data/strict_di_loans_nec_measurement_audit_summary.json",
        "site/data/strict_results_closeout_summary.json",
        "site/data/tdcest_ladder_integration_summary.json",
        "site/data/tdcest_broad_object_comparison_summary.json",
        "site/data/tdcest_broad_treatment_sensitivity_summary.json",
        "site/data/strict_release_framing_summary.json",
        "site/data/strict_redesign_summary.json",
        "headline sample",
    ]:
        assert needle in text
