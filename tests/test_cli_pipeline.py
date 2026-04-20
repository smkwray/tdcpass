from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tdcpass.analysis.treatment_fingerprint import build_headline_treatment_fingerprint
from tdcpass.cli import build_parser, main
from tdcpass.pipeline.quarterly import _should_refuse_public_mirror


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
    payload = build_headline_treatment_fingerprint(
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
    payload["analysis_tree"] = {"status": "clean", "tracked_change_count": 0, "untracked_change_count": 0}
    return payload


def test_should_refuse_public_mirror_only_for_repo_root_when_provenance_fails(tmp_path: Path) -> None:
    failed_payload = {"status": "failed"}
    repo_root = Path(__file__).resolve().parents[1]

    assert _should_refuse_public_mirror(root=repo_root, provenance_validation_payload=failed_payload) is True
    assert _should_refuse_public_mirror(root=tmp_path, provenance_validation_payload=failed_payload) is False
    assert _should_refuse_public_mirror(root=tmp_path, provenance_validation_payload={"status": "passed"}) is False


def test_pipeline_run_command_is_wired(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"

    panel_header = [
        "quarter",
        "tdc_bank_only_qoq",
        "tdc_domestic_bank_only_qoq",
        "tdc_us_chartered_bank_only_qoq",
        "tdc_no_foreign_bank_sectors_qoq",
        "tdc_no_toc_bank_only_qoq",
        "tdc_no_toc_no_row_bank_only_qoq",
        "total_deposits_bank_qoq",
        "deposits_only_bank_qoq",
        "broad_bank_deposits_qoq",
        "checkable_deposits_bank_qoq",
        "checkable_deposits_foreign_offices_qoq",
        "checkable_deposits_affiliated_areas_qoq",
        "interbank_transactions_bank_qoq",
        "time_savings_deposits_bank_qoq",
        "time_savings_deposits_foreign_offices_qoq",
        "time_savings_deposits_affiliated_areas_qoq",
        "checkable_federal_govt_bank_qoq",
        "checkable_state_local_bank_qoq",
        "checkable_rest_of_world_bank_qoq",
        "checkable_private_domestic_bank_qoq",
        "interbank_transactions_foreign_banks_liability_qoq",
        "interbank_transactions_foreign_banks_asset_qoq",
        "deposits_at_foreign_banks_asset_qoq",
        "other_component_qoq",
        "other_component_domestic_bank_only_qoq",
        "other_component_us_chartered_bank_only_qoq",
        "other_component_no_foreign_bank_sectors_qoq",
        "other_component_no_toc_bank_only_qoq",
        "other_component_no_toc_no_row_bank_only_qoq",
        "deposits_only_other_component_qoq",
        "deposits_only_other_component_domestic_bank_only_qoq",
        "deposits_only_other_component_us_chartered_bank_only_qoq",
        "broad_bank_other_component_qoq",
        "accounting_deposit_substitution_qoq",
        "accounting_bank_balance_sheet_qoq",
        "accounting_public_liquidity_qoq",
        "accounting_external_flow_qoq",
        "accounting_identity_total_qoq",
        "accounting_identity_gap_qoq",
        "strict_loan_source_qoq",
        "strict_loan_mortgages_qoq",
        "strict_loan_consumer_credit_qoq",
        "strict_loan_di_loans_nec_qoq",
        "strict_di_loans_nec_households_nonprofits_qoq",
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        "strict_di_loans_nec_state_local_qoq",
        "strict_di_loans_nec_domestic_financial_qoq",
        "strict_di_loans_nec_rest_of_world_qoq",
        "strict_di_loans_nec_systemwide_liability_total_qoq",
        "strict_di_loans_nec_systemwide_borrower_total_qoq",
        "strict_di_loans_nec_systemwide_borrower_gap_qoq",
        "strict_di_loans_nec_private_domestic_borrower_qoq",
        "strict_di_loans_nec_noncore_system_borrower_qoq",
        "strict_loan_other_advances_qoq",
        "strict_loan_core_min_qoq",
        "strict_loan_core_plus_private_borrower_qoq",
        "strict_loan_core_plus_nonfinancial_corporate_qoq",
        "strict_loan_noncore_system_qoq",
        "strict_non_treasury_agency_gse_qoq",
        "strict_non_treasury_municipal_qoq",
        "strict_non_treasury_corporate_foreign_bonds_qoq",
        "strict_non_treasury_securities_qoq",
        "strict_identifiable_total_qoq",
        "strict_identifiable_gap_qoq",
        "strict_funding_fedfunds_repo_qoq",
        "strict_funding_debt_securities_qoq",
        "strict_funding_fhlb_advances_qoq",
        "strict_funding_offset_total_qoq",
        "strict_identifiable_net_after_funding_qoq",
        "strict_gap_after_funding_qoq",
        "broad_strict_loan_foreign_offices_qoq",
        "broad_strict_loan_affiliated_areas_qoq",
        "broad_strict_loan_source_qoq",
        "broad_strict_gap_qoq",
        "bank_credit_private_qoq",
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
        "loans_to_commercial_banks_qoq",
        "loans_to_nondepository_financial_institutions_qoq",
        "loans_for_purchasing_or_carrying_securities_qoq",
        "treasury_securities_bank_qoq",
        "agency_gse_backed_securities_bank_qoq",
        "municipal_securities_bank_qoq",
        "corporate_foreign_bonds_bank_qoq",
        "fedfunds_repo_liabilities_bank_qoq",
        "commercial_bank_borrowings_qoq",
        "fed_borrowings_depository_institutions_qoq",
        "debt_securities_bank_liability_qoq",
        "fhlb_advances_sallie_mae_loans_bank_qoq",
        "holding_company_parent_funding_bank_qoq",
        "commercial_industrial_loans_ex_chargeoffs_qoq",
        "consumer_loans_ex_chargeoffs_qoq",
        "credit_card_revolving_loans_ex_chargeoffs_qoq",
        "other_consumer_loans_ex_chargeoffs_qoq",
        "closed_end_residential_loans_ex_chargeoffs_qoq",
        "cb_nonts_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
        "on_rrp_reallocation_qoq",
        "household_treasury_securities_reallocation_qoq",
        "mmf_treasury_bills_reallocation_qoq",
        "currency_reallocation_qoq",
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
        "lag_tdc_us_chartered_bank_only_qoq",
        "lag_tdc_no_foreign_bank_sectors_qoq",
        "lag_tdc_no_toc_bank_only_qoq",
        "lag_tdc_no_toc_no_row_bank_only_qoq",
        "lag_total_deposits_bank_qoq",
        "lag_deposits_only_bank_qoq",
        "lag_broad_bank_deposits_qoq",
        "lag_checkable_deposits_bank_qoq",
        "lag_checkable_deposits_foreign_offices_qoq",
        "lag_checkable_deposits_affiliated_areas_qoq",
        "lag_interbank_transactions_bank_qoq",
        "lag_time_savings_deposits_bank_qoq",
        "lag_time_savings_deposits_foreign_offices_qoq",
        "lag_time_savings_deposits_affiliated_areas_qoq",
        "lag_checkable_federal_govt_bank_qoq",
        "lag_checkable_state_local_bank_qoq",
        "lag_checkable_rest_of_world_bank_qoq",
        "lag_checkable_private_domestic_bank_qoq",
        "lag_interbank_transactions_foreign_banks_liability_qoq",
        "lag_interbank_transactions_foreign_banks_asset_qoq",
        "lag_deposits_at_foreign_banks_asset_qoq",
        "lag_strict_loan_source_qoq",
        "lag_strict_loan_mortgages_qoq",
        "lag_strict_loan_consumer_credit_qoq",
        "lag_strict_loan_di_loans_nec_qoq",
        "lag_strict_di_loans_nec_households_nonprofits_qoq",
        "lag_strict_di_loans_nec_nonfinancial_corporate_qoq",
        "lag_strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        "lag_strict_di_loans_nec_state_local_qoq",
        "lag_strict_di_loans_nec_domestic_financial_qoq",
        "lag_strict_di_loans_nec_rest_of_world_qoq",
        "lag_strict_di_loans_nec_systemwide_liability_total_qoq",
        "lag_strict_di_loans_nec_systemwide_borrower_total_qoq",
        "lag_strict_di_loans_nec_systemwide_borrower_gap_qoq",
        "lag_strict_di_loans_nec_private_domestic_borrower_qoq",
        "lag_strict_di_loans_nec_noncore_system_borrower_qoq",
        "lag_strict_loan_other_advances_qoq",
        "lag_strict_loan_core_min_qoq",
        "lag_strict_loan_core_plus_private_borrower_qoq",
        "lag_strict_loan_core_plus_nonfinancial_corporate_qoq",
        "lag_strict_loan_noncore_system_qoq",
        "lag_strict_non_treasury_agency_gse_qoq",
        "lag_strict_non_treasury_municipal_qoq",
        "lag_strict_non_treasury_corporate_foreign_bonds_qoq",
        "lag_strict_non_treasury_securities_qoq",
        "lag_strict_identifiable_total_qoq",
        "lag_strict_identifiable_gap_qoq",
        "lag_strict_funding_fedfunds_repo_qoq",
        "lag_strict_funding_debt_securities_qoq",
        "lag_strict_funding_fhlb_advances_qoq",
        "lag_strict_funding_offset_total_qoq",
        "lag_strict_identifiable_net_after_funding_qoq",
        "lag_strict_gap_after_funding_qoq",
        "lag_broad_strict_loan_foreign_offices_qoq",
        "lag_broad_strict_loan_affiliated_areas_qoq",
        "lag_broad_strict_loan_source_qoq",
        "lag_broad_strict_gap_qoq",
        "lag_bank_credit_private_qoq",
        "lag_commercial_industrial_loans_qoq",
        "lag_construction_land_development_loans_qoq",
        "lag_cre_multifamily_loans_qoq",
        "lag_cre_nonfarm_nonresidential_loans_qoq",
        "lag_consumer_loans_qoq",
        "lag_credit_card_revolving_loans_qoq",
        "lag_auto_loans_qoq",
        "lag_other_consumer_loans_qoq",
        "lag_heloc_loans_qoq",
        "lag_closed_end_residential_loans_qoq",
        "lag_loans_to_commercial_banks_qoq",
        "lag_loans_to_nondepository_financial_institutions_qoq",
        "lag_loans_for_purchasing_or_carrying_securities_qoq",
        "lag_treasury_securities_bank_qoq",
        "lag_agency_gse_backed_securities_bank_qoq",
        "lag_municipal_securities_bank_qoq",
        "lag_corporate_foreign_bonds_bank_qoq",
        "lag_fedfunds_repo_liabilities_bank_qoq",
        "lag_commercial_bank_borrowings_qoq",
        "lag_fed_borrowings_depository_institutions_qoq",
        "lag_debt_securities_bank_liability_qoq",
        "lag_fhlb_advances_sallie_mae_loans_bank_qoq",
        "lag_holding_company_parent_funding_bank_qoq",
        "lag_commercial_industrial_loans_ex_chargeoffs_qoq",
        "lag_consumer_loans_ex_chargeoffs_qoq",
        "lag_credit_card_revolving_loans_ex_chargeoffs_qoq",
        "lag_other_consumer_loans_ex_chargeoffs_qoq",
        "lag_closed_end_residential_loans_ex_chargeoffs_qoq",
        "lag_tga_qoq",
        "lag_reserves_qoq",
        "lag_on_rrp_reallocation_qoq",
        "lag_household_treasury_securities_reallocation_qoq",
        "lag_mmf_treasury_bills_reallocation_qoq",
        "lag_currency_reallocation_qoq",
        "lag_bill_share",
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]
    _write_csv(source_root / "data" / "derived" / "quarterly_panel.csv", panel_header, ["2000Q1"] + [1] * (len(panel_header) - 1))
    _write_csv(
        source_root / "data" / "derived" / "call_report_deposit_components.csv",
        [
            "quarter",
            "account_type",
            "depositor_class",
            "amount_bil_usd",
            "institution_count",
            "source_quarter",
            "source_kind",
            "universe_basis",
        ],
        [["2000Q1", "transaction", "individuals_partnerships_corporations", 1.0, 1, "2000Q1", "stub", "insured_institutions_aggregate"]],
    )

    for rel in [
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
        "output/models/strict_top_gap_anomaly_component_split_summary.json",
        "output/models/strict_top_gap_anomaly_di_loans_split_summary.json",
        "output/models/strict_top_gap_anomaly_backdrop_summary.json",
        "output/models/big_picture_synthesis_summary.json",
        "output/models/treatment_object_comparison_summary.json",
        "output/models/split_treatment_architecture_summary.json",
        "output/models/core_treatment_promotion_summary.json",
        "output/models/strict_redesign_summary.json",
        "output/models/strict_loan_core_redesign_summary.json",
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
            "output/models/toc_row_incidence_audit_summary.json",
            "output/models/toc_row_liability_incidence_raw_summary.json",
            "output/models/toc_validated_share_candidate_summary.json",
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
        "output/models/deposit_component_scorecard.json",
        "output/models/deposit_type_side_read.csv",
        "output/models/counterpart_channel_scorecard.json",
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
        elif rel.endswith("lp_irf_accounting_identity.csv"):
            _write_csv(
                source_root / rel,
                ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"],
                [["accounting_identity_total_qoq", 0, 0.05, 0.01, 0.0, 0.1, 1, "baseline"]],
            )
        elif rel.endswith("lp_irf_strict_identifiable.csv"):
            _write_csv(
                source_root / rel,
                ["outcome", "horizon", "beta", "se", "lower95", "upper95", "n", "spec_name"],
                [["strict_identifiable_total_qoq", 0, 0.04, 0.01, 0.0, 0.08, 1, "baseline"]],
            )
        elif rel.endswith("accounting_identity_alignment.csv"):
            _write_csv(
                source_root / rel,
                [
                    "horizon",
                    "residual_beta",
                    "accounting_total_beta",
                    "identity_gap_beta",
                    "arithmetic_residual_minus_total_beta",
                    "identity_gap_share_of_residual",
                    "residual_n",
                    "accounting_total_n",
                    "identity_gap_n",
                    "interpretation",
                ],
                [[0, 0.1, 0.05, 0.05, 0.05, 0.5, 1, 1, 1, "partial_closure"]],
            )
        elif rel.endswith("strict_identifiable_alignment.csv"):
            _write_csv(
                source_root / rel,
                [
                    "horizon",
                    "residual_beta",
                    "strict_loan_source_beta",
                    "strict_non_treasury_securities_beta",
                    "strict_identifiable_total_beta",
                    "strict_identifiable_gap_beta",
                    "arithmetic_residual_minus_total_beta",
                    "strict_gap_share_of_residual",
                    "residual_n",
                    "strict_total_n",
                    "strict_gap_n",
                    "interpretation",
                ],
                [[0, 0.1, 0.03, 0.01, 0.04, 0.06, 0.06, 0.6, 1, 1, 1, "large_unidentified_remainder"]],
            )
        elif rel.endswith("strict_funding_offset_alignment.csv"):
            _write_csv(
                source_root / rel,
                [
                    "horizon",
                    "strict_identifiable_total_beta",
                    "strict_funding_offset_total_beta",
                    "strict_funding_offset_share_of_identifiable_total_beta",
                    "strict_identifiable_net_after_funding_beta",
                    "strict_gap_after_funding_beta",
                    "identifiable_total_n",
                    "funding_total_n",
                    "net_after_funding_n",
                    "gap_after_funding_n",
                    "interpretation",
                ],
                [[0, 0.04, 0.02, 0.5, 0.02, 0.08, 1, 1, 1, 1, "funding_offsets_material_relative_to_identifiable_total"]],
            )
        elif rel.endswith("deposit_type_side_read.csv"):
            _write_csv(
                source_root / rel,
                ["outcome", "display_name", "horizon", "horizon_label", "beta", "se", "lower95", "upper95", "n", "ci_excludes_zero", "sign_label", "interpretation_note"],
                [["checkable_deposits_bank_qoq", "Checkable deposits", 0, "impact", 1.0, 0.5, 0.1, 1.9, 1, True, "positive", "stub"]],
            )
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
                [
                    ["domestic_bank_only", "exploratory", "measurement", "tdc_domestic_bank_only_qoq", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_measurement_ladder", "tdc_domestic_bank_only_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"],
                    ["us_chartered_bank_only", "exploratory", "measurement", "tdc_us_chartered_bank_only_qoq", "total_deposits_bank_qoq", 0, 0.12, 0.01, 0.02, 0.22, 1, "identity_measurement_ladder", "tdc_us_chartered_bank_only_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"],
                    ["domestic_bank_only", "exploratory", "measurement", "tdc_domestic_bank_only_qoq", "deposits_only_bank_qoq", 0, 0.08, 0.01, -0.02, 0.18, 1, "identity_measurement_ladder", "tdc_domestic_bank_only_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"],
                    ["us_chartered_bank_only", "exploratory", "measurement", "tdc_us_chartered_bank_only_qoq", "deposits_only_bank_qoq", 0, 0.09, 0.01, -0.01, 0.19, 1, "identity_measurement_ladder", "tdc_us_chartered_bank_only_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"],
                ],
            )
        elif rel.endswith("identity_treatment_sensitivity.csv"):
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
                [["baseline", "core", "headline", "tdc_bank_only_qoq", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_treatment_sensitivity", "tdc_residual_z", "exact_identity_baseline", "estimated_common_design", "bootstrap"]],
            )
        elif rel.endswith("identity_control_sensitivity.csv"):
            _write_csv(
                source_root / rel,
                [
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
                ],
                [["headline_lagged_macro", "headline", "lag_tdc_bank_only_qoq|lag_fedfunds|lag_unemployment|lag_inflation", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_control_sensitivity", "tdc_residual_z", "rolling_oos_standard_deviation", "cumulative_sum_h0_to_h", "exact_identity_baseline", "estimated_common_design", "bootstrap"]],
            )
        elif rel.endswith("identity_sample_sensitivity.csv"):
            _write_csv(
                source_root / rel,
                [
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
                ],
                [["all_usable_shocks", "headline", "all_usable_shocks", "total_deposits_bank_qoq", 0, 0.1, 0.01, 0.0, 0.2, 1, "identity_sample_sensitivity", "tdc_residual_z", "rolling_oos_standard_deviation", "cumulative_sum_h0_to_h", "exact_identity_baseline", "estimated_common_design", "bootstrap"]],
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
        elif rel.endswith("accounting_identity_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "source_kind": "fixture_accounting_bundle",
                    "headline_question": "stub",
                    "estimation_path": {"primary_artifact": "lp_irf_accounting_identity.csv"},
                    "component_outcomes_present": ["accounting_deposit_substitution_qoq"],
                    "horizons": {"h0": {"interpretation": "partial_closure"}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_identifiable_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "source_kind": "z1_transactions_via_fred",
                    "headline_question": "stub",
                    "estimation_path": {"primary_artifact": "lp_irf_strict_identifiable.csv"},
                    "component_outcomes_present": ["strict_loan_source_qoq"],
                    "horizons": {"h0": {"interpretation": "large_unidentified_remainder"}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_identifiable_followup_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "strict_source_kind": "z1_transactions_via_fred",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_identifiable_followup_summary.json"},
                    "measurement_variant_comparison": {
                        "baseline_variant": "bank_only",
                        "comparison_variants": ["domestic_bank_only", "us_chartered_bank_only"],
                        "key_horizons": {},
                    },
                    "strict_component_diagnostics": {"key_horizons": {}},
                    "di_loans_nec_borrower_diagnostics": {"key_horizons": {}},
                    "funding_offset_sensitivity": {"key_horizons": {}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("scope_alignment_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "variant_definitions": {
                        "baseline": {"target": "tdc_bank_only_qoq"},
                        "domestic_bank_only": {"target": "tdc_domestic_bank_only_qoq"},
                        "us_chartered_bank_only": {"target": "tdc_us_chartered_bank_only_qoq"},
                    },
                    "deposit_concepts": {
                        "total_deposits_including_interbank": {
                            "comparison_variants": ["domestic_bank_only", "us_chartered_bank_only"],
                            "key_horizons": {},
                        },
                        "deposits_only_ex_interbank": {
                            "comparison_variants": ["domestic_bank_only", "us_chartered_bank_only"],
                            "key_horizons": {},
                        },
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("broad_scope_system_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "broad_scope_system_summary.json"},
                    "usc_matched_context": {"key_horizons": {}},
                    "broad_matched_system": {"key_horizons": {}},
                    "tdc_component_audit": {"key_horizons": {}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("treasury_operating_cash_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "treasury_operating_cash_audit_summary.json"},
                    "quarterly_alignment": {"status": "available"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("treasury_cash_regime_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "treasury_cash_regime_audit_summary.json"},
                    "definitions": {"tga_qoq": "stub"},
                    "regime_windows": {"pre_shift_ttl_regime": {"status": "available"}},
                    "full_sample": {"classification": "stub"},
                    "classification": {"pre_shift_regime_classification": "mixed_or_transition_regime"},
                    "recommendation": {"status": "historical_ttl_era_reestimate_still_worth_running"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("historical_cash_term_reestimation_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "historical_cash_term_reestimation_summary.json"},
                    "definitions": {"current_cash_term_qoq": "stub"},
                    "windows": {"historical_backfill_window": {"start": "1990Q1", "end": "2002Q3"}},
                    "comparison": {"historical_backfill_window": {"mean_abs_adjustment": 1.0}},
                    "top_adjustment_quarters": [],
                    "classification": {"historical_adjustment_classification": "historical_backfill_changes_only_modestly"},
                    "recommendation": {"status": "historical_cash_term_difference_is_small_but_document_it"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("rest_of_world_treasury_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "rest_of_world_treasury_audit_summary.json"},
                    "quarterly_alignment": {"status": "available"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_row_bundle_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_row_bundle_audit_summary.json"},
                    "quarterly_alignment": {"status": "available"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_row_path_split_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_row_path_split_summary.json"},
                    "path_definitions": {"bundle": "stub"},
                    "quarterly_split": {"status": "available"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_row_excluded_interpretation_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_row_excluded_interpretation_summary.json"},
                    "comparison_definition": {"release_role": "secondary_interpretation_only"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_missing_channel_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_missing_channel_summary.json"},
                    "comparison_definition": {"release_role": "strict_missing_channel_diagnostic"},
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_sign_mismatch_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_sign_mismatch_audit_summary.json"},
                    "shock_alignment": {"overlap_rows": 5, "shock_corr": 0.42, "same_sign_share": 0.72},
                    "quarter_concentration": {"top5_abs_gap_share": 0.64, "dominant_period_bucket": "covid_post"},
                    "gap_driver_alignment": {
                        "shock_gap_driver_correlations": {"baseline_minus_excluded_target_qoq": 0.88}
                    },
                    "component_alignment": {"strict_identifiable_total_qoq": {}},
                    "interpretation": "excluded_shock_rotates_toward_positive_direct_count_channels",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_shock_composition_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_shock_composition_summary.json"},
                    "top_gap_quarters": [{"quarter": "2020Q1", "period_bucket": "covid_post"}],
                    "period_bucket_profiles": [{"period_bucket": "covid_post", "abs_gap_share": 0.61}],
                    "trim_diagnostics": {
                        "drop_top5_gap_quarters": {"shock_corr": 0.57, "same_sign_share": 0.79, "interpretation": "excluded_shock_moderately_aligned_but_distinct"},
                        "drop_covid_post": {"shock_corr": 0.66, "same_sign_share": 0.82, "interpretation": "excluded_shock_close_to_baseline"},
                    },
                    "interpretation": "rotation_is_mostly_covid_post_specific",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_quarter_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_quarter_audit_summary.json"},
                    "top_gap_quarters": [{"quarter": "2020Q3", "dominant_leg": "mixed", "contribution_pattern": "offsetting"}],
                    "dominant_leg_summary": [{"dominant_leg": "mixed", "abs_gap_share": 0.58}],
                    "contribution_pattern_summary": [{"contribution_pattern": "offsetting", "abs_gap_share": 0.62}],
                    "interpretation": "top_gap_quarters_are_mixed_or_offsetting_toc_row_bundles",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_quarter_direction_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_quarter_direction_summary.json"},
                    "top_gap_quarters": [{"quarter": "2020Q3", "gap_alignment_to_bundle": "opposed", "directional_driver": "toc_driven_gap_direction"}],
                    "gap_bundle_alignment_summary": [{"gap_alignment_to_bundle": "opposed", "abs_gap_share": 0.63}],
                    "directional_driver_summary": [{"directional_driver": "toc_driven_gap_direction", "abs_gap_share": 0.55}],
                    "interpretation": "top_gap_gap_direction_often_opposes_bundle_sign",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_inversion_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_inversion_summary.json"},
                    "top_gap_quarters": [{"quarter": "2020Q3", "directional_driver": "toc_driven_gap_direction", "excluded_other_component_qoq": -217.3, "strict_identifiable_total_qoq": 89.0}],
                    "directional_driver_context_summary": [{"directional_driver": "both_legs_oppose_gap", "abs_gap_share": 0.57, "weighted_mean_excluded_other_component_qoq": 177.9, "weighted_mean_strict_identifiable_total_qoq": 352.1}],
                    "residual_strict_pattern_summary": [{"residual_strict_pattern": "positive_residual_positive_strict", "abs_gap_share": 0.47}],
                    "interpretation": "both_leg_inversion_quarters_still_tend_to_show_positive_residual_and_positive_strict_support",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_anomaly_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_anomaly_summary.json"},
                    "anomaly_quarter": {"quarter": "2009Q4", "excluded_other_component_qoq": 73.6, "strict_identifiable_total_qoq": -68.4},
                    "peer_quarters": [{"quarter": "2020Q1"}, {"quarter": "2021Q1"}],
                    "peer_pattern_summary": [{"residual_strict_pattern": "positive_residual_positive_strict", "abs_gap_share": 0.82}],
                    "weighted_peer_means": {"strict_identifiable_total_qoq": 352.1},
                    "anomaly_vs_peer_deltas": {"strict_identifiable_total_qoq": -420.5, "strict_loan_source_qoq": -359.7},
                    "ranked_anomaly_component_deltas": [
                        {"metric": "strict_identifiable_total_qoq", "anomaly_minus_peer_delta": -420.5, "abs_delta": 420.5}
                    ],
                    "interpretation": "anomaly_flips_strict_total_negative_while_peer_bucket_stays_positive",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_anomaly_component_split_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_anomaly_component_split_summary.json"},
                    "anomaly_quarter": {"quarter": "2009Q4"},
                    "peer_quarters": [{"quarter": "2020Q1"}, {"quarter": "2021Q1"}],
                    "peer_bucket_weight": 14.3,
                    "loan_subcomponent_deltas": [{"metric": "strict_loan_di_loans_nec_qoq", "label": "DI loans n.e.c.", "anomaly_minus_peer_delta": -352.5}],
                    "securities_subcomponent_deltas": [{"metric": "strict_non_treasury_corporate_foreign_bonds_qoq", "label": "Corporate and foreign bonds", "anomaly_minus_peer_delta": -71.3}],
                    "funding_subcomponent_deltas": [{"metric": "strict_funding_fedfunds_repo_qoq", "label": "Fed funds / repo funding", "anomaly_minus_peer_delta": -141.2}],
                    "liquidity_external_deltas": [{"metric": "reserves_qoq", "label": "Reserves", "anomaly_minus_peer_delta": -469.1}],
                    "ranked_component_deltas": [{"metric": "reserves_qoq", "anomaly_minus_peer_delta": -469.1, "abs_delta": 469.1}],
                    "interpretation": "anomaly_is_di_loans_nec_contraction_with_weaker_liquidity_and_external_support",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_anomaly_di_loans_split_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_anomaly_di_loans_split_summary.json"},
                    "anomaly_quarter": {"quarter": "2009Q4"},
                    "peer_quarters": [{"quarter": "2020Q1"}, {"quarter": "2021Q1"}],
                    "peer_bucket_weight": 14.3,
                    "di_loans_nec_component_deltas": [{"metric": "strict_di_loans_nec_domestic_financial_qoq", "label": "Domestic financial", "anomaly_minus_peer_delta": -280.4}],
                    "dominant_borrower_component": {"metric": "strict_di_loans_nec_domestic_financial_qoq", "label": "Domestic financial", "anomaly_minus_peer_delta": -280.4},
                    "borrower_gap_row": {"metric": "strict_di_loans_nec_systemwide_borrower_gap_qoq", "label": "Systemwide borrower gap", "anomaly_minus_peer_delta": 15.2},
                    "interpretation": "di_loans_nec_anomaly_is_domestic_financial_shortfall",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_top_gap_anomaly_backdrop_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_top_gap_anomaly_backdrop_summary.json"},
                    "anomaly_quarter": {"quarter": "2009Q4"},
                    "peer_quarters": [{"quarter": "2020Q1"}, {"quarter": "2021Q1"}],
                    "peer_bucket_weight": 14.3,
                    "backdrop_rows": [],
                    "corporate_credit_row": {"metric": "strict_di_loans_nec_nonfinancial_corporate_qoq", "anomaly_minus_peer_delta": -345.9},
                    "loan_source_row": {"metric": "strict_loan_source_qoq", "anomaly_minus_peer_delta": -359.7},
                    "reserves_row": {"metric": "reserves_qoq", "anomaly_minus_peer_delta": -469.1},
                    "foreign_nonts_row": {"metric": "foreign_nonts_qoq", "anomaly_minus_peer_delta": -331.4},
                    "tga_row": {"metric": "tga_qoq", "anomaly_minus_peer_delta": 263.4},
                    "residual_row": {"metric": "other_component_no_toc_no_row_bank_only_qoq", "anomaly_minus_peer_delta": -127.4},
                    "liquidity_external_abs_to_corporate_abs_ratio": 2.31,
                    "interpretation": "anomaly_combines_corporate_credit_shortfall_with_even_larger_liquidity_external_shortfall",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("big_picture_synthesis_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "big_picture_synthesis_summary.json"},
                    "h0_snapshot": {
                        "toc_row_excluded_residual_beta": -5.5,
                        "toc_row_excluded_strict_identifiable_total_beta": 10.8,
                    },
                    "quarter_composition": {"dominant_period_bucket": "covid_post"},
                    "supporting_case": {"anomaly_quarter": "2009Q4"},
                    "classification": {"independent_lane_status": "not_validated"},
                    "interpretation": "treatment_side_problem_dominates_residual_but_independent_lane_still_not_validated",
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("treatment_object_comparison_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "treatment_object_comparison_summary.json"},
                    "candidate_objects": [{"candidate": "baseline_full_tdc"}, {"candidate": "toc_row_excluded_core"}],
                    "recommendation": {
                        "recommended_next_branch": "split_core_plus_support_bundle",
                        "headline_decision_now": "keep current headline provisional and do not promote the TOC_ROW_excluded object",
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("split_treatment_architecture_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "split_treatment_architecture_summary.json"},
                    "series_definitions": {
                        "baseline_treatment": "tdc_bank_only_qoq",
                        "core_deposit_proximate_treatment": "tdc_core_deposit_proximate_bank_only_qoq",
                        "support_bundle_treatment": "tdc_toc_row_support_bundle_qoq",
                    },
                    "quarterly_alignment": {"status": "available"},
                    "architecture_recommendation": {
                        "recommended_next_branch": "split_core_plus_support_bundle",
                    },
                    "key_horizons": {"h0": {"support_bundle_beta": 67.2}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("core_treatment_promotion_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "core_treatment_promotion_summary.json"},
                    "series_alias_check": {"status": "available", "max_abs_gap_beta": 0.0},
                    "shock_quality": {
                        "baseline_vs_core_overlap": {"status": "available", "shock_corr": 0.42, "same_sign_share": 0.72}
                    },
                    "key_horizons": {"h0": {"core_residual_response": {"beta": -5.5}}},
                    "strict_validation_check": {
                        "status": "available",
                        "h0_core_residual_beta": -5.5,
                        "h0_strict_identifiable_total_beta": 10.8,
                    },
                    "promotion_recommendation": {"status": "keep_interpretive_only"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_redesign_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_redesign_summary.json"},
                    "current_strict_problem_definition": {
                        "h0_core_residual_beta": -5.5,
                        "h0_toc_row_excluded_strict_identifiable_total_beta": 10.8,
                    },
                    "failure_modes": {
                        "scope_mismatch_not_primary": {"h0_remaining_share_of_baseline_strict_gap": 0.92},
                        "loan_bucket_shape": {"h0_dominant_loan_component": "strict_loan_consumer_credit_qoq"},
                        "funding_offset_instability": {"h0_funding_offset_share_of_identifiable_total_beta": 0.78},
                    },
                    "recommended_build_order": [{"step": "redesign_strict_loan_core_before_adding_more_channels"}],
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_loan_core_redesign_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_loan_core_redesign_summary.json"},
                    "candidate_definitions": {
                        "current_broad_loan_source": "strict_loan_source_qoq",
                        "redesigned_direct_min_core": "strict_loan_core_min_qoq",
                        "private_borrower_augmented_core": "strict_loan_core_plus_private_borrower_qoq",
                        "noncore_system_diagnostic": "strict_loan_noncore_system_qoq",
                    },
                    "published_roles": {
                        "headline_direct_core": {"series": "strict_loan_core_min_qoq"},
                        "standard_secondary_comparison": {"series": "strict_loan_core_plus_private_borrower_qoq"},
                        "di_bucket_diagnostic": {"series": "strict_loan_di_loans_nec_qoq"},
                    },
                    "recommendation": {
                        "status": "promote_direct_core_role_design",
                        "release_headline_candidate": "strict_loan_core_min_qoq",
                        "standard_secondary_candidate": "strict_loan_core_plus_private_borrower_qoq",
                        "diagnostic_di_bucket": "strict_loan_di_loans_nec_qoq",
                    },
                    "key_horizons": {
                        "h0": {
                            "core_deposit_proximate": {
                                "core_residual_response": {"beta": -5.5},
                                "redesigned_direct_min_core_response": {"beta": 2.7},
                            }
                        }
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_di_bucket_role_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_di_bucket_role_summary.json"},
                    "release_taxonomy": {
                        "headline_direct_core": {"series": "strict_loan_core_min_qoq"},
                        "standard_secondary_comparison": {"series": "strict_loan_core_plus_private_borrower_qoq"},
                        "di_bucket_diagnostic": {"series": "strict_loan_di_loans_nec_qoq"},
                    },
                    "recommendation": {"status": "keep_di_bucket_diagnostic_only"},
                    "key_horizons": {"h0": {"dominant_borrower_component": "strict_di_loans_nec_nonfinancial_corporate_qoq"}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_di_bucket_bridge_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_di_bucket_bridge_summary.json"},
                    "bridge_definitions": {"di_asset": "strict_loan_di_loans_nec_qoq"},
                    "recommendation": {"next_branch": "build_counterpart_alignment_surface"},
                    "key_horizons": {
                        "h0": {
                            "core_deposit_proximate": {
                                "di_asset_response": {"beta": -2.4},
                                "private_borrower_bridge_response": {"beta": 1.1},
                                "noncore_system_bridge_response": {"beta": 0.5},
                                "bridge_residual_beta": -4.0,
                                "interpretation": "cross_scope_bridge_residual_large",
                            }
                        }
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_private_borrower_bridge_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_private_borrower_bridge_summary.json"},
                    "bridge_definitions": {"private_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq"},
                    "recommendation": {"next_branch": "build_nonfinancial_corporate_bridge_surface"},
                    "key_horizons": {
                        "h0": {
                            "core_deposit_proximate": {
                                "private_bridge_response": {"beta": 20.8},
                                "households_nonprofits_response": {"beta": 1.3},
                                "nonfinancial_corporate_response": {"beta": 20.8},
                                "nonfinancial_noncorporate_response": {"beta": -1.9},
                                "dominant_private_component": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                            }
                        }
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_nonfinancial_corporate_bridge_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_nonfinancial_corporate_bridge_summary.json"},
                    "bridge_definitions": {
                        "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
                    },
                    "recommendation": {
                        "next_branch": "assess_household_and_nonfinancial_noncorporate_offset_residual",
                    },
                    "key_horizons": {
                        "h0": {
                            "core_deposit_proximate": {
                                "private_bridge_response": {"beta": 20.8},
                                "nonfinancial_corporate_response": {"beta": 20.8},
                                "households_nonprofits_response": {"beta": 1.3},
                                "nonfinancial_noncorporate_response": {"beta": -1.9},
                            }
                        }
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_private_offset_residual_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_private_offset_residual_summary.json"},
                    "bridge_definitions": {
                        "private_offset_total": "strict_di_loans_nec_private_offset_residual_qoq",
                    },
                    "recommendation": {
                        "next_branch": "assess_corporate_bridge_secondary_comparison_role",
                    },
                    "key_horizons": {
                        "h0": {
                            "core_deposit_proximate": {
                                "private_offset_total_response": {"beta": -0.57},
                                "private_bridge_response": {"beta": 20.8},
                                "households_nonprofits_response": {"beta": 1.3},
                                "nonfinancial_noncorporate_response": {"beta": -1.9},
                            }
                        }
                    },
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_corporate_bridge_secondary_comparison_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {
                        "summary_artifact": "strict_corporate_bridge_secondary_comparison_summary.json"
                    },
                    "candidate_definitions": {"headline_direct_core": "strict_loan_core_min_qoq"},
                    "recommendation": {
                        "standard_secondary_candidate": "strict_loan_core_plus_private_borrower_qoq"
                    },
                    "key_horizons": {},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_component_framework_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_component_framework_summary.json"},
                    "frozen_roles": {
                        "headline_direct_core": "strict_loan_core_min_qoq",
                        "standard_secondary_comparison": "strict_loan_core_plus_nonfinancial_corporate_qoq",
                    },
                    "h0_snapshot": {"core_residual_beta": -5.5},
                    "classification": {"framework_state": "external_critique_incorporated_and_frozen"},
                    "recommendation": {"next_branch": "run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_release_framing_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_release_framing_summary.json"},
                    "release_position": {
                        "full_tdc_release_role": "broad_treasury_attributed_object_only",
                        "strict_object_rule": "exclude_toc_and_row_under_current_evidence",
                    },
                    "evidence_tiers": {"independent_evidence": ["strict_loan_core_min_qoq"]},
                    "classification": {"release_state": "strict_release_framing_finalized"},
                    "h0_snapshot": {"core_residual_beta": -5.5},
                    "recommendation": {"status": "strict_release_framing_finalized"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_direct_core_component_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_direct_core_component_summary.json"},
                    "candidate_definitions": {"headline_direct_core": "strict_loan_core_min_qoq"},
                    "key_horizons": {"h0": {"core_deposit_proximate": {}}},
                    "classification": {"h0_dominant_component": "strict_loan_consumer_credit_qoq"},
                    "recommendation": {"status": "keep_bundled_direct_core"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_direct_core_horizon_stability_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_direct_core_horizon_stability_summary.json"},
                    "horizon_winners": {
                        "h0": "strict_loan_mortgages_qoq",
                        "h4": "strict_loan_core_min_qoq",
                        "h8": "strict_loan_core_min_qoq",
                    },
                    "classification": {
                        "impact_winner": "strict_loan_mortgages_qoq",
                        "medium_horizon_winner": "strict_loan_core_min_qoq",
                        "long_horizon_winner": "strict_loan_core_min_qoq",
                        "recommendation_status": "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate",
                    },
                    "recommendation": {"status": "keep_bundled_core_for_multihorizon_use_flag_mortgages_as_impact_candidate"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_additional_creator_candidate_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_additional_creator_candidate_summary.json"},
                    "candidate_groups": {
                        "validation_proxies": ["closed_end_residential_loans_qoq"],
                        "extension_candidates": ["commercial_industrial_loans_qoq"],
                    },
                    "key_horizons": {"h0": {"core_deposit_proximate": {}}},
                    "classification": {"h0_best_extension_candidate": "commercial_industrial_loans_qoq"},
                    "recommendation": {"status": "no_additional_extension_candidate_supported"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_di_loans_nec_measurement_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_di_loans_nec_measurement_audit_summary.json"},
                    "candidate_groups": {
                        "same_scope_transaction_subcomponents": [],
                        "cross_scope_transaction_bridges": ["strict_di_loans_nec_nonfinancial_corporate_qoq"],
                        "same_scope_proxies": ["loans_to_nondepository_financial_institutions_qoq"],
                    },
                    "key_horizons": {"h0": {"core_deposit_proximate": {}}},
                    "classification": {"promotion_gate": "no_promotable_same_scope_transaction_subcomponent_supported"},
                    "recommendation": {"status": "no_promotable_same_scope_transaction_subcomponent_supported"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("strict_results_closeout_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "strict_results_closeout_summary.json"},
                    "release_position": {"headline_direct_benchmark": "strict_loan_core_min_qoq"},
                    "settled_findings": ["stub"],
                    "evidence_tiers": {"independent_evidence": ["strict_loan_core_min_qoq"]},
                    "unresolved_questions": ["stub"],
                    "classification": {"branch_state": "strict_empirical_expansion_effectively_complete"},
                    "recommendation": {"status": "move_to_writeup_and_results_packaging"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("tdcest_ladder_integration_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "tdcest_ladder_integration_summary.json"},
                    "classification": {"decision": "selective_integration_not_wholesale_pivot"},
                    "series_roles": [],
                    "recommendation": {"status": "import_selected_tdcest_ladder_rows_only"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("tdcest_broad_object_comparison_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "tdcest_broad_object_comparison_summary.json"},
                    "latest_common_broad_comparison": {"quarter": "2025Q4"},
                    "supplemental_surfaces": {
                        "historical_bank_receipt_overlay": {"latest_nonzero_quarter": "2024Q4"},
                        "row_mrv_nondefault_pilot": {"latest_nonzero_quarter": "2025Q3"},
                    },
                    "classification": {"role": "broad_object_comparison_only"},
                    "recommendation": {"status": "use_as_broad_object_comparison_layer_only"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("tdcest_broad_treatment_sensitivity_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "tdcest_broad_treatment_sensitivity_summary.json"},
                    "classification": {"headline_direction_status": "unchanged_across_corrected_broad_variants"},
                    "key_horizons": {"h0": {"baseline": {}, "variants": {}}},
                    "recommendation": {"status": "use_as_broad_object_sensitivity_only"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_row_incidence_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_row_incidence_audit_summary.json"},
                    "leg_definitions": {"toc_leg": "tdc_treasury_operating_cash_qoq"},
                    "quarterly_alignment": {"toc_tga_corr": 0.95},
                    "key_horizons": {"h0": {"toc_leg": {}, "row_leg": {}, "bundle": {}}},
                    "classification": {"bundle_role": "measured_support_bundle_with_unresolved_strict_deposit_incidence"},
                    "recommendation": {"next_branch": "run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_row_liability_incidence_raw_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_row_liability_incidence_raw_summary.json"},
                    "leg_definitions": {"toc_signed_leg": "tdc_toc_signed_qoq"},
                    "quarterly_alignment": {"toc_leg": {}, "row_leg": {}},
                    "key_horizons": {"h0": {"toc_leg": {}, "row_leg": {}}},
                    "classification": {"decision_gate": "full_reincorporation_not_supported"},
                    "recommendation": {"next_branch": "decide_whether_any_validated_toc_share_belongs_in_strict_object"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("toc_validated_share_candidate_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "toc_validated_share_candidate_summary.json"},
                    "candidate_definitions": {"headline_direct_core": "strict_loan_core_min_qoq"},
                    "quarterly_gate": {"status": "fails"},
                    "key_horizons": {"h0": {"best_candidate": {}}},
                    "classification": {"decision": "keep_toc_outside_strict_object_under_current_evidence"},
                    "recommendation": {"next_branch": "finalize_release_framing_that_toc_and_row_stay_outside_strict_object"},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("tdc_treatment_audit_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"summary_artifact": "tdc_treatment_audit_summary.json"},
                    "component_definitions": [],
                    "variant_definitions": [],
                    "baseline_target": "tdc_bank_only_qoq",
                    "construction_alignment": {"status": "available", "rows": {}},
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
        elif rel.endswith("call_report_deposit_components_summary.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "row_count": 1,
                    "qa": {"quarterly_aggregation_confirmed": True},
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
                    "analysis_tree": {"status": "clean", "tracked_change_count": 0, "untracked_change_count": 0},
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
                    "analysis_tree_check": {"status": "passed"},
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
                    "counterpart_channel_context": {},
                    "scope_alignment_context": {},
                    "strict_gap_scope_check_context": {},
                    "broad_scope_system_context": {},
                    "tdc_treatment_audit_context": {},
                    "treasury_operating_cash_audit_context": {},
                    "rest_of_world_treasury_audit_context": {},
                    "toc_row_path_split_context": {},
                    "toc_row_excluded_interpretation_context": {},
                    "strict_missing_channel_context": {},
                    "strict_sign_mismatch_audit_context": {},
                    "strict_shock_composition_context": {},
                    "strict_top_gap_quarter_audit_context": {},
                    "strict_top_gap_quarter_direction_context": {},
                    "strict_top_gap_inversion_context": {},
                    "strict_top_gap_anomaly_context": {},
                    "strict_top_gap_anomaly_component_split_context": {},
                    "strict_top_gap_anomaly_di_loans_split_context": {},
                    "strict_top_gap_anomaly_backdrop_context": {},
                    "big_picture_synthesis_context": {},
                    "treatment_object_comparison_context": {},
                    "split_treatment_architecture_context": {},
                    "core_treatment_promotion_context": {},
                    "strict_redesign_context": {},
                    "strict_loan_core_redesign_context": {},
                    "strict_di_bucket_role_context": {},
                    "strict_di_bucket_bridge_context": {},
                    "strict_private_borrower_bridge_context": {},
                    "strict_nonfinancial_corporate_bridge_context": {},
                        "strict_private_offset_residual_context": {},
                        "strict_corporate_bridge_secondary_comparison_context": {},
                        "strict_component_framework_context": {},
                        "strict_release_framing_context": {},
                        "strict_direct_core_component_context": {},
                    "strict_direct_core_horizon_stability_context": {},
                    "strict_additional_creator_candidate_context": {},
                    "tdcest_ladder_integration_context": {},
                    "tdcest_broad_object_comparison_context": {},
                    "tdcest_broad_treatment_sensitivity_context": {},
                    "toc_row_incidence_audit_context": {},
                    "published_regime_contexts": [],
                    "readiness_reasons": ["stub"],
                    "readiness_warnings": [],
                },
            )
        elif rel.endswith("deposit_component_scorecard.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
                    "component_outcomes_present": ["checkable_deposits_bank_qoq"],
                    "creator_channel_outcomes_present": ["commercial_industrial_loans_qoq"],
                    "horizons": {"h0": {}},
                    "takeaways": ["stub"],
                },
            )
        elif rel.endswith("counterpart_channel_scorecard.json"):
            _write_json(
                source_root / rel,
                {
                    "status": "available",
                    "headline_question": "stub",
                    "estimation_path": {"primary_decomposition_mode": "exact_identity_baseline"},
                    "legacy_private_credit_proxy_role": "coarse_legacy_creator_proxy",
                    "creator_channel_outcomes_present": ["commercial_industrial_loans_qoq"],
                    "funding_accommodation_outcomes_present": ["commercial_bank_borrowings_qoq"],
                    "horizons": {"h0": {}},
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
                    "counterpart_channel_context": {},
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


def test_pipeline_closeout_reads_existing_artifacts(tmp_path: Path, capsys, monkeypatch) -> None:
    monkeypatch.setattr("tdcpass.cli.validate_headline_treatment_fingerprint", lambda *args, **kwargs: [])
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


def test_pipeline_run_supports_offline_raw_fixture(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "tdcpass.analysis.treatment_fingerprint._git_working_tree_payload",
        lambda _repo_root: {"status": "clean", "tracked_change_count": 0, "untracked_change_count": 0},
    )
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "offline_raw_fixture"
    dest_root = tmp_path / "offline-dest"

    exit_code = main(["pipeline", "run", "--root", str(dest_root), "--raw-fixture-root", str(fixture_root), "--reuse-mode", "rebuild"])

    assert exit_code == 0
    assert (dest_root / "data" / "derived" / "quarterly_panel.csv").exists()
    assert (dest_root / "output" / "models" / "sample_construction_summary.json").exists()
    assert (dest_root / "output" / "models" / "lp_irf_identity_baseline.csv").exists()
    assert (dest_root / "output" / "models" / "identity_measurement_ladder.csv").exists()
    assert (dest_root / "output" / "models" / "scope_alignment_summary.json").exists()
    assert (dest_root / "output" / "models" / "broad_scope_system_summary.json").exists()
    assert (dest_root / "output" / "models" / "treasury_operating_cash_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "rest_of_world_treasury_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "toc_row_bundle_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "toc_row_path_split_summary.json").exists()
    assert (dest_root / "output" / "models" / "toc_row_excluded_interpretation_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_missing_channel_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_sign_mismatch_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_shock_composition_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_top_gap_quarter_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_top_gap_quarter_direction_summary.json").exists()
    assert (dest_root / "output" / "models" / "tdc_treatment_audit_summary.json").exists()
    assert (dest_root / "output" / "models" / "split_treatment_architecture_summary.json").exists()
    assert (dest_root / "output" / "models" / "core_treatment_promotion_summary.json").exists()
    assert (dest_root / "output" / "models" / "strict_redesign_summary.json").exists()
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
    scope_alignment = json.loads((dest_root / "output" / "models" / "scope_alignment_summary.json").read_text(encoding="utf-8"))

    assert sample_summary["treatment_freeze_status"] == "frozen"
    assert shock_diagnostics["treatment_freeze_status"] == "frozen"
    assert direct_identification["treatment_freeze_status"] == "frozen"
    assert readiness["treatment_freeze_status"] == "frozen"
    assert pass_through["treatment_freeze_status"] == "frozen"
    assert scope_alignment["status"] in {"available", "not_available"}
    assert "strict_sign_mismatch_audit_context" in pass_through
    assert "strict_shock_composition_context" in pass_through
    assert "strict_top_gap_quarter_audit_context" in pass_through
    assert "strict_top_gap_quarter_direction_context" in pass_through
    assert "strict_top_gap_inversion_context" in pass_through
    assert "strict_top_gap_anomaly_context" in pass_through
    assert "strict_top_gap_anomaly_component_split_context" in pass_through
    assert "strict_top_gap_anomaly_di_loans_split_context" in pass_through
    assert "big_picture_synthesis_context" in pass_through
    assert "treatment_object_comparison_context" in pass_through
    assert "split_treatment_architecture_context" in pass_through
    assert "strict_redesign_context" in pass_through
