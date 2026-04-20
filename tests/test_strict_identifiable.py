from __future__ import annotations

import pandas as pd

from tdcpass.analysis.strict_identifiable import (
    build_strict_funding_offset_alignment_frame,
    build_strict_identifiable_alignment_frame,
    build_strict_identifiable_followup_summary,
    build_strict_identifiable_summary,
    slice_strict_identifiable_lp_irf,
)


def _lp_irf_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -10.0, "se": 1.0, "lower95": -12.0, "upper95": -8.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_source_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_mortgages_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_consumer_credit_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_di_loans_nec_qoq", "horizon": 0, "beta": -1.5, "se": 1.0, "lower95": -3.5, "upper95": 0.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_private_domestic_borrower_qoq", "horizon": 0, "beta": -0.7, "se": 1.0, "lower95": -2.7, "upper95": 1.3, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_noncore_system_borrower_qoq", "horizon": 0, "beta": -1.3, "se": 1.0, "lower95": -3.3, "upper95": 0.7, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_liability_total_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_households_nonprofits_qoq", "horizon": 0, "beta": -0.2, "se": 1.0, "lower95": -2.2, "upper95": 1.8, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_nonfinancial_corporate_qoq", "horizon": 0, "beta": -0.4, "se": 1.0, "lower95": -2.4, "upper95": 1.6, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_nonfinancial_noncorporate_qoq", "horizon": 0, "beta": -0.1, "se": 1.0, "lower95": -2.1, "upper95": 1.9, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_state_local_qoq", "horizon": 0, "beta": -0.3, "se": 1.0, "lower95": -2.3, "upper95": 1.7, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_domestic_financial_qoq", "horizon": 0, "beta": -0.2, "se": 1.0, "lower95": -2.2, "upper95": 1.8, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_rest_of_world_qoq", "horizon": 0, "beta": -0.1, "se": 1.0, "lower95": -2.1, "upper95": 1.9, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_borrower_total_qoq", "horizon": 0, "beta": -1.3, "se": 1.0, "lower95": -3.3, "upper95": 0.7, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_borrower_gap_qoq", "horizon": 0, "beta": -0.7, "se": 1.0, "lower95": -2.7, "upper95": 1.3, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_other_advances_qoq", "horizon": 0, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_core_plus_private_borrower_qoq", "horizon": 0, "beta": -2.7, "se": 1.0, "lower95": -4.7, "upper95": -0.7, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_loan_noncore_system_qoq", "horizon": 0, "beta": -1.8, "se": 1.0, "lower95": -3.8, "upper95": 0.2, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_non_treasury_agency_gse_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_non_treasury_municipal_qoq", "horizon": 0, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_non_treasury_corporate_foreign_bonds_qoq", "horizon": 0, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_funding_fedfunds_repo_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_funding_debt_securities_qoq", "horizon": 0, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_funding_fhlb_advances_qoq", "horizon": 0, "beta": 0.5, "se": 1.0, "lower95": -1.5, "upper95": 2.5, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 0, "beta": 2.0, "se": 1.0, "lower95": 0.0, "upper95": 4.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 0, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "strict_gap_after_funding_qoq", "horizon": 0, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_loan_source_qoq", "horizon": 4, "beta": -5.0, "se": 1.0, "lower95": -7.0, "upper95": -3.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_loan_di_loans_nec_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_loan_core_min_qoq", "horizon": 4, "beta": -3.0, "se": 1.0, "lower95": -5.0, "upper95": -1.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_liability_total_qoq", "horizon": 4, "beta": -2.5, "se": 1.0, "lower95": -4.5, "upper95": -0.5, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_nonfinancial_corporate_qoq", "horizon": 4, "beta": -1.1, "se": 1.0, "lower95": -3.1, "upper95": 0.9, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_borrower_total_qoq", "horizon": 4, "beta": -1.5, "se": 1.0, "lower95": -3.5, "upper95": 0.5, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_di_loans_nec_systemwide_borrower_gap_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_non_treasury_securities_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_total_qoq", "horizon": 4, "beta": -7.0, "se": 1.0, "lower95": -9.0, "upper95": -5.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_gap_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_funding_offset_total_qoq", "horizon": 4, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_identifiable_net_after_funding_qoq", "horizon": 4, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "strict_gap_after_funding_qoq", "horizon": 4, "beta": 0.0, "se": 1.0, "lower95": -2.0, "upper95": 2.0, "n": 19, "spec_name": "baseline"},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -1.0, "upper95": 3.0, "n": 20, "spec_name": "baseline"},
        ]
    )


def _identity_baseline_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 20},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.0, "se": 1.0, "lower95": 3.0, "upper95": 7.0, "n": 20},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -7.0, "se": 1.0, "lower95": -9.0, "upper95": -5.0, "n": 20},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 18.0, "se": 1.0, "lower95": 16.0, "upper95": 20.0, "n": 19},
            {"outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 7.0, "se": 1.0, "lower95": 5.0, "upper95": 9.0, "n": 19},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -11.0, "se": 1.0, "lower95": -13.0, "upper95": -9.0, "n": 19},
        ]
    )


def _identity_measurement_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "tdc_domestic_bank_only_qoq", "horizon": 0, "beta": 10.0, "se": 1.0, "lower95": 8.0, "upper95": 12.0, "n": 20},
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.5, "se": 1.0, "lower95": 3.5, "upper95": 7.5, "n": 20},
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "other_component_qoq", "horizon": 0, "beta": -4.5, "se": 1.0, "lower95": -6.5, "upper95": -2.5, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "tdc_us_chartered_bank_only_qoq", "horizon": 0, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 5.2, "se": 1.0, "lower95": 3.2, "upper95": 7.2, "n": 20},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "other_component_qoq", "horizon": 0, "beta": -2.8, "se": 1.0, "lower95": -4.8, "upper95": -0.8, "n": 20},
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "tdc_domestic_bank_only_qoq", "horizon": 4, "beta": 14.0, "se": 1.0, "lower95": 12.0, "upper95": 16.0, "n": 19},
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 19},
            {"treatment_variant": "domestic_bank_only", "target": "tdc_domestic_bank_only_qoq", "outcome": "other_component_qoq", "horizon": 4, "beta": -6.0, "se": 1.0, "lower95": -8.0, "upper95": -4.0, "n": 19},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "tdc_us_chartered_bank_only_qoq", "horizon": 4, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 19},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "total_deposits_bank_qoq", "horizon": 4, "beta": 7.5, "se": 1.0, "lower95": 5.5, "upper95": 9.5, "n": 19},
            {"treatment_variant": "us_chartered_bank_only", "target": "tdc_us_chartered_bank_only_qoq", "outcome": "other_component_qoq", "horizon": 4, "beta": -4.5, "se": 1.0, "lower95": -6.5, "upper95": -2.5, "n": 19},
        ]
    )


def test_slice_strict_identifiable_lp_irf_filters_and_orders_outcomes() -> None:
    sliced = slice_strict_identifiable_lp_irf(_lp_irf_fixture())

    assert "bank_credit_private_qoq" not in sliced["outcome"].tolist()
    assert sliced["outcome"].tolist()[:3] == [
        "other_component_qoq",
        "other_component_qoq",
        "strict_loan_source_qoq",
    ]


def test_build_strict_identifiable_alignment_frame_reports_gap_share() -> None:
    alignment = build_strict_identifiable_alignment_frame(_lp_irf_fixture(), horizons=(0, 4))

    h0 = alignment.loc[alignment["horizon"] == 0].iloc[0]
    h4 = alignment.loc[alignment["horizon"] == 4].iloc[0]

    assert abs(h0["arithmetic_residual_minus_total_beta"] + 4.0) < 1e-12
    assert abs(h0["strict_loan_core_min_beta"] + 2.0) < 1e-12
    assert abs(h0["strict_gap_share_of_residual"] - 0.4) < 1e-12
    assert h0["interpretation"] == "partial_identifiable_coverage"
    assert abs(h4["strict_gap_share_of_residual"] - 0.125) < 1e-12
    assert h4["interpretation"] == "partial_identifiable_coverage"


def test_build_strict_identifiable_summary_marks_available_when_total_present() -> None:
    summary = build_strict_identifiable_summary(
        lp_irf=_lp_irf_fixture(),
        strict_source_kind="z1_transactions_via_fred",
        horizons=(0, 4),
    )

    assert summary["status"] == "available"
    assert summary["source_kind"] == "z1_transactions_via_fred"
    assert summary["key_horizons"]["h0"]["strict_headline_direct_core"]["beta"] == -2.0
    assert "strict_loan_source_qoq" in summary["component_outcomes_present"]
    assert "strict_loan_core_min_qoq" in summary["component_outcomes_present"]
    assert "strict_non_treasury_securities_qoq" in summary["component_outcomes_present"]
    assert summary["key_horizons"]["h0"]["interpretation"] == "partial_identifiable_coverage"
    assert summary["key_horizons"]["h4"]["interpretation"] == "partial_identifiable_coverage"


def test_build_strict_funding_offset_alignment_frame_reports_material_offsets() -> None:
    alignment = build_strict_funding_offset_alignment_frame(_lp_irf_fixture(), horizons=(0, 4))

    h0 = alignment.loc[alignment["horizon"] == 0].iloc[0]
    h4 = alignment.loc[alignment["horizon"] == 4].iloc[0]

    assert abs(h0["strict_funding_offset_share_of_identifiable_total_beta"] - (-1.0 / 3.0)) < 1e-12
    assert h0["interpretation"] == "funding_offsets_material_relative_to_identifiable_total"
    assert abs(h4["strict_funding_offset_share_of_identifiable_total_beta"] - (-1.0 / 7.0)) < 1e-12


def test_build_strict_identifiable_summary_notes_when_gap_stays_large() -> None:
    summary = build_strict_identifiable_summary(
        lp_irf=pd.DataFrame(
            [
                {"outcome": "other_component_qoq", "horizon": 0, "beta": -1.0, "se": 0.1, "lower95": -1.2, "upper95": -0.8, "n": 20, "spec_name": "baseline"},
                {"outcome": "strict_loan_source_qoq", "horizon": 0, "beta": -0.1, "se": 0.1, "lower95": -0.3, "upper95": 0.1, "n": 20, "spec_name": "baseline"},
                {"outcome": "strict_identifiable_total_qoq", "horizon": 0, "beta": -0.1, "se": 0.1, "lower95": -0.3, "upper95": 0.1, "n": 20, "spec_name": "baseline"},
                {"outcome": "strict_identifiable_gap_qoq", "horizon": 0, "beta": -0.9, "se": 0.1, "lower95": -1.1, "upper95": -0.7, "n": 20, "spec_name": "baseline"},
            ]
        ),
        strict_source_kind="z1_transactions_via_fred",
        horizons=(0,),
    )

    assert any("large unidentified remainder" in takeaway for takeaway in summary["takeaways"])


def test_build_strict_identifiable_followup_summary_reports_measurement_and_component_diagnostics() -> None:
    summary = build_strict_identifiable_followup_summary(
        identity_baseline_lp_irf=_identity_baseline_fixture(),
        identity_measurement_ladder=_identity_measurement_fixture(),
        lp_irf=_lp_irf_fixture(),
        strict_source_kind="z1_transactions_via_fred",
        horizons=(0, 4),
    )

    assert summary["status"] == "available"
    assert summary["measurement_variant_comparison"]["comparison_variants"] == [
        "domestic_bank_only",
        "us_chartered_bank_only",
    ]
    assert summary["recommended_measurement_comparison"]["preferred_variant"] == "us_chartered_bank_only"
    assert summary["recommended_measurement_comparison"]["secondary_variant"] == "domestic_bank_only"
    h0_measurement = summary["measurement_variant_comparison"]["key_horizons"]["h0"]["measurement_variants"]["domestic_bank_only"]
    assert h0_measurement["target"] == "tdc_domestic_bank_only_qoq"
    assert abs(h0_measurement["differences_vs_baseline_beta"]["target_response"] + 2.0) < 1e-12
    assert abs(h0_measurement["differences_vs_baseline_beta"]["other_component_qoq"] - 2.5) < 1e-12
    h0_us_chartered = summary["measurement_variant_comparison"]["key_horizons"]["h0"]["measurement_variants"]["us_chartered_bank_only"]
    assert h0_us_chartered["target"] == "tdc_us_chartered_bank_only_qoq"
    assert abs(h0_us_chartered["differences_vs_baseline_beta"]["other_component_qoq"] - 4.2) < 1e-12
    h0_scope_gap = summary["scope_check_gap_assessment"]["key_horizons"]["h0"]["variant_gap_assessments"]
    assert abs(h0_scope_gap["domestic_bank_only"]["descriptive_gap_if_strict_total_held_fixed_beta"] + 1.5) < 1e-12
    assert abs(h0_scope_gap["domestic_bank_only"]["remaining_share_of_baseline_strict_gap"] - 0.375) < 1e-12
    assert h0_scope_gap["domestic_bank_only"]["interpretation"] == "scope_check_relief_eliminates_most_of_baseline_strict_gap"
    assert abs(h0_scope_gap["us_chartered_bank_only"]["descriptive_gap_if_strict_total_held_fixed_beta"] - 0.2) < 1e-12
    assert abs(h0_scope_gap["us_chartered_bank_only"]["relief_share_of_baseline_strict_gap"] - 0.95) < 1e-12
    h0_components = summary["strict_component_diagnostics"]["key_horizons"]["h0"]
    assert h0_components["strict_headline_direct_core"]["beta"] == -2.0
    assert h0_components["strict_loan_core_plus_private_borrower"]["beta"] == -2.7
    assert h0_components["strict_loan_noncore_system"]["beta"] == -1.8
    assert h0_components["dominant_loan_component"] == "strict_loan_di_loans_nec_qoq"
    assert abs(h0_components["strict_loan_di_loans_nec_share_of_loan_source_beta"] - 0.375) < 1e-12
    h0_borrowers = summary["di_loans_nec_borrower_diagnostics"]["key_horizons"]["h0"]
    assert h0_borrowers["dominant_borrower_component"] == "strict_di_loans_nec_nonfinancial_corporate_qoq"
    assert abs(h0_borrowers["us_chartered_share_of_systemwide_liability_beta"] - 0.75) < 1e-12
    assert abs(h0_borrowers["systemwide_borrower_total_share_of_systemwide_liability_beta"] - 0.65) < 1e-12
    assert abs(h0_borrowers["systemwide_borrower_gap_share_of_systemwide_liability_beta"] - 0.35) < 1e-12
    h0_funding = summary["funding_offset_sensitivity"]["key_horizons"]["h0"]
    assert h0_funding["dominant_funding_component"] == "strict_funding_fedfunds_repo_qoq"
    assert abs(h0_funding["strict_funding_offset_share_of_identifiable_total_beta"] - (-1.0 / 3.0)) < 1e-12
    assert any("upstream no-ROW `domestic_bank_only`" in takeaway for takeaway in summary["takeaways"])
    assert any("U.S.-chartered bank-leg-matched treatment" in takeaway for takeaway in summary["takeaways"])
    assert any("standard scope check" in takeaway for takeaway in summary["takeaways"])
    assert any("about 0.05 of the baseline strict gap remains" in takeaway for takeaway in summary["takeaways"])
