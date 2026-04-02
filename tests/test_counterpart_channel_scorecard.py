from __future__ import annotations

import pandas as pd

from tdcpass.analysis.counterpart_channel_scorecard import build_counterpart_channel_scorecard


def test_counterpart_channel_scorecard_tracks_creator_channels_and_decisiveness() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 10.0, "se": 4.0, "lower95": 2.16, "upper95": 17.84, "n": 68},
            {"outcome": "commercial_industrial_loans_qoq", "horizon": 0, "beta": 18.0, "se": 6.0, "lower95": 6.24, "upper95": 29.76, "n": 68},
            {"outcome": "commercial_industrial_loans_ex_chargeoffs_qoq", "horizon": 0, "beta": 20.0, "se": 6.0, "lower95": 8.24, "upper95": 31.76, "n": 68},
            {"outcome": "loans_to_nondepository_financial_institutions_qoq", "horizon": 0, "beta": 4.0, "se": 1.0, "lower95": 2.04, "upper95": 5.96, "n": 68},
            {"outcome": "treasury_securities_bank_qoq", "horizon": 0, "beta": 3.0, "se": 1.0, "lower95": 1.04, "upper95": 4.96, "n": 68},
            {"outcome": "interbank_transactions_bank_qoq", "horizon": 0, "beta": 2.0, "se": 0.8, "lower95": 0.43, "upper95": 3.57, "n": 68},
            {"outcome": "fedfunds_repo_liabilities_bank_qoq", "horizon": 0, "beta": 1.5, "se": 0.9, "lower95": -0.26, "upper95": 3.26, "n": 68},
            {"outcome": "commercial_bank_borrowings_qoq", "horizon": 0, "beta": 8.0, "se": 2.0, "lower95": 4.08, "upper95": 11.92, "n": 68},
            {"outcome": "fed_borrowings_depository_institutions_qoq", "horizon": 0, "beta": 5.0, "se": 2.5, "lower95": 0.10, "upper95": 9.90, "n": 68},
            {"outcome": "debt_securities_bank_liability_qoq", "horizon": 0, "beta": -2.0, "se": 0.8, "lower95": -3.57, "upper95": -0.43, "n": 68},
            {"outcome": "fhlb_advances_sallie_mae_loans_bank_qoq", "horizon": 0, "beta": 3.2, "se": 1.0, "lower95": 1.24, "upper95": 5.16, "n": 68},
            {"outcome": "holding_company_parent_funding_bank_qoq", "horizon": 0, "beta": 0.5, "se": 0.7, "lower95": -0.87, "upper95": 1.87, "n": 68},
            {"outcome": "tga_qoq", "horizon": 0, "beta": 2.5, "se": 1.0, "lower95": 0.54, "upper95": 4.46, "n": 68},
            {"outcome": "reserves_qoq", "horizon": 0, "beta": 0.5, "se": 1.0, "lower95": -1.46, "upper95": 2.46, "n": 68},
            {"outcome": "cb_nonts_qoq", "horizon": 0, "beta": 1.0, "se": 1.0, "lower95": -0.96, "upper95": 2.96, "n": 68},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": -5.0, "se": 2.0, "lower95": -8.92, "upper95": -1.08, "n": 68},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 1.5, "se": 1.0, "lower95": -0.46, "upper95": 3.46, "n": 68},
            {"outcome": "on_rrp_reallocation_qoq", "horizon": 0, "beta": 6.0, "se": 2.0, "lower95": 2.08, "upper95": 9.92, "n": 68},
            {"outcome": "household_treasury_securities_reallocation_qoq", "horizon": 0, "beta": -7.0, "se": 2.0, "lower95": -10.92, "upper95": -3.08, "n": 68},
            {"outcome": "mmf_treasury_bills_reallocation_qoq", "horizon": 0, "beta": -4.0, "se": 1.5, "lower95": -6.94, "upper95": -1.06, "n": 68},
            {"outcome": "currency_reallocation_qoq", "horizon": 0, "beta": 2.5, "se": 1.0, "lower95": 0.54, "upper95": 4.46, "n": 68},
            {"outcome": "foreign_nonts_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -2.96, "upper95": 0.96, "n": 68},
            {"outcome": "checkable_rest_of_world_bank_qoq", "horizon": 0, "beta": 2.2, "se": 0.8, "lower95": 0.63, "upper95": 3.77, "n": 68},
            {"outcome": "interbank_transactions_foreign_banks_liability_qoq", "horizon": 0, "beta": 3.5, "se": 1.0, "lower95": 1.54, "upper95": 5.46, "n": 68},
            {"outcome": "interbank_transactions_foreign_banks_asset_qoq", "horizon": 0, "beta": -2.5, "se": 0.9, "lower95": -4.26, "upper95": -0.74, "n": 68},
            {"outcome": "deposits_at_foreign_banks_asset_qoq", "horizon": 0, "beta": -0.5, "se": 0.8, "lower95": -2.07, "upper95": 1.07, "n": 68},
            {"outcome": "closed_end_residential_loans_qoq", "horizon": 0, "beta": -3.5, "se": 1.0, "lower95": -5.46, "upper95": -1.54, "n": 68},
        ]
    )
    identity_lp = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 120.0, "se": 30.0, "lower95": 61.2, "upper95": 178.8, "n": 68},
            {"outcome": "total_deposits_bank_qoq", "horizon": 0, "beta": 55.0, "se": 25.0, "lower95": 6.0, "upper95": 104.0, "n": 68},
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -65.0, "se": 30.0, "lower95": -123.8, "upper95": -6.2, "n": 68},
        ]
    )

    payload = build_counterpart_channel_scorecard(
        lp_irf=lp_irf,
        identity_lp_irf=identity_lp,
        proxy_coverage_summary={
            "major_uncovered_channel_families": ["loan_repayment_and_other_escape_channels"],
            "key_horizons": {"h0": {"coverage_label": "creator_channels_unresolved"}},
        },
    )

    assert payload["status"] == "available"
    assert payload["estimation_path"]["primary_decomposition_mode"] == "exact_identity_baseline"
    assert payload["creator_channel_outcomes_present"] == [
        "closed_end_residential_loans_qoq",
        "commercial_industrial_loans_ex_chargeoffs_qoq",
        "commercial_industrial_loans_qoq",
        "loans_to_nondepository_financial_institutions_qoq",
        "treasury_securities_bank_qoq",
    ]
    assert payload["legacy_private_credit_proxy_role"] == "coarse_legacy_creator_proxy"
    assert payload["key_horizons"]["h0"]["legacy_private_credit_proxy"]["role"] == "coarse_legacy_creator_proxy"
    assert payload["key_horizons"]["h0"]["legacy_private_credit_proxy"]["snapshot"]["beta"] == 10.0
    assert payload["key_horizons"]["h0"]["creator_lending_channels"]["commercial_industrial_loans_qoq"]["beta"] == 18.0
    assert payload["key_horizons"]["h0"]["creator_lending_channels"]["commercial_industrial_loans_ex_chargeoffs_qoq"]["beta"] == 20.0
    assert payload["key_horizons"]["h0"]["noncore_creator_lending_channels"]["loans_to_nondepository_financial_institutions_qoq"]["beta"] == 4.0
    assert payload["key_horizons"]["h0"]["creator_asset_purchase_channels"]["treasury_securities_bank_qoq"]["beta"] == 3.0
    assert payload["key_horizons"]["h0"]["funding_accommodation_channels"]["commercial_bank_borrowings_qoq"]["beta"] == 8.0
    assert payload["key_horizons"]["h0"]["funding_accommodation_channels"]["debt_securities_bank_liability_qoq"]["beta"] == -2.0
    assert payload["key_horizons"]["h0"]["funding_accommodation_cleanup_candidates"]["holding_company_parent_funding_bank_qoq"]["beta"] == 0.5
    assert payload["key_horizons"]["h0"]["deposit_retention_support_channels"]["on_rrp_reallocation_qoq"]["beta"] == 6.0
    assert payload["key_horizons"]["h0"]["deposit_retention_support_channels"]["household_treasury_securities_reallocation_qoq"]["beta"] == -7.0
    assert payload["key_horizons"]["h0"]["deposit_retention_support_channels"]["mmf_treasury_bills_reallocation_qoq"]["beta"] == -4.0
    assert payload["key_horizons"]["h0"]["deposit_retention_support_channels"]["currency_reallocation_qoq"]["beta"] == 2.5
    assert payload["key_horizons"]["h0"]["external_escape_channels"]["foreign_nonts_qoq"]["beta"] == -1.0
    assert payload["key_horizons"]["h0"]["external_escape_channels"]["checkable_rest_of_world_bank_qoq"]["beta"] == 2.2
    assert payload["key_horizons"]["h0"]["external_escape_channels"]["interbank_transactions_foreign_banks_liability_qoq"]["beta"] == 3.5
    assert payload["key_horizons"]["h0"]["external_escape_channels"]["interbank_transactions_foreign_banks_asset_qoq"]["beta"] == -2.5
    assert payload["key_horizons"]["h0"]["asset_purchase_plumbing_context"]["channels"]["tga_qoq"]["beta"] == 2.5
    assert payload["key_horizons"]["h0"]["asset_purchase_plumbing_context"]["treasury_drain_signal"] is True
    assert payload["key_horizons"]["h0"]["asset_purchase_plumbing_context"]["interpretation"] == "treasury_drain_context"
    assert payload["key_horizons"]["h0"]["escape_support_context"]["interpretation"] == "mixed_escape_and_support_signals"
    assert payload["key_horizons"]["h0"]["decisive_positive_creator_channels"] == [
        "commercial_industrial_loans_ex_chargeoffs_qoq",
        "commercial_industrial_loans_qoq",
        "loans_to_nondepository_financial_institutions_qoq",
        "treasury_securities_bank_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_positive_noncore_creator_channels"] == [
        "loans_to_nondepository_financial_institutions_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_positive_asset_purchase_channels"] == [
        "treasury_securities_bank_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_positive_funding_accommodation_channels"] == [
        "commercial_bank_borrowings_qoq",
        "fed_borrowings_depository_institutions_qoq",
        "fhlb_advances_sallie_mae_loans_bank_qoq",
        "interbank_transactions_bank_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_negative_funding_accommodation_channels"] == [
        "debt_securities_bank_liability_qoq",
    ]
    assert payload["key_horizons"]["h0"]["funding_accommodation_context"]["interpretation"] == "mixed_funding_accommodation_signals"
    assert payload["key_horizons"]["h0"]["decisive_positive_retention_support_channels"] == [
        "currency_reallocation_qoq",
        "on_rrp_reallocation_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_negative_retention_support_channels"] == [
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "household_treasury_securities_reallocation_qoq",
        "mmf_treasury_bills_reallocation_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_positive_external_escape_channels"] == [
        "checkable_rest_of_world_bank_qoq",
        "interbank_transactions_foreign_banks_liability_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_negative_external_escape_channels"] == [
        "interbank_transactions_foreign_banks_asset_qoq",
    ]
    assert payload["target_mapping"]["priority_gap"] == "ci_us_qoq"
    assert payload["target_mapping"]["core_creator_targets"][0]["current_live_proxy"] == "commercial_industrial_loans_qoq"
    assert payload["target_mapping"]["core_creator_targets"][0]["status"] == "scope_mismatch_current_public_path"
    assert payload["target_mapping"]["excluded_or_noncore_families"] == [
        {
            "excluded_family": "loans_to_depositories_qoq",
            "current_live_proxy": "loans_to_commercial_banks_qoq",
            "status": "materialized",
            "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
        },
        {
            "excluded_family": "loans_to_ndfis_qoq",
            "current_live_proxy": "loans_to_nondepository_financial_institutions_qoq",
            "status": "materialized",
            "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
        },
        {
            "excluded_family": "securities_purpose_loans_qoq",
            "current_live_proxy": "loans_for_purchasing_or_carrying_securities_qoq",
            "status": "materialized",
            "interpretation": "Separated from the core domestic creator lane and should stay excluded from a domestic nonfinancial lending subtotal.",
        },
    ]
    assert payload["target_mapping"]["funding_accommodation_lane"]["status"] == "materialized_with_cleanup_candidate"
    assert payload["target_mapping"]["funding_accommodation_lane"]["cleanup_candidates"] == [
        "holding_company_parent_funding_bank_qoq",
    ]
    assert payload["key_horizons"]["h0"]["decisive_negative_creator_channels"] == [
        "closed_end_residential_loans_qoq",
    ]
    assert payload["key_horizons"]["h0"]["proxy_coverage_label"] == "creator_channels_unresolved"
    assert payload["takeaways"][-1] == (
        "The biggest remaining creator-target gap is ci_us_qoq: the live public path still uses all-commercial-bank C&I rather than exact U.S.-addressee domestic nonfinancial C&I."
    )
    assert "bank_asset_purchase_lane" in payload["target_mapping"]
    assert "destroyer_escape_lane" in payload["target_mapping"]
    assert payload["target_mapping"]["external_escape_lane"]["status"] == "expanded"
    assert payload["funding_accommodation_outcomes_present"] == [
        "commercial_bank_borrowings_qoq",
        "debt_securities_bank_liability_qoq",
        "fed_borrowings_depository_institutions_qoq",
        "fedfunds_repo_liabilities_bank_qoq",
        "fhlb_advances_sallie_mae_loans_bank_qoq",
        "interbank_transactions_bank_qoq",
    ]
    assert payload["funding_accommodation_cleanup_candidates"] == [
        "holding_company_parent_funding_bank_qoq",
    ]
