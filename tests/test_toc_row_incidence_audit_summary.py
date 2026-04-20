from __future__ import annotations

from tdcpass.analysis.toc_row_incidence_audit_summary import build_toc_row_incidence_audit_summary


def test_toc_row_incidence_audit_summary_classifies_toc_and_row_legs() -> None:
    payload = build_toc_row_incidence_audit_summary(
        treasury_operating_cash_audit_summary={
            "status": "available",
            "quarterly_alignment": {"contemporaneous_corr_tga_vs_toc": 0.95},
            "key_horizons": {
                "h0": {
                    "treasury_operating_cash_signed_contribution_beta": 70.44,
                    "treasury_operating_cash_response": {"beta": -70.44},
                    "tga_response": {"beta": -61.19},
                    "reserves_response": {"beta": 88.87},
                    "cb_nonts_response": {"beta": 27.68},
                },
                "h1": {
                    "treasury_operating_cash_signed_contribution_beta": 54.24,
                    "treasury_operating_cash_response": {"beta": -54.24},
                    "tga_response": {"beta": -48.0},
                    "reserves_response": {"beta": 64.0},
                    "cb_nonts_response": {"beta": 15.0},
                },
            },
        },
        rest_of_world_treasury_audit_summary={
            "status": "available",
            "quarterly_alignment": {
                "counterparts": {
                    "checkable_rest_of_world_bank_qoq": {"contemporaneous_corr": 0.02},
                    "foreign_nonts_qoq": {"contemporaneous_corr": -0.17},
                }
            },
            "key_horizons": {
                "h0": {
                    "rest_of_world_treasury_response": {"beta": 11.76},
                    "checkable_rest_of_world_bank_response": {"beta": 0.22},
                    "foreign_nonts_response": {"beta": 26.59},
                    "interbank_transactions_foreign_banks_asset_response": {"beta": 5.0},
                },
                "h1": {
                    "rest_of_world_treasury_response": {"beta": 12.21},
                    "checkable_rest_of_world_bank_response": {"beta": 0.10},
                    "foreign_nonts_response": {"beta": 20.0},
                    "interbank_transactions_foreign_banks_asset_response": {"beta": 4.0},
                },
            },
        },
        toc_row_path_split_summary={
            "status": "available",
            "quarterly_split": {
                "preferred_quarterly_path": "direct_deposit_path_dominant",
                "bundle_contemporaneous_corr": {
                    "direct_deposit_path": 0.86,
                    "broad_support_path": 0.75,
                },
            },
            "key_horizons": {
                "h0": {
                    "broad_support_path_response": {"beta": 87.77},
                    "direct_deposit_path_response": {"beta": 61.41},
                    "preferred_horizon_path": "broad_support_path_dominant",
                },
                "h1": {
                    "broad_support_path_response": {"beta": 71.94},
                    "direct_deposit_path_response": {"beta": 48.22},
                    "preferred_horizon_path": "broad_support_path_dominant",
                },
            },
        },
        split_treatment_architecture_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "support_bundle_beta": 65.40,
                    "core_deposit_proximate_residual_response": {"beta": -5.51},
                    "toc_signed_beta": 70.44,
                    "row_signed_beta": 11.76,
                }
            },
        },
    )

    assert payload["status"] == "available"
    assert payload["classification"]["bundle_role"] == "measured_support_bundle_with_unresolved_strict_deposit_incidence"
    assert payload["classification"]["toc_leg_status"] == "reserve_plumbing_real_but_strict_deposit_incidence_partial"
    assert payload["classification"]["row_leg_status"] == "weak_in_scope_deposit_incidence_external_support_dominant"
    assert payload["recommendation"]["next_branch"] == "run_leg_split_scope_and_timing_matched_liability_incidence_audit_in_raw_units"
    assert payload["key_horizons"]["h0"]["toc_leg"]["in_scope_deposit_proxy_share_of_toc_beta"] is not None
    assert payload["key_horizons"]["h0"]["row_leg"]["external_support_share_of_row_beta"] is not None
