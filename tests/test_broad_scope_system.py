from __future__ import annotations

import pandas as pd

from tdcpass.analysis import broad_scope_system


def test_build_broad_scope_system_summary_reports_broad_gap_and_tdc_component_audit(monkeypatch) -> None:
    broad_identity = pd.DataFrame(
        [
            {"outcome": "tdc_bank_only_qoq", "horizon": 0, "beta": 12.0, "se": 1.0, "lower95": 10.0, "upper95": 14.0, "n": 20},
            {"outcome": "broad_bank_deposits_qoq", "horizon": 0, "beta": 7.0, "se": 1.0, "lower95": 5.0, "upper95": 9.0, "n": 20},
            {"outcome": "broad_bank_other_component_qoq", "horizon": 0, "beta": -5.0, "se": 1.0, "lower95": -7.0, "upper95": -3.0, "n": 20},
            {"outcome": "tdc_bank_only_qoq", "horizon": 4, "beta": 18.0, "se": 1.0, "lower95": 16.0, "upper95": 20.0, "n": 19},
            {"outcome": "broad_bank_deposits_qoq", "horizon": 4, "beta": 8.0, "se": 1.0, "lower95": 6.0, "upper95": 10.0, "n": 19},
            {"outcome": "broad_bank_other_component_qoq", "horizon": 4, "beta": -10.0, "se": 1.0, "lower95": -12.0, "upper95": -8.0, "n": 19},
        ]
    )
    broad_strict = pd.DataFrame(
        [
            {"outcome": "broad_strict_loan_foreign_offices_qoq", "horizon": 0, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 20},
            {"outcome": "broad_strict_loan_affiliated_areas_qoq", "horizon": 0, "beta": -0.25, "se": 1.0, "lower95": -2.25, "upper95": 1.75, "n": 20},
            {"outcome": "broad_strict_loan_source_qoq", "horizon": 0, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 20},
            {"outcome": "broad_strict_gap_qoq", "horizon": 0, "beta": -4.0, "se": 1.0, "lower95": -6.0, "upper95": -2.0, "n": 20},
            {"outcome": "broad_strict_loan_foreign_offices_qoq", "horizon": 4, "beta": -1.0, "se": 1.0, "lower95": -3.0, "upper95": 1.0, "n": 19},
            {"outcome": "broad_strict_loan_affiliated_areas_qoq", "horizon": 4, "beta": -0.5, "se": 1.0, "lower95": -2.5, "upper95": 1.5, "n": 19},
            {"outcome": "broad_strict_loan_source_qoq", "horizon": 4, "beta": -2.0, "se": 1.0, "lower95": -4.0, "upper95": 0.0, "n": 19},
            {"outcome": "broad_strict_gap_qoq", "horizon": 4, "beta": -8.0, "se": 1.0, "lower95": -10.0, "upper95": -6.0, "n": 19},
        ]
    )
    monkeypatch.setattr(broad_scope_system, "build_identity_baseline_irf", lambda *args, **kwargs: broad_identity.copy())
    monkeypatch.setattr(broad_scope_system, "run_local_projections", lambda *args, **kwargs: broad_strict.copy())

    summary = broad_scope_system.build_broad_scope_system_summary(
        shocked=pd.DataFrame({"quarter": ["2000Q1"], "tdc_bank_only_qoq": [1.0]}),
        baseline_lp_spec={
            "shock_column": "tdc_residual_z",
            "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds"],
            "horizons": [0, 4],
            "cumulative": True,
            "nw_lags": 4,
        },
        baseline_shock_spec={"target": "tdc_bank_only_qoq"},
        scope_alignment_summary={
            "deposit_concepts": {
                "total_deposits_including_interbank": {
                    "key_horizons": {
                        "h0": {"variants": {"us_chartered_bank_only": {"residual_response": {"beta": -2.8}, "total_response": {"beta": 5.2}, "differences_vs_baseline_beta": {"residual_response": 4.2}}}},
                        "h4": {"variants": {"us_chartered_bank_only": {"residual_response": {"beta": -4.5}, "total_response": {"beta": 7.5}, "differences_vs_baseline_beta": {"residual_response": 6.5}}}},
                    }
                }
            }
        },
        strict_identifiable_followup_summary={
            "scope_check_gap_assessment": {
                "key_horizons": {
                    "h0": {"variant_gap_assessments": {"us_chartered_bank_only": {"remaining_share_of_baseline_strict_gap": 0.92}}},
                    "h4": {"variant_gap_assessments": {"us_chartered_bank_only": {"remaining_share_of_baseline_strict_gap": 0.85}}},
                }
            }
        },
        tdc_treatment_audit_summary={
            "status": "available",
            "key_horizons": {
                "h0": {
                    "largest_residual_shift_variant": "us_chartered_bank_only",
                    "largest_abs_residual_shift_beta": 4.2,
                    "variant_removal_diagnostics": {
                        "domestic_bank_only": {"residual_shift_vs_baseline_beta": 2.5},
                        "no_foreign_bank_sectors": {"residual_shift_vs_baseline_beta": 3.1},
                    },
                }
            },
            "takeaways": ["stub"],
        },
        horizons=(0, 4),
        bootstrap_reps=0,
    )

    assert summary["status"] == "available"
    h0 = summary["broad_matched_system"]["key_horizons"]["h0"]
    assert abs(h0["broad_strict_gap_share_of_residual"] - 0.8) < 1e-12
    assert h0["interpretation"] == "partial_broad_scope_strict_coverage"
    assert abs(h0["broad_strict_loan_foreign_offices"]["beta"] + 0.5) < 1e-12
    usc_h0 = summary["usc_matched_context"]["key_horizons"]["h0"]
    assert usc_h0["us_chartered_remaining_share_of_baseline_strict_gap"] == 0.92
    audit_h0 = summary["tdc_component_audit"]["key_horizons"]["h0"]
    assert audit_h0["largest_residual_shift_variant"] == "us_chartered_bank_only"
    assert abs(audit_h0["variant_removal_diagnostics"]["domestic_bank_only"]["residual_shift_vs_baseline_beta"] - 2.5) < 1e-12
    assert abs(audit_h0["variant_removal_diagnostics"]["no_foreign_bank_sectors"]["residual_shift_vs_baseline_beta"] - 3.1) < 1e-12
    assert any("about 0.80 of the broad non-TDC residual" in takeaway for takeaway in summary["takeaways"])
    assert any("removing only ROW changes the headline residual by about 2.50" in takeaway for takeaway in summary["takeaways"])
