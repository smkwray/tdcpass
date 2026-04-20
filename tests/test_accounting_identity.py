from __future__ import annotations

import pandas as pd

from tdcpass.analysis.accounting_identity import (
    build_accounting_identity_alignment_frame,
    build_accounting_identity_summary,
    slice_accounting_identity_lp_irf,
)


def _lp_irf_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -1.0, "se": 0.1, "lower95": -1.2, "upper95": -0.8, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_deposit_substitution_qoq", "horizon": 0, "beta": -0.2, "se": 0.1, "lower95": -0.4, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_bank_balance_sheet_qoq", "horizon": 0, "beta": -0.1, "se": 0.1, "lower95": -0.3, "upper95": 0.1, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_public_liquidity_qoq", "horizon": 0, "beta": -0.05, "se": 0.1, "lower95": -0.25, "upper95": 0.15, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_external_flow_qoq", "horizon": 0, "beta": -0.15, "se": 0.1, "lower95": -0.35, "upper95": 0.05, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_identity_total_qoq", "horizon": 0, "beta": -0.5, "se": 0.1, "lower95": -0.7, "upper95": -0.3, "n": 20, "spec_name": "baseline"},
            {"outcome": "accounting_identity_gap_qoq", "horizon": 0, "beta": -0.5, "se": 0.1, "lower95": -0.7, "upper95": -0.3, "n": 20, "spec_name": "baseline"},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": 0.25, "se": 0.1, "lower95": 0.05, "upper95": 0.45, "n": 20, "spec_name": "baseline"},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -0.6, "se": 0.1, "lower95": -0.8, "upper95": -0.4, "n": 19, "spec_name": "baseline"},
            {"outcome": "accounting_identity_total_qoq", "horizon": 4, "beta": -0.54, "se": 0.1, "lower95": -0.74, "upper95": -0.34, "n": 19, "spec_name": "baseline"},
            {"outcome": "accounting_identity_gap_qoq", "horizon": 4, "beta": -0.06, "se": 0.1, "lower95": -0.26, "upper95": 0.14, "n": 19, "spec_name": "baseline"},
        ]
    )


def test_slice_accounting_identity_lp_irf_filters_and_orders_outcomes() -> None:
    sliced = slice_accounting_identity_lp_irf(_lp_irf_fixture())

    assert "bank_credit_private_qoq" not in sliced["outcome"].tolist()
    assert sliced["outcome"].tolist()[:3] == [
        "other_component_qoq",
        "other_component_qoq",
        "accounting_deposit_substitution_qoq",
    ]


def test_build_accounting_identity_alignment_frame_reports_gap_share() -> None:
    alignment = build_accounting_identity_alignment_frame(_lp_irf_fixture(), horizons=(0, 4))

    h0 = alignment.loc[alignment["horizon"] == 0].iloc[0]
    h4 = alignment.loc[alignment["horizon"] == 4].iloc[0]

    assert abs(h0["arithmetic_residual_minus_total_beta"] + 0.5) < 1e-12
    assert abs(h0["identity_gap_share_of_residual"] - 0.5) < 1e-12
    assert h0["interpretation"] == "partial_closure"
    assert abs(h4["identity_gap_share_of_residual"] - 0.1) < 1e-12
    assert h4["interpretation"] == "tight_closure"


def test_build_accounting_identity_summary_marks_available_when_identity_total_present() -> None:
    summary = build_accounting_identity_summary(
        lp_irf=_lp_irf_fixture(),
        accounting_source_kind="ea_tdc_accounting_bundle",
        horizons=(0, 4),
    )

    assert summary["status"] == "available"
    assert summary["source_kind"] == "ea_tdc_accounting_bundle"
    assert "accounting_deposit_substitution_qoq" in summary["component_outcomes_present"]
    assert summary["key_horizons"]["h0"]["interpretation"] == "partial_closure"
    assert summary["key_horizons"]["h4"]["interpretation"] == "tight_closure"


def test_build_accounting_identity_summary_warns_on_persistent_large_gap() -> None:
    summary = build_accounting_identity_summary(
        lp_irf=pd.DataFrame(
            [
                {"outcome": "other_component_qoq", "horizon": 0, "beta": -1.0, "se": 0.1, "lower95": -1.2, "upper95": -0.8, "n": 20, "spec_name": "baseline"},
                {"outcome": "accounting_deposit_substitution_qoq", "horizon": 0, "beta": 0.0, "se": 0.1, "lower95": -0.2, "upper95": 0.2, "n": 20, "spec_name": "baseline"},
                {"outcome": "accounting_identity_total_qoq", "horizon": 0, "beta": -100.0, "se": 1.0, "lower95": -102.0, "upper95": -98.0, "n": 20, "spec_name": "baseline"},
                {"outcome": "accounting_identity_gap_qoq", "horizon": 0, "beta": 99.0, "se": 1.0, "lower95": 97.0, "upper95": 101.0, "n": 20, "spec_name": "baseline"},
            ]
        ),
        accounting_source_kind="local_accounting_bundle",
        horizons=(0,),
    )

    assert any("large closure gap" in takeaway for takeaway in summary["takeaways"])


def test_build_accounting_identity_summary_notes_when_all_horizons_tight() -> None:
    summary = build_accounting_identity_summary(
        lp_irf=pd.DataFrame(
                [
                    {"outcome": "other_component_qoq", "horizon": 0, "beta": -1.0, "se": 0.1, "lower95": -1.2, "upper95": -0.8, "n": 20, "spec_name": "baseline"},
                    {"outcome": "accounting_deposit_substitution_qoq", "horizon": 0, "beta": -0.2, "se": 0.1, "lower95": -0.4, "upper95": 0.0, "n": 20, "spec_name": "baseline"},
                    {"outcome": "accounting_identity_total_qoq", "horizon": 0, "beta": -0.98, "se": 0.1, "lower95": -1.18, "upper95": -0.78, "n": 20, "spec_name": "baseline"},
                    {"outcome": "accounting_identity_gap_qoq", "horizon": 0, "beta": -0.02, "se": 0.1, "lower95": -0.22, "upper95": 0.18, "n": 20, "spec_name": "baseline"},
                    {"outcome": "other_component_qoq", "horizon": 4, "beta": -1.5, "se": 0.1, "lower95": -1.7, "upper95": -1.3, "n": 19, "spec_name": "baseline"},
                    {"outcome": "accounting_deposit_substitution_qoq", "horizon": 4, "beta": -0.3, "se": 0.1, "lower95": -0.5, "upper95": -0.1, "n": 19, "spec_name": "baseline"},
                    {"outcome": "accounting_identity_total_qoq", "horizon": 4, "beta": -1.48, "se": 0.1, "lower95": -1.68, "upper95": -1.28, "n": 19, "spec_name": "baseline"},
                    {"outcome": "accounting_identity_gap_qoq", "horizon": 4, "beta": -0.02, "se": 0.1, "lower95": -0.22, "upper95": 0.18, "n": 19, "spec_name": "baseline"},
                ]
        ),
        accounting_source_kind="local_accounting_bundle",
        horizons=(0, 4),
    )

    assert any("All reported horizons show tight accounting closure" in takeaway for takeaway in summary["takeaways"])
