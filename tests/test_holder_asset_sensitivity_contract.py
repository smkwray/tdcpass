from __future__ import annotations

import pandas as pd

from tdcpass.analysis import scope_alignment
from tdcpass.core.paths import repo_root
from tdcpass.core.yaml_utils import load_yaml
from tdcpass.pipeline.build_panel import _z1_holder_asset_sensitivity_qoq

NARROW_KEY = "z1_households_nonprofits_nonfinancial_business_currency_deposit_assets_qoq"
BROAD_KEY = "z1_selected_domestic_holder_currency_deposit_assets_broad_qoq"
SOURCE_CODES = [
    "FL153020005",
    "FL103020005",
    "FL113020005",
    "FL153030005",
    "FL103030003",
    "FL113030003",
]


def test_holder_asset_registry_exports_the_bounded_contract() -> None:
    registry = load_yaml(repo_root() / "config" / "series_registry.yml")
    rows = {row["key"]: row for row in registry["series"]}

    narrow = rows[NARROW_KEY]
    assert narrow["role"] == "outcome_sensitivity_unmatched"
    assert narrow["quality_tier"] == "tier_2_constructed"
    assert narrow["status"] == "retained_appendix_sensitivity"
    assert narrow["units"] == "billions_usd"
    assert narrow["source_codes"] == SOURCE_CODES
    assert all(f"diff({code})" in narrow["formula"] for code in SOURCE_CODES)
    assert "raw_downloads.json" in narrow["vintage"]
    assert "not reconciled" in narrow["mismatch_declaration"]

    broad = rows[BROAD_KEY]
    assert broad["role"] == "rejected_boundary_candidate"
    assert broad["status"] == "parked_boundary_failure"
    assert "explicit RS positions" in broad["mismatch_declaration"]

    output_contract = load_yaml(repo_root() / "config" / "output_contract.yml")
    panel = next(
        artifact
        for artifact in output_contract["artifacts"]
        if artifact["path"] == "data/derived/quarterly_panel.csv"
    )
    assert NARROW_KEY in panel["required_columns"]
    assert BROAD_KEY not in panel["required_columns"]


def test_holder_asset_formula_sums_six_quarterly_first_differences() -> None:
    columns = {
        "z1_households_nonprofits_checkable_currency_level": [100.0, 101.0],
        "z1_nonfinancial_corporate_checkable_currency_level": [200.0, 202.0],
        "z1_nonfinancial_noncorporate_checkable_currency_level": [300.0, 303.0],
        "z1_households_nonprofits_time_savings_level": [400.0, 404.0],
        "z1_nonfinancial_corporate_time_savings_level": [500.0, 505.0],
        "z1_nonfinancial_noncorporate_time_savings_level": [600.0, 606.0],
    }
    result = _z1_holder_asset_sensitivity_qoq(pd.DataFrame(columns))
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == 21.0


def test_holder_asset_sensitivity_stays_out_of_tdcpass_estimators_and_identity(
    monkeypatch,
) -> None:
    lp_specs = load_yaml(repo_root() / "config" / "lp_specs.yml")
    model_outcomes = {
        str(outcome)
        for spec in lp_specs["specs"].values()
        if isinstance(spec, dict)
        for outcome in spec.get("outcomes", [])
    }
    assert NARROW_KEY not in model_outcomes
    assert BROAD_KEY not in model_outcomes

    identity_outcomes: list[str] = []

    def fake_identity_ladder(_shocked: pd.DataFrame, *, total_outcome_col: str, **_kwargs: object) -> pd.DataFrame:
        identity_outcomes.append(total_outcome_col)
        return pd.DataFrame()

    monkeypatch.setattr(scope_alignment, "build_identity_variant_ladder", fake_identity_ladder)
    scope_alignment.build_scope_alignment_summary(
        shocked=pd.DataFrame(),
        lp_specs={
            "specs": {
                "sensitivity": {
                    "horizons": [],
                    "cumulative": True,
                    "identity_bootstrap_reps": 1,
                    "identity_bootstrap_block_length": 1,
                }
            }
        },
        shock_specs={
            "unexpected_tdc_default": {
                "standardized_column": "baseline_z",
                "target": "tdc_bank_only_qoq",
            },
            "unexpected_tdc_domestic_bank_only": {
                "standardized_column": "domestic_z",
                "target": "tdc_domestic_bank_only_qoq",
            },
            "unexpected_tdc_us_chartered_bank_only": {
                "standardized_column": "us_chartered_z",
                "target": "tdc_us_chartered_bank_only_qoq",
            },
        },
    )
    assert identity_outcomes == ["total_deposits_bank_qoq", "deposits_only_bank_qoq"]


def test_prohibited_holder_and_scorecard_names_are_absent_from_public_runtime() -> None:
    root = repo_root()
    public_files = [root / "README.md", *sorted((root / "config").glob("*.yml"))]
    public_files.extend(sorted((root / "src").rglob("*.py")))
    public_text = "\n".join(path.read_text(encoding="utf-8") for path in public_files)
    prohibited = {
        "domestic_" + "nonbank_deposits_qoq",
        "domestic_nonfinancial_" + "nonbank_deposits_qoq",
        "domestic_" + "nonbank_other_component_qoq",
        "domestic_" + "nonbank_other_component_core_deposit_proximate_qoq",
        "domestic_nonfinancial_" + "nonbank_other_component_qoq",
        "domestic_nonfinancial_" + "nonbank_other_component_core_deposit_proximate_qoq",
        "destroyer_" + "escape_lane",
        "deposit_" + "retention_support_channels",
        "escape_" + "support_context",
        "external_" + "escape_lane",
    }
    assert not {name for name in prohibited if name in public_text}
