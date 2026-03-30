import pandas as pd
from pathlib import Path

from tdcpass.analysis.local_projections import (
    run_local_projections,
    run_lp_from_specs,
    run_regime_split_local_projections,
)
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.core.yaml_utils import load_yaml


def _build_panel(n: int = 64) -> pd.DataFrame:
    rows = list(range(n))
    lag_reserves_qoq = [0.15 * i for i in rows]
    tdc_values = [0.3 * i + (i % 5) for i in rows]
    return pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in rows],
            "tdc_bank_only_qoq": tdc_values,
            "lag_total_deposits_bank_qoq": [0.2 * i for i in rows],
            "lag_tdc_bank_only_qoq": [None, *tdc_values[:-1]],
            "lag_tga_qoq": [(-1) ** i * 0.2 * i for i in rows],
            "lag_reserves_qoq": lag_reserves_qoq,
            "lag_bill_share": [0.35 + 0.002 * i for i in rows],
            "lag_fedfunds": [2.0 + 0.01 * i for i in rows],
            "lag_unemployment": [5.0 - 0.01 * i for i in rows],
            "lag_inflation": [2.0 + 0.005 * i for i in rows],
            "total_deposits_bank_qoq": [0.5 * i + (i % 3) for i in rows],
            "other_component_qoq": [0.25 * i - (i % 4) for i in rows],
            "bank_credit_private_qoq": [0.3 * i for i in rows],
            "cb_nonts_qoq": [0.2 * i for i in rows],
            "foreign_nonts_qoq": [0.1 * i for i in rows],
            "domestic_nonfinancial_mmf_reallocation_qoq": [0.12 * i for i in rows],
            "domestic_nonfinancial_repo_reallocation_qoq": [0.08 * i for i in rows],
            "fedfunds": [2.0 + 0.01 * i for i in rows],
            "unemployment": [5.0 - 0.01 * i for i in rows],
            "inflation": [2.0 + 0.005 * i for i in rows],
            "bill_share": [0.40 + 0.001 * i for i in rows],
            "bank_absorption_share": [0.35 + 0.004 * i for i in rows],
            "reserve_drain_pressure": [-value for value in lag_reserves_qoq],
            "quarter_index": rows,
            "slr_tight": [1.0 if i > n // 2 else 0.0 for i in rows],
            "tdc_broad_depository_qoq": [0.4 * i + (i % 2) for i in rows],
        }
    )


def _shock_predictors() -> list[str]:
    return [
        "lag_total_deposits_bank_qoq",
        "lag_tga_qoq",
        "lag_reserves_qoq",
        "lag_bill_share",
        "lag_fedfunds",
        "lag_unemployment",
        "lag_inflation",
    ]


def test_shock_builder_and_lp_canonical_contract():
    df = _build_panel()
    shocked = expanding_window_residual(
        df,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
        model_name="unexpected_tdc_default",
    )
    required_shock_cols = {
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
        "shock_flag",
    }
    assert required_shock_cols.issubset(shocked.columns)
    assert shocked["tdc_residual"].notna().sum() > 0
    mask = shocked["tdc_residual"].notna()
    assert shocked.loc[mask, "model_name"].eq("unexpected_tdc_default").all()

    lp = run_local_projections(
        shocked,
        shock_col="tdc_residual_z",
        outcome_cols=["total_deposits_bank_qoq", "other_component_qoq"],
        controls=["fedfunds", "unemployment", "inflation"],
        horizons=[0, 1, 2],
        nw_lags=1,
        spec_name="baseline",
    )
    assert not lp.empty
    assert {"outcome", "horizon", "beta", "se", "spec_name"}.issubset(lp.columns)
    assert {"shock_column", "shock_scale", "response_type"}.issubset(lp.columns)
    assert set(lp["spec_name"]) == {"baseline"}
    assert set(lp["shock_column"]) == {"tdc_residual_z"}
    assert set(lp["shock_scale"]) == {"rolling_oos_standard_deviation"}
    assert set(lp["response_type"]) == {"cumulative_sum_h0_to_h"}


def test_shock_standardization_is_strict_oos_no_future_leakage():
    df = _build_panel()
    shocked_a = expanding_window_residual(
        df,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    cutoff = 40
    df_mut = df.copy()
    df_mut.loc[df_mut.index > cutoff, "tdc_bank_only_qoq"] = (
        df_mut.loc[df_mut.index > cutoff, "tdc_bank_only_qoq"] * 50.0
    )
    shocked_b = expanding_window_residual(
        df_mut,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    left = shocked_a.loc[:cutoff, "tdc_residual_z"].fillna(0.0).round(12).tolist()
    right = shocked_b.loc[:cutoff, "tdc_residual_z"].fillna(0.0).round(12).tolist()
    assert left == right


def test_lp_spec_scaffolding_baseline_and_regimes_contract_columns():
    panel = _build_panel()
    shocked = expanding_window_residual(
        panel,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_broad_depository_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
        model_name="unexpected_tdc_broad_depository",
        fitted_column="tdc_broad_depository_fitted",
        residual_column="tdc_broad_depository_residual",
        standardized_column="tdc_broad_depository_residual_z",
        train_start_obs_column="tdc_broad_depository_train_start_obs",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_bank_only_qoq",
        predictors=[col for col in _shock_predictors() if col != "lag_bill_share"],
        min_train_obs=24,
        model_name="unexpected_tdc_no_bill_share",
        model_name_column="tdc_no_bill_share_model_name",
        fitted_column="tdc_no_bill_share_fitted",
        residual_column="tdc_no_bill_share_residual",
        standardized_column="tdc_no_bill_share_residual_z",
        train_start_obs_column="tdc_no_bill_share_train_start_obs",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_bank_only_qoq",
        predictors=["lag_tdc_bank_only_qoq", "lag_bill_share", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        min_train_obs=32,
        model_name="unexpected_tdc_long_burnin",
        model_name_column="tdc_long_burnin_model_name",
        fitted_column="tdc_long_burnin_fitted",
        residual_column="tdc_long_burnin_residual",
        standardized_column="tdc_long_burnin_residual_z",
        train_start_obs_column="tdc_long_burnin_train_start_obs",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
        model_name="unexpected_tdc_legacy_totaldep",
        model_name_column="tdc_legacy_totaldep_model_name",
        fitted_column="tdc_legacy_totaldep_fitted",
        residual_column="tdc_legacy_totaldep_residual",
        standardized_column="tdc_legacy_totaldep_residual_z",
        train_start_obs_column="tdc_legacy_totaldep_train_start_obs",
    )
    lp_specs = load_yaml(Path("config/lp_specs.yml"))
    regime_specs = load_yaml(Path("config/regime_specs.yml"))
    outputs = run_lp_from_specs(shocked, lp_specs=lp_specs, regime_specs=regime_specs)

    baseline = outputs["lp_irf"]
    assert {
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
    }.issubset(baseline.columns)
    assert set(baseline["spec_name"]) == {"baseline"}

    regimes = outputs["lp_irf_regimes"]
    assert {
        "regime",
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
    }.issubset(regimes.columns)
    assert set(regimes["spec_name"]) == {"regimes"}

    sensitivity = outputs["tdc_sensitivity_ladder"]
    assert {
        "treatment_variant",
        "treatment_role",
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
    }.issubset(sensitivity.columns)
    assert set(sensitivity["spec_name"]) == {"sensitivity"}
    assert set(sensitivity["treatment_variant"]) == {
        "baseline",
        "bank_only_long_burnin",
        "bank_only_no_bill_share",
        "legacy_totaldep_long_burnin",
        "broad_depository",
    }
    assert set(sensitivity["treatment_role"]) == {"core", "exploratory"}

    control_sensitivity = outputs["control_set_sensitivity"]
    assert {
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
    }.issubset(control_sensitivity.columns)
    assert set(control_sensitivity["spec_name"]) == {"control_sensitivity"}
    assert set(control_sensitivity["control_variant"]) == {
        "headline_lagged_macro",
        "lagged_macro_plus_trend",
        "lagged_macro_plus_bill",
    }
    assert set(control_sensitivity["control_role"]) == {"headline", "core", "exploratory"}

    sample_sensitivity = outputs["shock_sample_sensitivity"]
    assert {
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
    }.issubset(sample_sensitivity.columns)
    assert set(sample_sensitivity["spec_name"]) == {"sample_sensitivity"}
    assert set(sample_sensitivity["sample_variant"]) == {"all_usable_shocks", "drop_flagged_shocks"}
    assert set(sample_sensitivity["sample_role"]) == {"headline", "exploratory"}


def test_alternate_shock_does_not_overwrite_baseline_model_name_metadata() -> None:
    panel = _build_panel()
    shocked = expanding_window_residual(
        panel,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
        model_name="unexpected_tdc_default",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_broad_depository_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
        model_name="unexpected_tdc_broad_depository",
        model_name_column="tdc_broad_depository_model_name",
        fitted_column="tdc_broad_depository_fitted",
        residual_column="tdc_broad_depository_residual",
        standardized_column="tdc_broad_depository_residual_z",
        train_start_obs_column="tdc_broad_depository_train_start_obs",
        condition_number_column="tdc_broad_depository_train_condition_number",
        target_sd_column="tdc_broad_depository_train_target_sd",
        residual_sd_column="tdc_broad_depository_train_resid_sd",
        scale_ratio_column="tdc_broad_depository_fitted_to_target_scale_ratio",
        flag_column="tdc_broad_depository_shock_flag",
    )

    baseline_mask = shocked["tdc_residual"].notna()
    alternate_mask = shocked["tdc_broad_depository_residual"].notna()
    assert shocked.loc[baseline_mask, "model_name"].eq("unexpected_tdc_default").all()
    assert shocked.loc[alternate_mask, "tdc_broad_depository_model_name"].eq("unexpected_tdc_broad_depository").all()
    assert "shock_flag" in shocked.columns
    assert "tdc_broad_depository_shock_flag" in shocked.columns


def test_regime_split_uses_calendar_forward_horizons() -> None:
    n = 30
    frame = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in range(n)],
            "tdc_residual_z": [float(i) for i in range(n)],
            "total_deposits_bank_qoq": [float(i + 1) for i in range(n)],
            "regime_flag": [1.0 if i % 2 == 0 else 0.0 for i in range(n)],
        }
    )

    out = run_regime_split_local_projections(
        frame,
        shock_col="tdc_residual_z",
        outcome_cols=["total_deposits_bank_qoq"],
        controls=[],
        horizons=[1],
        nw_lags=1,
        cumulative=True,
        regime_definitions={"alternating": {"column": "regime_flag", "threshold": 0.5}},
    )

    high_row = out[(out["regime"] == "alternating_high") & (out["outcome"] == "total_deposits_bank_qoq")].iloc[0]
    assert int(high_row["n"]) == 15


def test_sensitivity_variants_require_explicit_treatment_role() -> None:
    panel = _build_panel()
    shocked = expanding_window_residual(
        panel,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    bad_lp_specs = {
        "specs": {
            "baseline": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["fedfunds"],
                "horizons": [0],
            },
            "regimes": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["fedfunds"],
                "horizons": [0],
                "regime_columns": ["reserve_drain_pressure"],
            },
            "sensitivity": {
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["fedfunds"],
                "horizons": [0],
                "shock_variants": {
                    "baseline": {
                        "shock_column": "tdc_residual_z",
                    }
                },
            },
            "control_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "horizons": [0],
                "control_variants": {
                    "headline": {
                        "controls": ["lag_fedfunds"],
                        "control_role": "headline",
                    }
                },
            },
            "sample_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "horizons": [0],
                "sample_variants": {
                    "headline": {
                        "sample_role": "headline",
                        "exclude_flagged_shocks": False,
                    }
                },
            },
        }
    }
    regime_specs = {"regimes": {"reserve_drain": {"column": "reserve_drain_pressure", "threshold": "median"}}}

    try:
        run_lp_from_specs(shocked, lp_specs=bad_lp_specs, regime_specs=regime_specs)
    except ValueError as exc:
        assert "missing required treatment_role" in str(exc)
    else:
        raise AssertionError("Expected ValueError when treatment_role is missing.")


def test_control_sensitivity_variants_require_explicit_control_role() -> None:
    panel = _build_panel()
    shocked = expanding_window_residual(
        panel,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    bad_lp_specs = {
        "specs": {
            "baseline": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_fedfunds"],
                "horizons": [0],
            },
            "regimes": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_fedfunds"],
                "horizons": [0],
                "regime_columns": ["reserve_drain_pressure"],
            },
            "sensitivity": {
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_fedfunds"],
                "horizons": [0],
                "shock_variants": {
                    "baseline": {
                        "shock_column": "tdc_residual_z",
                        "treatment_role": "core",
                    }
                },
            },
            "control_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "horizons": [0],
                "control_variants": {
                    "headline": {
                        "controls": ["lag_fedfunds"],
                    }
                },
            },
            "sample_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "horizons": [0],
                "sample_variants": {
                    "headline": {
                        "sample_role": "headline",
                        "exclude_flagged_shocks": False,
                    }
                },
            },
        }
    }
    regime_specs = {"regimes": {"reserve_drain": {"column": "reserve_drain_pressure", "threshold": "median"}}}

    try:
        run_lp_from_specs(shocked, lp_specs=bad_lp_specs, regime_specs=regime_specs)
    except ValueError as exc:
        assert "missing required control_role" in str(exc)
    else:
        raise AssertionError("Expected ValueError when control_role is missing.")
