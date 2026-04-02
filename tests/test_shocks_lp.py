import numpy as np
import pandas as pd
from pathlib import Path

from tdcpass.analysis.local_projections import (
    run_local_projections,
    run_lp_from_specs,
    run_regime_split_local_projections,
    run_state_dependent_local_projections,
)
from tdcpass.analysis.shocks import expanding_window_residual
from tdcpass.core.yaml_utils import load_yaml


def _build_panel(n: int = 64) -> pd.DataFrame:
    rows = list(range(n))
    lag_reserves_qoq = [0.15 * i for i in rows]
    tdc_values = [0.3 * i + (i % 5) for i in rows]
    frame = pd.DataFrame(
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
            "commercial_industrial_loans_qoq": [0.35 * i for i in rows],
            "construction_land_development_loans_qoq": [0.1 * i for i in rows],
            "cre_multifamily_loans_qoq": [0.08 * i for i in rows],
            "cre_nonfarm_nonresidential_loans_qoq": [0.14 * i for i in rows],
            "consumer_loans_qoq": [0.2 * i for i in rows],
            "credit_card_revolving_loans_qoq": [0.07 * i for i in rows],
            "auto_loans_qoq": [0.05 * i for i in rows],
            "other_consumer_loans_qoq": [0.09 * i for i in rows],
            "heloc_loans_qoq": [0.04 * i for i in rows],
            "closed_end_residential_loans_qoq": [0.11 * i for i in rows],
            "loans_to_commercial_banks_qoq": [0.03 * i for i in rows],
            "loans_to_nondepository_financial_institutions_qoq": [0.06 * i for i in rows],
            "loans_for_purchasing_or_carrying_securities_qoq": [0.02 * i for i in rows],
            "treasury_securities_bank_qoq": [0.025 * i for i in rows],
            "agency_gse_backed_securities_bank_qoq": [0.03 * i for i in rows],
            "municipal_securities_bank_qoq": [0.01 * i for i in rows],
            "corporate_foreign_bonds_bank_qoq": [0.015 * i for i in rows],
            "fedfunds_repo_liabilities_bank_qoq": [0.011 * i for i in rows],
            "commercial_bank_borrowings_qoq": [0.07 * i for i in rows],
            "fed_borrowings_depository_institutions_qoq": [0.05 * i for i in rows],
            "debt_securities_bank_liability_qoq": [0.016 * i for i in rows],
            "fhlb_advances_sallie_mae_loans_bank_qoq": [0.013 * i for i in rows],
            "holding_company_parent_funding_bank_qoq": [0.009 * i for i in rows],
            "commercial_industrial_loans_ex_chargeoffs_qoq": [0.37 * i for i in rows],
            "consumer_loans_ex_chargeoffs_qoq": [0.22 * i for i in rows],
            "credit_card_revolving_loans_ex_chargeoffs_qoq": [0.08 * i for i in rows],
            "other_consumer_loans_ex_chargeoffs_qoq": [0.1 * i for i in rows],
            "closed_end_residential_loans_ex_chargeoffs_qoq": [0.12 * i for i in rows],
            "cb_nonts_qoq": [0.2 * i for i in rows],
            "tga_qoq": [(-1) ** (i + 1) * 0.25 * i for i in rows],
            "reserves_qoq": [0.18 * i for i in rows],
            "foreign_nonts_qoq": [0.1 * i for i in rows],
            "domestic_nonfinancial_mmf_reallocation_qoq": [0.12 * i for i in rows],
            "domestic_nonfinancial_repo_reallocation_qoq": [0.08 * i for i in rows],
            "on_rrp_reallocation_qoq": [0.06 * i for i in rows],
            "household_treasury_securities_reallocation_qoq": [0.05 * i for i in rows],
            "mmf_treasury_bills_reallocation_qoq": [0.04 * i for i in rows],
            "currency_reallocation_qoq": [0.03 * i for i in rows],
            "fedfunds": [2.0 + 0.01 * i for i in rows],
            "unemployment": [5.0 - 0.01 * i for i in rows],
            "inflation": [2.0 + 0.005 * i for i in rows],
            "bill_share": [0.40 + 0.001 * i for i in rows],
            "bank_absorption_share": [0.35 + 0.004 * i for i in rows],
            "reserve_drain_pressure": [-value for value in lag_reserves_qoq],
            "quarter_index": rows,
            "slr_tight": [1.0 if i > n // 2 else 0.0 for i in rows],
            "tdc_broad_depository_qoq": [0.4 * i + (i % 2) for i in rows],
            "checkable_deposits_bank_qoq": [0.18 * i + (i % 2) for i in rows],
            "interbank_transactions_bank_qoq": [0.04 * i - (i % 3) for i in rows],
            "time_savings_deposits_bank_qoq": [0.22 * i + (i % 4) for i in rows],
            "checkable_federal_govt_bank_qoq": [0.03 * i for i in rows],
            "checkable_state_local_bank_qoq": [0.02 * i + 0.5 for i in rows],
            "checkable_rest_of_world_bank_qoq": [0.01 * i - 0.25 for i in rows],
            "checkable_private_domestic_bank_qoq": [0.12 * i + (i % 2) for i in rows],
            "interbank_transactions_foreign_banks_liability_qoq": [0.015 * i for i in rows],
            "interbank_transactions_foreign_banks_asset_qoq": [0.013 * i for i in rows],
            "deposits_at_foreign_banks_asset_qoq": [0.007 * i for i in rows],
        }
    )
    for column in [
        "tdc_broad_depository_qoq",
        "other_component_qoq",
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
        "tga_qoq",
        "reserves_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
        "on_rrp_reallocation_qoq",
        "household_treasury_securities_reallocation_qoq",
        "mmf_treasury_bills_reallocation_qoq",
        "currency_reallocation_qoq",
        "checkable_deposits_bank_qoq",
        "interbank_transactions_bank_qoq",
        "time_savings_deposits_bank_qoq",
        "checkable_federal_govt_bank_qoq",
        "checkable_state_local_bank_qoq",
        "checkable_rest_of_world_bank_qoq",
        "checkable_private_domestic_bank_qoq",
        "interbank_transactions_foreign_banks_liability_qoq",
        "interbank_transactions_foreign_banks_asset_qoq",
        "deposits_at_foreign_banks_asset_qoq",
    ]:
        frame[f"lag_{column}"] = [None, *frame[column].tolist()[:-1]]
    return frame


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
        "fitted_to_train_target_sd_ratio",
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


def test_shock_scale_flag_uses_realized_target_scale_not_train_sd() -> None:
    values = [100.0 if i % 2 == 0 else -100.0 for i in range(25)] + [0.1, 0.2, -0.1]
    df = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in range(len(values))],
            "tdc_bank_only_qoq": values,
            "lag_tdc_bank_only_qoq": [None, *values[:-1]],
        }
    )

    shocked = expanding_window_residual(
        df,
        target="tdc_bank_only_qoq",
        predictors=["lag_tdc_bank_only_qoq"],
        min_train_obs=24,
        max_scale_ratio=5,
    )

    flagged_row = shocked.loc[shocked["quarter"] == "2006Q2"].iloc[0]
    assert flagged_row["shock_flag"] == "scale_ratio"
    assert flagged_row["fitted_to_target_scale_ratio"] > 500
    assert flagged_row["fitted_to_train_target_sd_ratio"] < 5


def test_condition_number_flag_is_scale_invariant() -> None:
    rows = 32
    seq = np.arange(rows, dtype=float)
    level = [200_000.0 + 8_000.0 * i + (i % 5) * 750.0 for i in range(rows)]
    df = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in range(rows)],
            "tdc_bank_only_qoq": level,
            "lag_tdc_bank_only_qoq": [None, *level[:-1]],
            "lag_fedfunds": (2.0 + 0.05 * seq + 0.15 * np.sin(seq / 3.0)).tolist(),
            "lag_unemployment": (6.0 - 0.03 * seq + 0.2 * np.cos(seq / 4.0)).tolist(),
            "lag_inflation": (1.5 + 0.01 * seq + 0.1 * np.sin(seq / 5.0)).tolist(),
        }
    )

    shocked = expanding_window_residual(
        df,
        target="tdc_bank_only_qoq",
        predictors=["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        min_train_obs=24,
        max_condition_number=1_000_000,
    )

    usable = shocked.dropna(subset=["tdc_residual"])
    assert not usable.empty
    assert usable["train_condition_number"].max() < 100.0
    assert usable["shock_flag"].fillna("").eq("").all()


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
        predictors=["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        min_train_obs=24,
        max_train_obs=40,
        model_name="unexpected_tdc_bank_only_macro_rolling40",
        model_name_column="tdc_bank_only_macro_rolling40_model_name",
        fitted_column="tdc_bank_only_macro_rolling40_fitted",
        residual_column="tdc_bank_only_macro_rolling40_residual",
        standardized_column="tdc_bank_only_macro_rolling40_residual_z",
        train_start_obs_column="tdc_bank_only_macro_rolling40_train_start_obs",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_bank_only_qoq",
        predictors=["lag_tdc_bank_only_qoq", "lag_bill_share", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        min_train_obs=24,
        model_name="unexpected_tdc_legacy_billshare_expanding",
        model_name_column="tdc_legacy_billshare_expanding_model_name",
        fitted_column="tdc_legacy_billshare_expanding_fitted",
        residual_column="tdc_legacy_billshare_expanding_residual",
        standardized_column="tdc_legacy_billshare_expanding_residual_z",
        train_start_obs_column="tdc_legacy_billshare_expanding_train_start_obs",
    )
    shocked = expanding_window_residual(
        shocked,
        target="tdc_bank_only_qoq",
        predictors=["lag_tdc_bank_only_qoq", "lag_bill_share", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
        min_train_obs=24,
        max_train_obs=40,
        model_name="unexpected_tdc_bank_only_billshare_macro_rolling40",
        model_name_column="tdc_bank_only_billshare_macro_rolling40_model_name",
        fitted_column="tdc_bank_only_billshare_macro_rolling40_fitted",
        residual_column="tdc_bank_only_billshare_macro_rolling40_residual",
        standardized_column="tdc_bank_only_billshare_macro_rolling40_residual_z",
        train_start_obs_column="tdc_bank_only_billshare_macro_rolling40_train_start_obs",
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

    state_dependence = outputs["lp_irf_state_dependence"]
    assert {
        "state_variant",
        "state_role",
        "state_column",
        "state_label",
        "state_quantile",
        "state_value",
        "state_mean",
        "state_centered_value",
        "outcome",
        "horizon",
        "beta",
        "se",
        "lower95",
        "upper95",
        "interaction_beta",
        "interaction_se",
        "interaction_lower95",
        "interaction_upper95",
        "n",
        "spec_name",
        "shock_column",
        "shock_scale",
        "response_type",
    }.issubset(state_dependence.columns)
    assert set(state_dependence["spec_name"]) == {"state_dependence"}
    assert set(state_dependence["state_variant"]) == {"reserve_drain"}
    assert set(state_dependence["state_role"]) == {"exploratory"}
    assert set(state_dependence["state_label"]) == {"low", "high"}

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
        "bank_only_billshare_macro_rolling40",
        "legacy_rolling40_ols",
        "legacy_billshare_expanding",
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

    factor_control_sensitivity = outputs["factor_control_sensitivity"]
    assert {
        "factor_variant",
        "factor_role",
        "factor_columns",
        "source_columns",
        "factor_count",
        "min_train_obs",
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
    }.issubset(factor_control_sensitivity.columns)
    assert set(factor_control_sensitivity["spec_name"]) == {"factor_control_sensitivity"}
    assert set(factor_control_sensitivity["factor_variant"]) == {
        "recursive_macro_factors2",
        "recursive_macro_plumbing_factors3",
    }
    assert set(factor_control_sensitivity["factor_role"]) == {"core", "exploratory"}
    assert set(factor_control_sensitivity["factor_count"]) == {2, 3}
    assert set(factor_control_sensitivity["min_train_obs"]) == {24, 40}

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
    assert set(sample_sensitivity["sample_variant"]) == {
        "all_usable_shocks",
        "drop_flagged_shocks",
        "drop_severe_scale_tail",
    }
    assert set(sample_sensitivity["sample_role"]) == {"headline", "exploratory"}
    severe_tail_filters = sample_sensitivity.loc[
        sample_sensitivity["sample_variant"] == "drop_severe_scale_tail", "sample_filter"
    ].drop_duplicates()
    assert severe_tail_filters.tolist() == ["fitted_to_target_scale_ratio<=25.0"]

    period_sensitivity = outputs["period_sensitivity"]
    assert {
        "period_variant",
        "period_role",
        "start_quarter",
        "end_quarter",
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
    }.issubset(period_sensitivity.columns)
    assert set(period_sensitivity["spec_name"]) == {"period_sensitivity"}
    assert {"all_usable", "post_gfc_early"}.issubset(set(period_sensitivity["period_variant"]))
    assert set(period_sensitivity["period_role"]).issubset({"headline", "core"})


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


def test_state_dependent_lp_reports_low_and_high_implied_responses() -> None:
    n = 96
    rows = list(range(n))
    shock = pd.Series([np.sin(i / 5.0) + 0.25 * np.cos(i / 9.0) for i in rows], dtype=float)
    state = pd.Series(np.linspace(-1.0, 1.0, n), dtype=float)
    state_centered = state - float(state.mean())
    outcome = 1.0 + 2.0 * shock + 3.0 * shock * state_centered
    frame = pd.DataFrame(
        {
            "quarter": [f"20{i // 4:02d}Q{(i % 4) + 1}" for i in rows],
            "tdc_residual_z": shock,
            "bank_absorption_share": state,
            "total_deposits_bank_qoq": outcome,
        }
    )

    out = run_state_dependent_local_projections(
        frame,
        shock_col="tdc_residual_z",
        outcome_cols=["total_deposits_bank_qoq"],
        controls=[],
        horizons=[0],
        nw_lags=1,
        cumulative=False,
        state_definitions={
            "bank_absorption": {
                "column": "bank_absorption_share",
                "state_role": "core",
                "low_quantile": 0.25,
                "high_quantile": 0.75,
            }
        },
    )

    assert not out.empty
    low_row = out[(out["state_variant"] == "bank_absorption") & (out["state_label"] == "low")].iloc[0]
    high_row = out[(out["state_variant"] == "bank_absorption") & (out["state_label"] == "high")].iloc[0]
    assert float(high_row["beta"]) > float(low_row["beta"])
    assert float(high_row["interaction_beta"]) > 0.0
    assert float(low_row["state_value"]) < float(high_row["state_value"])


def test_factor_augmented_controls_materialize_after_recursive_burn_in() -> None:
    panel = _build_panel(n=96)
    shocked = expanding_window_residual(
        panel,
        target="tdc_bank_only_qoq",
        predictors=_shock_predictors(),
        min_train_obs=24,
    )
    lp_specs = {
        "specs": {
            "baseline": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                "include_lagged_outcome": True,
                "horizons": [0],
            },
            "regimes": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                "include_lagged_outcome": True,
                "horizons": [0],
                "regime_columns": ["reserve_drain_pressure"],
            },
            "state_dependence": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                "include_lagged_outcome": True,
                "horizons": [0],
                "state_variants": {
                    "reserve_drain": {"state_role": "exploratory"}
                },
            },
            "sensitivity": {
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                "include_lagged_outcome": True,
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
                "include_lagged_outcome": True,
                "horizons": [0],
                "control_variants": {
                    "headline_lagged_macro": {
                        "control_role": "headline",
                        "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                        "include_lagged_outcome": True,
                    }
                },
            },
            "factor_control_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq"],
                "include_lagged_outcome": True,
                "horizons": [0],
                "factor_variants": {
                    "recursive_macro_factors2": {
                        "factor_role": "core",
                        "source_columns": [
                            "lag_fedfunds",
                            "lag_unemployment",
                            "lag_inflation",
                        ],
                        "factor_count": 2,
                        "min_train_obs": 24,
                    },
                    "recursive_macro_plumbing_factors3": {
                        "factor_role": "exploratory",
                        "source_columns": [
                            "lag_tga_qoq",
                            "lag_reserves_qoq",
                            "lag_bill_share",
                            "lag_fedfunds",
                            "lag_unemployment",
                            "lag_inflation",
                        ],
                        "factor_count": 3,
                        "min_train_obs": 40,
                    }
                },
            },
            "sample_sensitivity": {
                "shock_column": "tdc_residual_z",
                "outcomes": ["total_deposits_bank_qoq"],
                "controls": ["lag_tdc_bank_only_qoq", "lag_fedfunds", "lag_unemployment", "lag_inflation"],
                "include_lagged_outcome": True,
                "horizons": [0],
                "sample_variants": {
                    "all_usable_shocks": {
                        "sample_role": "headline",
                        "exclude_flagged_shocks": False,
                    }
                },
            },
        }
    }
    regime_specs = {"regimes": {"reserve_drain": {"column": "reserve_drain_pressure", "threshold": "median"}}}

    outputs = run_lp_from_specs(shocked, lp_specs=lp_specs, regime_specs=regime_specs)
    factors = outputs["factor_control_sensitivity"]

    assert not factors.empty
    macro_row = factors[factors["factor_variant"] == "recursive_macro_factors2"].iloc[0]
    plumbing_row = factors[factors["factor_variant"] == "recursive_macro_plumbing_factors3"].iloc[0]
    assert "recursive_macro_factors2_factor1" in str(macro_row["factor_columns"])
    assert "lag_fedfunds" in str(macro_row["source_columns"])
    assert "recursive_macro_plumbing_factors3_factor1" in str(plumbing_row["factor_columns"])
    assert "lag_tga_qoq" in str(plumbing_row["source_columns"])
    assert int(macro_row["n"]) >= int(plumbing_row["n"])


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
