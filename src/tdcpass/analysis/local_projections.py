from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm


def cumulative_forward_sum(series: pd.Series, horizon: int) -> pd.Series:
    values = series.to_numpy(dtype=float)
    out = np.full(len(values), np.nan, dtype=float)
    for i in range(len(values) - horizon):
        window = values[i : i + horizon + 1]
        if np.isnan(window).any():
            continue
        out[i] = window.sum()
    return pd.Series(out, index=series.index)


def _forward_transform(series: pd.Series, horizon: int, cumulative: bool) -> pd.Series:
    if cumulative:
        return cumulative_forward_sum(series, horizon)
    return series.shift(-horizon)


def _resolve_regime_threshold(series: pd.Series, threshold_value: object) -> float:
    if isinstance(threshold_value, str):
        if threshold_value == "median":
            return float(series.median(skipna=True))
        raise ValueError(f"Unsupported regime threshold rule: {threshold_value}")
    return float(threshold_value)


def _resolve_quantile(quantile: object, *, label: str) -> float:
    value = float(quantile)
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{label} quantile must be between 0 and 1, got {value}")
    return value


def _quarter_mask(
    quarters: pd.Series,
    *,
    start_quarter: str | None = None,
    end_quarter: str | None = None,
) -> pd.Series:
    period_index = pd.PeriodIndex(quarters.astype(str), freq="Q")
    mask = pd.Series(True, index=quarters.index, dtype=bool)
    if start_quarter is not None:
        mask &= period_index >= pd.Period(start_quarter, freq="Q")
    if end_quarter is not None:
        mask &= period_index <= pd.Period(end_quarter, freq="Q")
    return mask


def _recursive_factor_controls(
    df: pd.DataFrame,
    *,
    source_columns: Sequence[str],
    factor_count: int,
    min_train_obs: int,
    prefix: str,
) -> pd.DataFrame:
    if factor_count <= 0:
        raise ValueError(f"factor_count must be positive, got {factor_count}")
    if min_train_obs <= 1:
        raise ValueError(f"min_train_obs must exceed 1, got {min_train_obs}")

    missing = [col for col in source_columns if col not in df.columns]
    if missing:
        raise KeyError(f"Missing factor source columns: {missing}")

    factor_columns = [f"{prefix}_factor{i + 1}" for i in range(factor_count)]
    out = pd.DataFrame(np.nan, index=df.index, columns=factor_columns, dtype=float)
    source_frame = df[list(source_columns)].apply(pd.to_numeric, errors="coerce")

    for idx in range(len(source_frame)):
        train = source_frame.iloc[:idx].dropna()
        if len(train) < min_train_obs:
            continue
        current = source_frame.iloc[idx]
        if current.isna().any():
            continue

        means = train.mean()
        stds = train.std(ddof=0)
        valid_columns = stds[stds > 0.0].index.tolist()
        if not valid_columns:
            continue

        z_train = ((train[valid_columns] - means[valid_columns]) / stds[valid_columns]).to_numpy(dtype=float)
        z_current = ((current[valid_columns] - means[valid_columns]) / stds[valid_columns]).to_numpy(dtype=float)
        if np.isnan(z_train).any() or np.isnan(z_current).any():
            continue

        _, _, vh = np.linalg.svd(z_train, full_matrices=False)
        component_count = min(factor_count, vh.shape[0], len(valid_columns))
        for component_idx in range(component_count):
            loading = vh[component_idx].copy()
            anchor_idx = int(np.argmax(np.abs(loading)))
            if loading[anchor_idx] < 0:
                loading *= -1.0
            out.iat[idx, component_idx] = float(z_current @ loading)

    return out


def _run_lp_on_sample(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str],
    include_lagged_outcome: bool,
    horizons: Sequence[int],
    nw_lags: int,
    cumulative: bool,
    spec_name: str,
    extra_row_fields: Mapping[str, str] | None = None,
    sample_mask: pd.Series | None = None,
) -> pd.DataFrame:
    required = [shock_col, *controls, *outcome_cols]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    rows: list[dict[str, float | int | str]] = []
    output_columns = [
        *(list(extra_row_fields.keys()) if extra_row_fields else []),
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
    ]
    shock_scale = "rolling_oos_standard_deviation" if shock_col.endswith("_z") else "raw_unit"
    response_type = "cumulative_sum_h0_to_h" if cumulative else "lead_h"
    for outcome in outcome_cols:
        outcome_controls = list(controls)
        if include_lagged_outcome:
            lagged_outcome_col = f"lag_{outcome}"
            if lagged_outcome_col not in df.columns:
                raise KeyError(f"Missing lagged outcome control column: {lagged_outcome_col}")
            outcome_controls.append(lagged_outcome_col)
        # Preserve control order while avoiding duplicated columns such as lagged TDC in the TDC regression.
        outcome_controls = list(dict.fromkeys(outcome_controls))
        for horizon in horizons:
            dep = _forward_transform(df[outcome], horizon, cumulative=cumulative)
            sample = pd.DataFrame({"dep": dep, shock_col: df[shock_col]})
            for control in outcome_controls:
                sample[control] = df[control]
            if sample_mask is not None:
                sample = sample.loc[sample_mask.fillna(False)]
            sample = sample.dropna()
            if len(sample) < len(outcome_controls) + 12:
                continue

            x = sm.add_constant(sample[[shock_col, *outcome_controls]])
            model = sm.OLS(sample["dep"], x)
            fit = model.fit(cov_type="HAC", cov_kwds={"maxlags": nw_lags})
            beta = float(fit.params[shock_col])
            se = float(fit.bse[shock_col])

            row: dict[str, float | int | str] = {
                "outcome": outcome,
                "horizon": int(horizon),
                "beta": beta,
                "se": se,
                "lower95": beta - 1.96 * se,
                "upper95": beta + 1.96 * se,
                "n": int(fit.nobs),
                "spec_name": spec_name,
                "shock_column": shock_col,
                "shock_scale": shock_scale,
                "response_type": response_type,
            }
            if extra_row_fields:
                row.update(extra_row_fields)
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns)


def run_local_projections(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str] = (),
    include_lagged_outcome: bool = False,
    horizons: Sequence[int] = tuple(range(0, 9)),
    nw_lags: int = 4,
    cumulative: bool = True,
    spec_name: str = "baseline",
) -> pd.DataFrame:
    return _run_lp_on_sample(
        df,
        shock_col=shock_col,
        outcome_cols=outcome_cols,
        controls=controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name=spec_name,
    )


def run_regime_split_local_projections(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str] = (),
    include_lagged_outcome: bool = False,
    horizons: Sequence[int] = tuple(range(0, 9)),
    nw_lags: int = 4,
    cumulative: bool = True,
    regime_definitions: Mapping[str, Mapping[str, object]],
    spec_name: str = "regimes",
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for regime_name, regime_def in regime_definitions.items():
        regime_type = regime_def.get("type", "threshold")
        if regime_type != "threshold":
            raise ValueError(f"Unsupported regime type for {regime_name}: {regime_type}")
        column = str(regime_def["column"])
        if column not in df.columns:
            raise KeyError(f"Missing regime column: {column}")
        threshold_sample = df.dropna(subset=[shock_col, column, *controls])
        threshold = _resolve_regime_threshold(threshold_sample[column], regime_def.get("threshold", 0.5))

        frames.append(
            _run_lp_on_sample(
                df,
                shock_col=shock_col,
                outcome_cols=outcome_cols,
                controls=controls,
                include_lagged_outcome=include_lagged_outcome,
                horizons=horizons,
                nw_lags=nw_lags,
                cumulative=cumulative,
                spec_name=spec_name,
                extra_row_fields={"regime": f"{regime_name}_high"},
                sample_mask=df[column] >= threshold,
            )
        )
        frames.append(
            _run_lp_on_sample(
                df,
                shock_col=shock_col,
                outcome_cols=outcome_cols,
                controls=controls,
                include_lagged_outcome=include_lagged_outcome,
                horizons=horizons,
                nw_lags=nw_lags,
                cumulative=cumulative,
                spec_name=spec_name,
                extra_row_fields={"regime": f"{regime_name}_low"},
                sample_mask=df[column] < threshold,
            )
        )

    if not frames:
        return pd.DataFrame(
            columns=[
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
            ]
        )
    out = pd.concat(frames, ignore_index=True)
    expected = [
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
    ]
    for col in expected:
        if col not in out.columns:
            out[col] = np.nan
    return out[expected]


def run_state_dependent_local_projections(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str] = (),
    include_lagged_outcome: bool = False,
    horizons: Sequence[int] = tuple(range(0, 9)),
    nw_lags: int = 4,
    cumulative: bool = True,
    state_definitions: Mapping[str, Mapping[str, object]],
    spec_name: str = "state_dependence",
) -> pd.DataFrame:
    required = [shock_col, *controls, *outcome_cols]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    shock_scale = "rolling_oos_standard_deviation" if shock_col.endswith("_z") else "raw_unit"
    response_type = "cumulative_sum_h0_to_h" if cumulative else "lead_h"
    output_columns = [
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
    ]

    rows: list[dict[str, float | int | str]] = []
    for state_name, state_def in state_definitions.items():
        column = str(state_def["column"])
        if column not in df.columns:
            raise KeyError(f"Missing state variable column: {column}")
        state_role = str(state_def.get("state_role", "exploratory"))
        low_quantile = _resolve_quantile(state_def.get("low_quantile", 0.25), label=f"{state_name} low")
        high_quantile = _resolve_quantile(state_def.get("high_quantile", 0.75), label=f"{state_name} high")
        if low_quantile >= high_quantile:
            raise ValueError(f"{state_name} low quantile must be strictly below high quantile.")

        for outcome in outcome_cols:
            outcome_controls = list(controls)
            if include_lagged_outcome:
                lagged_outcome_col = f"lag_{outcome}"
                if lagged_outcome_col not in df.columns:
                    raise KeyError(f"Missing lagged outcome control column: {lagged_outcome_col}")
                outcome_controls.append(lagged_outcome_col)
            outcome_controls = list(dict.fromkeys(outcome_controls))

            for horizon in horizons:
                dep = _forward_transform(df[outcome], horizon, cumulative=cumulative)
                sample = pd.DataFrame(
                    {
                        "dep": dep,
                        shock_col: df[shock_col],
                        column: df[column],
                    }
                )
                for control in outcome_controls:
                    sample[control] = df[control]
                sample = sample.dropna()
                if len(sample) < len(outcome_controls) + 14:
                    continue

                state_mean = float(sample[column].mean())
                sample["_state_centered"] = sample[column] - state_mean
                interaction_col = "_shock_state_interaction"
                sample[interaction_col] = sample[shock_col] * sample["_state_centered"]

                x = sm.add_constant(sample[[shock_col, "_state_centered", interaction_col, *outcome_controls]])
                model = sm.OLS(sample["dep"], x)
                fit = model.fit(cov_type="HAC", cov_kwds={"maxlags": nw_lags})
                cov = fit.cov_params()
                interaction_beta = float(fit.params[interaction_col])
                interaction_se = float(fit.bse[interaction_col])

                for state_label, quantile in (("low", low_quantile), ("high", high_quantile)):
                    state_value = float(sample[column].quantile(quantile))
                    state_centered_value = state_value - state_mean
                    beta = float(fit.params[shock_col] + state_centered_value * interaction_beta)
                    beta_var = float(
                        cov.loc[shock_col, shock_col]
                        + (state_centered_value**2) * cov.loc[interaction_col, interaction_col]
                        + 2.0 * state_centered_value * cov.loc[shock_col, interaction_col]
                    )
                    se = float(np.sqrt(max(beta_var, 0.0)))

                    rows.append(
                        {
                            "state_variant": str(state_name),
                            "state_role": state_role,
                            "state_column": column,
                            "state_label": state_label,
                            "state_quantile": float(quantile),
                            "state_value": state_value,
                            "state_mean": state_mean,
                            "state_centered_value": state_centered_value,
                            "outcome": outcome,
                            "horizon": int(horizon),
                            "beta": beta,
                            "se": se,
                            "lower95": beta - 1.96 * se,
                            "upper95": beta + 1.96 * se,
                            "interaction_beta": interaction_beta,
                            "interaction_se": interaction_se,
                            "interaction_lower95": interaction_beta - 1.96 * interaction_se,
                            "interaction_upper95": interaction_beta + 1.96 * interaction_se,
                            "n": int(fit.nobs),
                            "spec_name": spec_name,
                            "shock_column": shock_col,
                            "shock_scale": shock_scale,
                            "response_type": response_type,
                        }
                    )

    if not rows:
        return pd.DataFrame(columns=output_columns)
    return pd.DataFrame(rows, columns=output_columns)


def run_factor_augmented_control_sensitivity(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str] = (),
    include_lagged_outcome: bool = False,
    horizons: Sequence[int] = tuple(range(0, 9)),
    nw_lags: int = 4,
    cumulative: bool = True,
    factor_variants: Mapping[str, Mapping[str, object]],
    spec_name: str = "factor_control_sensitivity",
) -> pd.DataFrame:
    output_columns = [
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
    ]

    frames: list[pd.DataFrame] = []
    for variant_name, variant_spec in factor_variants.items():
        factor_role = str(variant_spec["factor_role"])
        source_columns = [str(col) for col in variant_spec.get("source_columns", [])]
        factor_count = int(variant_spec.get("factor_count", 0))
        min_train_obs = int(variant_spec.get("min_train_obs", 40))
        factor_prefix = str(variant_spec.get("factor_prefix", variant_name))

        augmented = df.copy()
        factor_frame = _recursive_factor_controls(
            augmented,
            source_columns=source_columns,
            factor_count=factor_count,
            min_train_obs=min_train_obs,
            prefix=factor_prefix,
        )
        factor_columns = factor_frame.columns.tolist()
        augmented = augmented.join(factor_frame)
        control_lp = run_local_projections(
            augmented,
            shock_col=shock_col,
            outcome_cols=outcome_cols,
            controls=[*controls, *factor_columns],
            include_lagged_outcome=include_lagged_outcome,
            horizons=horizons,
            nw_lags=nw_lags,
            cumulative=cumulative,
            spec_name=spec_name,
        )
        if control_lp.empty:
            continue
        control_lp.insert(0, "factor_variant", str(variant_name))
        control_lp.insert(1, "factor_role", factor_role)
        control_lp.insert(2, "factor_columns", "|".join(factor_columns))
        control_lp.insert(3, "source_columns", "|".join(source_columns))
        control_lp.insert(4, "factor_count", factor_count)
        control_lp.insert(5, "min_train_obs", min_train_obs)
        frames.append(control_lp)

    if not frames:
        return pd.DataFrame(columns=output_columns)
    return pd.concat(frames, ignore_index=True)[output_columns]


def run_period_sensitivity(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str] = (),
    include_lagged_outcome: bool = False,
    horizons: Sequence[int] = tuple(range(0, 9)),
    nw_lags: int = 4,
    cumulative: bool = True,
    period_variants: Mapping[str, Mapping[str, object]],
    spec_name: str = "period_sensitivity",
) -> pd.DataFrame:
    if "quarter" not in df.columns:
        raise KeyError("Missing required quarter column for period sensitivity.")

    output_columns = [
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
    ]

    frames: list[pd.DataFrame] = []
    for variant_name, variant_spec in period_variants.items():
        period_role = str(variant_spec["period_role"])
        start_quarter = None if variant_spec.get("start_quarter") is None else str(variant_spec["start_quarter"])
        end_quarter = None if variant_spec.get("end_quarter") is None else str(variant_spec["end_quarter"])
        sample_mask = _quarter_mask(
            df["quarter"],
            start_quarter=start_quarter,
            end_quarter=end_quarter,
        )
        period_lp = _run_lp_on_sample(
            df,
            shock_col=shock_col,
            outcome_cols=outcome_cols,
            controls=controls,
            include_lagged_outcome=include_lagged_outcome,
            horizons=horizons,
            nw_lags=nw_lags,
            cumulative=cumulative,
            spec_name=spec_name,
            sample_mask=sample_mask,
        )
        if period_lp.empty:
            continue
        period_lp.insert(0, "period_variant", str(variant_name))
        period_lp.insert(1, "period_role", period_role)
        period_lp.insert(2, "start_quarter", start_quarter)
        period_lp.insert(3, "end_quarter", end_quarter)
        frames.append(period_lp)

    if not frames:
        return pd.DataFrame(columns=output_columns)
    return pd.concat(frames, ignore_index=True)[output_columns]


def run_lp_from_specs(
    df: pd.DataFrame,
    *,
    lp_specs: Mapping[str, object],
    regime_specs: Mapping[str, object],
) -> dict[str, pd.DataFrame]:
    specs_obj = lp_specs.get("specs")
    if not isinstance(specs_obj, Mapping):
        raise ValueError("lp_specs must contain a 'specs' mapping.")

    baseline = specs_obj.get("baseline")
    regimes = specs_obj.get("regimes")
    state_dependence = specs_obj.get("state_dependence", {})
    sensitivity = specs_obj.get("sensitivity")
    control_sensitivity = specs_obj.get("control_sensitivity")
    factor_control_sensitivity = specs_obj.get("factor_control_sensitivity", {})
    sample_sensitivity = specs_obj.get("sample_sensitivity")
    period_sensitivity = specs_obj.get("period_sensitivity", {})
    if not isinstance(baseline, Mapping):
        raise ValueError("lp_specs.specs.baseline is required.")
    if not isinstance(regimes, Mapping):
        raise ValueError("lp_specs.specs.regimes is required.")
    if not isinstance(state_dependence, Mapping):
        raise ValueError("lp_specs.specs.state_dependence must be a mapping when provided.")
    if not isinstance(sensitivity, Mapping):
        raise ValueError("lp_specs.specs.sensitivity is required.")
    if not isinstance(control_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.control_sensitivity is required.")
    if not isinstance(factor_control_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.factor_control_sensitivity must be a mapping when provided.")
    if not isinstance(sample_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.sample_sensitivity is required.")
    if not isinstance(period_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.period_sensitivity must be a mapping when provided.")

    baseline_df = run_local_projections(
        df,
        shock_col=str(baseline["shock_column"]),
        outcome_cols=[str(col) for col in baseline["outcomes"]],
        controls=[str(col) for col in baseline.get("controls", [])],
        include_lagged_outcome=bool(baseline.get("include_lagged_outcome", False)),
        horizons=[int(h) for h in baseline["horizons"]],
        nw_lags=int(baseline.get("nw_lags", 4)),
        cumulative=bool(baseline.get("cumulative", True)),
        spec_name="baseline",
    )

    regime_columns = {str(col) for col in regimes.get("regime_columns", [])}
    regime_catalog_obj = regime_specs.get("regimes")
    if not isinstance(regime_catalog_obj, Mapping):
        raise ValueError("regime_specs must contain a 'regimes' mapping.")
    selected_regimes = {
        str(name): definition
        for name, definition in regime_catalog_obj.items()
        if isinstance(definition, Mapping) and str(definition.get("column")) in regime_columns
    }
    regimes_df = run_regime_split_local_projections(
        df,
        shock_col=str(regimes["shock_column"]),
        outcome_cols=[str(col) for col in regimes["outcomes"]],
        controls=[str(col) for col in regimes.get("controls", [])],
        include_lagged_outcome=bool(regimes.get("include_lagged_outcome", False)),
        horizons=[int(h) for h in regimes["horizons"]],
        nw_lags=int(regimes.get("nw_lags", 4)),
        cumulative=bool(regimes.get("cumulative", True)),
        regime_definitions=selected_regimes,
        spec_name="regimes",
    )

    state_dependence_frames: list[pd.DataFrame] = []
    state_variants = state_dependence.get("state_variants", {})
    if isinstance(state_variants, Mapping) and state_variants:
        selected_states: dict[str, dict[str, object]] = {}
        for state_name, state_spec in state_variants.items():
            if not isinstance(state_spec, Mapping):
                continue
            if "state_role" not in state_spec:
                raise ValueError(
                    f"lp_specs state_dependence variant '{state_name}' is missing required state_role."
                )
            state_role = str(state_spec["state_role"])
            if state_role not in {"headline", "core", "exploratory"}:
                raise ValueError(
                    f"lp_specs state_dependence variant '{state_name}' has unsupported state_role: {state_role}"
                )
            regime_definition = regime_catalog_obj.get(str(state_name))
            if not isinstance(regime_definition, Mapping):
                raise ValueError(
                    f"lp_specs state_dependence variant '{state_name}' does not match a regime in regime_specs."
                )
            selected_states[str(state_name)] = {
                "column": str(regime_definition["column"]),
                "state_role": state_role,
                "low_quantile": state_spec.get("low_quantile", 0.25),
                "high_quantile": state_spec.get("high_quantile", 0.75),
            }
        if selected_states:
            state_dependence_df = run_state_dependent_local_projections(
                df,
                shock_col=str(state_dependence.get("shock_column", baseline["shock_column"])),
                outcome_cols=[str(col) for col in state_dependence.get("outcomes", baseline["outcomes"])],
                controls=[str(col) for col in state_dependence.get("controls", baseline.get("controls", []))],
                include_lagged_outcome=bool(
                    state_dependence.get("include_lagged_outcome", baseline.get("include_lagged_outcome", False))
                ),
                horizons=[int(h) for h in state_dependence.get("horizons", baseline["horizons"])],
                nw_lags=int(state_dependence.get("nw_lags", baseline.get("nw_lags", 4))),
                cumulative=bool(state_dependence.get("cumulative", baseline.get("cumulative", True))),
                state_definitions=selected_states,
                spec_name="state_dependence",
            )
            if not state_dependence_df.empty:
                state_dependence_frames.append(state_dependence_df)
    if state_dependence_frames:
        state_dependence_out = pd.concat(state_dependence_frames, ignore_index=True)
    else:
        state_dependence_out = pd.DataFrame(
            columns=[
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
            ]
        )

    sensitivity_frames: list[pd.DataFrame] = []
    shock_variants = sensitivity.get("shock_variants", {})
    if isinstance(shock_variants, Mapping):
        for variant_name, variant_spec in shock_variants.items():
            if not isinstance(variant_spec, Mapping):
                continue
            if "treatment_role" not in variant_spec:
                raise ValueError(f"lp_specs sensitivity shock variant '{variant_name}' is missing required treatment_role.")
            treatment_role = str(variant_spec["treatment_role"])
            if treatment_role not in {"core", "exploratory"}:
                raise ValueError(
                    f"lp_specs sensitivity shock variant '{variant_name}' has unsupported treatment_role: {treatment_role}"
                )
            shock_col = str(variant_spec["shock_column"])
            sensitivity_lp = run_local_projections(
                df,
                shock_col=shock_col,
                outcome_cols=[str(col) for col in sensitivity["outcomes"]],
                controls=[str(col) for col in sensitivity.get("controls", [])],
                include_lagged_outcome=bool(sensitivity.get("include_lagged_outcome", False)),
                horizons=[int(h) for h in sensitivity["horizons"]],
                nw_lags=int(sensitivity.get("nw_lags", 4)),
                cumulative=bool(sensitivity.get("cumulative", True)),
                spec_name="sensitivity",
            )
            if sensitivity_lp.empty:
                continue
            sensitivity_lp.insert(0, "treatment_variant", str(variant_name))
            sensitivity_lp.insert(1, "treatment_role", treatment_role)
            sensitivity_frames.append(sensitivity_lp)
    else:
        for treatment in sensitivity.get("treatments", []):
            treatment_col = str(treatment)
            sensitivity_lp = run_local_projections(
                df,
                shock_col=str(sensitivity["shock_column"]),
                outcome_cols=[str(col) for col in sensitivity["outcomes"]],
                controls=[treatment_col],
                include_lagged_outcome=bool(sensitivity.get("include_lagged_outcome", False)),
                horizons=[int(h) for h in sensitivity["horizons"]],
                nw_lags=int(sensitivity.get("nw_lags", 4)),
                cumulative=bool(sensitivity.get("cumulative", True)),
                spec_name="sensitivity",
            )
            if sensitivity_lp.empty:
                continue
            sensitivity_lp.insert(0, "treatment_variant", treatment_col)
            sensitivity_lp.insert(1, "treatment_role", "legacy_control_sensitivity")
            sensitivity_frames.append(sensitivity_lp)
    if sensitivity_frames:
        sensitivity_df = pd.concat(sensitivity_frames, ignore_index=True)
    else:
        sensitivity_df = pd.DataFrame(
            columns=[
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
            ]
        )

    control_sensitivity_frames: list[pd.DataFrame] = []
    control_variants = control_sensitivity.get("control_variants", {})
    if isinstance(control_variants, Mapping):
        for variant_name, variant_spec in control_variants.items():
            if not isinstance(variant_spec, Mapping):
                continue
            if "control_role" not in variant_spec:
                raise ValueError(
                    f"lp_specs control_sensitivity variant '{variant_name}' is missing required control_role."
                )
            control_role = str(variant_spec["control_role"])
            if control_role not in {"headline", "core", "exploratory"}:
                raise ValueError(
                    f"lp_specs control_sensitivity variant '{variant_name}' has unsupported control_role: {control_role}"
                )
            controls = [str(col) for col in variant_spec.get("controls", [])]
            control_lp = run_local_projections(
                df,
                shock_col=str(control_sensitivity["shock_column"]),
                outcome_cols=[str(col) for col in control_sensitivity["outcomes"]],
                controls=controls,
                include_lagged_outcome=bool(variant_spec.get("include_lagged_outcome", control_sensitivity.get("include_lagged_outcome", False))),
                horizons=[int(h) for h in control_sensitivity["horizons"]],
                nw_lags=int(control_sensitivity.get("nw_lags", 4)),
                cumulative=bool(control_sensitivity.get("cumulative", True)),
                spec_name="control_sensitivity",
            )
            if control_lp.empty:
                continue
            control_lp.insert(0, "control_variant", str(variant_name))
            control_lp.insert(1, "control_role", control_role)
            control_lp.insert(2, "control_columns", "|".join(controls))
            control_sensitivity_frames.append(control_lp)
    if control_sensitivity_frames:
        control_sensitivity_df = pd.concat(control_sensitivity_frames, ignore_index=True)
    else:
        control_sensitivity_df = pd.DataFrame(
            columns=[
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
            ]
        )

    factor_control_sensitivity_frames: list[pd.DataFrame] = []
    factor_variants = factor_control_sensitivity.get("factor_variants", {})
    if isinstance(factor_variants, Mapping):
        selected_factor_variants: dict[str, Mapping[str, object]] = {}
        for variant_name, variant_spec in factor_variants.items():
            if not isinstance(variant_spec, Mapping):
                continue
            if "factor_role" not in variant_spec:
                raise ValueError(
                    f"lp_specs factor_control_sensitivity variant '{variant_name}' is missing required factor_role."
                )
            factor_role = str(variant_spec["factor_role"])
            if factor_role not in {"headline", "core", "exploratory"}:
                raise ValueError(
                    f"lp_specs factor_control_sensitivity variant '{variant_name}' has unsupported factor_role: {factor_role}"
                )
            selected_factor_variants[str(variant_name)] = variant_spec
        if selected_factor_variants:
            factor_control_sensitivity_df = run_factor_augmented_control_sensitivity(
                df,
                shock_col=str(factor_control_sensitivity.get("shock_column", control_sensitivity["shock_column"])),
                outcome_cols=[
                    str(col)
                    for col in factor_control_sensitivity.get("outcomes", control_sensitivity["outcomes"])
                ],
                controls=[
                    str(col)
                    for col in factor_control_sensitivity.get("controls", control_sensitivity.get("controls", []))
                ],
                include_lagged_outcome=bool(
                    factor_control_sensitivity.get(
                        "include_lagged_outcome",
                        control_sensitivity.get("include_lagged_outcome", False),
                    )
                ),
                horizons=[
                    int(h)
                    for h in factor_control_sensitivity.get("horizons", control_sensitivity["horizons"])
                ],
                nw_lags=int(factor_control_sensitivity.get("nw_lags", control_sensitivity.get("nw_lags", 4))),
                cumulative=bool(
                    factor_control_sensitivity.get("cumulative", control_sensitivity.get("cumulative", True))
                ),
                factor_variants=selected_factor_variants,
                spec_name="factor_control_sensitivity",
            )
            if not factor_control_sensitivity_df.empty:
                factor_control_sensitivity_frames.append(factor_control_sensitivity_df)
    if factor_control_sensitivity_frames:
        factor_control_sensitivity_out = pd.concat(factor_control_sensitivity_frames, ignore_index=True)
    else:
        factor_control_sensitivity_out = pd.DataFrame(
            columns=[
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
            ]
        )

    sample_sensitivity_frames: list[pd.DataFrame] = []
    sample_variants = sample_sensitivity.get("sample_variants", {})
    if isinstance(sample_variants, Mapping):
        for variant_name, variant_spec in sample_variants.items():
            if not isinstance(variant_spec, Mapping):
                continue
            if "sample_role" not in variant_spec:
                raise ValueError(
                    f"lp_specs sample_sensitivity variant '{variant_name}' is missing required sample_role."
                )
            sample_role = str(variant_spec["sample_role"])
            if sample_role not in {"headline", "core", "exploratory"}:
                raise ValueError(
                    f"lp_specs sample_sensitivity variant '{variant_name}' has unsupported sample_role: {sample_role}"
                )
            flag_column = str(variant_spec.get("flag_column", "shock_flag"))
            exclude_flagged = bool(variant_spec.get("exclude_flagged_shocks", False))
            sample_mask = pd.Series(True, index=df.index, dtype=bool)
            sample_filters: list[str] = []
            if exclude_flagged:
                if flag_column not in df.columns:
                    raise KeyError(f"Missing sample_sensitivity flag column: {flag_column}")
                sample_mask = df[flag_column].fillna("").astype(str).eq("")
                sample_filters.append(f"{flag_column}==''")
            max_value_column = variant_spec.get("max_value_column")
            if max_value_column is not None:
                max_value_column = str(max_value_column)
                if max_value_column not in df.columns:
                    raise KeyError(f"Missing sample_sensitivity max_value_column: {max_value_column}")
                max_value = float(variant_spec["max_value"])
                sample_mask = sample_mask & df[max_value_column].le(max_value)
                sample_filters.append(f"{max_value_column}<={max_value}")
            sample_filter = "all_usable_shocks" if not sample_filters else " & ".join(sample_filters)
            sample_lp = _run_lp_on_sample(
                df,
                shock_col=str(sample_sensitivity["shock_column"]),
                outcome_cols=[str(col) for col in sample_sensitivity["outcomes"]],
                controls=[str(col) for col in sample_sensitivity.get("controls", [])],
                include_lagged_outcome=bool(sample_sensitivity.get("include_lagged_outcome", False)),
                horizons=[int(h) for h in sample_sensitivity["horizons"]],
                nw_lags=int(sample_sensitivity.get("nw_lags", 4)),
                cumulative=bool(sample_sensitivity.get("cumulative", True)),
                spec_name="sample_sensitivity",
                sample_mask=sample_mask,
            )
            if sample_lp.empty:
                continue
            sample_lp.insert(0, "sample_variant", str(variant_name))
            sample_lp.insert(1, "sample_role", sample_role)
            sample_lp.insert(2, "sample_filter", sample_filter)
            sample_sensitivity_frames.append(sample_lp)
    if sample_sensitivity_frames:
        sample_sensitivity_df = pd.concat(sample_sensitivity_frames, ignore_index=True)
    else:
        sample_sensitivity_df = pd.DataFrame(
            columns=[
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
            ]
        )

    period_sensitivity_frames: list[pd.DataFrame] = []
    period_variants = period_sensitivity.get("period_variants", {})
    if isinstance(period_variants, Mapping):
        selected_period_variants: dict[str, Mapping[str, object]] = {}
        for variant_name, variant_spec in period_variants.items():
            if not isinstance(variant_spec, Mapping):
                continue
            if "period_role" not in variant_spec:
                raise ValueError(
                    f"lp_specs period_sensitivity variant '{variant_name}' is missing required period_role."
                )
            period_role = str(variant_spec["period_role"])
            if period_role not in {"headline", "core", "exploratory"}:
                raise ValueError(
                    f"lp_specs period_sensitivity variant '{variant_name}' has unsupported period_role: {period_role}"
                )
            selected_period_variants[str(variant_name)] = variant_spec
        if selected_period_variants:
            period_sensitivity_df = run_period_sensitivity(
                df,
                shock_col=str(period_sensitivity.get("shock_column", baseline["shock_column"])),
                outcome_cols=[str(col) for col in period_sensitivity.get("outcomes", baseline["outcomes"])],
                controls=[str(col) for col in period_sensitivity.get("controls", baseline.get("controls", []))],
                include_lagged_outcome=bool(
                    period_sensitivity.get("include_lagged_outcome", baseline.get("include_lagged_outcome", False))
                ),
                horizons=[int(h) for h in period_sensitivity.get("horizons", baseline["horizons"])],
                nw_lags=int(period_sensitivity.get("nw_lags", baseline.get("nw_lags", 4))),
                cumulative=bool(period_sensitivity.get("cumulative", baseline.get("cumulative", True))),
                period_variants=selected_period_variants,
                spec_name="period_sensitivity",
            )
            if not period_sensitivity_df.empty:
                period_sensitivity_frames.append(period_sensitivity_df)
    if period_sensitivity_frames:
        period_sensitivity_out = pd.concat(period_sensitivity_frames, ignore_index=True)
    else:
        period_sensitivity_out = pd.DataFrame(
            columns=[
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
            ]
        )

    return {
        "lp_irf": baseline_df,
        "lp_irf_regimes": regimes_df,
        "lp_irf_state_dependence": state_dependence_out,
        "tdc_sensitivity_ladder": sensitivity_df,
        "control_set_sensitivity": control_sensitivity_df,
        "factor_control_sensitivity": factor_control_sensitivity_out,
        "shock_sample_sensitivity": sample_sensitivity_df,
        "period_sensitivity": period_sensitivity_out,
    }
