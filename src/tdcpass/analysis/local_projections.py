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


def _run_lp_on_sample(
    df: pd.DataFrame,
    *,
    shock_col: str,
    outcome_cols: Sequence[str],
    controls: Sequence[str],
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
        for horizon in horizons:
            dep = _forward_transform(df[outcome], horizon, cumulative=cumulative)
            sample = pd.DataFrame({"dep": dep, shock_col: df[shock_col]})
            for control in controls:
                sample[control] = df[control]
            if sample_mask is not None:
                sample = sample.loc[sample_mask.fillna(False)]
            sample = sample.dropna()
            if len(sample) < len(controls) + 12:
                continue

            x = sm.add_constant(sample[[shock_col, *controls]])
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
    sensitivity = specs_obj.get("sensitivity")
    control_sensitivity = specs_obj.get("control_sensitivity")
    sample_sensitivity = specs_obj.get("sample_sensitivity")
    if not isinstance(baseline, Mapping):
        raise ValueError("lp_specs.specs.baseline is required.")
    if not isinstance(regimes, Mapping):
        raise ValueError("lp_specs.specs.regimes is required.")
    if not isinstance(sensitivity, Mapping):
        raise ValueError("lp_specs.specs.sensitivity is required.")
    if not isinstance(control_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.control_sensitivity is required.")
    if not isinstance(sample_sensitivity, Mapping):
        raise ValueError("lp_specs.specs.sample_sensitivity is required.")

    baseline_df = run_local_projections(
        df,
        shock_col=str(baseline["shock_column"]),
        outcome_cols=[str(col) for col in baseline["outcomes"]],
        controls=[str(col) for col in baseline.get("controls", [])],
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
        horizons=[int(h) for h in regimes["horizons"]],
        nw_lags=int(regimes.get("nw_lags", 4)),
        cumulative=bool(regimes.get("cumulative", True)),
        regime_definitions=selected_regimes,
        spec_name="regimes",
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
            sample_filter = "all_usable_shocks"
            if exclude_flagged:
                if flag_column not in df.columns:
                    raise KeyError(f"Missing sample_sensitivity flag column: {flag_column}")
                sample_mask = df[flag_column].fillna("").astype(str).eq("")
                sample_filter = f"{flag_column}==''"
            sample_lp = _run_lp_on_sample(
                df,
                shock_col=str(sample_sensitivity["shock_column"]),
                outcome_cols=[str(col) for col in sample_sensitivity["outcomes"]],
                controls=[str(col) for col in sample_sensitivity.get("controls", [])],
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

    return {
        "lp_irf": baseline_df,
        "lp_irf_regimes": regimes_df,
        "tdc_sensitivity_ladder": sensitivity_df,
        "control_set_sensitivity": control_sensitivity_df,
        "shock_sample_sensitivity": sample_sensitivity_df,
    }
