from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


def _resolve_threshold(series: pd.Series, threshold_value: object) -> float:
    if isinstance(threshold_value, str):
        if threshold_value == "median":
            return float(series.median(skipna=True))
        raise ValueError(f"Unsupported regime threshold rule: {threshold_value}")
    return float(threshold_value)


def _lp_row(df: pd.DataFrame, *, regime: str, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = df[(df["regime"] == regime) & (df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _row_snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
    }


def build_regime_diagnostics_summary(
    *,
    panel: pd.DataFrame,
    regime_specs: Mapping[str, Any],
    selected_regime_columns: set[str],
    lp_irf_regimes: pd.DataFrame,
    shock_column: str,
    controls: list[str],
    min_effective_obs: int = 16,
    min_shock_support_ratio: float = 0.4,
) -> dict[str, Any]:
    regimes_obj = regime_specs.get("regimes", {})
    if not isinstance(regimes_obj, Mapping):
        raise ValueError("regime_specs must contain a 'regimes' mapping.")

    regime_rows: list[dict[str, Any]] = []
    takeaways: list[str] = []
    informative_count = 0
    stable_count = 0
    full_sample = panel.dropna(subset=[shock_column, *controls])
    full_sample_shock_std = float(full_sample[shock_column].std(ddof=1)) if len(full_sample) >= 2 else 0.0

    for regime_name, regime_def in regimes_obj.items():
        if not isinstance(regime_def, Mapping):
            continue
        column = str(regime_def["column"])
        if column not in selected_regime_columns or column not in panel.columns:
            continue

        threshold_rule = regime_def.get("threshold", 0.5)
        threshold_source = panel.dropna(subset=[shock_column, column, *controls])
        threshold_value = _resolve_threshold(threshold_source[column], threshold_rule)
        high_mask = threshold_source[column] >= threshold_value
        low_mask = threshold_source[column] < threshold_value
        high_sample = threshold_source.loc[high_mask]
        low_sample = threshold_source.loc[low_mask]
        high_shock_std = float(high_sample[shock_column].std(ddof=1)) if len(high_sample) >= 2 else 0.0
        low_shock_std = float(low_sample[shock_column].std(ddof=1)) if len(low_sample) >= 2 else 0.0
        high_support_ratio = high_shock_std / full_sample_shock_std if full_sample_shock_std > 0 else 0.0
        low_support_ratio = low_shock_std / full_sample_shock_std if full_sample_shock_std > 0 else 0.0

        high_label = f"{regime_name}_high"
        low_label = f"{regime_name}_low"
        high_h0 = _lp_row(lp_irf_regimes, regime=high_label, outcome="total_deposits_bank_qoq", horizon=0)
        low_h0 = _lp_row(lp_irf_regimes, regime=low_label, outcome="total_deposits_bank_qoq", horizon=0)
        high_h4 = _lp_row(lp_irf_regimes, regime=high_label, outcome="total_deposits_bank_qoq", horizon=4)
        low_h4 = _lp_row(lp_irf_regimes, regime=low_label, outcome="total_deposits_bank_qoq", horizon=4)

        informative = (
            int(high_mask.sum()) >= min_effective_obs
            and int(low_mask.sum()) >= min_effective_obs
            and high_h0 is not None
            and low_h0 is not None
        )
        if informative:
            informative_count += 1
        stability_warnings: list[str] = []
        if informative and high_support_ratio < min_shock_support_ratio:
            stability_warnings.append("high_state_shock_support_is_thin")
        if informative and low_support_ratio < min_shock_support_ratio:
            stability_warnings.append("low_state_shock_support_is_thin")
        stable_for_interpretation = informative and not stability_warnings
        if stable_for_interpretation:
            stable_count += 1

        regime_rows.append(
            {
                "regime": str(regime_name),
                "column": column,
                "publication_role": str(regime_def.get("publication_role", "published")),
                "threshold_rule": str(threshold_rule),
                "threshold_value": threshold_value,
                "high_rows": int(high_mask.sum()),
                "low_rows": int(low_mask.sum()),
                "high_share": float(high_mask.mean()) if len(threshold_source) else 0.0,
                "low_share": float(low_mask.mean()) if len(threshold_source) else 0.0,
                "informative": informative,
                "stable_for_interpretation": stable_for_interpretation,
                "stability_warnings": stability_warnings,
                "full_sample_shock_std": full_sample_shock_std,
                "high_shock_std": high_shock_std,
                "low_shock_std": low_shock_std,
                "high_shock_support_ratio": high_support_ratio,
                "low_shock_support_ratio": low_support_ratio,
                "total_deposits_h0_high": _row_snapshot(high_h0),
                "total_deposits_h0_low": _row_snapshot(low_h0),
                "total_deposits_h4_high": _row_snapshot(high_h4),
                "total_deposits_h4_low": _row_snapshot(low_h4),
            }
        )

    if not regime_rows:
        takeaways.append("No configured regime splits were active in the LP export.")
    else:
        degenerate = [row["regime"] for row in regime_rows if row["high_rows"] == 0 or row["low_rows"] == 0]
        if degenerate:
            takeaways.append(
                "Some regime splits are degenerate in the current sample: " + ", ".join(degenerate) + "."
            )
        sparse = [row["regime"] for row in regime_rows if not row["informative"]]
        if sparse and len(sparse) != len(regime_rows):
            takeaways.append(
                "Some regime splits remain too sparse for stable interpretation: " + ", ".join(sparse) + "."
            )
        support_thin = [row["regime"] for row in regime_rows if row["informative"] and not row["stable_for_interpretation"]]
        if support_thin:
            takeaways.append(
                "Some regime splits have thin within-state shock support and remain extrapolative: "
                + ", ".join(support_thin)
                + "."
            )
        if informative_count == 0:
            takeaways.append("Current regime splits do not yet provide enough balanced rows for reliable interpretation.")
        else:
            takeaways.append(f"{informative_count} regime splits have enough balanced rows for first-pass interpretation.")
        if stable_count > 0:
            takeaways.append(f"{stable_count} regime splits also have enough within-state shock support for cautious interpretation.")

    return {
        "informative_regime_count": informative_count,
        "stable_regime_count": stable_count,
        "regimes": regime_rows,
        "takeaways": takeaways,
    }
