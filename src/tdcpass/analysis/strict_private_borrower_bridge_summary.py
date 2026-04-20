from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

PRIVATE_COMPONENT_COLUMNS: tuple[str, ...] = (
    "strict_di_loans_nec_households_nonprofits_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
)

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    *PRIVATE_COMPONENT_COLUMNS,
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    *PRIVATE_COMPONENT_COLUMNS,
)


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    if df.empty or "outcome" not in df.columns or "horizon" not in df.columns:
        return None
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _comparison_controls(
    *,
    baseline_controls: Sequence[str],
    baseline_shock_spec: dict[str, Any],
    comparison_shock_spec: dict[str, Any],
) -> list[str]:
    baseline_predictors = {str(value) for value in baseline_shock_spec.get("predictors", [])}
    macro_controls = [str(value) for value in baseline_controls if str(value) not in baseline_predictors]
    comparison_predictors = [str(value) for value in comparison_shock_spec.get("predictors", [])]
    return _dedupe([*comparison_predictors, *macro_controls])


def _ensure_private_total_columns(shocked: pd.DataFrame) -> pd.DataFrame:
    if "strict_di_loans_nec_private_domestic_borrower_qoq" in shocked.columns:
        return shocked
    if not all(column in shocked.columns for column in PRIVATE_COMPONENT_COLUMNS):
        return shocked
    panel = shocked.copy()
    panel["strict_di_loans_nec_private_domestic_borrower_qoq"] = panel[list(PRIVATE_COMPONENT_COLUMNS)].sum(
        axis=1,
        min_count=len(PRIVATE_COMPONENT_COLUMNS),
    )
    lag_col = "lag_strict_di_loans_nec_private_domestic_borrower_qoq"
    if lag_col not in panel.columns:
        panel[lag_col] = panel["strict_di_loans_nec_private_domestic_borrower_qoq"].shift(1)
    return panel


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _private_payload(
    *,
    residual: dict[str, Any] | None,
    private_total: dict[str, Any] | None,
    households: dict[str, Any] | None,
    nonfinancial_corporate: dict[str, Any] | None,
    nonfinancial_noncorporate: dict[str, Any] | None,
) -> dict[str, Any]:
    private_beta = None if private_total is None else float(private_total["beta"])
    hh_beta = None if households is None else float(households["beta"])
    corp_beta = None if nonfinancial_corporate is None else float(nonfinancial_corporate["beta"])
    noncorp_beta = None if nonfinancial_noncorporate is None else float(nonfinancial_noncorporate["beta"])

    dominant_component = None
    if hh_beta is not None and corp_beta is not None and noncorp_beta is not None:
        component_pairs = [
            ("strict_di_loans_nec_households_nonprofits_qoq", hh_beta),
            ("strict_di_loans_nec_nonfinancial_corporate_qoq", corp_beta),
            ("strict_di_loans_nec_nonfinancial_noncorporate_qoq", noncorp_beta),
        ]
        dominant_component = max(component_pairs, key=lambda item: abs(item[1]))[0]

    interpretation = "private_bridge_not_classified"
    if dominant_component == "strict_di_loans_nec_nonfinancial_corporate_qoq":
        interpretation = "private_bridge_is_nonfinancial_corporate_dominant"
    elif dominant_component == "strict_di_loans_nec_nonfinancial_noncorporate_qoq":
        interpretation = "private_bridge_is_nonfinancial_noncorporate_dominant"
    elif dominant_component == "strict_di_loans_nec_households_nonprofits_qoq":
        interpretation = "private_bridge_is_household_dominant"

    residual_gap = None
    if residual is not None and private_total is not None:
        residual_gap = float(private_total["beta"]) - float(residual["beta"])

    return {
        "core_residual_response": residual,
        "private_bridge_response": private_total,
        "households_nonprofits_response": households,
        "nonfinancial_corporate_response": nonfinancial_corporate,
        "nonfinancial_noncorporate_response": nonfinancial_noncorporate,
        "dominant_private_component": dominant_component,
        "nonfinancial_corporate_share_of_private_bridge_beta": _ratio(corp_beta, private_beta),
        "households_share_of_private_bridge_beta": _ratio(hh_beta, private_beta),
        "nonfinancial_noncorporate_share_of_private_bridge_beta": _ratio(noncorp_beta, private_beta),
        "private_bridge_minus_core_residual_beta": residual_gap,
        "interpretation": interpretation,
    }


def build_strict_private_borrower_bridge_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_di_bucket_bridge_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_di_bucket_bridge_summary is None:
        return {"status": "not_available", "reason": "strict_di_bucket_bridge_summary_not_available"}
    if str(strict_di_bucket_bridge_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_di_bucket_bridge_summary_not_available"}

    panel = _ensure_private_total_columns(shocked)
    baseline_controls = [str(value) for value in baseline_lp_spec.get("controls", [])]
    core_controls = _comparison_controls(
        baseline_controls=baseline_controls,
        baseline_shock_spec=baseline_shock_spec,
        comparison_shock_spec=core_shock_spec,
    )
    baseline_horizons = [int(value) for value in baseline_lp_spec.get("horizons", [])]
    cumulative = bool(baseline_lp_spec.get("cumulative", True))
    include_lagged_outcome = bool(baseline_lp_spec.get("include_lagged_outcome", False))
    nw_lags = int(baseline_lp_spec.get("nw_lags", 4))

    baseline_lp_irf = run_local_projections(
        panel,
        shock_col=str(baseline_shock_spec.get("standardized_column", "tdc_residual_z")),
        outcome_cols=list(BASELINE_OUTCOMES),
        controls=baseline_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_private_borrower_bridge_baseline_reference",
    )
    core_lp_irf = run_local_projections(
        panel,
        shock_col=str(core_shock_spec.get("standardized_column", "tdc_core_deposit_proximate_bank_only_residual_z")),
        outcome_cols=list(CORE_OUTCOMES),
        controls=core_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_private_borrower_bridge_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _private_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            private_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            households=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_households_nonprofits_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            nonfinancial_noncorporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_noncorporate_qoq", horizon=horizon)
            ),
        )
        core_payload = _private_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            private_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            households=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_households_nonprofits_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            nonfinancial_noncorporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_nonfinancial_noncorporate_qoq", horizon=horizon)
            ),
        )
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "core_deposit_proximate": core_payload,
        }

    h0_core = dict(key_horizons.get("h0", {}).get("core_deposit_proximate", {}) or {})
    dominant_component = h0_core.get("dominant_private_component")
    next_branch = {
        "strict_di_loans_nec_nonfinancial_corporate_qoq": "build_nonfinancial_corporate_bridge_surface",
        "strict_di_loans_nec_households_nonprofits_qoq": "build_household_private_bridge_surface",
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq": "build_nonfinancial_noncorporate_bridge_surface",
    }.get(str(dominant_component), "build_nonfinancial_corporate_bridge_surface")

    takeaways = [
        "This surface narrows the DI-bucket bridge to the private-borrower slice before attempting a broader counterpart-alignment framework.",
        "It asks which private borrower block actually drives the bridge rather than treating the private total as a single undifferentiated add-on.",
    ]
    private_beta = dict(h0_core.get("private_bridge_response", {}) or {}).get("beta")
    hh_beta = dict(h0_core.get("households_nonprofits_response", {}) or {}).get("beta")
    corp_beta = dict(h0_core.get("nonfinancial_corporate_response", {}) or {}).get("beta")
    noncorp_beta = dict(h0_core.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    if None not in (private_beta, hh_beta, corp_beta, noncorp_beta):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the private bridge split is explicit: "
            f"private total ≈ {float(private_beta):.2f}, households/nonprofits ≈ {float(hh_beta):.2f}, "
            f"nonfinancial corporate ≈ {float(corp_beta):.2f}, nonfinancial noncorporate ≈ {float(noncorp_beta):.2f}."
        )
    corp_share = h0_core.get("nonfinancial_corporate_share_of_private_bridge_beta")
    if corp_share is not None and dominant_component is not None:
        takeaways.append(
            f"The dominant private component at h0 is `{str(dominant_component)}`, with nonfinancial-corporate share of the signed private bridge ≈ {float(corp_share):.2f}."
        )

    return {
        "status": "available",
        "headline_question": "Which private-borrower block actually drives the DI-bucket bridge once the bridge is narrowed away from the noncore/system piece?",
        "estimation_path": {
            "summary_artifact": "strict_private_borrower_bridge_summary.json",
            "baseline_spec": "identity_baseline",
            "comparison_spec": "unexpected_tdc_core_deposit_proximate_bank_only",
        },
        "bridge_definitions": {
            "private_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
            "households_nonprofits": "strict_di_loans_nec_households_nonprofits_qoq",
            "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
            "nonfinancial_noncorporate": "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        },
        "recommendation": {
            "status": "private_bridge_split_first",
            "dominant_private_component": dominant_component,
            "next_branch": next_branch,
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
