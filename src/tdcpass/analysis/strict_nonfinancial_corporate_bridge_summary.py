from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_households_nonprofits_qoq",
    "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_households_nonprofits_qoq",
    "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
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


def _ensure_private_total_column(shocked: pd.DataFrame) -> pd.DataFrame:
    if "strict_di_loans_nec_private_domestic_borrower_qoq" in shocked.columns:
        return shocked
    required = (
        "strict_di_loans_nec_households_nonprofits_qoq",
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
    )
    if not all(column in shocked.columns for column in required):
        return shocked
    panel = shocked.copy()
    panel["strict_di_loans_nec_private_domestic_borrower_qoq"] = panel[list(required)].sum(
        axis=1,
        min_count=len(required),
    )
    lag_col = "lag_strict_di_loans_nec_private_domestic_borrower_qoq"
    if lag_col not in panel.columns:
        panel[lag_col] = panel["strict_di_loans_nec_private_domestic_borrower_qoq"].shift(1)
    return panel


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _bridge_payload(
    *,
    residual: dict[str, Any] | None,
    private_total: dict[str, Any] | None,
    nonfinancial_corporate: dict[str, Any] | None,
    households: dict[str, Any] | None,
    nonfinancial_noncorporate: dict[str, Any] | None,
) -> dict[str, Any]:
    private_beta = None if private_total is None else float(private_total["beta"])
    corp_beta = None if nonfinancial_corporate is None else float(nonfinancial_corporate["beta"])
    households_beta = None if households is None else float(households["beta"])
    noncorp_beta = None if nonfinancial_noncorporate is None else float(nonfinancial_noncorporate["beta"])

    corporate_minus_private = None
    if corp_beta is not None and private_beta is not None:
        corporate_minus_private = corp_beta - private_beta
    corporate_minus_residual = None
    if corp_beta is not None and residual is not None:
        corporate_minus_residual = corp_beta - float(residual["beta"])
    offset_total = None
    if households_beta is not None and noncorp_beta is not None:
        offset_total = households_beta + noncorp_beta

    interpretation = "nonfinancial_corporate_bridge_not_classified"
    corporate_share = _ratio(corp_beta, private_beta)
    if corporate_share is not None:
        if corporate_share > 0.8:
            interpretation = "nonfinancial_corporate_explains_private_bridge"
        else:
            interpretation = "nonfinancial_corporate_incomplete_private_bridge"

    return {
        "core_residual_response": residual,
        "private_bridge_response": private_total,
        "nonfinancial_corporate_response": nonfinancial_corporate,
        "households_nonprofits_response": households,
        "nonfinancial_noncorporate_response": nonfinancial_noncorporate,
        "nonfinancial_corporate_share_of_private_bridge_beta": corporate_share,
        "households_share_of_private_bridge_beta": _ratio(households_beta, private_beta),
        "nonfinancial_noncorporate_share_of_private_bridge_beta": _ratio(noncorp_beta, private_beta),
        "households_plus_nonfinancial_noncorporate_beta": offset_total,
        "nonfinancial_corporate_minus_private_bridge_beta": corporate_minus_private,
        "nonfinancial_corporate_minus_core_residual_beta": corporate_minus_residual,
        "interpretation": interpretation,
    }


def build_strict_nonfinancial_corporate_bridge_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_private_borrower_bridge_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_private_borrower_bridge_summary is None:
        return {"status": "not_available", "reason": "strict_private_borrower_bridge_summary_not_available"}
    if str(strict_private_borrower_bridge_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_private_borrower_bridge_summary_not_available"}

    panel = _ensure_private_total_column(shocked)
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
        spec_name="strict_nonfinancial_corporate_bridge_baseline_reference",
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
        spec_name="strict_nonfinancial_corporate_bridge_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _bridge_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            private_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            households=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_households_nonprofits_qoq", horizon=horizon)
            ),
            nonfinancial_noncorporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_noncorporate_qoq", horizon=horizon)
            ),
        )
        core_payload = _bridge_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            private_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            households=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_households_nonprofits_qoq", horizon=horizon)
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
    takeaways = [
        "This surface narrows the private-borrower bridge again, treating the nonfinancial-corporate block as the next active bridge object.",
        "It keeps households/nonprofits and nonfinancial-noncorporate visible as offsets instead of burying them inside the private total.",
    ]
    corp_beta = dict(h0_core.get("nonfinancial_corporate_response", {}) or {}).get("beta")
    private_beta = dict(h0_core.get("private_bridge_response", {}) or {}).get("beta")
    hh_beta = dict(h0_core.get("households_nonprofits_response", {}) or {}).get("beta")
    noncorp_beta = dict(h0_core.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    if None not in (corp_beta, private_beta, hh_beta, noncorp_beta):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the nonfinancial-corporate bridge is explicit: "
            f"nonfinancial corporate ≈ {float(corp_beta):.2f}, private total ≈ {float(private_beta):.2f}, "
            f"households/nonprofits ≈ {float(hh_beta):.2f}, nonfinancial noncorporate ≈ {float(noncorp_beta):.2f}."
        )
    corp_share = h0_core.get("nonfinancial_corporate_share_of_private_bridge_beta")
    if corp_share is not None:
        takeaways.append(
            f"Nonfinancial corporate now carries about {float(corp_share):.2f} of the signed private bridge at h0, so the remaining private residual should be interpreted as an offset problem, not as the main bridge."
        )

    return {
        "status": "available",
        "headline_question": "How much of the narrowed private-borrower bridge is really a nonfinancial-corporate bridge once the smaller offsets are kept visible?",
        "estimation_path": {
            "summary_artifact": "strict_nonfinancial_corporate_bridge_summary.json",
            "baseline_spec": "identity_baseline",
            "comparison_spec": "unexpected_tdc_core_deposit_proximate_bank_only",
        },
        "bridge_definitions": {
            "private_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
            "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
            "households_nonprofits": "strict_di_loans_nec_households_nonprofits_qoq",
            "nonfinancial_noncorporate": "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        },
        "recommendation": {
            "status": "nonfinancial_corporate_bridge_first",
            "next_branch": "assess_household_and_nonfinancial_noncorporate_offset_residual",
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
