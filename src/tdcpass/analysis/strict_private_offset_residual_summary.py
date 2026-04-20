from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_private_offset_residual_qoq",
    "strict_di_loans_nec_private_minus_corporate_qoq",
    "strict_di_loans_nec_households_nonprofits_qoq",
    "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
)

CORE_OUTCOMES: tuple[str, ...] = (
    "other_component_core_deposit_proximate_bank_only_qoq",
    "strict_di_loans_nec_private_domestic_borrower_qoq",
    "strict_di_loans_nec_nonfinancial_corporate_qoq",
    "strict_di_loans_nec_private_offset_residual_qoq",
    "strict_di_loans_nec_private_minus_corporate_qoq",
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


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator) / float(denominator)


def _ensure_offset_columns(shocked: pd.DataFrame) -> pd.DataFrame:
    panel = shocked.copy()
    required = (
        "strict_di_loans_nec_households_nonprofits_qoq",
        "strict_di_loans_nec_nonfinancial_corporate_qoq",
        "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
    )
    if not all(column in panel.columns for column in required):
        return panel
    if "strict_di_loans_nec_private_domestic_borrower_qoq" not in panel.columns:
        panel["strict_di_loans_nec_private_domestic_borrower_qoq"] = panel[list(required)].sum(
            axis=1,
            min_count=len(required),
        )
    if "strict_di_loans_nec_private_offset_residual_qoq" not in panel.columns:
        panel["strict_di_loans_nec_private_offset_residual_qoq"] = panel[
            ["strict_di_loans_nec_households_nonprofits_qoq", "strict_di_loans_nec_nonfinancial_noncorporate_qoq"]
        ].sum(axis=1, min_count=2)
    if "strict_di_loans_nec_private_minus_corporate_qoq" not in panel.columns:
        panel["strict_di_loans_nec_private_minus_corporate_qoq"] = (
            panel["strict_di_loans_nec_private_domestic_borrower_qoq"]
            - panel["strict_di_loans_nec_nonfinancial_corporate_qoq"]
        )
    for column in (
        "strict_di_loans_nec_private_domestic_borrower_qoq",
        "strict_di_loans_nec_private_offset_residual_qoq",
        "strict_di_loans_nec_private_minus_corporate_qoq",
    ):
        lag_col = f"lag_{column}"
        if lag_col not in panel.columns:
            panel[lag_col] = panel[column].shift(1)
    return panel


def _offset_payload(
    *,
    residual: dict[str, Any] | None,
    private_total: dict[str, Any] | None,
    nonfinancial_corporate: dict[str, Any] | None,
    offset_total: dict[str, Any] | None,
    private_minus_corporate: dict[str, Any] | None,
    households: dict[str, Any] | None,
    nonfinancial_noncorporate: dict[str, Any] | None,
) -> dict[str, Any]:
    private_beta = None if private_total is None else float(private_total["beta"])
    corporate_beta = None if nonfinancial_corporate is None else float(nonfinancial_corporate["beta"])
    offset_beta = None if offset_total is None else float(offset_total["beta"])
    minus_corp_beta = None if private_minus_corporate is None else float(private_minus_corporate["beta"])
    households_beta = None if households is None else float(households["beta"])
    noncorp_beta = None if nonfinancial_noncorporate is None else float(nonfinancial_noncorporate["beta"])
    residual_beta = None if residual is None else float(residual["beta"])

    offset_alignment_gap = None
    if offset_beta is not None and minus_corp_beta is not None:
        offset_alignment_gap = offset_beta - minus_corp_beta

    offset_component_dominant = None
    if households_beta is not None and noncorp_beta is not None:
        offset_component_dominant = (
            "strict_di_loans_nec_nonfinancial_noncorporate_qoq"
            if abs(noncorp_beta) >= abs(households_beta)
            else "strict_di_loans_nec_households_nonprofits_qoq"
        )

    interpretation = "offset_residual_not_classified"
    signs_oppose = (
        households_beta is not None
        and noncorp_beta is not None
        and households_beta != 0.0
        and noncorp_beta != 0.0
        and households_beta * noncorp_beta < 0.0
    )
    if offset_beta is not None and private_beta is not None:
        if abs(float(offset_beta)) <= 0.25 * abs(float(private_beta)) and signs_oppose:
            interpretation = "offset_is_small_opposing_structure"
        elif offset_component_dominant == "strict_di_loans_nec_nonfinancial_noncorporate_qoq":
            interpretation = "offset_is_nonfinancial_noncorporate_dominant"
        elif offset_component_dominant == "strict_di_loans_nec_households_nonprofits_qoq":
            interpretation = "offset_is_household_dominant"

    return {
        "core_residual_response": residual,
        "private_bridge_response": private_total,
        "nonfinancial_corporate_response": nonfinancial_corporate,
        "private_offset_total_response": offset_total,
        "private_minus_corporate_response": private_minus_corporate,
        "households_nonprofits_response": households,
        "nonfinancial_noncorporate_response": nonfinancial_noncorporate,
        "private_offset_share_of_private_bridge_beta": _ratio(offset_beta, private_beta),
        "private_offset_share_of_core_residual_beta": _ratio(offset_beta, residual_beta),
        "households_share_of_private_offset_beta": _ratio(households_beta, offset_beta),
        "nonfinancial_noncorporate_share_of_private_offset_beta": _ratio(noncorp_beta, offset_beta),
        "offset_alignment_gap_beta": offset_alignment_gap,
        "offset_component_dominant": offset_component_dominant,
        "interpretation": interpretation,
    }


def build_strict_private_offset_residual_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    core_shock_spec: dict[str, Any],
    strict_nonfinancial_corporate_bridge_summary: dict[str, Any] | None,
    horizons: tuple[int, ...] = (0, 4),
) -> dict[str, Any]:
    if strict_nonfinancial_corporate_bridge_summary is None:
        return {"status": "not_available", "reason": "strict_nonfinancial_corporate_bridge_summary_not_available"}
    if str(strict_nonfinancial_corporate_bridge_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_nonfinancial_corporate_bridge_summary_not_available"}

    panel = _ensure_offset_columns(shocked)
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
        spec_name="strict_private_offset_residual_baseline_reference",
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
        spec_name="strict_private_offset_residual_core_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _offset_payload(
            residual=_snapshot(_lp_row(baseline_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            private_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            offset_total=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_offset_residual_qoq", horizon=horizon)
            ),
            private_minus_corporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_private_minus_corporate_qoq", horizon=horizon)
            ),
            households=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_households_nonprofits_qoq", horizon=horizon)
            ),
            nonfinancial_noncorporate=_snapshot(
                _lp_row(baseline_lp_irf, outcome="strict_di_loans_nec_nonfinancial_noncorporate_qoq", horizon=horizon)
            ),
        )
        core_payload = _offset_payload(
            residual=_snapshot(
                _lp_row(core_lp_irf, outcome="other_component_core_deposit_proximate_bank_only_qoq", horizon=horizon)
            ),
            private_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_domestic_borrower_qoq", horizon=horizon)
            ),
            nonfinancial_corporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_nonfinancial_corporate_qoq", horizon=horizon)
            ),
            offset_total=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_offset_residual_qoq", horizon=horizon)
            ),
            private_minus_corporate=_snapshot(
                _lp_row(core_lp_irf, outcome="strict_di_loans_nec_private_minus_corporate_qoq", horizon=horizon)
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
    offset_beta = dict(h0_core.get("private_offset_total_response", {}) or {}).get("beta")
    private_beta = dict(h0_core.get("private_bridge_response", {}) or {}).get("beta")
    corporate_beta = dict(h0_core.get("nonfinancial_corporate_response", {}) or {}).get("beta")
    hh_beta = dict(h0_core.get("households_nonprofits_response", {}) or {}).get("beta")
    noncorp_beta = dict(h0_core.get("nonfinancial_noncorporate_response", {}) or {}).get("beta")
    offset_interp = str(h0_core.get("interpretation", "offset_residual_not_classified"))
    next_branch = (
        "assess_corporate_bridge_secondary_comparison_role"
        if offset_interp == "offset_is_small_opposing_structure"
        else "assess_nonfinancial_noncorporate_offset_role"
    )

    takeaways = [
        "This surface isolates the remaining private detail after the nonfinancial-corporate bridge, treating households/nonprofits plus nonfinancial-noncorporate as the offset block.",
        "It asks whether the residual private detail is mostly offset structure or a meaningful rival bridge that should keep the broader private-borrower secondary comparison alive.",
    ]
    if None not in (offset_beta, private_beta, corporate_beta, hh_beta, noncorp_beta):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the private offset block is explicit: "
            f"offset total ≈ {float(offset_beta):.2f}, private total ≈ {float(private_beta):.2f}, "
            f"nonfinancial corporate ≈ {float(corporate_beta):.2f}, households/nonprofits ≈ {float(hh_beta):.2f}, "
            f"nonfinancial noncorporate ≈ {float(noncorp_beta):.2f}."
        )
    offset_share = h0_core.get("private_offset_share_of_private_bridge_beta")
    if offset_share is not None:
        takeaways.append(
            f"The offset block is about {float(offset_share):.2f} of the signed private bridge at h0, so it should be treated as a diagnostic residual around the corporate bridge rather than as the main positive bridge candidate."
        )

    return {
        "status": "available",
        "headline_question": "After isolating the nonfinancial-corporate bridge, is the remaining private detail a meaningful bridge of its own or mainly a small offset structure?",
        "estimation_path": {
            "summary_artifact": "strict_private_offset_residual_summary.json",
            "baseline_spec": "identity_baseline",
            "comparison_spec": "unexpected_tdc_core_deposit_proximate_bank_only",
        },
        "bridge_definitions": {
            "private_bridge": "strict_di_loans_nec_private_domestic_borrower_qoq",
            "nonfinancial_corporate": "strict_di_loans_nec_nonfinancial_corporate_qoq",
            "private_offset_total": "strict_di_loans_nec_private_offset_residual_qoq",
            "private_minus_corporate": "strict_di_loans_nec_private_minus_corporate_qoq",
            "households_nonprofits": "strict_di_loans_nec_households_nonprofits_qoq",
            "nonfinancial_noncorporate": "strict_di_loans_nec_nonfinancial_noncorporate_qoq",
        },
        "recommendation": {
            "status": "private_offset_diagnostic_role",
            "next_branch": next_branch,
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
