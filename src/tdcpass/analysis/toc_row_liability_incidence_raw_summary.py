from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

_TOC_SIGNED_COLUMN = "tdc_toc_signed_qoq"
_TGA_RELEASE_COLUMN = "tga_release_qoq"
_ROW_SIGNED_COLUMN = "tdc_row_treasury_transactions_qoq"

_TOC_IN_SCOPE_COLUMNS: tuple[str, ...] = (
    "total_deposits_bank_qoq",
    "deposits_only_bank_qoq",
    "checkable_private_domestic_bank_qoq",
)
_TOC_SUPPORT_COLUMNS: tuple[str, ...] = (
    "reserves_qoq",
    _TGA_RELEASE_COLUMN,
    "cb_nonts_qoq",
)
_ROW_IN_SCOPE_COLUMNS: tuple[str, ...] = (
    "total_deposits_bank_qoq",
    "deposits_only_bank_qoq",
    "checkable_private_domestic_bank_qoq",
    "checkable_rest_of_world_bank_qoq",
)
_ROW_SUPPORT_COLUMNS: tuple[str, ...] = (
    "foreign_nonts_qoq",
    "interbank_transactions_foreign_banks_asset_qoq",
    "interbank_transactions_foreign_banks_liability_qoq",
    "deposits_at_foreign_banks_asset_qoq",
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


def _augment_signed_leg_columns(shocked: pd.DataFrame) -> pd.DataFrame:
    frame = shocked.copy()
    frame[_TOC_SIGNED_COLUMN] = -frame["tdc_treasury_operating_cash_qoq"]
    frame[_TGA_RELEASE_COLUMN] = -frame["tga_qoq"]
    return frame


def _ols_slope(x: pd.Series, y: pd.Series) -> float | None:
    aligned = pd.concat([x, y], axis=1).dropna()
    if aligned.shape[0] < 2:
        return None
    x_values = aligned.iloc[:, 0].astype(float)
    y_values = aligned.iloc[:, 1].astype(float)
    if float(x_values.std(ddof=0)) == 0.0:
        return None
    slope, _ = np.polyfit(x_values.to_numpy(dtype=float), y_values.to_numpy(dtype=float), deg=1)
    return float(slope)


def _corr(x: pd.Series, y: pd.Series) -> float | None:
    value = x.corr(y)
    return None if pd.isna(value) else float(value)


def _quarterly_pair_summary(*, leg: pd.Series, counterpart: pd.Series) -> dict[str, Any]:
    same_quarter_corr = _corr(leg, counterpart)
    next_quarter_corr = _corr(leg.iloc[:-1], counterpart.iloc[1:])
    prior_quarter_corr = _corr(leg.iloc[1:], counterpart.iloc[:-1])
    same_quarter_slope = _ols_slope(leg, counterpart)
    next_quarter_slope = _ols_slope(leg.iloc[:-1], counterpart.iloc[1:])
    nonzero = leg != 0.0
    ratio = (counterpart[nonzero] / leg[nonzero]).replace([np.inf, -np.inf], np.nan).dropna()
    timing_strengths = {
        "same_quarter": abs(same_quarter_corr) if same_quarter_corr is not None else -1.0,
        "next_quarter": abs(next_quarter_corr) if next_quarter_corr is not None else -1.0,
    }
    preferred_timing = max(timing_strengths, key=timing_strengths.get)
    return {
        "same_quarter_corr": same_quarter_corr,
        "next_quarter_corr": next_quarter_corr,
        "prior_quarter_corr": prior_quarter_corr,
        "same_quarter_ols_slope": same_quarter_slope,
        "next_quarter_ols_slope": next_quarter_slope,
        "same_quarter_sign_match_share": float(((leg > 0.0) == (counterpart > 0.0)).mean()),
        "same_quarter_ratio": {
            "mean": float(ratio.mean()) if not ratio.empty else None,
            "median": float(ratio.median()) if not ratio.empty else None,
            "p10": float(ratio.quantile(0.1)) if not ratio.empty else None,
            "p90": float(ratio.quantile(0.9)) if not ratio.empty else None,
        },
        "preferred_timing": preferred_timing if timing_strengths[preferred_timing] >= 0.0 else "not_available",
    }


def _best_corr(summary_map: dict[str, dict[str, Any]]) -> float | None:
    candidates: list[float] = []
    for payload in summary_map.values():
        for key in ("same_quarter_corr", "next_quarter_corr"):
            value = payload.get(key)
            if value is not None:
                candidates.append(abs(float(value)))
    if not candidates:
        return None
    return float(max(candidates))


def _best_share_from_horizon(
    horizon_payload: dict[str, Any],
    *,
    candidate_columns: tuple[str, ...],
) -> float | None:
    candidates: list[float] = []
    shares = dict(horizon_payload.get("counterpart_share_of_leg_beta", {}) or {})
    for column in candidate_columns:
        value = shares.get(column)
        if value is not None:
            candidates.append(abs(float(value)))
    if not candidates:
        return None
    return float(max(candidates))


def _quarterly_leg_summary(
    *,
    frame: pd.DataFrame,
    leg_column: str,
    in_scope_columns: tuple[str, ...],
    support_columns: tuple[str, ...],
) -> dict[str, Any]:
    required = {leg_column, *in_scope_columns, *support_columns}
    if not required.issubset(frame.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    sample = frame[list(required)].dropna().copy()
    if sample.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    leg = sample[leg_column].astype(float)
    in_scope = {
        column: _quarterly_pair_summary(leg=leg, counterpart=sample[column].astype(float)) for column in in_scope_columns
    }
    support = {
        column: _quarterly_pair_summary(leg=leg, counterpart=sample[column].astype(float)) for column in support_columns
    }
    return {
        "status": "available",
        "rows": int(sample.shape[0]),
        "in_scope_counterparts": in_scope,
        "support_counterparts": support,
        "best_in_scope_corr": _best_corr(in_scope),
        "best_support_corr": _best_corr(support),
    }


def _horizon_leg_summary(
    *,
    lp: pd.DataFrame,
    leg_column: str,
    in_scope_columns: tuple[str, ...],
    support_columns: tuple[str, ...],
    horizon: int,
) -> dict[str, Any]:
    leg_response = _snapshot(_lp_row(lp, outcome=leg_column, horizon=horizon))
    counterpart_responses: dict[str, Any] = {}
    share_map: dict[str, float | None] = {}
    leg_beta = None if leg_response is None else float(leg_response["beta"])
    for column in (*in_scope_columns, *support_columns):
        response = _snapshot(_lp_row(lp, outcome=column, horizon=horizon))
        counterpart_responses[column] = response
        beta = None if response is None else float(response["beta"])
        share_map[column] = None if leg_beta in (None, 0.0) or beta is None else beta / leg_beta
    best_in_scope_share = _best_share_from_horizon(
        {"counterpart_share_of_leg_beta": share_map},
        candidate_columns=in_scope_columns,
    )
    best_support_share = _best_share_from_horizon(
        {"counterpart_share_of_leg_beta": share_map},
        candidate_columns=support_columns,
    )
    return {
        "leg_response": leg_response,
        "counterpart_responses": counterpart_responses,
        "counterpart_share_of_leg_beta": share_map,
        "best_in_scope_share_abs": best_in_scope_share,
        "best_support_share_abs": best_support_share,
    }


def _classify_toc_leg(*, quarterly: dict[str, Any], h0: dict[str, Any], h1: dict[str, Any]) -> str:
    best_in_scope_corr = quarterly.get("best_in_scope_corr")
    best_support_corr = quarterly.get("best_support_corr")
    h0_in_scope = h0.get("best_in_scope_share_abs")
    h0_support = h0.get("best_support_share_abs")
    active = bool(dict(h0.get("leg_response", {}) or {}).get("ci_excludes_zero")) or bool(
        dict(h1.get("leg_response", {}) or {}).get("ci_excludes_zero")
    )
    if (
        active
        and
        best_in_scope_corr is not None
        and best_support_corr is not None
        and h0_in_scope is not None
        and h0_support is not None
        and best_in_scope_corr >= 0.4
        and h0_in_scope >= 0.75
        and best_in_scope_corr >= best_support_corr * 0.75
    ):
        return "validated_or_near_validated_in_scope_deposit_incidence"
    if (
        active
        and
        best_in_scope_corr is not None
        and best_support_corr is not None
        and h0_in_scope is not None
        and h0_support is not None
        and best_in_scope_corr >= 0.15
        and h0_in_scope > 0.0
        and h0_in_scope < h0_support
    ):
        return "partial_in_scope_deposit_incidence_support_channels_still_dominate"
    return "weak_or_unvalidated_in_scope_deposit_incidence"


def _classify_row_leg(*, quarterly: dict[str, Any], h0: dict[str, Any], h1: dict[str, Any]) -> str:
    best_in_scope_corr = quarterly.get("best_in_scope_corr")
    best_support_corr = quarterly.get("best_support_corr")
    h0_in_scope = h0.get("best_in_scope_share_abs")
    h0_support = h0.get("best_support_share_abs")
    if (
        bool(dict(h0.get("leg_response", {}) or {}).get("ci_excludes_zero"))
        and
        best_in_scope_corr is not None
        and best_support_corr is not None
        and h0_in_scope is not None
        and h0_support is not None
        and best_in_scope_corr >= 0.4
        and h0_in_scope >= 0.75
        and best_in_scope_corr >= best_support_corr * 0.75
    ):
        return "validated_or_near_validated_in_scope_deposit_incidence"
    if (
        bool(dict(h0.get("leg_response", {}) or {}).get("ci_excludes_zero"))
        and
        best_in_scope_corr is not None
        and best_in_scope_corr >= 0.15
        and h0_in_scope is not None
        and h0_in_scope > 0.0
    ):
        return "partial_in_scope_deposit_incidence"
    return "weak_in_scope_deposit_incidence_external_or_interbank_channels_dominate"


def build_toc_row_liability_incidence_raw_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1),
) -> dict[str, Any]:
    augmented = _augment_signed_leg_columns(shocked)
    required = {
        _TOC_SIGNED_COLUMN,
        _ROW_SIGNED_COLUMN,
        *_TOC_IN_SCOPE_COLUMNS,
        *_TOC_SUPPORT_COLUMNS,
        *_ROW_IN_SCOPE_COLUMNS,
        *_ROW_SUPPORT_COLUMNS,
    }
    if not required.issubset(augmented.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    quarterly_alignment = {
        "toc_leg": _quarterly_leg_summary(
            frame=augmented,
            leg_column=_TOC_SIGNED_COLUMN,
            in_scope_columns=_TOC_IN_SCOPE_COLUMNS,
            support_columns=_TOC_SUPPORT_COLUMNS,
        ),
        "row_leg": _quarterly_leg_summary(
            frame=augmented,
            leg_column=_ROW_SIGNED_COLUMN,
            in_scope_columns=_ROW_IN_SCOPE_COLUMNS,
            support_columns=_ROW_SUPPORT_COLUMNS,
        ),
    }

    lp = run_local_projections(
        augmented,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[
            _TOC_SIGNED_COLUMN,
            *_TOC_IN_SCOPE_COLUMNS,
            *_TOC_SUPPORT_COLUMNS,
            _ROW_SIGNED_COLUMN,
            *_ROW_IN_SCOPE_COLUMNS,
            *_ROW_SUPPORT_COLUMNS,
        ],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="toc_row_liability_incidence_raw",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        key_horizons[f"h{horizon}"] = {
            "toc_leg": _horizon_leg_summary(
                lp=lp,
                leg_column=_TOC_SIGNED_COLUMN,
                in_scope_columns=_TOC_IN_SCOPE_COLUMNS,
                support_columns=_TOC_SUPPORT_COLUMNS,
                horizon=horizon,
            ),
            "row_leg": _horizon_leg_summary(
                lp=lp,
                leg_column=_ROW_SIGNED_COLUMN,
                in_scope_columns=_ROW_IN_SCOPE_COLUMNS,
                support_columns=_ROW_SUPPORT_COLUMNS,
                horizon=horizon,
            ),
        }

    toc_status = _classify_toc_leg(
        quarterly=quarterly_alignment["toc_leg"],
        h0=key_horizons.get("h0", {}).get("toc_leg", {}),
        h1=key_horizons.get("h1", {}).get("toc_leg", {}),
    )
    row_status = _classify_row_leg(
        quarterly=quarterly_alignment["row_leg"],
        h0=key_horizons.get("h0", {}).get("row_leg", {}),
        h1=key_horizons.get("h1", {}).get("row_leg", {}),
    )

    decision_gate = "full_reincorporation_not_supported"
    if toc_status == "validated_or_near_validated_in_scope_deposit_incidence" and row_status == "validated_or_near_validated_in_scope_deposit_incidence":
        decision_gate = "full_reincorporation_supported"
    elif toc_status == "validated_or_near_validated_in_scope_deposit_incidence" and row_status != "validated_or_near_validated_in_scope_deposit_incidence":
        decision_gate = "partial_toc_only_reincorporation_candidate"

    takeaways = [
        "This is the raw-units liability-incidence audit for the TOC and ROW legs. It asks whether each measured leg lands in the in-scope bank-deposit aggregate strongly enough to count toward a strict deposit component.",
        "The audit is scope matched to the bank-deposit outcomes and timing matched across same-quarter and next-quarter raw mappings, with h0/h1 shock overlays kept separate.",
    ]

    toc_quarterly = quarterly_alignment["toc_leg"]
    row_quarterly = quarterly_alignment["row_leg"]
    toc_in_scope_corr = toc_quarterly.get("best_in_scope_corr")
    toc_support_corr = toc_quarterly.get("best_support_corr")
    row_in_scope_corr = row_quarterly.get("best_in_scope_corr")
    row_support_corr = row_quarterly.get("best_support_corr")
    if toc_in_scope_corr is not None and toc_support_corr is not None:
        takeaways.append(
            "TOC still looks only partially deposit-incidence-valid in raw units: "
            f"best in-scope deposit corr ≈ {float(toc_in_scope_corr):.2f}, best support corr ≈ {float(toc_support_corr):.2f}."
        )
    toc_h0 = dict(key_horizons.get("h0", {}).get("toc_leg", {}) or {})
    toc_shares = dict(toc_h0.get("counterpart_share_of_leg_beta", {}) or {})
    toc_dep_only = toc_shares.get("deposits_only_bank_qoq")
    toc_total = toc_shares.get("total_deposits_bank_qoq")
    toc_reserves = toc_shares.get("reserves_qoq")
    if None not in (toc_dep_only, toc_total, toc_reserves):
        takeaways.append(
            "The h0 TOC overlay points the same way: "
            f"deposits-only share ≈ {float(toc_dep_only):.2f}, total-deposits share ≈ {float(toc_total):.2f}, reserves share ≈ {float(toc_reserves):.2f}."
        )
    if row_in_scope_corr is not None and row_support_corr is not None:
        takeaways.append(
            "ROW looks weaker as a strict deposit leg in raw units: "
            f"best in-scope deposit corr ≈ {float(row_in_scope_corr):.2f}, best support/external corr ≈ {float(row_support_corr):.2f}."
        )
    row_h0 = dict(key_horizons.get("h0", {}).get("row_leg", {}) or {})
    row_shares = dict(row_h0.get("counterpart_share_of_leg_beta", {}) or {})
    row_dep_only = row_shares.get("deposits_only_bank_qoq")
    row_row_checkable = row_shares.get("checkable_rest_of_world_bank_qoq")
    row_external = row_shares.get("foreign_nonts_qoq")
    if None not in (row_dep_only, row_row_checkable, row_external):
        takeaways.append(
            "The h0 ROW overlay is even less supportive of strict incidence: "
            f"deposits-only share ≈ {float(row_dep_only):.2f}, ROW-checkable share ≈ {float(row_row_checkable):.2f}, foreign-NONTS share ≈ {float(row_external):.2f}."
        )
    if decision_gate == "full_reincorporation_not_supported":
        takeaways.append(
            "Current binary-gate verdict: do not reincorporate the full TOC/ROW block into the strict object. At most, TOC remains a candidate for a narrower validated share in a later branch."
        )

    return {
        "status": "available",
        "headline_question": "In raw units, does each dollar of TOC or ROW land in the in-scope bank-deposit aggregate strongly enough to belong inside the strict deposit component?",
        "estimation_path": {
            "lp_spec_name": "toc_row_liability_incidence_raw",
            "summary_artifact": "toc_row_liability_incidence_raw_summary.json",
            "raw_quarterly_scope": "same-quarter and next-quarter raw unit mappings plus h0/h1 shock overlays",
        },
        "leg_definitions": {
            "toc_signed_leg": _TOC_SIGNED_COLUMN,
            "row_signed_leg": _ROW_SIGNED_COLUMN,
            "toc_in_scope_outcomes": list(_TOC_IN_SCOPE_COLUMNS),
            "toc_support_outcomes": list(_TOC_SUPPORT_COLUMNS),
            "row_in_scope_outcomes": list(_ROW_IN_SCOPE_COLUMNS),
            "row_support_outcomes": list(_ROW_SUPPORT_COLUMNS),
        },
        "quarterly_alignment": quarterly_alignment,
        "key_horizons": key_horizons,
        "classification": {
            "toc_leg_status": toc_status,
            "row_leg_status": row_status,
            "decision_gate": decision_gate,
            "bundle_role": "measured_support_bundle_with_unresolved_strict_deposit_incidence",
        },
        "recommendation": {
            "status": "raw_incidence_binary_gate_completed",
            "strict_rule": "only_reincorporate_validated_in_scope_deposit_share",
            "toc_rule": (
                "candidate_for_narrow_validated_share_only"
                if toc_status != "validated_or_near_validated_in_scope_deposit_incidence"
                else "may_be_reincorporated_if_share_is_scope_stable"
            ),
            "row_rule": (
                "keep_outside_strict_object"
                if row_status != "validated_or_near_validated_in_scope_deposit_incidence"
                else "may_be_reincorporated_if_share_is_scope_stable"
            ),
            "next_branch": "decide_whether_any_validated_toc_share_belongs_in_strict_object",
        },
        "takeaways": takeaways,
    }
