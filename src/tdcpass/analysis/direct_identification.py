from __future__ import annotations

from typing import Any

import pandas as pd


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    beta = float(row["beta"])
    se = float(row["se"])
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": beta,
        "se": se,
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
    }


def _sign(value: float | None) -> str:
    if value is None:
        return "missing"
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "zero"


def _contrast_rows(
    frame: pd.DataFrame,
    *,
    scope: str,
    variant_column: str | None,
    role_column: str | None,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []

    rows: list[dict[str, Any]] = []
    if variant_column is None:
        grouped_items = [("baseline", frame)]
    else:
        grouped_items = list(frame.groupby(variant_column, sort=False))

    for variant, variant_frame in grouped_items:
        role = "headline"
        if role_column is not None and role_column in variant_frame.columns:
            role = str(variant_frame.iloc[0][role_column])
        for horizon in sorted(variant_frame["horizon"].drop_duplicates().tolist()):
            total_row = _lp_row(variant_frame, outcome="total_deposits_bank_qoq", horizon=int(horizon))
            other_row = _lp_row(variant_frame, outcome="other_component_qoq", horizon=int(horizon))
            direct_row = _lp_row(variant_frame, outcome="tdc_bank_only_qoq", horizon=int(horizon))
            total = _snapshot(total_row)
            other = _snapshot(other_row)
            direct = _snapshot(direct_row)
            beta_implied = None
            beta_direct = None
            gap_implied_minus_direct = None
            abs_gap = None
            if total is not None and other is not None:
                beta_implied = float(total["beta"]) - float(other["beta"])
            if direct is not None:
                beta_direct = float(direct["beta"])
            if beta_implied is not None and beta_direct is not None:
                gap_implied_minus_direct = beta_implied - beta_direct
                abs_gap = abs(gap_implied_minus_direct)
            n_total = int(total["n"]) if total is not None else None
            n_other = int(other["n"]) if other is not None else None
            n_direct = int(direct["n"]) if direct is not None else None
            sample_mismatch = (
                n_total is None
                or n_other is None
                or n_direct is None
                or len({n_total, n_other, n_direct}) != 1
            )
            rows.append(
                {
                    "scope": scope,
                    "variant": str(variant),
                    "role": role,
                    "horizon": int(horizon),
                    "beta_total": float(total["beta"]) if total is not None else None,
                    "beta_other": float(other["beta"]) if other is not None else None,
                    "beta_implied": beta_implied,
                    "beta_direct": beta_direct,
                    "direct_lower95": float(direct["lower95"]) if direct is not None else None,
                    "direct_upper95": float(direct["upper95"]) if direct is not None else None,
                    "direct_ci_excludes_zero": bool(direct["ci_excludes_zero"]) if direct is not None else False,
                    "gap_implied_minus_direct": gap_implied_minus_direct,
                    "abs_gap": abs_gap,
                    "n_total": n_total,
                    "n_other": n_other,
                    "n_direct": n_direct,
                    "sample_mismatch_flag": sample_mismatch,
                    "contrast_consistent": (
                        beta_implied is not None and beta_direct is not None and (not sample_mismatch) and abs_gap is not None and abs_gap <= 1e-8
                    ),
                    "implied_sign": _sign(beta_implied),
                    "direct_sign": _sign(beta_direct),
                }
            )
    return rows


def build_total_minus_other_contrast(
    *,
    lp_irf: pd.DataFrame,
    sensitivity: pd.DataFrame,
    control_sensitivity: pd.DataFrame,
    sample_sensitivity: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    rows.extend(_contrast_rows(lp_irf, scope="baseline", variant_column=None, role_column=None))
    rows.extend(
        _contrast_rows(
            sensitivity,
            scope="treatment_variant",
            variant_column="treatment_variant",
            role_column="treatment_role",
        )
    )
    rows.extend(
        _contrast_rows(
            control_sensitivity,
            scope="control_variant",
            variant_column="control_variant",
            role_column="control_role",
        )
    )
    rows.extend(
        _contrast_rows(
            sample_sensitivity,
            scope="sample_variant",
            variant_column="sample_variant",
            role_column="sample_role",
        )
    )
    return pd.DataFrame(rows)


def build_direct_identification_summary(
    *,
    lp_irf: pd.DataFrame,
    contrast: pd.DataFrame,
    sample_sensitivity: pd.DataFrame,
    shock_metadata: dict[str, Any] | None = None,
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    horizon_evidence: dict[str, Any] = {}
    baseline_contrast = contrast[contrast["scope"] == "baseline"].copy() if not contrast.empty else pd.DataFrame()
    key_horizons = {0, 4}

    first_stage_positive = False
    first_stage_decisive = False
    first_stage_nontrivial = False
    decomposition_separates = False
    contrast_missing = False
    contrast_inconsistent = False

    for horizon in horizons:
        tdc_row = _lp_row(lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)
        total_row = _lp_row(lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)
        other_row = _lp_row(lp_irf, outcome="other_component_qoq", horizon=horizon)
        tdc = _snapshot(tdc_row)
        total = _snapshot(total_row)
        other = _snapshot(other_row)
        contrast_row = None
        if not baseline_contrast.empty:
            sample = baseline_contrast[baseline_contrast["horizon"] == horizon]
            if not sample.empty:
                contrast_row = sample.iloc[0].to_dict()

        beta_gap = None
        direct_gap = None
        pass_through_ratio = None
        crowd_out_ratio = None
        if total is not None and other is not None:
            beta_gap = float(total["beta"]) - float(other["beta"])
        if tdc is not None and beta_gap is not None:
            direct_gap = float(tdc["beta"]) - beta_gap
        if tdc is not None and abs(float(tdc["beta"])) > 1e-12:
            pass_through_ratio = (float(total["beta"]) / float(tdc["beta"])) if total is not None else None
            crowd_out_ratio = (-float(other["beta"]) / float(tdc["beta"])) if other is not None else None

        if horizon in key_horizons and tdc is not None:
            first_stage_positive = first_stage_positive or float(tdc["beta"]) > 0.0
            first_stage_decisive = first_stage_decisive or bool(tdc["ci_excludes_zero"])
            first_stage_nontrivial = first_stage_nontrivial or abs(float(tdc["beta"])) >= float(tdc["se"])
        if horizon in key_horizons and total is not None and other is not None:
            total_sign = _sign(float(total["beta"]))
            other_sign = _sign(float(other["beta"]))
            if total_sign != other_sign and (total["ci_excludes_zero"] or other["ci_excludes_zero"]):
                decomposition_separates = True
        if horizon in key_horizons:
            if contrast_row is None:
                contrast_missing = True
            elif not bool(contrast_row.get("contrast_consistent", False)):
                contrast_inconsistent = True

        horizon_evidence[f"h{horizon}"] = {
            "tdc": tdc,
            "total": total,
            "other": other,
            "pass_through_ratio_total_over_tdc": pass_through_ratio,
            "crowd_out_ratio_neg_other_over_tdc": crowd_out_ratio,
            "beta_gap_total_minus_other": beta_gap,
            "direct_vs_identity_tdc_gap": direct_gap,
            "contrast_consistent": bool(contrast_row.get("contrast_consistent", False)) if contrast_row is not None else False,
        }

    warnings: list[str] = []
    reasons: list[str] = []
    if not first_stage_positive or not first_stage_decisive:
        reasons.append("The baseline shock does not move TDC itself clearly enough at key horizons.")
    if not decomposition_separates:
        reasons.append("Total deposits and the non-TDC component still do not separate enough at key horizons.")
    if contrast_missing:
        warnings.append("Some baseline total-minus-other contrast rows are missing.")
    if contrast_inconsistent:
        warnings.append("Direct TDC response and total-minus-other contrast do not line up cleanly at key horizons.")

    sample_fragility = {
        "impact_sign_flip": False,
        "h4_sign_flip": False,
        "impact_magnitude_shift_gt_100pct": False,
        "h4_magnitude_shift_gt_100pct": False,
    }
    if not sample_sensitivity.empty:
        total_sample = sample_sensitivity[sample_sensitivity["outcome"] == "total_deposits_bank_qoq"]
        headline_rows = total_sample[total_sample["sample_role"] == "headline"]
        exploratory_rows = total_sample[total_sample["sample_role"] == "exploratory"]
        if not headline_rows.empty and not exploratory_rows.empty:
            for horizon, sign_key, magnitude_key in [
                (0, "impact_sign_flip", "impact_magnitude_shift_gt_100pct"),
                (4, "h4_sign_flip", "h4_magnitude_shift_gt_100pct"),
            ]:
                headline_row = headline_rows[headline_rows["horizon"] == horizon]
                exploratory_row = exploratory_rows[exploratory_rows["horizon"] == horizon]
                if headline_row.empty or exploratory_row.empty:
                    continue
                beta_headline = float(headline_row.iloc[0]["beta"])
                beta_exploratory = float(exploratory_row.iloc[0]["beta"])
                if beta_headline * beta_exploratory < 0.0:
                    sample_fragility[sign_key] = True
                baseline_abs = abs(beta_headline)
                if baseline_abs > 1e-12 and abs(beta_exploratory - beta_headline) / baseline_abs > 1.0:
                    sample_fragility[magnitude_key] = True
        if any(sample_fragility.values()):
            warnings.append("Excluding flagged shock windows materially changes the headline total-deposit response.")

    status = "ready_for_interpretation"
    if reasons:
        status = "not_ready"
    elif warnings:
        status = "provisional"

    return {
        "status": status,
        "headline_question": "Does the baseline unexpected-TDC shock move TDC enough to identify pass-through versus crowd-out?",
        "shock_definition": {
            "shock_column": "tdc_residual_z",
            "treatment_outcome": "tdc_bank_only_qoq",
            "response_type": "cumulative_sum_h0_to_h",
            "key_horizons": list(horizons),
            "model_name": None if shock_metadata is None else str(shock_metadata.get("model_name", "")),
            "predictors": [] if shock_metadata is None else [str(item) for item in shock_metadata.get("predictors", [])],
            "min_train_obs": None if shock_metadata is None else int(shock_metadata.get("min_train_obs", 0)),
        },
        "horizon_evidence": horizon_evidence,
        "first_stage_checks": {
            "tdc_positive_at_h0_or_h4": first_stage_positive,
            "tdc_ci_excludes_zero_at_h0_or_h4": first_stage_decisive,
            "tdc_nontrivial_denominator_at_h0_or_h4": first_stage_nontrivial,
            "decomposition_separates_at_h0_or_h4": decomposition_separates,
        },
        "sample_fragility": sample_fragility,
        "answer_ready": status == "ready_for_interpretation",
        "reasons": reasons,
        "warnings": warnings,
        "answer_ready_when": [
            "the baseline shock moves TDC itself clearly enough at h0 or h4",
            "total deposits and the non-TDC component separate enough to support a pass-through versus crowd-out statement",
            "excluding flagged shock windows does not overturn the headline interpretation",
        ],
    }
