from __future__ import annotations

from typing import Any

import pandas as pd

CONTRAST_ABS_GAP_TOLERANCE = 0.05


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


def _treatment_freeze_status(shock_metadata: dict[str, Any] | None) -> str:
    if shock_metadata is None:
        return "frozen"
    return str(shock_metadata.get("freeze_status", "frozen"))


def _treatment_candidates(shock_specs: dict[str, Any] | None) -> list[dict[str, Any]]:
    if shock_specs is None:
        return []
    candidates: list[dict[str, Any]] = []
    for name, spec in shock_specs.items():
        if not isinstance(spec, dict):
            continue
        if str(spec.get("candidate_role", "")) != "repair_candidate":
            continue
        candidates.append(
            {
                "name": str(name),
                "model_name": str(spec.get("model_name", "")),
                "shock_column": str(spec.get("standardized_column", "")),
                "raw_shock_column": str(spec.get("residual_column", "")),
                "target": str(spec.get("target", "")),
                "method": str(spec.get("method", "expanding_window_ols")),
                "min_train_obs": int(spec.get("min_train_obs", 0)),
                "max_train_obs": None if spec.get("max_train_obs") is None else int(spec.get("max_train_obs")),
                "predictors": [str(item) for item in spec.get("predictors", [])],
            }
        )
    return candidates


def _ratio_reporting_gate(
    *,
    raw_tdc: dict[str, Any] | None,
    usable_target_sd: float | None,
) -> dict[str, Any]:
    ci_excludes_zero = bool(raw_tdc["ci_excludes_zero"]) if raw_tdc is not None else False
    abs_raw_tdc_beta = None if raw_tdc is None else abs(float(raw_tdc["beta"]))
    beta_large_enough = (
        raw_tdc is not None
        and usable_target_sd is not None
        and pd.notna(usable_target_sd)
        and abs_raw_tdc_beta is not None
        and abs_raw_tdc_beta >= float(usable_target_sd)
    )
    allowed = ci_excludes_zero and beta_large_enough
    if allowed:
        explanation = "Ratios are reported because the raw-unit TDC response is statistically decisive and at least one usable-sample target standard deviation."
    elif raw_tdc is None:
        explanation = "Ratios are suppressed because the raw-unit TDC response is unavailable."
    elif not ci_excludes_zero:
        explanation = "Ratios are suppressed because the raw-unit TDC response does not exclude zero."
    elif usable_target_sd is None or pd.isna(usable_target_sd):
        explanation = "Ratios are suppressed because the usable-sample target volatility could not be computed."
    else:
        explanation = "Ratios are suppressed because the raw-unit TDC response is smaller than one usable-sample target standard deviation."
    return {
        "allowed": allowed,
        "usable_target_sd": usable_target_sd,
        "abs_raw_tdc_beta": abs_raw_tdc_beta,
        "conditions": {
            "tdc_ci_excludes_zero": ci_excludes_zero,
            "abs_raw_tdc_beta_ge_usable_target_sd": beta_large_enough,
        },
        "explanation": explanation,
    }


def _contrast_rows(
    frame: pd.DataFrame,
    *,
    scope: str,
    variant_column: str | None,
    role_column: str | None,
    identity_check_mode: str,
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
                    "identity_check_mode": identity_check_mode,
                    "contrast_consistent": (
                        beta_implied is not None
                        and beta_direct is not None
                        and (not sample_mismatch)
                        and abs_gap is not None
                        and abs_gap <= CONTRAST_ABS_GAP_TOLERANCE
                    ),
                    "implied_sign": _sign(beta_implied),
                    "direct_sign": _sign(beta_direct),
                }
            )
    return rows


def build_total_minus_other_contrast(
    *,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    sensitivity: pd.DataFrame,
    control_sensitivity: pd.DataFrame,
    sample_sensitivity: pd.DataFrame,
    identity_check_mode: str = "exact_accounting_identity",
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    rows.extend(
        _contrast_rows(
            lp_irf,
            scope="baseline",
            variant_column=None,
            role_column=None,
            identity_check_mode=identity_check_mode,
        )
    )
    if identity_lp_irf is not None and not identity_lp_irf.empty:
        rows.extend(
            _contrast_rows(
                identity_lp_irf,
                scope="exact_identity_baseline",
                variant_column=None,
                role_column=None,
                identity_check_mode="exact_identity_baseline",
            )
        )
    rows.extend(
        _contrast_rows(
            sensitivity,
            scope="treatment_variant",
            variant_column="treatment_variant",
            role_column="treatment_role",
            identity_check_mode=identity_check_mode,
        )
    )
    rows.extend(
        _contrast_rows(
            control_sensitivity,
            scope="control_variant",
            variant_column="control_variant",
            role_column="control_role",
            identity_check_mode=identity_check_mode,
        )
    )
    rows.extend(
        _contrast_rows(
            sample_sensitivity,
            scope="sample_variant",
            variant_column="sample_variant",
            role_column="sample_role",
            identity_check_mode=identity_check_mode,
        )
    )
    return pd.DataFrame(rows)


def build_direct_identification_summary(
    *,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    contrast: pd.DataFrame,
    sample_sensitivity: pd.DataFrame,
    shock_metadata: dict[str, Any] | None = None,
    shock_specs: dict[str, Any] | None = None,
    shocks: pd.DataFrame | None = None,
    raw_tdc_lp: pd.DataFrame | None = None,
    shock_column: str = "tdc_residual_z",
    horizons: tuple[int, ...] = (0, 4, 8),
) -> dict[str, Any]:
    horizon_evidence: dict[str, Any] = {}
    baseline_contrast = contrast[contrast["scope"] == "baseline"].copy() if not contrast.empty else pd.DataFrame()
    primary_lp_irf = identity_lp_irf if identity_lp_irf is not None and not identity_lp_irf.empty else lp_irf
    primary_decomposition_mode = (
        "exact_identity_baseline"
        if identity_lp_irf is not None and not identity_lp_irf.empty
        else "approximate_dynamic_decomposition"
    )
    key_horizons = {0, 4}
    treatment_freeze_status = _treatment_freeze_status(shock_metadata)
    treatment_candidates = _treatment_candidates(shock_specs)
    treatment_target = "tdc_bank_only_qoq" if shock_metadata is None else str(shock_metadata.get("target", "tdc_bank_only_qoq"))
    usable_target_sd = None
    if shocks is not None and shock_column in shocks.columns and treatment_target in shocks.columns:
        usable = shocks.dropna(subset=[shock_column, treatment_target]).copy()
        if not usable.empty:
            usable_target_sd = float(usable[treatment_target].std(ddof=1))
            if not pd.notna(usable_target_sd) or usable_target_sd <= 0.0:
                usable_target_sd = None

    first_stage_positive = False
    first_stage_decisive = False
    first_stage_nontrivial = False
    decomposition_separates = False
    contrast_missing = False
    contrast_inconsistent = False
    contrast_identity_mode = "exact_accounting_identity"
    approximate_dynamic_robustness = {
        "status": "not_available",
        "identity_check_mode": contrast_identity_mode,
        "artifact": None,
        "max_abs_gap_key_horizons": None,
        "key_horizon_consistent": None,
        "note": "No approximate dynamic decomposition robustness check is available.",
    }
    ratio_reporting_gate = {
        "rule": [
            "tdc_ci_excludes_zero == true",
            "abs(raw_unit_tdc_beta) >= usable_target_sd",
        ],
        "usable_target_sd": usable_target_sd,
        "horizons": {},
    }

    for horizon in horizons:
        tdc_row = _lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=horizon)
        total_row = _lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)
        other_row = _lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=horizon)
        tdc = _snapshot(tdc_row)
        total = _snapshot(total_row)
        other = _snapshot(other_row)
        raw_tdc = _snapshot(_lp_row(raw_tdc_lp, outcome="tdc_bank_only_qoq", horizon=horizon)) if raw_tdc_lp is not None else None
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
        gate = _ratio_reporting_gate(raw_tdc=raw_tdc, usable_target_sd=usable_target_sd)
        ratio_reporting_gate["horizons"][f"h{horizon}"] = gate
        if gate["allowed"] and tdc is not None and abs(float(tdc["beta"])) > 1e-12:
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
            else:
                contrast_identity_mode = str(contrast_row.get("identity_check_mode", contrast_identity_mode))
                if not bool(contrast_row.get("contrast_consistent", False)):
                    contrast_inconsistent = True

        horizon_evidence[f"h{horizon}"] = {
            "tdc": tdc,
            "raw_unit_tdc": raw_tdc,
            "total": total,
            "other": other,
            "pass_through_ratio_total_over_tdc": pass_through_ratio,
            "crowd_out_ratio_neg_other_over_tdc": crowd_out_ratio,
            "ratio_reporting_gate": gate,
            "beta_gap_total_minus_other": beta_gap,
            "direct_vs_identity_tdc_gap": direct_gap,
            "identity_check_mode": str(contrast_row.get("identity_check_mode", contrast_identity_mode))
            if contrast_row is not None
            else contrast_identity_mode,
            "contrast_consistent": bool(contrast_row.get("contrast_consistent", False)) if contrast_row is not None else False,
        }

    if not baseline_contrast.empty:
        key_contrast = baseline_contrast[baseline_contrast["horizon"].isin(key_horizons)].copy()
        max_abs_gap = None
        if not key_contrast.empty and key_contrast["abs_gap"].notna().any():
            max_abs_gap = float(key_contrast["abs_gap"].dropna().max())
        key_consistent = None if key_contrast.empty else bool(key_contrast["contrast_consistent"].fillna(False).all())
        approximate_dynamic_robustness = {
            "status": (
                "divergent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline" and key_consistent is False
                else "consistent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline" and key_consistent is True
                else "primary_check"
            ),
            "identity_check_mode": contrast_identity_mode,
            "artifact": "total_minus_other_contrast.csv",
            "max_abs_gap_key_horizons": max_abs_gap,
            "key_horizon_consistent": key_consistent,
            "note": (
                "Primary decomposition uses the exact identity-preserving baseline; the approximate dynamic path is retained only as a secondary robustness check."
                if primary_decomposition_mode == "exact_identity_baseline"
                else "The total-minus-other contrast is the active decomposition check for this specification."
            ),
        }

    warnings: list[str] = []
    reasons: list[str] = []
    if treatment_freeze_status != "frozen":
        reasons.append("The baseline unexpected-TDC shock is not yet a credibly frozen treatment object.")
    if not first_stage_positive or not first_stage_decisive:
        reasons.append("The baseline shock does not move TDC itself clearly enough at key horizons.")
    if not decomposition_separates:
        reasons.append("Total deposits and the non-TDC component still do not separate enough at key horizons.")
    if contrast_missing:
        warnings.append("Some baseline total-minus-other contrast rows are missing.")
    if contrast_inconsistent:
        if primary_decomposition_mode != "exact_identity_baseline" and contrast_identity_mode == "approximate_with_outcome_specific_lags":
            warnings.append(
                "Direct TDC response and total-minus-other contrast diverge at key horizons, but this is an approximate LP cross-check because the regressions use outcome-specific lagged dependent variables."
            )
        elif primary_decomposition_mode != "exact_identity_baseline":
            warnings.append("Direct TDC response and total-minus-other contrast differ by more than the numeric tolerance at key horizons.")

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
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": "lp_irf_identity_baseline.csv"
            if primary_decomposition_mode == "exact_identity_baseline"
            else "lp_irf.csv",
            "approximate_robustness_mode": contrast_identity_mode,
            "approximate_robustness_artifact": "total_minus_other_contrast.csv" if not baseline_contrast.empty else None,
            "approximate_dynamic_robustness": approximate_dynamic_robustness,
        },
        "treatment_freeze_status": treatment_freeze_status,
        "treatment_candidates": treatment_candidates,
        "shock_definition": {
            "shock_column": shock_column,
            "treatment_outcome": treatment_target,
            "response_type": "cumulative_sum_h0_to_h",
            "key_horizons": list(horizons),
            "model_name": None if shock_metadata is None else str(shock_metadata.get("model_name", "")),
            "predictors": [] if shock_metadata is None else [str(item) for item in shock_metadata.get("predictors", [])],
            "min_train_obs": None if shock_metadata is None else int(shock_metadata.get("min_train_obs", 0)),
        },
        "contrast_check": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "identity_check_mode": contrast_identity_mode,
            "explanation": (
                "Primary decomposition uses the exact identity-preserving baseline; total-minus-other contrast remains an approximate dynamic robustness check."
                if primary_decomposition_mode == "exact_identity_baseline"
                else (
                    "Total-minus-other is an approximate LP cross-check here because the regressions include outcome-specific lagged dependent variables."
                    if contrast_identity_mode == "approximate_with_outcome_specific_lags"
                    else "Total-minus-other is treated as an exact accounting identity check."
                )
            ),
        },
        "ratio_reporting_gate": ratio_reporting_gate,
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
            "the baseline unexpected-TDC shock is a credibly frozen treatment object",
            "the baseline shock moves TDC itself clearly enough at h0 or h4",
            "total deposits and the non-TDC component separate enough to support a pass-through versus crowd-out statement",
            "excluding flagged shock windows does not overturn the headline interpretation",
        ],
    }
