from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.accounting import AccountingSummary


def _lp_row(df: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = df[(df["outcome"] == outcome) & (df["horizon"] == horizon)]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


def _ci_excludes_zero(row: dict[str, Any] | None) -> bool:
    if row is None:
        return False
    return float(row["lower95"]) > 0.0 or float(row["upper95"]) < 0.0


def _beta_sign(row: dict[str, Any] | None) -> str:
    if row is None:
        return "missing"
    beta = float(row["beta"])
    if beta > 0:
        return "positive"
    if beta < 0:
        return "negative"
    return "zero"


def _row_snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "beta": float(row["beta"]),
        "se": float(row["se"]),
        "lower95": float(row["lower95"]),
        "upper95": float(row["upper95"]),
        "n": int(row["n"]),
        "ci_excludes_zero": _ci_excludes_zero(row),
    }


def build_result_readiness_summary(
    *,
    accounting_summary: AccountingSummary,
    shocks: pd.DataFrame,
    lp_irf: pd.DataFrame,
    identity_lp_irf: pd.DataFrame | None = None,
    lp_irf_regimes: pd.DataFrame,
    sensitivity: pd.DataFrame,
    control_sensitivity: pd.DataFrame | None = None,
    sample_sensitivity: pd.DataFrame | None = None,
    regime_diagnostics: dict[str, Any] | None = None,
    direct_identification: dict[str, Any] | None = None,
    contrast: pd.DataFrame | None = None,
    structural_proxy_evidence: dict[str, Any] | None = None,
    proxy_coverage_summary: dict[str, Any] | None = None,
    shock_diagnostics: dict[str, Any] | None = None,
    headline_shock_metadata: dict[str, Any] | None = None,
    shock_column: str = "tdc_residual_z",
) -> dict[str, Any]:
    sensitivity_columns = {
        "outcome",
        "horizon",
        "treatment_variant",
        "treatment_role",
    }
    if sensitivity.empty and not sensitivity_columns.issubset(sensitivity.columns):
        sensitivity = pd.DataFrame(columns=sorted(sensitivity_columns))
    control_sensitivity_columns = {
        "outcome",
        "horizon",
        "control_variant",
        "control_role",
    }
    if control_sensitivity is None or (
        control_sensitivity.empty and not control_sensitivity_columns.issubset(control_sensitivity.columns)
    ):
        control_sensitivity = pd.DataFrame(columns=sorted(control_sensitivity_columns))
    sample_sensitivity_columns = {
        "outcome",
        "horizon",
        "sample_variant",
        "sample_role",
    }
    if sample_sensitivity is None or (
        sample_sensitivity.empty and not sample_sensitivity_columns.issubset(sample_sensitivity.columns)
    ):
        sample_sensitivity = pd.DataFrame(columns=sorted(sample_sensitivity_columns))

    primary_lp_irf = identity_lp_irf if identity_lp_irf is not None and not identity_lp_irf.empty else lp_irf
    primary_decomposition_mode = (
        "exact_identity_baseline"
        if identity_lp_irf is not None and not identity_lp_irf.empty
        else "approximate_dynamic_decomposition"
    )

    usable_shocks = shocks.dropna(subset=[shock_column]).copy()
    shock_obs = int(len(usable_shocks))
    shock_start = str(usable_shocks["quarter"].iloc[0]) if shock_obs else None
    shock_end = str(usable_shocks["quarter"].iloc[-1]) if shock_obs else None
    flagged_shock_obs = int(usable_shocks["shock_flag"].fillna("").astype(str).ne("").sum()) if "shock_flag" in usable_shocks.columns else 0
    flagged_shock_share = float(flagged_shock_obs / shock_obs) if shock_obs else None

    total_h0 = _lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=0)
    total_h4 = _lp_row(primary_lp_irf, outcome="total_deposits_bank_qoq", horizon=4)
    other_h0 = _lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=0)
    other_h4 = _lp_row(primary_lp_irf, outcome="other_component_qoq", horizon=4)
    tdc_h0 = _lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=0)
    tdc_h4 = _lp_row(primary_lp_irf, outcome="tdc_bank_only_qoq", horizon=4)

    reasons: list[str] = []
    warnings: list[str] = []

    if shock_obs == 0:
        reasons.append("No usable unexpected-TDC shock observations are available after the training burn-in.")
    elif shock_obs < 36:
        warnings.append("The usable unexpected-TDC shock sample is short for quarterly LPs.")
    total_decisive = _ci_excludes_zero(total_h0) or _ci_excludes_zero(total_h4)
    other_decisive = _ci_excludes_zero(other_h0) or _ci_excludes_zero(other_h4)
    if not total_decisive:
        reasons.append("The baseline total-deposit response is not statistically distinguishable from zero at key horizons.")
    if not other_decisive:
        reasons.append("The baseline non-TDC response is not statistically distinguishable from zero at key horizons.")
    tdc_decisive = _ci_excludes_zero(tdc_h0) or _ci_excludes_zero(tdc_h4)
    if not tdc_decisive:
        reasons.append("The baseline shock does not move TDC itself clearly enough at key horizons.")

    if total_h0 and other_h0 and _beta_sign(total_h0) == _beta_sign(other_h0):
        warnings.append("At impact, total deposits and the non-TDC component move in the same point-estimate direction.")
    if total_h4 and other_h4 and _beta_sign(total_h4) == _beta_sign(other_h4):
        warnings.append("By horizon 4, total deposits and the non-TDC component still do not separate cleanly in point estimates.")

    sensitivity_h0 = sensitivity[
        (sensitivity["outcome"] == "total_deposits_bank_qoq") & (sensitivity["horizon"] == 0)
    ]
    core_sensitivity_h0 = sensitivity_h0
    if "treatment_role" in sensitivity_h0.columns:
        core_sensitivity_h0 = sensitivity_h0[sensitivity_h0["treatment_role"] == "core"]
    if len(core_sensitivity_h0) >= 2:
        signs = {_beta_sign(row) for row in core_sensitivity_h0.to_dict("records")}
        if "positive" in signs and "negative" in signs:
            warnings.append("Core sensitivity variants do not agree on the sign of the impact total-deposit response.")
    exploratory_sensitivity_h0 = sensitivity_h0
    if "treatment_role" in sensitivity_h0.columns:
        exploratory_sensitivity_h0 = sensitivity_h0[sensitivity_h0["treatment_role"] == "exploratory"]
    exploratory_variant_sign_disagreement = False
    if not exploratory_sensitivity_h0.empty:
        signs = {_beta_sign(row) for row in exploratory_sensitivity_h0.to_dict("records")}
        if "positive" in signs and "negative" in signs:
            exploratory_variant_sign_disagreement = True

    control_sensitivity_h0 = control_sensitivity[
        (control_sensitivity["outcome"] == "total_deposits_bank_qoq") & (control_sensitivity["horizon"] == 0)
    ]
    control_set_sign_disagreement = False
    exploratory_control_set_sign_disagreement = False
    if not control_sensitivity_h0.empty:
        headline_rows = control_sensitivity_h0[control_sensitivity_h0["control_role"] == "headline"]
        core_rows = control_sensitivity_h0[control_sensitivity_h0["control_role"] == "core"]
        exploratory_rows = control_sensitivity_h0[control_sensitivity_h0["control_role"] == "exploratory"]
        if not headline_rows.empty and not core_rows.empty:
            headline_sign = _beta_sign(headline_rows.iloc[0].to_dict())
            comparison_signs = {_beta_sign(row) for row in core_rows.to_dict("records")}
            if headline_sign in {"positive", "negative"} and (
                ("positive" in comparison_signs and headline_sign == "negative")
                or ("negative" in comparison_signs and headline_sign == "positive")
            ):
                control_set_sign_disagreement = True
        if not headline_rows.empty and not exploratory_rows.empty:
            headline_sign = _beta_sign(headline_rows.iloc[0].to_dict())
            exploratory_signs = {_beta_sign(row) for row in exploratory_rows.to_dict("records")}
            if headline_sign in {"positive", "negative"} and (
                ("positive" in exploratory_signs and headline_sign == "negative")
                or ("negative" in exploratory_signs and headline_sign == "positive")
            ):
                exploratory_control_set_sign_disagreement = True
    if control_set_sign_disagreement:
        warnings.append("Core control-set sensitivity variants do not agree on the sign of the impact total-deposit response.")

    sample_variant_sign_disagreement = False
    sample_variant_magnitude_instability = False
    total_sample_sensitivity = sample_sensitivity[sample_sensitivity["outcome"] == "total_deposits_bank_qoq"]
    if not total_sample_sensitivity.empty:
        for horizon in (0, 4):
            horizon_frame = total_sample_sensitivity[total_sample_sensitivity["horizon"] == horizon]
            headline_rows = horizon_frame[horizon_frame["sample_role"] == "headline"]
            exploratory_rows = horizon_frame[horizon_frame["sample_role"] == "exploratory"]
            if headline_rows.empty or exploratory_rows.empty:
                continue
            headline_beta = float(headline_rows.iloc[0]["beta"])
            for _, row in exploratory_rows.iterrows():
                beta = float(row["beta"])
                if headline_beta * beta < 0.0:
                    sample_variant_sign_disagreement = True
                baseline_abs = abs(headline_beta)
                if baseline_abs > 1e-12 and abs(beta - headline_beta) / baseline_abs > 1.0:
                    sample_variant_magnitude_instability = True
    if sample_variant_sign_disagreement:
        warnings.append("Exploratory shock-sample variants overturn the impact sign of the headline total-deposit response.")
    elif sample_variant_magnitude_instability:
        warnings.append("Excluding flagged shock windows materially changes the impact total-deposit response.")

    regime_rows = int(len(lp_irf_regimes))
    if regime_rows == 0:
        warnings.append("No regime-split LP rows were produced.")
    informative_regime_count = None
    stable_regime_count = None
    if regime_diagnostics is not None:
        informative_regime_count = int(regime_diagnostics.get("informative_regime_count", 0))
        stable_regime_count = int(regime_diagnostics.get("stable_regime_count", 0))
        if informative_regime_count == 0:
            warnings.append("Current regime splits are too sparse or imbalanced for reliable interpretation.")
        elif stable_regime_count == 0:
            warnings.append("Current published regime splits remain too extrapolative for reliable interpretation.")

    contrast_max_abs_gap_h0_h4 = None
    contrast_rows_missing = None
    approximate_dynamic_robustness = {
        "status": "not_available",
        "artifact": None,
        "max_abs_gap_h0_h4": None,
        "key_horizon_consistent": None,
        "note": "No approximate dynamic decomposition robustness check is available.",
    }
    if contrast is not None and not contrast.empty:
        baseline_contrast = contrast[(contrast["scope"] == "baseline") & (contrast["horizon"].isin([0, 4]))].copy()
        if not baseline_contrast.empty and baseline_contrast["abs_gap"].notna().any():
            contrast_max_abs_gap_h0_h4 = float(baseline_contrast["abs_gap"].dropna().max())
        contrast_rows_missing = bool(baseline_contrast["beta_direct"].isna().any()) if "beta_direct" in baseline_contrast.columns else None
        approximate_dynamic_robustness = {
            "status": (
                "divergent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline" and baseline_contrast["contrast_consistent"].eq(False).any()
                else "consistent_secondary_check"
                if primary_decomposition_mode == "exact_identity_baseline"
                else "primary_check"
            ),
            "artifact": "total_minus_other_contrast.csv",
            "max_abs_gap_h0_h4": contrast_max_abs_gap_h0_h4,
            "key_horizon_consistent": None
            if baseline_contrast.empty
            else bool(baseline_contrast["contrast_consistent"].fillna(False).all()),
            "note": (
                "Primary decomposition uses the exact identity-preserving baseline; the approximate dynamic path is retained only as a secondary robustness check."
                if primary_decomposition_mode == "exact_identity_baseline"
                else "The total-minus-other contrast remains part of the active decomposition check for this specification."
            ),
        }
        if baseline_contrast["contrast_consistent"].eq(False).any():
            contrast_mode = str(baseline_contrast.iloc[0].get("identity_check_mode", "exact_accounting_identity"))
            if primary_decomposition_mode != "exact_identity_baseline" and contrast_mode == "approximate_with_outcome_specific_lags":
                warnings.append(
                    "Direct TDC response and total-minus-other contrast diverge at key horizons, but this is an approximate LP cross-check because the regressions use outcome-specific lagged dependent variables."
                )
            elif primary_decomposition_mode != "exact_identity_baseline":
                warnings.append("Direct TDC response and total-minus-other contrast differ by more than the numeric tolerance at key horizons.")

    structural_proxy_status = None
    structural_proxy_supportive_key_horizons = 0
    structural_proxy_discordant_key_horizons = 0
    structural_proxy_weak_key_horizons = 0
    if structural_proxy_evidence is not None:
        structural_proxy_status = str(structural_proxy_evidence.get("status", "weak"))
        key_horizons = structural_proxy_evidence.get("key_horizons", {})
        for horizon_payload in key_horizons.values():
            if not isinstance(horizon_payload, dict):
                continue
            interpretation = str(horizon_payload.get("interpretation", ""))
            if interpretation == "proxy_evidence_supportive":
                structural_proxy_supportive_key_horizons += 1
            elif interpretation == "proxy_evidence_discordant":
                structural_proxy_discordant_key_horizons += 1
            elif interpretation in {"proxy_evidence_weak", "other_component_not_decisive"}:
                structural_proxy_weak_key_horizons += 1
        if structural_proxy_status == "weak":
            reasons.append("Structural proxy cross-checks do not yet corroborate the non-TDC residual at key horizons.")
        elif structural_proxy_status == "mixed":
            warnings.append("Structural proxy cross-checks are mixed across key horizons.")

    proxy_coverage_status = None
    proxy_coverage_large_gap_key_horizons = 0
    proxy_coverage_partial_support_key_horizons = 0
    proxy_coverage_same_sign_not_decisive_key_horizons = 0
    proxy_coverage_published_regime_count = 0
    if proxy_coverage_summary is not None:
        proxy_coverage_status = str(proxy_coverage_summary.get("status", "weak"))
        key_horizons = proxy_coverage_summary.get("key_horizons", {})
        for horizon_payload in key_horizons.values():
            if not isinstance(horizon_payload, dict):
                continue
            label = str(horizon_payload.get("coverage_label", ""))
            if label == "proxy_bundle_uncovered_remainder_large":
                proxy_coverage_large_gap_key_horizons += 1
            elif label in {"proxy_bundle_mostly_covers_other", "proxy_bundle_partial_same_sign_support"}:
                proxy_coverage_partial_support_key_horizons += 1
            elif label == "proxy_bundle_same_sign_but_not_decisive":
                proxy_coverage_same_sign_not_decisive_key_horizons += 1
        proxy_coverage_published_regime_count = int(len(proxy_coverage_summary.get("published_regime_contexts", [])))
        if proxy_coverage_large_gap_key_horizons > 0:
            reasons.append("The structural proxy bundle still leaves a large uncovered remainder at key horizons.")
        elif proxy_coverage_same_sign_not_decisive_key_horizons > 0:
            warnings.append("The structural proxy bundle lines up in sign at some key horizons but remains statistically weak.")

    direct_identification_status = None
    treatment_freeze_status = "frozen" if headline_shock_metadata is None else str(headline_shock_metadata.get("freeze_status", "frozen"))
    treatment_candidates = []
    ratio_reporting_gate = None
    treatment_quality_status = None
    treatment_quality_gate = None
    if shock_diagnostics is not None:
        treatment_quality_status = str(shock_diagnostics.get("treatment_quality_status", "not_evaluated"))
        treatment_quality_gate = shock_diagnostics.get("treatment_quality_gate")
        if treatment_quality_status == "fail":
            reasons.append("The frozen baseline unexpected-TDC shock still fails its publishable shock-quality gate.")
        severe_tail_rows = int(
            ((shock_diagnostics.get("severe_realized_scale_tail_audit") or {}).get("tail_rows", 0))
        )
        mild_flagged_rows = max(flagged_shock_obs - severe_tail_rows, 0)
        if flagged_shock_obs > 0 and not sample_variant_sign_disagreement and not sample_variant_magnitude_instability:
            if severe_tail_rows > 0 and mild_flagged_rows > 0:
                warnings.append(
                    f"{flagged_shock_obs} unexpected-TDC shock windows are scale-ratio flagged ({severe_tail_rows} severe tail and {mild_flagged_rows} milder threshold breaches), but the published sample-sensitivity trims do not overturn the headline sign pattern."
                )
            elif severe_tail_rows > 0:
                warnings.append(
                    f"{flagged_shock_obs} unexpected-TDC shock windows are scale-ratio flagged, including {severe_tail_rows} severe tail quarter(s), but the published sample-sensitivity trims do not overturn the headline sign pattern."
                )
            else:
                warnings.append(
                    f"{flagged_shock_obs} unexpected-TDC shock windows are scale-ratio flagged, but the published sample-sensitivity trims do not overturn the headline sign pattern."
                )
    elif flagged_shock_obs > 0 and not sample_variant_sign_disagreement and not sample_variant_magnitude_instability:
        warnings.append("Some unexpected-TDC shock windows are scale-ratio flagged, but the published sample-sensitivity trims do not overturn the headline sign pattern.")
    if direct_identification is not None:
        direct_identification_status = str(direct_identification.get("status", "not_ready"))
        treatment_freeze_status = str(direct_identification.get("treatment_freeze_status", treatment_freeze_status))
        treatment_candidates = list(direct_identification.get("treatment_candidates", []))
        ratio_reporting_gate = direct_identification.get("ratio_reporting_gate")
        for reason in direct_identification.get("reasons", []):
            if reason not in reasons:
                reasons.append(str(reason))
        for warning in direct_identification.get("warnings", []):
            if warning not in warnings:
                warnings.append(str(warning))
    if treatment_freeze_status != "frozen":
        under_review_reason = "The baseline unexpected-TDC shock is not yet a credibly frozen treatment object."
        if under_review_reason not in reasons:
            reasons.append(under_review_reason)

    status = "ready_for_interpretation"
    if reasons:
        status = "not_ready"
    elif warnings:
        status = "provisional"

    if status == "ready_for_interpretation":
        headline = "Current backend outputs are strong enough to support a first deposit-response interpretation with partial mechanism cross-checks."
    elif status == "provisional":
        headline = "Current outputs are usable for deposit-response interpretation, but mechanism attribution remains sensitive or incomplete."
    elif treatment_freeze_status != "frozen":
        headline = "Current backend outputs remain a reproducibility preview only because the baseline unexpected-TDC shock is still under review and not yet a credibly frozen treatment object."
    elif treatment_quality_status == "fail":
        headline = "Current backend outputs remain below release standard because the frozen baseline unexpected-TDC shock still fails its publishable shock-quality gate."
    else:
        headline = "Current backend outputs are not yet strong enough to distinguish deposit-response patterns or support broad mechanism attribution with confidence."

    return {
        "status": status,
        "estimation_path": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "primary_artifact": "lp_irf_identity_baseline.csv"
            if primary_decomposition_mode == "exact_identity_baseline"
            else "lp_irf.csv",
            "approximate_robustness_artifact": "total_minus_other_contrast.csv" if contrast is not None else None,
            "approximate_dynamic_robustness": approximate_dynamic_robustness,
        },
        "treatment_freeze_status": treatment_freeze_status,
        "treatment_candidates": treatment_candidates,
        "treatment_quality_status": treatment_quality_status,
        "treatment_quality_gate": treatment_quality_gate,
        "ratio_reporting_gate": ratio_reporting_gate,
        "headline_assessment": headline,
        "reasons": reasons,
        "warnings": warnings,
        "diagnostics": {
            "primary_decomposition_mode": primary_decomposition_mode,
            "shock_usable_obs": shock_obs,
            "shock_start_quarter": shock_start,
            "shock_end_quarter": shock_end,
            "flagged_shock_obs": flagged_shock_obs,
            "flagged_shock_share": flagged_shock_share,
            "share_other_negative": float(accounting_summary.share_other_negative),
            "regime_row_count": regime_rows,
            "informative_regime_count": informative_regime_count,
            "stable_regime_count": stable_regime_count,
            "sensitivity_variant_count": int(sensitivity["treatment_variant"].nunique()) if not sensitivity.empty else 0,
            "control_set_variant_count": int(control_sensitivity["control_variant"].nunique())
            if not control_sensitivity.empty
            else 0,
            "control_set_core_variant_count": int(
                control_sensitivity.loc[control_sensitivity["control_role"].isin(["headline", "core"]), "control_variant"].nunique()
            )
            if not control_sensitivity.empty and "control_role" in control_sensitivity.columns
            else 0,
            "control_set_exploratory_variant_count": int(
                control_sensitivity.loc[control_sensitivity["control_role"] == "exploratory", "control_variant"].nunique()
            )
            if not control_sensitivity.empty and "control_role" in control_sensitivity.columns
            else 0,
            "sensitivity_core_variant_count": int(
                sensitivity.loc[sensitivity["treatment_role"] == "core", "treatment_variant"].nunique()
            )
            if not sensitivity.empty and "treatment_role" in sensitivity.columns
            else 0,
            "sensitivity_exploratory_variant_count": int(
                sensitivity.loc[sensitivity["treatment_role"] == "exploratory", "treatment_variant"].nunique()
            )
            if not sensitivity.empty and "treatment_role" in sensitivity.columns
            else 0,
            "exploratory_variant_sign_disagreement": exploratory_variant_sign_disagreement,
            "control_set_sign_disagreement": control_set_sign_disagreement,
            "exploratory_control_set_sign_disagreement": exploratory_control_set_sign_disagreement,
            "sample_sensitivity_variant_count": int(sample_sensitivity["sample_variant"].nunique())
            if not sample_sensitivity.empty
            else 0,
            "sample_variant_sign_disagreement": sample_variant_sign_disagreement,
            "sample_variant_magnitude_instability": sample_variant_magnitude_instability,
            "direct_identification_status": direct_identification_status,
            "contrast_max_abs_gap_h0_h4": contrast_max_abs_gap_h0_h4,
            "contrast_rows_missing": contrast_rows_missing,
            "structural_proxy_status": structural_proxy_status,
            "structural_proxy_supportive_key_horizons": structural_proxy_supportive_key_horizons,
            "structural_proxy_discordant_key_horizons": structural_proxy_discordant_key_horizons,
            "structural_proxy_weak_key_horizons": structural_proxy_weak_key_horizons,
            "proxy_coverage_status": proxy_coverage_status,
            "proxy_coverage_large_gap_key_horizons": proxy_coverage_large_gap_key_horizons,
            "proxy_coverage_partial_support_key_horizons": proxy_coverage_partial_support_key_horizons,
            "proxy_coverage_same_sign_not_decisive_key_horizons": proxy_coverage_same_sign_not_decisive_key_horizons,
            "proxy_coverage_published_regime_count": proxy_coverage_published_regime_count,
            "treatment_freeze_status": treatment_freeze_status,
            "treatment_candidate_count": int(len(treatment_candidates)),
            "treatment_quality_status": treatment_quality_status,
            "treatment_quality_gate_failed_checks": []
            if not isinstance(treatment_quality_gate, dict)
            else list(treatment_quality_gate.get("failed_checks", [])),
            "headline_shock_model_name": None
            if headline_shock_metadata is None
            else str(headline_shock_metadata.get("model_name", "")),
            "headline_shock_min_train_obs": None
            if headline_shock_metadata is None
            else int(headline_shock_metadata.get("min_train_obs", 0)),
        },
        "key_estimates": {
            "tdc_h0": _row_snapshot(tdc_h0),
            "tdc_h4": _row_snapshot(tdc_h4),
            "total_deposits_h0": _row_snapshot(total_h0),
            "total_deposits_h4": _row_snapshot(total_h4),
            "other_component_h0": _row_snapshot(other_h0),
            "other_component_h4": _row_snapshot(other_h4),
        },
        "answer_ready_when": [
            "the baseline total-deposit response is directionally interpretable at key horizons",
            "the baseline non-TDC response is directionally interpretable at key horizons",
            "the total-deposit and non-TDC responses separate enough to support a pass-through versus crowd-out statement",
            "structural proxies corroborate the direction of the non-TDC residual at key horizons",
            "headline and core control-set variants do not overturn the impact sign of the total-deposit response",
        ],
    }
