from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from tdcpass.analysis.local_projections import run_local_projections

BASELINE_OUTCOMES: tuple[str, ...] = (
    "other_component_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_source_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_noncore_system_qoq",
    "strict_non_treasury_securities_qoq",
    "strict_identifiable_total_qoq",
    "strict_identifiable_gap_qoq",
    "strict_funding_offset_total_qoq",
    "strict_identifiable_net_after_funding_qoq",
    "strict_gap_after_funding_qoq",
)

EXCLUDED_OUTCOMES: tuple[str, ...] = (
    "other_component_no_toc_no_row_bank_only_qoq",
    "strict_loan_core_min_qoq",
    "strict_loan_source_qoq",
    "strict_loan_core_plus_private_borrower_qoq",
    "strict_loan_noncore_system_qoq",
    "strict_non_treasury_securities_qoq",
    "strict_identifiable_total_qoq",
    "strict_identifiable_gap_no_toc_no_row_qoq",
    "strict_funding_offset_total_qoq",
    "strict_identifiable_net_after_funding_qoq",
    "strict_gap_after_funding_no_toc_no_row_qoq",
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


def _abs_share(component: dict[str, Any] | None, total: dict[str, Any] | None) -> float | None:
    if component is None or total is None:
        return None
    denominator = abs(float(total["beta"]))
    if denominator == 0.0:
        return None
    return abs(float(component["beta"])) / denominator


def _signed_share(component: dict[str, Any] | None, total: dict[str, Any] | None) -> float | None:
    if component is None or total is None:
        return None
    denominator = float(total["beta"])
    if denominator == 0.0:
        return None
    return float(component["beta"]) / denominator


def _component_payload(
    *,
    residual: dict[str, Any] | None,
    direct_core: dict[str, Any] | None,
    loan: dict[str, Any] | None,
    private_augmented_core: dict[str, Any] | None,
    noncore_system: dict[str, Any] | None,
    securities: dict[str, Any] | None,
    total: dict[str, Any] | None,
    gap: dict[str, Any] | None,
    funding: dict[str, Any] | None,
    net_after_funding: dict[str, Any] | None,
    gap_after_funding: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "residual_response": residual,
        "strict_headline_direct_core_response": direct_core,
        "strict_loan_source_response": loan,
        "strict_loan_core_plus_private_borrower_response": private_augmented_core,
        "strict_loan_noncore_system_response": noncore_system,
        "strict_non_treasury_securities_response": securities,
        "strict_identifiable_total_response": total,
        "strict_identifiable_gap_response": gap,
        "strict_funding_offset_total_response": funding,
        "strict_identifiable_net_after_funding_response": net_after_funding,
        "strict_gap_after_funding_response": gap_after_funding,
        "strict_headline_direct_core_share_of_residual_abs": _abs_share(direct_core, residual),
        "strict_loan_share_of_residual_abs": _abs_share(loan, residual),
        "strict_securities_share_of_residual_abs": _abs_share(securities, residual),
        "strict_identifiable_share_of_residual_abs": _abs_share(total, residual),
        "strict_gap_share_of_residual_abs": _abs_share(gap, residual),
        "strict_funding_offset_share_of_identifiable_total_beta": _signed_share(funding, total),
        "strict_net_after_funding_share_of_residual_abs": _abs_share(net_after_funding, residual),
        "strict_gap_after_funding_share_of_residual_abs": _abs_share(gap_after_funding, residual),
    }


def _interpretation_label(
    *,
    baseline: dict[str, Any],
    excluded: dict[str, Any],
) -> str:
    baseline_residual = baseline.get("residual_response")
    excluded_residual = excluded.get("residual_response")
    if baseline_residual is None or excluded_residual is None:
        return "missing_missing_channel_inputs"
    baseline_abs = abs(float(baseline_residual["beta"]))
    excluded_abs = abs(float(excluded_residual["beta"]))
    if baseline_abs == 0.0:
        return "baseline_residual_zero"
    residual_ratio = excluded_abs / baseline_abs
    baseline_gap_share = baseline.get("strict_gap_share_of_residual_abs")
    excluded_gap_share = excluded.get("strict_gap_share_of_residual_abs")
    excluded_gap_after_funding_share = excluded.get("strict_gap_after_funding_share_of_residual_abs")
    excluded_identifiable_share = excluded.get("strict_identifiable_share_of_residual_abs")
    excluded_total = excluded.get("strict_identifiable_total_response")
    sign_mismatch = (
        excluded_total is not None
        and float(excluded_total["beta"]) != 0.0
        and float(excluded_residual["beta"]) != 0.0
        and float(excluded_total["beta"]) * float(excluded_residual["beta"]) < 0.0
    )
    if residual_ratio <= 0.35:
        if sign_mismatch:
            return "toc_row_exclusion_exposes_sign_mismatch_in_direct_counts"
        if excluded_identifiable_share is not None and excluded_identifiable_share >= 0.75:
            return "toc_row_exclusion_reveals_high_direct_count_coverage"
        if excluded_gap_after_funding_share is not None and excluded_gap_after_funding_share <= 0.5:
            return "toc_row_exclusion_relaxes_residual_and_funding_adjusted_gap"
        return "toc_row_exclusion_relaxes_residual_but_missing_channels_still_dominate"
    if (
        baseline_gap_share is not None
        and excluded_gap_share is not None
        and excluded_gap_share < baseline_gap_share
        and excluded_gap_after_funding_share is not None
        and excluded_gap_after_funding_share < excluded_gap_share
        and excluded_gap_after_funding_share <= 0.5
    ):
        return "funding_adjustment_improves_missing_channel_read"
    return "missing_channel_story_broadly_unchanged"


def build_strict_missing_channel_summary(
    *,
    strict_lp_irf: pd.DataFrame,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    excluded_shock_spec: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    baseline_controls = [str(value) for value in baseline_lp_spec.get("controls", [])]
    comparison_controls = _comparison_controls(
        baseline_controls=baseline_controls,
        baseline_shock_spec=baseline_shock_spec,
        comparison_shock_spec=excluded_shock_spec,
    )
    baseline_horizons = [int(value) for value in baseline_lp_spec.get("horizons", [])]
    cumulative = bool(baseline_lp_spec.get("cumulative", True))
    include_lagged_outcome = bool(baseline_lp_spec.get("include_lagged_outcome", False))
    nw_lags = int(baseline_lp_spec.get("nw_lags", 4))

    excluded_frame = shocked.copy()
    excluded_frame["strict_identifiable_gap_no_toc_no_row_qoq"] = (
        excluded_frame["other_component_no_toc_no_row_bank_only_qoq"] - excluded_frame["strict_identifiable_total_qoq"]
    )
    excluded_frame["strict_gap_after_funding_no_toc_no_row_qoq"] = (
        excluded_frame["other_component_no_toc_no_row_bank_only_qoq"]
        - excluded_frame["strict_identifiable_net_after_funding_qoq"]
    )
    excluded_frame["lag_strict_identifiable_gap_no_toc_no_row_qoq"] = excluded_frame[
        "strict_identifiable_gap_no_toc_no_row_qoq"
    ].shift(1)
    excluded_frame["lag_strict_gap_after_funding_no_toc_no_row_qoq"] = excluded_frame[
        "strict_gap_after_funding_no_toc_no_row_qoq"
    ].shift(1)

    excluded_lp_irf = run_local_projections(
        excluded_frame,
        shock_col=str(excluded_shock_spec.get("standardized_column", "tdc_no_toc_no_row_bank_only_residual_z")),
        outcome_cols=list(EXCLUDED_OUTCOMES),
        controls=comparison_controls,
        include_lagged_outcome=include_lagged_outcome,
        horizons=baseline_horizons,
        nw_lags=nw_lags,
        cumulative=cumulative,
        spec_name="strict_missing_channel_toc_row_excluded_reference",
    )

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_payload = _component_payload(
            residual=_snapshot(_lp_row(strict_lp_irf, outcome="other_component_qoq", horizon=horizon)),
            direct_core=_snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            loan=_snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon)),
            private_augmented_core=_snapshot(
                _lp_row(strict_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(_lp_row(strict_lp_irf, outcome="strict_loan_noncore_system_qoq", horizon=horizon)),
            securities=_snapshot(_lp_row(strict_lp_irf, outcome="strict_non_treasury_securities_qoq", horizon=horizon)),
            total=_snapshot(_lp_row(strict_lp_irf, outcome="strict_identifiable_total_qoq", horizon=horizon)),
            gap=_snapshot(_lp_row(strict_lp_irf, outcome="strict_identifiable_gap_qoq", horizon=horizon)),
            funding=_snapshot(_lp_row(strict_lp_irf, outcome="strict_funding_offset_total_qoq", horizon=horizon)),
            net_after_funding=_snapshot(
                _lp_row(strict_lp_irf, outcome="strict_identifiable_net_after_funding_qoq", horizon=horizon)
            ),
            gap_after_funding=_snapshot(_lp_row(strict_lp_irf, outcome="strict_gap_after_funding_qoq", horizon=horizon)),
        )
        excluded_payload = _component_payload(
            residual=_snapshot(
                _lp_row(excluded_lp_irf, outcome="other_component_no_toc_no_row_bank_only_qoq", horizon=horizon)
            ),
            direct_core=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_loan_core_min_qoq", horizon=horizon)),
            loan=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_loan_source_qoq", horizon=horizon)),
            private_augmented_core=_snapshot(
                _lp_row(excluded_lp_irf, outcome="strict_loan_core_plus_private_borrower_qoq", horizon=horizon)
            ),
            noncore_system=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_loan_noncore_system_qoq", horizon=horizon)),
            securities=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_non_treasury_securities_qoq", horizon=horizon)),
            total=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_identifiable_total_qoq", horizon=horizon)),
            gap=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_identifiable_gap_no_toc_no_row_qoq", horizon=horizon)),
            funding=_snapshot(_lp_row(excluded_lp_irf, outcome="strict_funding_offset_total_qoq", horizon=horizon)),
            net_after_funding=_snapshot(
                _lp_row(excluded_lp_irf, outcome="strict_identifiable_net_after_funding_qoq", horizon=horizon)
            ),
            gap_after_funding=_snapshot(
                _lp_row(excluded_lp_irf, outcome="strict_gap_after_funding_no_toc_no_row_qoq", horizon=horizon)
            ),
        )
        if all(value is None for value in baseline_payload.values()) and all(value is None for value in excluded_payload.values()):
            continue
        key_horizons[f"h{horizon}"] = {
            "baseline": baseline_payload,
            "toc_row_excluded": excluded_payload,
            "excluded_minus_baseline_beta": {
                "residual_response": None
                if baseline_payload["residual_response"] is None or excluded_payload["residual_response"] is None
                else float(excluded_payload["residual_response"]["beta"]) - float(baseline_payload["residual_response"]["beta"]),
                "strict_headline_direct_core_response": None
                if baseline_payload["strict_headline_direct_core_response"] is None
                or excluded_payload["strict_headline_direct_core_response"] is None
                else float(excluded_payload["strict_headline_direct_core_response"]["beta"])
                - float(baseline_payload["strict_headline_direct_core_response"]["beta"]),
                "strict_loan_source_response": None
                if baseline_payload["strict_loan_source_response"] is None
                or excluded_payload["strict_loan_source_response"] is None
                else float(excluded_payload["strict_loan_source_response"]["beta"])
                - float(baseline_payload["strict_loan_source_response"]["beta"]),
                "strict_non_treasury_securities_response": None
                if baseline_payload["strict_non_treasury_securities_response"] is None
                or excluded_payload["strict_non_treasury_securities_response"] is None
                else float(excluded_payload["strict_non_treasury_securities_response"]["beta"])
                - float(baseline_payload["strict_non_treasury_securities_response"]["beta"]),
                "strict_identifiable_total_response": None
                if baseline_payload["strict_identifiable_total_response"] is None
                or excluded_payload["strict_identifiable_total_response"] is None
                else float(excluded_payload["strict_identifiable_total_response"]["beta"])
                - float(baseline_payload["strict_identifiable_total_response"]["beta"]),
                "strict_funding_offset_total_response": None
                if baseline_payload["strict_funding_offset_total_response"] is None
                or excluded_payload["strict_funding_offset_total_response"] is None
                else float(excluded_payload["strict_funding_offset_total_response"]["beta"])
                - float(baseline_payload["strict_funding_offset_total_response"]["beta"]),
                "strict_identifiable_net_after_funding_response": None
                if baseline_payload["strict_identifiable_net_after_funding_response"] is None
                or excluded_payload["strict_identifiable_net_after_funding_response"] is None
                else float(excluded_payload["strict_identifiable_net_after_funding_response"]["beta"])
                - float(baseline_payload["strict_identifiable_net_after_funding_response"]["beta"]),
                "strict_identifiable_gap_response": None
                if baseline_payload["strict_identifiable_gap_response"] is None
                or excluded_payload["strict_identifiable_gap_response"] is None
                else float(excluded_payload["strict_identifiable_gap_response"]["beta"])
                - float(baseline_payload["strict_identifiable_gap_response"]["beta"]),
                "strict_gap_after_funding_response": None
                if baseline_payload["strict_gap_after_funding_response"] is None
                or excluded_payload["strict_gap_after_funding_response"] is None
                else float(excluded_payload["strict_gap_after_funding_response"]["beta"])
                - float(baseline_payload["strict_gap_after_funding_response"]["beta"]),
                "strict_identifiable_share_of_residual_abs": None
                if baseline_payload["strict_identifiable_share_of_residual_abs"] is None
                or excluded_payload["strict_identifiable_share_of_residual_abs"] is None
                else float(excluded_payload["strict_identifiable_share_of_residual_abs"])
                - float(baseline_payload["strict_identifiable_share_of_residual_abs"]),
                "strict_gap_after_funding_share_of_residual_abs": None
                if baseline_payload["strict_gap_after_funding_share_of_residual_abs"] is None
                or excluded_payload["strict_gap_after_funding_share_of_residual_abs"] is None
                else float(excluded_payload["strict_gap_after_funding_share_of_residual_abs"])
                - float(baseline_payload["strict_gap_after_funding_share_of_residual_abs"]),
            },
            "interpretation": _interpretation_label(
                baseline=baseline_payload,
                excluded=excluded_payload,
            ),
        }

    status = "available" if key_horizons else "not_available"
    takeaways = [
        "This artifact returns to the strict lane after separating the treatment-side TOC/ROW issue: it asks which direct-count pieces still fail to verify the non-TDC residual once the suspect treatment bundle is excluded.",
        "Read the TOC/ROW-excluded block as a diagnostic comparison, not as a replacement headline treatment.",
    ]
    h0 = key_horizons.get("h0", {})
    baseline_h0 = dict(h0.get("baseline", {}))
    excluded_h0 = dict(h0.get("toc_row_excluded", {}))
    baseline_residual = dict(baseline_h0.get("residual_response", {}) or {}).get("beta")
    excluded_residual = dict(excluded_h0.get("residual_response", {}) or {}).get("beta")
    if baseline_residual is not None and excluded_residual is not None:
        takeaways.append(
            "At h0, the strict missing-channel comparison starts from a much smaller residual once TOC/ROW is excluded: "
            f"baseline residual ≈ {float(baseline_residual):.2f}, TOC/ROW-excluded residual ≈ {float(excluded_residual):.2f}."
        )
    excluded_direct_core = dict(excluded_h0.get("strict_headline_direct_core_response", {}) or {}).get("beta")
    excluded_loan = dict(excluded_h0.get("strict_loan_source_response", {}) or {}).get("beta")
    excluded_private_aug = dict(excluded_h0.get("strict_loan_core_plus_private_borrower_response", {}) or {}).get("beta")
    excluded_noncore = dict(excluded_h0.get("strict_loan_noncore_system_response", {}) or {}).get("beta")
    excluded_securities = dict(excluded_h0.get("strict_non_treasury_securities_response", {}) or {}).get("beta")
    excluded_net_after_funding = dict(excluded_h0.get("strict_identifiable_net_after_funding_response", {}) or {}).get("beta")
    if (
        excluded_direct_core is not None
        and excluded_loan is not None
        and excluded_private_aug is not None
        and excluded_noncore is not None
        and excluded_securities is not None
        and excluded_net_after_funding is not None
    ):
        takeaways.append(
            "Under the TOC/ROW-excluded comparison at h0, the directly counted pieces still need to be read separately: "
            f"headline direct core ≈ {float(excluded_direct_core):.2f}, current broad loan source ≈ {float(excluded_loan):.2f}, "
            f"private-borrower-augmented core ≈ {float(excluded_private_aug):.2f}, noncore/system diagnostic ≈ {float(excluded_noncore):.2f}, "
            f"securities ≈ {float(excluded_securities):.2f}, "
            f"funding-adjusted net ≈ {float(excluded_net_after_funding):.2f}."
        )
    excluded_total_beta = dict(excluded_h0.get("strict_identifiable_total_response", {}) or {}).get("beta")
    if (
        excluded_total_beta is not None
        and excluded_residual is not None
        and float(excluded_total_beta) != 0.0
        and float(excluded_residual) != 0.0
        and float(excluded_total_beta) * float(excluded_residual) < 0.0
    ):
        takeaways.append(
            "At h0, the TOC/ROW-excluded comparison exposes a sign mismatch inside the strict lane: "
            f"the residual stays negative at about {float(excluded_residual):.2f}, while the strict identifiable total turns positive at about {float(excluded_total_beta):.2f}."
        )
    excluded_gap_share = excluded_h0.get("strict_identifiable_gap_response")
    excluded_gap_after_funding_share = excluded_h0.get("strict_gap_after_funding_share_of_residual_abs")
    if excluded_h0.get("strict_gap_share_of_residual_abs") is not None and excluded_gap_after_funding_share is not None:
        takeaways.append(
            "Even after excluding TOC/ROW, the direct-count verification problem remains large: "
            f"strict gap share of residual ≈ {float(excluded_h0['strict_gap_share_of_residual_abs']):.2f}, "
            f"gap-after-funding share ≈ {float(excluded_gap_after_funding_share):.2f}."
        )
    if h0:
        takeaways.append(
            f"H0 interpretation: `{h0.get('interpretation', 'missing_missing_channel_inputs')}`."
        )

    return {
        "status": status,
        "headline_question": "Once the TOC/ROW treatment bundle is excluded as a diagnostic comparison, which direct-count strict pieces still fail to verify the non-TDC residual?",
        "estimation_path": {
            "baseline_strict_artifact": "lp_irf_strict_identifiable.csv",
            "comparison_artifact": "strict_missing_channel_summary.json",
            "comparison_shock_column": str(
                excluded_shock_spec.get("standardized_column", "tdc_no_toc_no_row_bank_only_residual_z")
            ),
        },
        "comparison_definition": {
            "baseline_target": str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
            "comparison_target": str(excluded_shock_spec.get("target", "tdc_no_toc_no_row_bank_only_qoq")),
            "baseline_residual_outcome": "other_component_qoq",
            "comparison_residual_outcome": "other_component_no_toc_no_row_bank_only_qoq",
            "release_role": "strict_missing_channel_diagnostic",
        },
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
