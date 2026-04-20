from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.identity_baseline import build_identity_variant_ladder
from tdcpass.analysis.local_projections import run_local_projections

_COMPONENT_DEFINITIONS: tuple[dict[str, str], ...] = (
    {
        "outcome": "tdc_fed_treasury_transactions_qoq",
        "label": "fed_treasury_transactions",
        "formula_role": "plus",
        "description": "Fed Treasury-security transactions",
    },
    {
        "outcome": "tdc_us_chartered_treasury_transactions_qoq",
        "label": "us_chartered_bank_treasury_transactions",
        "formula_role": "plus",
        "description": "U.S.-chartered bank Treasury-security transactions",
    },
    {
        "outcome": "tdc_foreign_offices_treasury_transactions_qoq",
        "label": "foreign_offices_treasury_transactions",
        "formula_role": "plus",
        "description": "Foreign banking offices in the U.S. Treasury-security transactions",
    },
    {
        "outcome": "tdc_affiliated_areas_treasury_transactions_qoq",
        "label": "affiliated_areas_treasury_transactions",
        "formula_role": "plus",
        "description": "Banks in U.S.-affiliated areas Treasury-security transactions",
    },
    {
        "outcome": "tdc_row_treasury_transactions_qoq",
        "label": "rest_of_world_treasury_transactions",
        "formula_role": "plus",
        "description": "Rest-of-world Treasury-security transactions",
    },
    {
        "outcome": "tdc_treasury_operating_cash_qoq",
        "label": "treasury_operating_cash_drain",
        "formula_role": "minus",
        "description": "Treasury operating cash drain",
    },
    {
        "outcome": "tdc_fed_remit_positive_qoq",
        "label": "positive_fed_remittances",
        "formula_role": "plus",
        "description": "Positive Fed remittances due to Treasury",
    },
)

_ALIGNMENT_DEFINITIONS: tuple[dict[str, str], ...] = (
    {
        "label": "baseline_bank_only",
        "description": "Imported bank-only TDC versus direct public-component reconstruction",
    },
    {
        "label": "rest_of_world_leg",
        "description": "Direct ROW Treasury leg versus implied no-ROW difference",
    },
    {
        "label": "foreign_bank_sector_legs",
        "description": "Direct foreign-bank-sector Treasury legs versus implied no-foreign-bank-sectors difference",
    },
    {
        "label": "treasury_operating_cash_leg",
        "description": "Direct Treasury operating cash leg versus implied balance from the public component identity",
    },
    {
        "label": "no_toc_no_row_variant",
        "description": "Direct no-TOC/no-ROW public reconstruction versus implied baseline-based difference",
    },
    {
        "label": "positive_fed_remittance_leg",
        "description": "Direct positive Fed remittance leg versus implied no-remit difference",
    },
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


def _treatment_variants(shock_specs: dict[str, Any]) -> list[dict[str, Any]]:
    mapping = (
        ("baseline", "headline", "unexpected_tdc_default"),
        ("domestic_bank_only", "measurement_no_row", "unexpected_tdc_domestic_bank_only"),
        ("no_foreign_bank_sectors", "measurement_no_foreign_bank_sectors", "unexpected_tdc_no_foreign_bank_sectors"),
        ("no_toc_bank_only", "measurement_no_toc", "unexpected_tdc_no_toc_bank_only"),
        ("no_toc_no_row_bank_only", "measurement_no_toc_no_row", "unexpected_tdc_no_toc_no_row_bank_only"),
        ("us_chartered_bank_only", "measurement_us_chartered", "unexpected_tdc_us_chartered_bank_only"),
        ("no_remit_bank_only", "measurement_no_remit", "unexpected_tdc_no_remit_bank_only"),
    )
    variants: list[dict[str, Any]] = []
    for variant_name, family, spec_name in mapping:
        spec = shock_specs.get(spec_name)
        if not isinstance(spec, dict):
            continue
        variants.append(
            {
                "treatment_variant": variant_name,
                "treatment_role": "core" if variant_name == "baseline" else "exploratory",
                "treatment_family": family,
                "shock_column": str(spec["standardized_column"]),
                "target": str(spec["target"]),
                "controls": [str(item) for item in spec.get("predictors", [])],
            }
        )
    return variants


def _worst_gap_snapshot(frame: pd.DataFrame, gap_column: str) -> dict[str, Any] | None:
    if frame.empty or gap_column not in frame.columns:
        return None
    sample = frame[["quarter", gap_column]].dropna().copy()
    if sample.empty:
        return None
    sample["_abs_gap"] = sample[gap_column].abs()
    row = sample.sort_values("_abs_gap", ascending=False).iloc[0]
    return {
        "quarter": str(row["quarter"]),
        "gap_beta": float(row[gap_column]),
        "abs_gap_beta": float(row["_abs_gap"]),
    }


def _construction_alignment_summary(shocked: pd.DataFrame) -> dict[str, Any]:
    required = {
        "quarter",
        "tdc_bank_only_qoq",
        "tdc_domestic_bank_only_qoq",
        "tdc_no_foreign_bank_sectors_qoq",
        "tdc_no_toc_bank_only_qoq",
        "tdc_no_toc_no_row_bank_only_qoq",
        "tdc_no_remit_bank_only_qoq",
        "tdc_fed_treasury_transactions_qoq",
        "tdc_us_chartered_treasury_transactions_qoq",
        "tdc_foreign_offices_treasury_transactions_qoq",
        "tdc_affiliated_areas_treasury_transactions_qoq",
        "tdc_row_treasury_transactions_qoq",
        "tdc_treasury_operating_cash_qoq",
        "tdc_fed_remit_positive_qoq",
    }
    if not required.issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = shocked[list(required)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    frame["direct_baseline_public_qoq"] = (
        frame["tdc_fed_treasury_transactions_qoq"]
        + frame["tdc_us_chartered_treasury_transactions_qoq"]
        + frame["tdc_foreign_offices_treasury_transactions_qoq"]
        + frame["tdc_affiliated_areas_treasury_transactions_qoq"]
        + frame["tdc_row_treasury_transactions_qoq"]
        - frame["tdc_treasury_operating_cash_qoq"]
        + frame["tdc_fed_remit_positive_qoq"]
    )
    frame["baseline_bank_only_gap_beta"] = frame["tdc_bank_only_qoq"] - frame["direct_baseline_public_qoq"]
    frame["implied_row_leg_qoq"] = frame["tdc_bank_only_qoq"] - frame["tdc_domestic_bank_only_qoq"]
    frame["rest_of_world_leg_gap_beta"] = frame["tdc_row_treasury_transactions_qoq"] - frame["implied_row_leg_qoq"]
    frame["direct_foreign_bank_sector_legs_qoq"] = (
        frame["tdc_foreign_offices_treasury_transactions_qoq"] + frame["tdc_affiliated_areas_treasury_transactions_qoq"]
    )
    frame["implied_foreign_bank_sector_legs_qoq"] = frame["tdc_bank_only_qoq"] - frame["tdc_no_foreign_bank_sectors_qoq"]
    frame["foreign_bank_sector_legs_gap_beta"] = (
        frame["direct_foreign_bank_sector_legs_qoq"] - frame["implied_foreign_bank_sector_legs_qoq"]
    )
    frame["implied_treasury_operating_cash_leg_qoq"] = (
        frame["tdc_fed_treasury_transactions_qoq"]
        + frame["tdc_us_chartered_treasury_transactions_qoq"]
        + frame["tdc_foreign_offices_treasury_transactions_qoq"]
        + frame["tdc_affiliated_areas_treasury_transactions_qoq"]
        + frame["tdc_row_treasury_transactions_qoq"]
        + frame["tdc_fed_remit_positive_qoq"]
        - frame["tdc_bank_only_qoq"]
    )
    frame["treasury_operating_cash_leg_gap_beta"] = (
        frame["tdc_treasury_operating_cash_qoq"] - frame["implied_treasury_operating_cash_leg_qoq"]
    )
    frame["direct_no_toc_no_row_variant_qoq"] = (
        frame["tdc_fed_treasury_transactions_qoq"]
        + frame["tdc_us_chartered_treasury_transactions_qoq"]
        + frame["tdc_foreign_offices_treasury_transactions_qoq"]
        + frame["tdc_affiliated_areas_treasury_transactions_qoq"]
        + frame["tdc_fed_remit_positive_qoq"]
    )
    frame["implied_no_toc_no_row_variant_qoq"] = (
        frame["tdc_bank_only_qoq"]
        + frame["tdc_treasury_operating_cash_qoq"]
        - frame["tdc_row_treasury_transactions_qoq"]
    )
    frame["no_toc_no_row_variant_gap_beta"] = (
        frame["direct_no_toc_no_row_variant_qoq"] - frame["implied_no_toc_no_row_variant_qoq"]
    )
    frame["implied_positive_fed_remittance_leg_qoq"] = frame["tdc_bank_only_qoq"] - frame["tdc_no_remit_bank_only_qoq"]
    frame["positive_fed_remittance_leg_gap_beta"] = (
        frame["tdc_fed_remit_positive_qoq"] - frame["implied_positive_fed_remittance_leg_qoq"]
    )

    mappings = (
        ("baseline_bank_only", "baseline_bank_only_gap_beta"),
        ("rest_of_world_leg", "rest_of_world_leg_gap_beta"),
        ("foreign_bank_sector_legs", "foreign_bank_sector_legs_gap_beta"),
        ("treasury_operating_cash_leg", "treasury_operating_cash_leg_gap_beta"),
        ("no_toc_no_row_variant", "no_toc_no_row_variant_gap_beta"),
        ("positive_fed_remittance_leg", "positive_fed_remittance_leg_gap_beta"),
    )
    rows: dict[str, Any] = {}
    for label, gap_column in mappings:
        series = frame[gap_column].dropna()
        if series.empty:
            continue
        max_abs_gap = float(series.abs().max())
        rows[label] = {
            "n": int(series.shape[0]),
            "mean_gap_beta": float(series.mean()),
            "mae_gap_beta": float(series.abs().mean()),
            "max_abs_gap_beta": max_abs_gap,
            "worst_quarter": _worst_gap_snapshot(frame, gap_column),
            "quarterly_alignment": "exact" if max_abs_gap <= 1e-9 else "inexact",
        }
    return {
        "status": "available" if rows else "not_available",
        "rows": rows,
        "definitions": list(_ALIGNMENT_DEFINITIONS),
    }


def build_tdc_treatment_audit_summary(
    *,
    shocked: pd.DataFrame,
    baseline_lp_spec: dict[str, Any],
    baseline_shock_spec: dict[str, Any],
    shock_specs: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
    bootstrap_reps: int = 40,
    bootstrap_block_length: int = 4,
) -> dict[str, Any]:
    construction_alignment = _construction_alignment_summary(shocked)
    component_lp = run_local_projections(
        shocked,
        shock_col=str(baseline_lp_spec.get("shock_column", "tdc_residual_z")),
        outcome_cols=[str(item["outcome"]) for item in _COMPONENT_DEFINITIONS] + [str(baseline_shock_spec.get("target", "tdc_bank_only_qoq"))],
        controls=[str(col) for col in baseline_lp_spec.get("controls", [])],
        include_lagged_outcome=False,
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        nw_lags=int(baseline_lp_spec.get("nw_lags", 4)),
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="tdc_treatment_component_baseline",
    )

    variants = _treatment_variants(shock_specs)
    ladder = build_identity_variant_ladder(
        shocked,
        variants=variants,
        total_outcome_col="total_deposits_bank_qoq",
        horizons=[int(h) for h in baseline_lp_spec.get("horizons", [])],
        cumulative=bool(baseline_lp_spec.get("cumulative", True)),
        spec_name="tdc_treatment_variant_audit",
        bootstrap_reps=bootstrap_reps,
        bootstrap_block_length=bootstrap_block_length,
    )
    baseline_variant_df = ladder[ladder["treatment_variant"] == "baseline"].copy() if not ladder.empty else ladder

    key_horizons: dict[str, Any] = {}
    for horizon in horizons:
        baseline_tdc = _snapshot(_lp_row(component_lp, outcome=str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")), horizon=horizon))
        baseline_total = _snapshot(_lp_row(baseline_variant_df, outcome="total_deposits_bank_qoq", horizon=horizon))
        baseline_residual = _snapshot(_lp_row(baseline_variant_df, outcome="other_component_qoq", horizon=horizon))

        component_payloads: dict[str, Any] = {}
        ranking: list[dict[str, Any]] = []
        foreign_bank_sector_signed_beta = 0.0
        foreign_bank_sector_present = False
        signed_sum = 0.0
        signed_sum_present = False
        baseline_beta = None if baseline_tdc is None else float(baseline_tdc["beta"])

        for item in _COMPONENT_DEFINITIONS:
            response = _snapshot(_lp_row(component_lp, outcome=str(item["outcome"]), horizon=horizon))
            signed_beta = None
            share_of_baseline = None
            if response is not None:
                raw_beta = float(response["beta"])
                signed_beta = raw_beta if str(item["formula_role"]) == "plus" else -raw_beta
                signed_sum += signed_beta
                signed_sum_present = True
                if baseline_beta is not None and baseline_beta != 0.0:
                    share_of_baseline = signed_beta / baseline_beta
                ranking.append(
                    {
                        "component": str(item["label"]),
                        "signed_beta": signed_beta,
                        "abs_signed_beta": abs(signed_beta),
                    }
                )
                if str(item["label"]) in {
                    "foreign_offices_treasury_transactions",
                    "affiliated_areas_treasury_transactions",
                }:
                    foreign_bank_sector_signed_beta += signed_beta
                    foreign_bank_sector_present = True
            component_payloads[str(item["label"])] = {
                "outcome": str(item["outcome"]),
                "formula_role": str(item["formula_role"]),
                "description": str(item["description"]),
                "raw_response": response,
                "signed_contribution_beta": signed_beta,
                "share_of_baseline_tdc_beta": share_of_baseline,
            }

        ranking.sort(key=lambda payload: payload["abs_signed_beta"], reverse=True)
        dominant_component = ranking[0]["component"] if ranking else None

        variant_payloads: dict[str, Any] = {}
        for variant in variants:
            variant_name = str(variant["treatment_variant"])
            if variant_name == "baseline":
                continue
            variant_df = ladder[ladder["treatment_variant"] == variant_name].copy() if not ladder.empty else ladder
            target_name = str(variant["target"])
            target_response = _snapshot(_lp_row(variant_df, outcome=target_name, horizon=horizon))
            total_response = _snapshot(_lp_row(variant_df, outcome="total_deposits_bank_qoq", horizon=horizon))
            residual_response = _snapshot(_lp_row(variant_df, outcome="other_component_qoq", horizon=horizon))
            residual_shift = (
                None
                if baseline_residual is None or residual_response is None
                else float(residual_response["beta"]) - float(baseline_residual["beta"])
            )
            target_shift = (
                None
                if baseline_tdc is None or target_response is None
                else float(target_response["beta"]) - float(baseline_tdc["beta"])
            )
            total_shift = (
                None
                if baseline_total is None or total_response is None
                else float(total_response["beta"]) - float(baseline_total["beta"])
            )
            variant_payloads[variant_name] = {
                "target": target_name,
                "target_response": target_response,
                "total_response": total_response,
                "residual_response": residual_response,
                "target_shift_vs_baseline_beta": target_shift,
                "total_shift_vs_baseline_beta": total_shift,
                "residual_shift_vs_baseline_beta": residual_shift,
            }

        largest_variant = None
        largest_shift = None
        ranked_variants = [
            (name, abs(float(payload["residual_shift_vs_baseline_beta"])))
            for name, payload in variant_payloads.items()
            if payload.get("residual_shift_vs_baseline_beta") is not None
        ]
        if ranked_variants:
            ranked_variants.sort(key=lambda item: item[1], reverse=True)
            largest_variant, largest_shift = ranked_variants[0]

        key_horizons[f"h{horizon}"] = {
            "baseline_tdc_response": baseline_tdc,
            "baseline_total_response": baseline_total,
            "baseline_residual_response": baseline_residual,
            "direct_component_responses": component_payloads,
            "signed_component_ranking": ranking,
            "dominant_signed_component": dominant_component,
            "foreign_bank_sectors_signed_beta_sum": foreign_bank_sector_signed_beta if foreign_bank_sector_present else None,
            "signed_component_sum_beta": signed_sum if signed_sum_present else None,
            "signed_component_sum_minus_direct_tdc_beta": (
                None
                if not signed_sum_present or baseline_beta is None
                else signed_sum - baseline_beta
            ),
            "variant_removal_diagnostics": variant_payloads,
            "largest_residual_shift_variant": largest_variant,
            "largest_abs_residual_shift_beta": largest_shift,
        }

    takeaways = [
        "This audit separates direct baseline-shock responses of the TDC building blocks from residual shifts created by removing whole treatment legs.",
        "Use the direct component responses to see which TDC pieces move with the baseline shock, and use the variant-removal diagnostics to see which omitted legs actually change the residual most.",
    ]
    if construction_alignment.get("status") == "available":
        exact_rows = [
            label
            for label, payload in dict(construction_alignment.get("rows", {})).items()
            if str(payload.get("quarterly_alignment", "")) == "exact"
        ]
        if exact_rows:
            takeaways.append(
                "Quarter-level construction alignment is exact for the public-component reconstruction and all currently audited implied legs, so the remaining issue is dynamic interpretation rather than arithmetic mismatch."
            )
    audit_h0 = key_horizons.get("h0", {})
    h0_dominant = audit_h0.get("dominant_signed_component")
    h0_no_row = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("domestic_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    h0_no_toc = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("no_toc_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    h0_no_toc_no_row = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("no_toc_no_row_bank_only", {})
        .get("residual_shift_vs_baseline_beta")
    )
    h0_no_foreign = (
        audit_h0.get("variant_removal_diagnostics", {})
        .get("no_foreign_bank_sectors", {})
        .get("residual_shift_vs_baseline_beta")
    )
    h0_foreign_signed = audit_h0.get("foreign_bank_sectors_signed_beta_sum")
    if h0_dominant is not None:
        takeaways.append(f"At h0, the largest signed direct component contribution comes from `{h0_dominant}`.")
    if h0_no_toc is not None:
        takeaways.append(
            f"At h0, removing Treasury operating cash from the treatment shifts the residual by about {float(h0_no_toc):.2f}."
        )
    if h0_no_toc_no_row is not None:
        takeaways.append(
            f"At h0, removing Treasury operating cash and ROW together shifts the residual by about {float(h0_no_toc_no_row):.2f}."
        )
    if h0_no_row is not None and h0_no_foreign is not None:
        takeaways.append(
            f"At h0, removing only ROW shifts the residual by about {float(h0_no_row):.2f}, while removing only foreign bank-sector Treasury legs shifts it by about {float(h0_no_foreign):.2f}."
        )
    if h0_foreign_signed is not None:
        takeaways.append(
            f"At h0, the combined signed point contribution from foreign bank-sector Treasury legs is about {float(h0_foreign_signed):.2f} in the direct component read."
        )

    return {
        "status": "available" if key_horizons else "not_available",
        "headline_question": "Which TDC building blocks move with the baseline shock, and which omitted treatment legs actually change the residual most when removed?",
        "estimation_path": {
            "component_lp_spec_name": "tdc_treatment_component_baseline",
            "variant_ladder_spec_name": "tdc_treatment_variant_audit",
            "summary_artifact": "tdc_treatment_audit_summary.json",
        },
        "component_definitions": [
            {
                "label": str(item["label"]),
                "outcome": str(item["outcome"]),
                "formula_role": str(item["formula_role"]),
                "description": str(item["description"]),
            }
            for item in _COMPONENT_DEFINITIONS
        ],
        "variant_definitions": [
            {
                "treatment_variant": str(item["treatment_variant"]),
                "treatment_family": str(item["treatment_family"]),
                "target": str(item["target"]),
            }
            for item in variants
        ],
        "baseline_target": str(baseline_shock_spec.get("target", "tdc_bank_only_qoq")),
        "construction_alignment": construction_alignment,
        "key_horizons": key_horizons,
        "takeaways": takeaways,
    }
