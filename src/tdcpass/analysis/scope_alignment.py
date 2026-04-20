from __future__ import annotations

from typing import Any

import pandas as pd

from tdcpass.analysis.identity_baseline import build_identity_variant_ladder

SCOPE_VARIANT_ORDER: tuple[str, ...] = (
    "baseline",
    "domestic_bank_only",
    "us_chartered_bank_only",
)


def _identity_row(
    df: pd.DataFrame,
    *,
    treatment_variant: str,
    outcome: str,
    horizon: int,
) -> dict[str, Any] | None:
    required = {"treatment_variant", "outcome", "horizon"}
    if df.empty or not required.issubset(df.columns):
        return None
    sample = df[
        (df["treatment_variant"] == treatment_variant) & (df["outcome"] == outcome) & (df["horizon"] == horizon)
    ]
    if sample.empty:
        return None
    return sample.iloc[0].to_dict()


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


def _build_variants(*, shock_specs: dict[str, Any]) -> list[dict[str, Any]]:
    mapping = {
        "baseline": "unexpected_tdc_default",
        "domestic_bank_only": "unexpected_tdc_domestic_bank_only",
        "us_chartered_bank_only": "unexpected_tdc_us_chartered_bank_only",
    }
    family = {
        "baseline": "headline",
        "domestic_bank_only": "measurement_no_row",
        "us_chartered_bank_only": "measurement_us_chartered",
    }
    variants: list[dict[str, Any]] = []
    for variant_name in SCOPE_VARIANT_ORDER:
        spec_name = mapping[variant_name]
        spec = shock_specs.get(spec_name)
        if not isinstance(spec, dict):
            continue
        variants.append(
            {
                "treatment_variant": variant_name,
                "treatment_role": "core" if variant_name == "baseline" else "exploratory",
                "treatment_family": family[variant_name],
                "shock_column": str(spec["standardized_column"]),
                "target": str(spec["target"]),
                "controls": [str(item) for item in spec.get("predictors", [])],
            }
        )
    return variants


def _concept_payload(
    *,
    ladder: pd.DataFrame,
    total_outcome_col: str,
    residual_outcome_col: str,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    key_horizons: dict[str, Any] = {}
    comparison_variants = [
        name
        for name in SCOPE_VARIANT_ORDER
        if name != "baseline"
        and name in set(ladder.get("treatment_variant", pd.Series(dtype="object")).dropna().astype(str).tolist())
    ]
    for horizon in horizons:
        baseline_target = _snapshot(_identity_row(ladder, treatment_variant="baseline", outcome="tdc_bank_only_qoq", horizon=horizon))
        baseline_total = _snapshot(_identity_row(ladder, treatment_variant="baseline", outcome=total_outcome_col, horizon=horizon))
        baseline_residual = _snapshot(
            _identity_row(ladder, treatment_variant="baseline", outcome=residual_outcome_col, horizon=horizon)
        )
        baseline_payload = {
            "target": "tdc_bank_only_qoq",
            "target_response": baseline_target,
            "total_response": baseline_total,
            "residual_response": baseline_residual,
        }
        variants_payload: dict[str, Any] = {}
        for variant in comparison_variants:
            target_name = "tdc_domestic_bank_only_qoq" if variant == "domestic_bank_only" else "tdc_us_chartered_bank_only_qoq"
            target_response = _snapshot(_identity_row(ladder, treatment_variant=variant, outcome=target_name, horizon=horizon))
            total_response = _snapshot(_identity_row(ladder, treatment_variant=variant, outcome=total_outcome_col, horizon=horizon))
            residual_response = _snapshot(
                _identity_row(ladder, treatment_variant=variant, outcome=residual_outcome_col, horizon=horizon)
            )
            if target_response is None and total_response is None and residual_response is None:
                continue
            variants_payload[variant] = {
                "target": target_name,
                "target_response": target_response,
                "total_response": total_response,
                "residual_response": residual_response,
                "differences_vs_baseline_beta": {
                    "target_response": None
                    if baseline_target is None or target_response is None
                    else float(target_response["beta"]) - float(baseline_target["beta"]),
                    "total_response": None
                    if baseline_total is None or total_response is None
                    else float(total_response["beta"]) - float(baseline_total["beta"]),
                    "residual_response": None
                    if baseline_residual is None or residual_response is None
                    else float(residual_response["beta"]) - float(baseline_residual["beta"]),
                },
            }
        if any(value is not None for value in baseline_payload.values()) or variants_payload:
            key_horizons[f"h{horizon}"] = {
                "baseline": baseline_payload,
                "variants": variants_payload,
            }
    return {
        "comparison_variants": comparison_variants,
        "key_horizons": key_horizons,
    }


def _recommended_policy() -> dict[str, Any]:
    return {
        "decision_status": "keep_current_headline_outcome_for_now",
        "headline_deposit_concept": "total_deposits_including_interbank",
        "headline_outcome": "total_deposits_bank_qoq",
        "preferred_scope_check_variant": "us_chartered_bank_only",
        "secondary_scope_sensitivity_variant": "domestic_bank_only",
        "secondary_outcome_sensitivity": "deposits_only_ex_interbank",
        "headline_read": (
            "Keep `total_deposits_bank_qoq` as the headline outcome for now, use `tdc_us_chartered_bank_only_qoq` as the "
            "standard scope-check comparison, and keep the no-ROW and deposits-only comparisons as secondary sensitivities."
        ),
        "rationale": (
            "A full headline migration to deposits-only would change the project object more broadly than a scope diagnostic. "
            "The cleaner immediate step is to keep the established headline outcome but standardize the true U.S.-chartered "
            "bank-leg match as the main scope check, because it isolates bank-leg matching rather than only removing the "
            "rest-of-world term."
        ),
    }


def build_scope_alignment_summary(
    *,
    shocked: pd.DataFrame,
    lp_specs: dict[str, Any],
    shock_specs: dict[str, Any],
    horizons: tuple[int, ...] = (0, 1, 4, 8),
) -> dict[str, Any]:
    sensitivity_spec = lp_specs["specs"]["sensitivity"]
    variants = _build_variants(shock_specs=shock_specs)
    if not variants:
        return {
            "status": "not_available",
            "headline_question": "How much of the current residual gap is scope mismatch versus the treatment definition?",
            "variant_definitions": {},
            "deposit_concepts": {},
            "takeaways": ["Scope-alignment variants are not available in the current shock configuration."],
        }

    total_ladder = build_identity_variant_ladder(
        shocked,
        variants=variants,
        total_outcome_col="total_deposits_bank_qoq",
        horizons=[int(value) for value in sensitivity_spec.get("horizons", [])],
        cumulative=bool(sensitivity_spec.get("cumulative", True)),
        spec_name="scope_alignment_total_deposits",
        bootstrap_reps=int(sensitivity_spec.get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(sensitivity_spec.get("identity_bootstrap_block_length", 4)),
    )
    deposits_only_ladder = build_identity_variant_ladder(
        shocked,
        variants=variants,
        total_outcome_col="deposits_only_bank_qoq",
        horizons=[int(value) for value in sensitivity_spec.get("horizons", [])],
        cumulative=bool(sensitivity_spec.get("cumulative", True)),
        spec_name="scope_alignment_deposits_only",
        bootstrap_reps=int(sensitivity_spec.get("identity_bootstrap_reps", 40)),
        bootstrap_block_length=int(sensitivity_spec.get("identity_bootstrap_block_length", 4)),
    )
    if not deposits_only_ladder.empty:
        deposits_only_ladder = deposits_only_ladder.copy()
        deposits_only_ladder.loc[
            deposits_only_ladder["outcome"] == "other_component_qoq", "outcome"
        ] = "deposits_only_other_component_qoq"

    deposit_concepts = {
        "total_deposits_including_interbank": {
            "total_outcome": "total_deposits_bank_qoq",
            "residual_outcome": "other_component_qoq",
            **_concept_payload(
                ladder=total_ladder,
                total_outcome_col="total_deposits_bank_qoq",
                residual_outcome_col="other_component_qoq",
                horizons=horizons,
            ),
        },
        "deposits_only_ex_interbank": {
            "total_outcome": "deposits_only_bank_qoq",
            "residual_outcome": "deposits_only_other_component_qoq",
            **_concept_payload(
                ladder=deposits_only_ladder,
                total_outcome_col="deposits_only_bank_qoq",
                residual_outcome_col="deposits_only_other_component_qoq",
                horizons=horizons,
            ),
        },
    }

    takeaways = [
        "The `domestic_bank_only` variant removes the rest-of-world Treasury-acquisition term only; it is not a true U.S.-chartered-only bank-leg match.",
        "The `us_chartered_bank_only` variant is the first true bank-leg-matched TDC sensitivity in this repo: Fed Treasury transactions + U.S.-chartered bank Treasury transactions - Treasury operating cash + positive Fed remittances.",
        "The deposits-only concept removes interbank-transactions liabilities from the outcome so the project can distinguish 'deposit scope' from the broader `total_deposits_bank_qoq` object.",
    ]
    for concept_name, concept_payload in deposit_concepts.items():
        h0_payload = concept_payload["key_horizons"].get("h0", {})
        us_chartered = h0_payload.get("variants", {}).get("us_chartered_bank_only", {})
        delta = us_chartered.get("differences_vs_baseline_beta", {}).get("residual_response")
        if delta is not None:
            direction = "less negative" if float(delta) > 0 else "more negative"
            takeaways.append(
                f"At h0 for `{concept_name}`, the U.S.-chartered bank-leg-matched TDC makes the derived non-TDC residual {direction} than the current baseline by about {abs(float(delta)):.2f}."
            )
        domestic = h0_payload.get("variants", {}).get("domestic_bank_only", {})
        domestic_delta = domestic.get("differences_vs_baseline_beta", {}).get("residual_response")
        if domestic_delta is not None:
            direction = "less negative" if float(domestic_delta) > 0 else "more negative"
            takeaways.append(
                f"At h0 for `{concept_name}`, removing only the rest-of-world term makes the derived non-TDC residual {direction} than the current baseline by about {abs(float(domestic_delta)):.2f}."
            )

    variant_definitions = {
        "baseline": {
            "target": "tdc_bank_only_qoq",
            "description": "Imported bank-only headline: Fed + bank-sector + rest-of-world Treasury transactions - Treasury operating cash + positive Fed remittances.",
        },
        "domestic_bank_only": {
            "target": "tdc_domestic_bank_only_qoq",
            "description": "No-ROW sensitivity from upstream `tdcest`; removes the rest-of-world Treasury-acquisition term only.",
        },
        "us_chartered_bank_only": {
            "target": "tdc_us_chartered_bank_only_qoq",
            "description": "Local matched-bank-leg sensitivity: Fed Treasury transactions + U.S.-chartered bank Treasury transactions - Treasury operating cash + positive Fed remittances.",
        },
    }
    status = "available" if any(payload["key_horizons"] for payload in deposit_concepts.values()) else "not_available"
    return {
        "status": status,
        "headline_question": "How much of the current residual gap is scope mismatch versus the treatment definition?",
        "variant_definitions": variant_definitions,
        "deposit_concepts": deposit_concepts,
        "recommended_policy": _recommended_policy(),
        "takeaways": takeaways,
    }
