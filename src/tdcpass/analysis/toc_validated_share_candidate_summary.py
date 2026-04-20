from __future__ import annotations

from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _candidate_payload(
    *,
    label: str,
    share: float | None,
    toc_leg_beta: float | None,
    core_residual_beta: float | None,
    direct_core_beta: float | None,
    baseline_gap_abs: float | None,
) -> dict[str, Any]:
    if None in (share, toc_leg_beta, core_residual_beta, direct_core_beta, baseline_gap_abs):
        return {
            "label": label,
            "share": share,
            "candidate_increment_beta": None,
            "implied_residual_beta": None,
            "abs_gap_to_direct_core": None,
            "improves_vs_core_residual_gap": False,
        }
    increment = float(share) * float(toc_leg_beta)
    implied_residual = float(core_residual_beta) - increment
    abs_gap = abs(implied_residual - float(direct_core_beta))
    return {
        "label": label,
        "share": float(share),
        "candidate_increment_beta": increment,
        "implied_residual_beta": implied_residual,
        "abs_gap_to_direct_core": abs_gap,
        "improves_vs_core_residual_gap": abs_gap < float(baseline_gap_abs),
    }


def build_toc_validated_share_candidate_summary(
    *,
    toc_row_liability_incidence_raw_summary: Mapping[str, Any] | None,
    strict_component_framework_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if toc_row_liability_incidence_raw_summary is None or strict_component_framework_summary is None:
        return {"status": "not_available", "reason": "missing_input_summary"}
    if str(toc_row_liability_incidence_raw_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "raw_incidence_summary_not_available"}
    if str(strict_component_framework_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_framework_summary_not_available"}

    framework_h0 = dict(strict_component_framework_summary.get("h0_snapshot", {}) or {})
    core_residual_beta = _safe_float(framework_h0.get("core_residual_beta"))
    direct_core_beta = _safe_float(framework_h0.get("headline_direct_core_beta"))
    baseline_gap_abs = None
    if None not in (core_residual_beta, direct_core_beta):
        baseline_gap_abs = abs(float(core_residual_beta) - float(direct_core_beta))

    raw_quarterly = dict(toc_row_liability_incidence_raw_summary.get("quarterly_alignment", {}) or {})
    toc_quarterly = dict(raw_quarterly.get("toc_leg", {}) or {})
    toc_in_scope = dict(toc_quarterly.get("in_scope_counterparts", {}) or {})
    toc_best_in_scope_corr = _safe_float(toc_quarterly.get("best_in_scope_corr"))
    toc_best_support_corr = _safe_float(toc_quarterly.get("best_support_corr"))

    raw_h0 = dict(toc_row_liability_incidence_raw_summary.get("key_horizons", {}).get("h0", {}) or {})
    toc_h0 = dict(raw_h0.get("toc_leg", {}) or {})
    toc_leg_beta = _safe_float(dict(toc_h0.get("leg_response", {}) or {}).get("beta"))
    toc_shares = dict(toc_h0.get("counterpart_share_of_leg_beta", {}) or {})

    deposits_only_candidate = _candidate_payload(
        label="h0_deposits_only_share_candidate",
        share=_safe_float(toc_shares.get("deposits_only_bank_qoq")),
        toc_leg_beta=toc_leg_beta,
        core_residual_beta=core_residual_beta,
        direct_core_beta=direct_core_beta,
        baseline_gap_abs=baseline_gap_abs,
    )
    private_checkable_candidate = _candidate_payload(
        label="h0_private_checkable_share_candidate",
        share=_safe_float(toc_shares.get("checkable_private_domestic_bank_qoq")),
        toc_leg_beta=toc_leg_beta,
        core_residual_beta=core_residual_beta,
        direct_core_beta=direct_core_beta,
        baseline_gap_abs=baseline_gap_abs,
    )
    total_deposits_candidate = _candidate_payload(
        label="h0_total_deposits_upper_envelope",
        share=_safe_float(toc_shares.get("total_deposits_bank_qoq")),
        toc_leg_beta=toc_leg_beta,
        core_residual_beta=core_residual_beta,
        direct_core_beta=direct_core_beta,
        baseline_gap_abs=baseline_gap_abs,
    )
    candidates = [
        private_checkable_candidate,
        deposits_only_candidate,
        total_deposits_candidate,
    ]

    available_candidates = [row for row in candidates if row.get("abs_gap_to_direct_core") is not None]
    best_candidate = None
    if available_candidates:
        best_candidate = min(available_candidates, key=lambda row: float(row["abs_gap_to_direct_core"]))

    best_same_sign_share = None
    if toc_in_scope:
        shares = [
            _safe_float(dict(payload or {}).get("same_quarter_sign_match_share"))
            for payload in toc_in_scope.values()
        ]
        numeric = [value for value in shares if value is not None]
        if numeric:
            best_same_sign_share = max(numeric)

    quarterly_gate = "fails"
    if (
        toc_best_in_scope_corr is not None
        and toc_best_support_corr is not None
        and best_same_sign_share is not None
        and toc_best_in_scope_corr >= 0.4
        and toc_best_in_scope_corr >= toc_best_support_corr * 0.75
        and best_same_sign_share >= 0.75
    ):
        quarterly_gate = "passes"

    fit_gate = "fails"
    if best_candidate is not None and bool(best_candidate.get("improves_vs_core_residual_gap")):
        fit_gate = "passes"

    decision = "keep_toc_outside_strict_object_under_current_evidence"
    if quarterly_gate == "passes" and fit_gate == "passes":
        decision = "candidate_toc_share_may_belong_in_strict_object"

    takeaways = [
        "This summary tests the only remaining reincorporation question after the raw-incidence gate: whether any narrow TOC share can be added back to the strict object without violating the strict standard.",
        "The rule is strict, not fit-chasing: a candidate must both clear the quarterly incidence gate and improve the direct-core comparison relative to the current core residual.",
    ]
    if None not in (core_residual_beta, direct_core_beta, baseline_gap_abs):
        takeaways.append(
            "The baseline comparison benchmark is the current core residual versus the headline direct strict core: "
            f"core residual ≈ {float(core_residual_beta):.2f}, direct core ≈ {float(direct_core_beta):.2f}, abs gap ≈ {float(baseline_gap_abs):.2f}."
        )
    if best_candidate is not None:
        takeaways.append(
            "Even the best narrow TOC candidate does not help under the current evidence: "
            f"`{str(best_candidate['label'])}` implies residual ≈ {float(best_candidate['implied_residual_beta']):.2f} "
            f"with abs gap to the direct core ≈ {float(best_candidate['abs_gap_to_direct_core']):.2f}."
        )
    if toc_best_in_scope_corr is not None and toc_best_support_corr is not None:
        takeaways.append(
            "The quarterly gate still fails for TOC incidence stability: "
            f"best in-scope deposit corr ≈ {float(toc_best_in_scope_corr):.2f} versus best support corr ≈ {float(toc_best_support_corr):.2f}."
        )
    if decision == "keep_toc_outside_strict_object_under_current_evidence":
        takeaways.append(
            "Current decision: keep TOC outside the strict object alongside ROW. Under the present evidence, TOC remains a real measured support leg, not a validated strict deposit component."
        )

    return {
        "status": "available",
        "headline_question": "Does any narrow validated TOC share belong in the strict deposit component once the raw-incidence gate and direct-core comparison are applied together?",
        "estimation_path": {
            "summary_artifact": "toc_validated_share_candidate_summary.json",
            "source_artifacts": [
                "toc_row_liability_incidence_raw_summary.json",
                "strict_component_framework_summary.json",
            ],
        },
        "candidate_definitions": {
            "current_strict_core_treatment": "tdc_core_deposit_proximate_bank_only_qoq",
            "headline_direct_core": "strict_loan_core_min_qoq",
            "toc_signed_leg": "tdc_toc_signed_qoq",
            "candidate_rows": [row["label"] for row in candidates],
        },
        "quarterly_gate": {
            "status": quarterly_gate,
            "best_in_scope_corr": toc_best_in_scope_corr,
            "best_support_corr": toc_best_support_corr,
            "best_in_scope_same_sign_share": best_same_sign_share,
        },
        "key_horizons": {
            "h0": {
                "core_residual_beta": core_residual_beta,
                "headline_direct_core_beta": direct_core_beta,
                "baseline_abs_gap_to_direct_core": baseline_gap_abs,
                "toc_leg_beta": toc_leg_beta,
                "candidates": {
                    row["label"]: row for row in candidates
                },
                "best_candidate": best_candidate,
            }
        },
        "classification": {
            "quarterly_stability_gate": quarterly_gate,
            "direct_core_fit_gate": fit_gate,
            "decision": decision,
            "row_status": "keep_outside_strict_object",
        },
        "recommendation": {
            "status": "toc_candidate_gate_completed",
            "toc_rule": (
                "keep_outside_strict_object"
                if decision == "keep_toc_outside_strict_object_under_current_evidence"
                else "candidate_share_may_be_reincorporated"
            ),
            "row_rule": "keep_outside_strict_object",
            "next_branch": (
                "finalize_release_framing_that_toc_and_row_stay_outside_strict_object"
                if decision == "keep_toc_outside_strict_object_under_current_evidence"
                else "test_validated_toc_share_as_strict_candidate_treatment"
            ),
        },
        "takeaways": takeaways,
    }
