from __future__ import annotations

from typing import Any


def build_strict_di_bucket_role_summary(
    *,
    strict_loan_core_redesign_summary: dict[str, Any] | None,
    strict_identifiable_followup_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if strict_loan_core_redesign_summary is None:
        return {"status": "not_available", "reason": "strict_loan_core_redesign_summary_not_available"}
    if str(strict_loan_core_redesign_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_loan_core_redesign_summary_not_available"}
    if strict_identifiable_followup_summary is None:
        return {"status": "not_available", "reason": "strict_identifiable_followup_summary_not_available"}
    if str(strict_identifiable_followup_summary.get("status", "not_available")) != "available":
        return {"status": "not_available", "reason": "strict_identifiable_followup_summary_not_available"}

    published_roles = dict(strict_loan_core_redesign_summary.get("published_roles", {}) or {})
    recommendation = dict(strict_loan_core_redesign_summary.get("recommendation", {}) or {})
    redesign_h0 = dict(
        strict_loan_core_redesign_summary.get("key_horizons", {}).get("h0", {}).get("core_deposit_proximate", {}) or {}
    )
    followup_h0 = dict(
        strict_identifiable_followup_summary.get("di_loans_nec_borrower_diagnostics", {})
        .get("key_horizons", {})
        .get("h0", {})
        or {}
    )

    headline_direct_core = dict(redesign_h0.get("redesigned_direct_min_core_response", {}) or {})
    secondary = dict(redesign_h0.get("private_borrower_augmented_core_response", {}) or {})
    broad_subtotal = dict(redesign_h0.get("current_broad_loan_source_response", {}) or {})
    noncore_system = dict(redesign_h0.get("noncore_system_diagnostic_response", {}) or {})
    core_residual = dict(redesign_h0.get("core_residual_response", {}) or {})
    private_increment_beta = None
    if headline_direct_core.get("beta") is not None and secondary.get("beta") is not None:
        private_increment_beta = float(secondary["beta"]) - float(headline_direct_core["beta"])
    residual_gap_vs_secondary_beta = None
    if core_residual.get("beta") is not None and secondary.get("beta") is not None:
        residual_gap_vs_secondary_beta = float(secondary["beta"]) - float(core_residual["beta"])

    di_bucket = dict(followup_h0.get("strict_loan_di_loans_nec", {}) or {})
    us_chartered_share = followup_h0.get("us_chartered_share_of_systemwide_liability_beta")
    borrower_gap_share = followup_h0.get("systemwide_borrower_gap_share_of_systemwide_liability_beta")
    dominant_borrower_component = followup_h0.get("dominant_borrower_component")

    takeaways = [
        "This surface fixes the published role of the broad DI-loans-n.e.c. bucket inside the redesigned strict loan lane.",
        "It is not a same-scope decomposition: the headline and secondary rows are strict source-side loan objects, while the borrower diagnostics remain systemwide F.215 counterpart rows.",
    ]
    if (
        core_residual.get("beta") is not None
        and headline_direct_core.get("beta") is not None
        and secondary.get("beta") is not None
        and broad_subtotal.get("beta") is not None
        and noncore_system.get("beta") is not None
    ):
        takeaways.append(
            "At h0 under the core-deposit-proximate shock, the role bridge is now explicit: "
            f"core residual ≈ {float(core_residual['beta']):.2f}, headline direct core ≈ {float(headline_direct_core['beta']):.2f}, "
            f"standard secondary comparison ≈ {float(secondary['beta']):.2f}, broad loan subtotal ≈ {float(broad_subtotal['beta']):.2f}, "
            f"noncore/system diagnostic ≈ {float(noncore_system['beta']):.2f}."
        )
    if private_increment_beta is not None:
        takeaways.append(
            f"The private-borrower increment relative to the headline direct core is about {float(private_increment_beta):.2f} at h0, which is why `strict_loan_core_plus_private_borrower_qoq` is the bounded secondary comparison rather than the headline."
        )
    if us_chartered_share is not None and dominant_borrower_component is not None:
        takeaways.append(
            f"The broad DI bucket should remain diagnostic because the borrower-side bridge is cross-scope: the U.S.-chartered DI-loans-n.e.c. asset response is about {float(us_chartered_share):.2f} of the signed systemwide borrower-liability total at h0, and the dominant borrower counterpart is `{str(dominant_borrower_component)}`."
        )
    if borrower_gap_share is not None:
        takeaways.append(
            f"The remaining named-borrower gap is about {float(borrower_gap_share):.2f} of the signed systemwide borrower-liability total at h0, which is another reason not to publish the broad DI bucket as if it were already decomposed cleanly."
        )

    return {
        "status": "available",
        "headline_question": "How should the broad DI-loans-n.e.c. bucket be interpreted inside the redesigned strict loan lane?",
        "estimation_path": {
            "summary_artifact": "strict_di_bucket_role_summary.json",
            "upstream_role_surface": "strict_loan_core_redesign_summary.json",
            "borrower_diagnostic_surface": "strict_identifiable_followup_summary.json",
        },
        "release_taxonomy": {
            "headline_direct_core": published_roles.get("headline_direct_core"),
            "standard_secondary_comparison": published_roles.get("standard_secondary_comparison"),
            "broad_loan_subtotal_diagnostic": published_roles.get("broad_loan_subtotal_diagnostic"),
            "di_bucket_diagnostic": published_roles.get("di_bucket_diagnostic"),
            "noncore_system_diagnostic": published_roles.get("noncore_system_diagnostic"),
        },
        "recommendation": {
            "status": "keep_di_bucket_diagnostic_only",
            "headline_direct_core": recommendation.get("release_headline_candidate", "strict_loan_core_min_qoq"),
            "standard_secondary_comparison": recommendation.get(
                "standard_secondary_candidate", "strict_loan_core_plus_private_borrower_qoq"
            ),
            "diagnostic_di_bucket": recommendation.get("diagnostic_di_bucket", "strict_loan_di_loans_nec_qoq"),
            "next_branch": "build_di_bucket_bridge_or_counterpart_surface",
        },
        "key_horizons": {
            "h0": {
                "core_residual_response": redesign_h0.get("core_residual_response"),
                "headline_direct_core_response": redesign_h0.get("redesigned_direct_min_core_response"),
                "standard_secondary_comparison_response": redesign_h0.get("private_borrower_augmented_core_response"),
                "broad_loan_subtotal_response": redesign_h0.get("current_broad_loan_source_response"),
                "noncore_system_diagnostic_response": redesign_h0.get("noncore_system_diagnostic_response"),
                "private_borrower_increment_beta": private_increment_beta,
                "secondary_minus_core_residual_beta": residual_gap_vs_secondary_beta,
                "di_bucket_response": followup_h0.get("strict_loan_di_loans_nec"),
                "dominant_borrower_component": dominant_borrower_component,
                "us_chartered_share_of_systemwide_liability_beta": us_chartered_share,
                "systemwide_borrower_gap_share_of_systemwide_liability_beta": borrower_gap_share,
            }
        },
        "takeaways": takeaways,
    }
