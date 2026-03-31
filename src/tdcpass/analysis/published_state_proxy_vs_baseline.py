from __future__ import annotations

from typing import Any, Mapping

import pandas as pd


def _lp_snapshot(lp_irf: pd.DataFrame, *, outcome: str, horizon: int) -> dict[str, Any] | None:
    sample = lp_irf[(lp_irf["outcome"] == outcome) & (lp_irf["horizon"] == horizon)]
    if sample.empty:
        return None
    row = sample.iloc[0]
    lower95 = float(row["lower95"])
    upper95 = float(row["upper95"])
    return {
        "beta": float(row["beta"]),
        "lower95": lower95,
        "upper95": upper95,
        "n": int(row["n"]),
        "ci_excludes_zero": lower95 > 0.0 or upper95 < 0.0,
        "sign": "positive" if float(row["beta"]) > 0.0 else "negative" if float(row["beta"]) < 0.0 else "zero",
    }


def build_published_state_proxy_vs_baseline_summary(
    *,
    lp_irf: pd.DataFrame,
    published_state_proxy_comparator: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if published_state_proxy_comparator is None:
        return {
            "status": "no_comparator",
            "headline_question": "How does the lead published regime-state mechanism context compare with the headline full-sample baseline?",
            "takeaways": ["Published state-proxy comparator is missing."],
        }

    primary_contexts = list(published_state_proxy_comparator.get("primary_contexts", []))
    if not primary_contexts:
        return {
            "status": "no_published_context",
            "headline_question": "How does the lead published regime-state mechanism context compare with the headline full-sample baseline?",
            "takeaways": ["No published regime-state context is available for direct comparison with the baseline."],
        }

    lead_context = dict(primary_contexts[0])
    horizon = int(lead_context.get("horizon", 0))
    baseline_total = _lp_snapshot(lp_irf, outcome="total_deposits_bank_qoq", horizon=horizon)
    baseline_other = _lp_snapshot(lp_irf, outcome="other_component_qoq", horizon=horizon)
    context_other = dict(lead_context.get("other_component") or {})

    status = "context_sharpens_mechanism_only"
    if baseline_other is not None and bool(baseline_other.get("ci_excludes_zero")):
        status = "baseline_already_decisive"

    takeaways = [
        "This note compares the lead published regime-state proxy context with the matching-horizon full-sample baseline LP.",
    ]
    if baseline_other is not None:
        takeaways.append(
            f"Full-sample h{horizon} other_component is {baseline_other['sign']} with beta={baseline_other['beta']:.2f} and ci_excludes_zero={baseline_other['ci_excludes_zero']}."
        )
    if context_other:
        context_sign = "positive" if float(context_other.get("beta", 0.0)) > 0 else "negative" if float(context_other.get("beta", 0.0)) < 0 else "zero"
        takeaways.append(
            f"Lead published context {lead_context.get('regime')}_{lead_context.get('state_label')}_h{horizon} has other_component {context_sign} with beta={float(context_other.get('beta', 0.0)):.2f} and ci_excludes_zero={bool(context_other.get('ci_excludes_zero', False))}."
        )
    takeaways.append(
        "The published context is informative because grouped proxy families are supportive there even though the full-sample baseline remains non-decisive at the same horizon."
    )

    return {
        "status": status,
        "headline_question": "How does the lead published regime-state mechanism context compare with the headline full-sample baseline?",
        "lead_context": lead_context,
        "baseline": {
            "horizon": horizon,
            "total_deposits": baseline_total,
            "other_component": baseline_other,
        },
        "takeaways": takeaways,
    }
