from __future__ import annotations

from typing import Any

import pandas as pd

_REQUIRED_COLUMNS = (
    "quarter",
    "tdc_treasury_operating_cash_qoq",
    "tga_qoq",
    "checkable_federal_govt_bank_qoq",
    "federal_govt_checkable_total_qoq",
    "federal_govt_time_savings_total_qoq",
    "federal_govt_cash_balance_proxy_qoq",
)

_REGIME_WINDOWS: tuple[tuple[str, str, str | None], ...] = (
    ("pre_shift_ttl_regime", "1990-03-31", "2008-06-30"),
    ("transition_2008_2009", "2008-09-30", "2009-12-31"),
    ("post_shift_tga_dominant_regime", "2010-03-31", None),
)


def _corr(x: pd.Series, y: pd.Series) -> float | None:
    value = x.corr(y)
    return None if pd.isna(value) else float(value)


def _ratio_summary(numerator: pd.Series, denominator: pd.Series) -> dict[str, float | None]:
    valid = denominator.astype(float).abs() > 0.0
    ratio = (numerator[valid].astype(float) / denominator[valid].astype(float)).replace([float("inf"), float("-inf")], pd.NA).dropna()
    if ratio.empty:
        return {"mean": None, "median": None, "p10": None, "p90": None}
    return {
        "mean": float(ratio.mean()),
        "median": float(ratio.median()),
        "p10": float(ratio.quantile(0.1)),
        "p90": float(ratio.quantile(0.9)),
    }


def _abs_share_summary(component: pd.Series, aggregate: pd.Series) -> dict[str, float | None]:
    valid = aggregate.astype(float).abs() > 0.0
    share = (component[valid].astype(float).abs() / aggregate[valid].astype(float).abs()).replace(
        [float("inf"), float("-inf")],
        pd.NA,
    ).dropna()
    if share.empty:
        return {"mean": None, "median": None, "p10": None, "p90": None}
    return {
        "mean": float(share.mean()),
        "median": float(share.median()),
        "p10": float(share.quantile(0.1)),
        "p90": float(share.quantile(0.9)),
    }


def _classify_regime(
    *,
    toc_vs_tga_corr: float | None,
    toc_vs_cash_proxy_corr: float | None,
    ttl_share_mean: float | None,
) -> str:
    if toc_vs_tga_corr is None or toc_vs_cash_proxy_corr is None or ttl_share_mean is None:
        return "not_enough_information"
    if toc_vs_cash_proxy_corr >= toc_vs_tga_corr + 0.10 and ttl_share_mean >= 0.15:
        return "broad_cash_proxy_tracks_toc_better_than_tga"
    if toc_vs_tga_corr >= toc_vs_cash_proxy_corr + 0.10 and ttl_share_mean < 0.10:
        return "tga_tracks_toc_better_than_broad_cash_proxy"
    return "mixed_or_transition_regime"


def _regime_summary(frame: pd.DataFrame) -> dict[str, Any]:
    toc = frame["tdc_treasury_operating_cash_qoq"].astype(float)
    tga = frame["tga_qoq"].astype(float)
    ttl_bank = frame["checkable_federal_govt_bank_qoq"].astype(float)
    checkable_total = frame["federal_govt_checkable_total_qoq"].astype(float)
    time_savings = frame["federal_govt_time_savings_total_qoq"].astype(float)
    cash_proxy = frame["federal_govt_cash_balance_proxy_qoq"].astype(float)
    implied_fed_side = checkable_total - ttl_bank

    ttl_share_cash = _abs_share_summary(ttl_bank, cash_proxy)
    ttl_share_checkable = _abs_share_summary(ttl_bank, checkable_total)
    classification = _classify_regime(
        toc_vs_tga_corr=_corr(toc, tga),
        toc_vs_cash_proxy_corr=_corr(toc, cash_proxy),
        ttl_share_mean=ttl_share_cash["mean"],
    )
    return {
        "rows": int(frame.shape[0]),
        "toc_vs_tga_corr": _corr(toc, tga),
        "toc_vs_broad_cash_proxy_corr": _corr(toc, cash_proxy),
        "toc_vs_total_checkable_corr": _corr(toc, checkable_total),
        "toc_vs_ttl_bank_component_corr": _corr(toc, ttl_bank),
        "toc_vs_implied_fed_side_checkable_corr": _corr(toc, implied_fed_side),
        "tga_vs_broad_cash_proxy_corr": _corr(tga, cash_proxy),
        "ttl_bank_share_of_cash_balance_proxy_abs": ttl_share_cash,
        "ttl_bank_share_of_total_checkable_abs": ttl_share_checkable,
        "time_savings_share_of_cash_balance_proxy_abs": _abs_share_summary(time_savings, cash_proxy),
        "tga_over_broad_cash_proxy": _ratio_summary(tga, cash_proxy),
        "classification": classification,
    }


def build_treasury_cash_regime_audit_summary(
    *,
    shocked: pd.DataFrame,
) -> dict[str, Any]:
    if not set(_REQUIRED_COLUMNS).issubset(shocked.columns):
        return {"status": "not_available", "reason": "missing_required_panel_columns"}

    frame = shocked[list(_REQUIRED_COLUMNS)].dropna().copy()
    if frame.empty:
        return {"status": "not_available", "reason": "no_complete_rows"}

    frame["quarter_end"] = pd.PeriodIndex(frame["quarter"].astype(str), freq="Q").to_timestamp(how="end")

    regime_summaries: dict[str, Any] = {}
    for regime_name, start, end in _REGIME_WINDOWS:
        sample = frame[frame["quarter_end"] >= pd.Timestamp(start)].copy()
        if end is not None:
            sample = sample[sample["quarter_end"] <= pd.Timestamp(end)].copy()
        if sample.shape[0] < 2:
            regime_summaries[regime_name] = {"status": "not_available", "reason": "insufficient_rows"}
            continue
        regime_summaries[regime_name] = {
            "status": "available",
            "window": {"start": start, "end": end},
            **_regime_summary(sample),
        }

    full_sample_summary = _regime_summary(frame)
    pre_shift = dict(regime_summaries.get("pre_shift_ttl_regime", {}) or {})
    pre_classification = str(pre_shift.get("classification", "not_enough_information"))
    pre_ttl_share = dict(pre_shift.get("ttl_bank_share_of_cash_balance_proxy_abs", {}) or {}).get("mean")

    recommendation_status = "current_operating_cash_term_appears_sufficient_under_available_audit"
    if pre_classification == "broad_cash_proxy_tracks_toc_better_than_tga":
        recommendation_status = "historical_reestimate_with_explicit_ttl_era_cash_term_warranted"
    elif pre_classification == "mixed_or_transition_regime" and pre_ttl_share is not None and float(pre_ttl_share) >= 0.15:
        recommendation_status = "historical_ttl_era_reestimate_still_worth_running"

    takeaways = [
        "This audit checks whether the current Treasury-operating-cash term behaves more like TGA alone or like a broader federal-cash-balance concept that includes TT&L-era bank-side balances.",
    ]
    pre_tga = pre_shift.get("toc_vs_tga_corr")
    pre_broad = pre_shift.get("toc_vs_broad_cash_proxy_corr")
    if pre_tga is not None and pre_broad is not None and pre_ttl_share is not None:
        takeaways.append(
            "In the pre-shift TT&L regime, "
            f"TOC/TGA corr ≈ {float(pre_tga):.2f}, TOC/broad-cash-proxy corr ≈ {float(pre_broad):.2f}, "
            f"and the bank-side federal-government component averages about {float(pre_ttl_share):.2f} of the broad cash-balance proxy in absolute-flow terms."
        )
    post_shift = dict(regime_summaries.get("post_shift_tga_dominant_regime", {}) or {})
    post_tga = post_shift.get("toc_vs_tga_corr")
    post_broad = post_shift.get("toc_vs_broad_cash_proxy_corr")
    if post_tga is not None and post_broad is not None:
        takeaways.append(
            "In the post-shift regime, "
            f"TOC/TGA corr ≈ {float(post_tga):.2f} versus TOC/broad-cash-proxy corr ≈ {float(post_broad):.2f}."
        )
    takeaways.append(
        "The decision question is historical only: whether older-period TDC should use an explicit TT&L-era Treasury-cash term rather than relying on TGA-style cash alone."
    )

    return {
        "status": "available",
        "headline_question": "In the historical TT&L regime, does the Treasury-operating-cash term line up with TGA alone or with a broader Treasury cash-balance concept that includes bank-side federal-government balances?",
        "estimation_path": {
            "summary_artifact": "treasury_cash_regime_audit_summary.json",
            "required_panel_columns": list(_REQUIRED_COLUMNS),
        },
        "definitions": {
            "tga_qoq": "Quarter-over-quarter change in the TGA proxy from FRED WTREGEN.",
            "checkable_federal_govt_bank_qoq": "Quarter-over-quarter change in U.S.-chartered bank liabilities to the federal government, including demand notes and tax-and-loan accounts.",
            "federal_govt_checkable_total_qoq": "Quarter-over-quarter change in federal-government checkable deposits and currency asset.",
            "federal_govt_time_savings_total_qoq": "Quarter-over-quarter change in federal-government time and savings deposits asset.",
            "federal_govt_cash_balance_proxy_qoq": "Broad federal cash-balance proxy = federal-government checkable total + federal-government time and savings deposits.",
        },
        "regime_windows": regime_summaries,
        "full_sample": full_sample_summary,
        "classification": {
            "pre_shift_regime_classification": pre_classification,
            "recommendation_gate": recommendation_status,
        },
        "recommendation": {
            "status": recommendation_status,
            "next_branch": "historical_cash_term_reestimation" if recommendation_status != "current_operating_cash_term_appears_sufficient_under_available_audit" else "keep_current_cash_term",
        },
        "takeaways": takeaways,
    }
