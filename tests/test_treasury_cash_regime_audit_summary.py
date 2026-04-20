from __future__ import annotations

import pandas as pd

from tdcpass.analysis.treasury_cash_regime_audit_summary import (
    build_treasury_cash_regime_audit_summary,
)


def test_treasury_cash_regime_audit_flags_ttl_era_reestimate_when_broad_proxy_dominates() -> None:
    quarters = pd.period_range("1990Q1", "2011Q4", freq="Q")
    quarter_labels = quarters.astype(str)
    pre_n = (quarters <= pd.Period("2008Q2", freq="Q")).sum()
    transition_n = ((quarters >= pd.Period("2008Q3", freq="Q")) & (quarters <= pd.Period("2009Q4", freq="Q"))).sum()
    post_n = (quarters >= pd.Period("2010Q1", freq="Q")).sum()

    pre_ttl = [10.0 if i % 2 == 0 else -10.0 for i in range(pre_n)]
    pre_implied_fed = [1.0 if i % 2 == 0 else -1.0 for i in range(pre_n)]
    pre_cash_proxy = [ttl + fed for ttl, fed in zip(pre_ttl, pre_implied_fed, strict=False)]
    pre_tga = [3.0, -1.0, -2.0, 2.5] * (pre_n // 4) + [3.0, -1.0, -2.0, 2.5][: pre_n % 4]

    transition_ttl = [2.0, -2.0, 1.0, -1.0, 1.5, -1.5]
    transition_fed = [3.0, -3.0, 2.0, -2.0, 1.0, -1.0]
    transition_cash_proxy = [ttl + fed for ttl, fed in zip(transition_ttl, transition_fed, strict=False)]
    transition_tga = [fed for fed in transition_fed]

    post_ttl = [0.5 if i % 2 == 0 else -0.5 for i in range(post_n)]
    post_tga = [5.0 if i % 2 == 0 else -5.0 for i in range(post_n)]
    post_cash_proxy = [ttl + tga for ttl, tga in zip(post_ttl, post_tga, strict=False)]

    ttl = pre_ttl + transition_ttl + post_ttl
    tga = pre_tga + transition_tga + post_tga
    cash_proxy = pre_cash_proxy + transition_cash_proxy + post_cash_proxy
    time_savings = [0.0] * len(quarters)

    shocked = pd.DataFrame(
        {
            "quarter": quarter_labels,
            "tdc_treasury_operating_cash_qoq": cash_proxy,
            "tga_qoq": tga,
            "checkable_federal_govt_bank_qoq": ttl,
            "federal_govt_checkable_total_qoq": cash_proxy,
            "federal_govt_time_savings_total_qoq": time_savings,
            "federal_govt_cash_balance_proxy_qoq": cash_proxy,
        }
    )

    summary = build_treasury_cash_regime_audit_summary(shocked=shocked)

    assert summary["status"] == "available"
    assert (
        summary["classification"]["pre_shift_regime_classification"]
        == "broad_cash_proxy_tracks_toc_better_than_tga"
    )
    assert (
        summary["recommendation"]["status"]
        == "historical_reestimate_with_explicit_ttl_era_cash_term_warranted"
    )
    assert (
        summary["regime_windows"]["pre_shift_ttl_regime"]["ttl_bank_share_of_cash_balance_proxy_abs"]["mean"]
        > 0.15
    )
