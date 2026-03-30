from __future__ import annotations

import pandas as pd

from tdcpass.analysis.structural_proxy_evidence import build_structural_proxy_evidence


def test_structural_proxy_evidence_summarizes_key_horizons() -> None:
    lp_irf = pd.DataFrame(
        [
            {"outcome": "other_component_qoq", "horizon": 0, "beta": -2.0, "se": 0.6, "lower95": -3.176, "upper95": -0.824, "n": 40},
            {"outcome": "bank_credit_private_qoq", "horizon": 0, "beta": -0.8, "se": 0.3, "lower95": -1.388, "upper95": -0.212, "n": 40},
            {"outcome": "cb_nonts_qoq", "horizon": 0, "beta": 0.2, "se": 0.4, "lower95": -0.584, "upper95": 0.984, "n": 40},
            {"outcome": "foreign_nonts_qoq", "horizon": 0, "beta": 0.5, "se": 0.2, "lower95": 0.108, "upper95": 0.892, "n": 40},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 0, "beta": 0.3, "se": 0.3, "lower95": -0.288, "upper95": 0.888, "n": 40},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 0, "beta": 0.1, "se": 0.3, "lower95": -0.488, "upper95": 0.688, "n": 40},
            {"outcome": "other_component_qoq", "horizon": 4, "beta": -1.0, "se": 0.8, "lower95": -2.568, "upper95": 0.568, "n": 36},
            {"outcome": "bank_credit_private_qoq", "horizon": 4, "beta": -0.2, "se": 0.4, "lower95": -0.984, "upper95": 0.584, "n": 36},
            {"outcome": "cb_nonts_qoq", "horizon": 4, "beta": -0.4, "se": 0.3, "lower95": -0.988, "upper95": 0.188, "n": 36},
            {"outcome": "foreign_nonts_qoq", "horizon": 4, "beta": 0.1, "se": 0.3, "lower95": -0.488, "upper95": 0.688, "n": 36},
            {"outcome": "domestic_nonfinancial_mmf_reallocation_qoq", "horizon": 4, "beta": -0.3, "se": 0.3, "lower95": -0.888, "upper95": 0.288, "n": 36},
            {"outcome": "domestic_nonfinancial_repo_reallocation_qoq", "horizon": 4, "beta": 0.2, "se": 0.3, "lower95": -0.388, "upper95": 0.788, "n": 36},
        ]
    )

    frame, summary = build_structural_proxy_evidence(lp_irf=lp_irf, horizons=(0, 4))

    assert set(frame["proxy_outcome"]) == {
        "bank_credit_private_qoq",
        "cb_nonts_qoq",
        "foreign_nonts_qoq",
        "domestic_nonfinancial_mmf_reallocation_qoq",
        "domestic_nonfinancial_repo_reallocation_qoq",
    }
    assert set(frame["horizon"]) == {0, 4}
    assert summary["status"] == "mixed"
    assert summary["key_horizons"]["h0"]["decisive_concordant_proxy_count"] == 1
    assert summary["key_horizons"]["h0"]["decisive_discordant_proxy_count"] == 1
    assert summary["key_horizons"]["h4"]["interpretation"] == "other_component_not_decisive"
