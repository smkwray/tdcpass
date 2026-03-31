from __future__ import annotations

from tdcpass.analysis.backend_decision_bundle import build_backend_decision_bundle


def test_backend_decision_bundle_recommends_stop_and_package_when_stack_is_mature() -> None:
    summary = build_backend_decision_bundle(
        readiness={"status": "not_ready"},
        direct_identification={"status": "not_ready"},
        shock_diagnostics={"treatment_quality_status": "pass"},
        smoothed_lp_diagnostics={"status": "stable"},
        factor_control_diagnostics={"status": "core_adequate"},
        proxy_factor_summary={"status": "weak"},
        state_proxy_factor_summary={"status": "published_supportive"},
        published_state_proxy_comparator={"status": "published_signal", "primary_contexts": [{"regime": "reserve_drain"}]},
        published_state_proxy_vs_baseline={"status": "context_sharpens_mechanism_only"},
    )

    assert summary["recommended_action"] == "stop_and_package"
    assert summary["status_board"]["methods_stack"] == "mature"


def test_backend_decision_bundle_recommends_continue_when_stack_is_not_mature() -> None:
    summary = build_backend_decision_bundle(
        readiness={"status": "not_ready"},
        direct_identification={"status": "not_ready"},
        shock_diagnostics={"treatment_quality_status": "pass"},
        smoothed_lp_diagnostics={"status": "unstable"},
        factor_control_diagnostics={"status": "short_history"},
        proxy_factor_summary={"status": "weak"},
        state_proxy_factor_summary={"status": "weak"},
        published_state_proxy_comparator={"status": "no_published_signal", "primary_contexts": []},
        published_state_proxy_vs_baseline={"status": "no_published_context"},
    )

    assert summary["recommended_action"] == "continue_backend_work"
    assert summary["status_board"]["methods_stack"] == "still_building"
