from __future__ import annotations

from tdcpass.analysis.backend_closeout_summary import build_backend_closeout_summary


def test_backend_closeout_summary_carries_stop_action_and_boundaries() -> None:
    summary = build_backend_closeout_summary(
        decision_bundle={
            "status": "not_ready",
            "recommended_action": "stop_and_package",
            "status_board": {"readiness": "not_ready"},
            "published_contexts": [
                {"regime": "reserve_drain", "state_label": "low", "horizon": 0}
            ],
        },
        evidence_packet={"packet_sections": [{"report_path": "/tmp/demo/output/reports/backend_decision_bundle.md"}]},
    )

    assert summary["recommended_action"] == "stop_and_package"
    assert any("reserve_drain_low_h0" in item for item in summary["settled_points"])
    assert any("not ready" in item.lower() for item in summary["unsupported_claims"])
