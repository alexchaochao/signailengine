from datetime import UTC, datetime

from sentinel.wallet_tracker import build_wallet_event


def test_build_wallet_event_normalizes_payload() -> None:
    event = build_wallet_event(
        {
            "token": "BONK",
            "observed_at": datetime.now(UTC),
            "wallet_inflow_score": 0.62,
            "wallet_outflow_score": 0.10,
            "tracked_wallet_count": 4,
        }
    )

    assert event.event_type == "wallet.cluster_snapshot"
    assert event.payload["wallet_inflow_score"] == 0.62
    assert event.payload["tracked_wallet_count"] == 4