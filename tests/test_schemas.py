from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.schemas import (
    ActionType,
    DeadLetterRecord,
    EventEnvelope,
    ExecutionIntent,
    RiskDecision,
    TokenSignal,
    TokenState,
    VenueType,
)


def test_event_envelope_accepts_minimum_valid_payload() -> None:
    envelope = EventEnvelope(
        event_id="evt-1",
        event_type="onchain.swap_detected",
        source="solana_listener",
        chain="solana",
        token="BONK",
        observed_at=datetime.now(UTC),
        ingested_at=datetime.now(UTC),
        payload={"tx": "abc"},
    )

    assert envelope.schema_version == "v1"


def test_token_signal_rejects_out_of_range_alpha_score() -> None:
    with pytest.raises(ValidationError):
        TokenSignal(
            token="BONK",
            chain="solana",
            state_candidate=TokenState.UNKNOWN,
            alpha_score=1.2,
            timestamp=1,
        )


def test_execution_intent_requires_non_negative_notional() -> None:
    with pytest.raises(ValidationError):
        ExecutionIntent(
            intent_id="int-1",
            token="BONK",
            chain="solana",
            venue_type=VenueType.DEX,
            venue="solana_primary",
            action=ActionType.BUY,
            confidence=0.8,
            target_notional_usd=-1,
            max_slippage_bps=100,
            state=TokenState.NARRATIVE_EXPLOSION,
            strategy="dex_momentum_v1",
        )


def test_risk_decision_accepts_valid_payload() -> None:
    decision = RiskDecision(
        intent_id="int-1",
        allowed=True,
        adjusted_notional_usd=100,
        timestamp=datetime.now(UTC),
    )

    assert decision.allowed is True


def test_dead_letter_record_accepts_minimum_valid_payload() -> None:
    record = DeadLetterRecord(
        source_stream="raw-events",
        message_id="1-0",
        kind="eventenvelope",
        reason="processing_failed",
        payload={"token": "BONK"},
        failed_at=datetime.now(UTC),
    )

    assert record.replay_count == 0