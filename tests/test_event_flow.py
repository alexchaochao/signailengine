from datetime import UTC, datetime
from typing import cast

from redis import Redis

from core.config import AppSettings
from core.event_flow import build_and_publish_signal, publish_raw_events
from core.schemas import EventEnvelope, TokenSignal
from core.signal_engine import SignalEngine
from infra.redis_stream import publish_dead_letter, read_models, replay_dead_letters
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.counter = 0

    def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream_name, []).append((message_id, mapping))
        return message_id

    def xrange(
        self,
        stream_name: str,
        min: str = "0-0",
        count: int = 100,
    ) -> list[tuple[str, dict[str, str]]]:
        messages = self.streams.get(stream_name, [])
        min_ms, min_seq = _parse_stream_message_id(min)
        filtered = [
            (message_id, payload)
            for message_id, payload in messages
            if _parse_stream_message_id(message_id) >= (min_ms, min_seq)
        ]
        return filtered[:count]


def _parse_stream_message_id(message_id: str) -> tuple[int, int]:
    left, _, right = message_id.partition("-")
    return int(left), int(right or "0")


def test_publish_raw_events_writes_envelopes_to_stream() -> None:
    client = FakeRedis()
    settings = AppSettings.load()
    event = build_onchain_event({"token": "BONK", "observed_at": datetime.now(UTC)})

    ids = publish_raw_events(cast(Redis, client), settings, event)
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert ids == ["1-0"]
    assert stored[0][1].token == "BONK"


def test_build_and_publish_signal_writes_signal_to_stream() -> None:
    client = FakeRedis()
    settings = AppSettings.load()
    engine = SignalEngine()
    observed_at = datetime.now(UTC)
    onchain_event = build_onchain_event(
        {
            "token": "BONK",
            "observed_at": observed_at,
            "liquidity_usd": 120_000,
            "volume_5m_usd": 45_000,
            "buy_pressure": 0.78,
        }
    )
    wallet_event = build_wallet_event(
        {
            "token": "BONK",
            "observed_at": observed_at,
            "wallet_inflow_score": 0.62,
        }
    )

    signal, message_id = build_and_publish_signal(
        cast(Redis, client),
        settings,
        engine,
        onchain_event,
        wallet_event,
    )
    stored = read_models(cast(Redis, client), settings.redis.signals_stream, TokenSignal)

    assert message_id == "1-0"
    assert signal.token == "BONK"
    assert stored[0][1].state_candidate == signal.state_candidate


def test_replay_dead_letters_republishes_raw_events() -> None:
    client = FakeRedis()
    settings = AppSettings.load()
    event = build_onchain_event({"token": "BONK", "observed_at": datetime.now(UTC)})

    publish_dead_letter(
        cast(Redis, client),
        settings,
        source_stream=settings.redis.raw_events_stream,
        message_id="1-0",
        kind=event.event_type,
        payload=event.model_dump(mode="json"),
        reason="processing_failed",
    )
    replayed = replay_dead_letters(cast(Redis, client), settings)
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert replayed == ["2-0"]
    assert stored[0][1].token == "BONK"