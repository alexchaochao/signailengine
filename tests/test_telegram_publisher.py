from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.schemas import CollectorCheckpoint, EventEnvelope
from infra.metrics import Metrics
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from notifications.telegram_publisher import TelegramPublisherService
from tests.test_pipeline import FakeRedis


def test_telegram_publisher_sends_qualified_launch_candidate_once() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "bot_token": "token-1",
                    "chat_id": "chat-1",
                    "publish_alpha_types": ["LAUNCH", "CATALYST"],
                    "consumer_group": "telegram-test",
                    "consumer_name": "telegram-test-1",
                }
            }
        }
    )
    client = FakeRedis()
    metrics = Metrics(settings.observability.service_namespace)
    sent_messages: list[tuple[str, str, str]] = []

    def transport(bot_token: str, chat_id: str, text: str) -> str:
        sent_messages.append((bot_token, chat_id, text))
        return "42"

    service = TelegramPublisherService(
        settings,
        cast(Redis, client),
        repository,
        transport=transport,
        metrics=metrics,
    )
    service.ensure_stream()
    publish_raw_events(
        cast(Redis, client),
        settings,
        EventEnvelope(
            event_id="launch-1:qualified",
            event_type="alpha.candidate_qualified",
            source="launch_alpha_live",
            chain="solana",
            token="NEWTKN",
            observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            payload={
                "alpha_type": "LAUNCH",
                "candidate_id": "solana:pool-1",
                "status": "QUALIFIED",
                "score": 0.93,
                "reasons": ["launch_depth_confirmed"],
                "candidate": {
                    "dex": "raydium",
                    "pool_address": "pool-1",
                    "initial_liquidity_usd": 25000.0,
                    "buy_notional_5m_usd": 18000.0,
                    "trade_count_5m": 16,
                    "unique_wallets_5m": 11,
                },
                "snapshot": {},
            },
        ),
    )

    assert service.process_once(count=10, block_ms=1) == 1
    assert len(sent_messages) == 1
    assert "ALPHA QUALIFIED | LAUNCH" in sent_messages[0][2]
    assert count_rows(engine, "notification_deliveries") == 1

    assert service.process_once(count=10, block_ms=1) == 1
    assert len(sent_messages) == 1

    delivery = repository.notifications.load_delivery(
        channel="telegram",
        destination="chat-1",
        candidate_id="solana:pool-1",
        event_type="alpha.candidate_qualified",
    )
    assert delivery is not None
    assert delivery["status"] == "sent"
    assert delivery["remote_message_id"] == "42"
    assert metrics.notification_deliveries.labels(channel="telegram", status="sent")._value.get() == 1.0
    assert metrics.worker_heartbeat.labels(service="telegram_publisher", mode="process_once")._value.get() > 0.0


def test_telegram_publisher_skips_flow_candidate_by_default() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "bot_token": "token-1",
                    "chat_id": "chat-1",
                    "publish_alpha_types": ["LAUNCH", "CATALYST"],
                    "consumer_group": "telegram-test",
                    "consumer_name": "telegram-test-1",
                }
            }
        }
    )
    client = FakeRedis()
    metrics = Metrics(settings.observability.service_namespace)
    sent_messages: list[str] = []

    def transport(bot_token: str, chat_id: str, text: str) -> str:
        _ = bot_token, chat_id
        sent_messages.append(text)
        return "99"

    service = TelegramPublisherService(
        settings,
        cast(Redis, client),
        repository,
        transport=transport,
        metrics=metrics,
    )
    service.ensure_stream()
    publish_raw_events(
        cast(Redis, client),
        settings,
        EventEnvelope(
            event_id="flow-1:qualified",
            event_type="alpha.candidate_qualified",
            source="flow_alpha_live",
            chain="base",
            token="AERO",
            observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            payload={
                "alpha_type": "FLOW",
                "candidate_id": "flow:base:AERO:1",
                "status": "QUALIFIED",
                "score": 0.91,
                "reasons": ["smart_money_rotation"],
                "candidate": {},
                "snapshot": {},
            },
        ),
    )

    assert service.process_once(count=10, block_ms=1) == 1
    assert sent_messages == []

    delivery = repository.notifications.load_delivery(
        channel="telegram",
        destination="chat-1",
        candidate_id="flow:base:AERO:1",
        event_type="alpha.candidate_qualified",
    )
    assert delivery is not None
    assert delivery["status"] == "skipped"
    assert metrics.notification_deliveries.labels(channel="telegram", status="skipped")._value.get() == 1.0


def test_telegram_publisher_tracks_failed_delivery_metric() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "bot_token": "token-1",
                    "chat_id": "chat-1",
                    "publish_alpha_types": ["LAUNCH"],
                    "consumer_group": "telegram-test",
                    "consumer_name": "telegram-test-1",
                }
            }
        }
    )
    client = FakeRedis()
    metrics = Metrics(settings.observability.service_namespace)

    def failing_transport(bot_token: str, chat_id: str, text: str) -> str:
        _ = bot_token, chat_id, text
        raise RuntimeError("telegram_unreachable")

    service = TelegramPublisherService(
        settings,
        cast(Redis, client),
        repository,
        transport=failing_transport,
        metrics=metrics,
    )
    service.ensure_stream()
    publish_raw_events(
        cast(Redis, client),
        settings,
        EventEnvelope(
            event_id="launch-fail:qualified",
            event_type="alpha.candidate_qualified",
            source="launch_alpha_live",
            chain="solana",
            token="NEWTKN",
            observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            payload={
                "alpha_type": "LAUNCH",
                "candidate_id": "solana:pool-fail",
                "status": "QUALIFIED",
                "score": 0.95,
                "candidate": {"dex": "raydium", "pool_address": "pool-fail"},
                "snapshot": {},
            },
        ),
    )

    assert service.process_once(count=10, block_ms=1) == 1
    assert metrics.notification_deliveries.labels(channel="telegram", status="failed")._value.get() == 1.0


def test_telegram_publisher_includes_fsm_context_when_available() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    repository.checkpoints.save(
        CollectorCheckpoint(
            checkpoint_key="fsm_state:solana:NEWTKN",
            cursor="EARLY_LIQUIDITY",
            observed_at=datetime(2026, 5, 3, 11, 59, 59, tzinfo=UTC),
            metadata={
                "last_transition_timestamp": int(
                    datetime(2026, 5, 3, 11, 59, 30, tzinfo=UTC).timestamp()
                ),
                "reasons": ["volume_and_liquidity_established"],
            },
        )
    )
    settings = AppSettings.load().model_copy(
        update={
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "bot_token": "token-1",
                    "chat_id": "chat-1",
                    "publish_alpha_types": ["LAUNCH"],
                    "consumer_group": "telegram-test",
                    "consumer_name": "telegram-test-1",
                }
            }
        }
    )
    client = FakeRedis()
    sent_messages: list[str] = []

    def transport(bot_token: str, chat_id: str, text: str) -> str:
        _ = bot_token, chat_id
        sent_messages.append(text)
        return "84"

    service = TelegramPublisherService(
        settings,
        cast(Redis, client),
        repository,
        transport=transport,
    )
    service.ensure_stream()
    publish_raw_events(
        cast(Redis, client),
        settings,
        EventEnvelope(
            event_id="launch-2:qualified",
            event_type="alpha.candidate_qualified",
            source="launch_alpha_live",
            chain="solana",
            token="NEWTKN",
            observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            payload={
                "alpha_type": "LAUNCH",
                "candidate_id": "solana:pool-2",
                "status": "QUALIFIED",
                "score": 0.95,
                "candidate": {
                    "dex": "raydium",
                    "pool_address": "pool-2",
                },
                "snapshot": {},
            },
        ),
    )

    assert service.process_once(count=10, block_ms=1) == 1
    assert len(sent_messages) == 1
    assert "FSM State: EARLY_LIQUIDITY" in sent_messages[0]
    assert "FSM Last Transition: 2026-05-03T11:59:30+00:00" in sent_messages[0]
    assert "FSM Reasons: volume_and_liquidity_established" in sent_messages[0]


def test_telegram_publisher_disabled_does_not_consume_events() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "notifications": {
                "telegram": {
                    "enabled": False,
                    "bot_token": "",
                    "chat_id": "",
                    "consumer_group": "telegram-test",
                    "consumer_name": "telegram-test-1",
                }
            }
        }
    )
    client = FakeRedis()
    service = TelegramPublisherService(settings, cast(Redis, client), repository)
    service.ensure_stream()
    publish_raw_events(
        cast(Redis, client),
        settings,
        EventEnvelope(
            event_id="launch-1:qualified",
            event_type="alpha.candidate_qualified",
            source="launch_alpha_live",
            chain="solana",
            token="NEWTKN",
            observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            payload={
                "alpha_type": "LAUNCH",
                "candidate_id": "solana:pool-1",
                "status": "QUALIFIED",
                "score": 0.93,
            },
        ),
    )

    assert service.process_once(count=10, block_ms=1) == 0
    assert client.acked == []
    assert count_rows(engine, "notification_deliveries") == 0