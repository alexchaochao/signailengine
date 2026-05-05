from __future__ import annotations

from pydantic import BaseModel
from redis import Redis

from core.config import AppSettings
from core.schemas import EventEnvelope, TokenSignal
from core.signal_engine import SignalEngine
from infra.redis_stream import publish_model


def publish_raw_events(client: Redis, settings: AppSettings, *events: EventEnvelope) -> list[str]:
    return [
        publish_model(client, settings.redis.raw_events_stream, event, kind=event.event_type)
        for event in events
    ]


def build_and_publish_signal(
    client: Redis,
    settings: AppSettings,
    signal_engine: SignalEngine,
    *events: EventEnvelope,
) -> tuple[TokenSignal, str]:
    signal = signal_engine.build_signal(*events)
    message_id = publish_model(client, settings.redis.signals_stream, signal, kind="token_signal")
    return signal, message_id


def publish_decision_bundle(
    client: Redis,
    settings: AppSettings,
    *models: BaseModel,
) -> list[str]:
    message_ids: list[str] = []

    for model in models:
        kind = model.__class__.__name__.lower()
        message_ids.append(
            publish_model(client, settings.redis.decisions_stream, model, kind=kind)
        )

    return message_ids