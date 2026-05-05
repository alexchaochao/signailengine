from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TypeVar, cast

from pydantic import BaseModel
from redis import Redis
from redis.exceptions import RedisError

from core.config import AppSettings
from core.schemas import DeadLetterRecord

ModelT = TypeVar("ModelT", bound=BaseModel)


def get_redis_client(settings: AppSettings) -> Redis:
    return Redis.from_url(settings.redis.url, decode_responses=True)


def ping_redis(settings: AppSettings) -> bool:
    client = get_redis_client(settings)
    try:
        return bool(client.ping())
    except RedisError:
        return False


def publish_model(client: Redis, stream_name: str, model: BaseModel, kind: str) -> str:
    return str(
        client.xadd(
            stream_name,
            {
                "kind": kind,
                "payload": model.model_dump_json(),
            },
        )
    )


def publish_dead_letter(
    client: Redis,
    settings: AppSettings,
    *,
    source_stream: str,
    message_id: str,
    kind: str,
    payload: dict[str, object],
    reason: str,
    replay_count: int = 0,
) -> str:
    record = DeadLetterRecord(
        source_stream=source_stream,
        message_id=message_id,
        kind=kind,
        reason=reason,
        payload=payload,
        replay_count=replay_count,
        failed_at=datetime.now(UTC),
    )
    return publish_model(client, settings.redis.dead_letter_stream, record, kind="dead_letter")


def replay_dead_letters(
    client: Redis,
    settings: AppSettings,
    *,
    last_id: str = "0-0",
    count: int = 100,
) -> list[str]:
    replayed_message_ids: list[str] = []
    dead_letters = read_models(
        client,
        settings.redis.dead_letter_stream,
        DeadLetterRecord,
        last_id=last_id,
        count=count,
    )

    for _, record in dead_letters:
        replayed_message_ids.append(
            str(
                client.xadd(
                    settings.redis.raw_events_stream,
                    {
                        "kind": record.kind,
                        "payload": json.dumps(record.payload),
                    },
                )
            )
        )

    return replayed_message_ids


def ensure_consumer_group(
    client: Redis,
    stream_name: str,
    group_name: str,
    *,
    create_from_id: str = "0-0",
) -> None:
    try:
        client.xgroup_create(stream_name, group_name, id=create_from_id, mkstream=True)
    except RedisError as error:
        if "BUSYGROUP" not in str(error):
            raise


def read_models(
    client: Redis,
    stream_name: str,
    model_type: type[ModelT],
    *,
    last_id: str = "0-0",
    count: int = 100,
) -> list[tuple[str, ModelT]]:
    messages = cast(
        list[tuple[str, dict[str, str]]],
        client.xrange(stream_name, min=last_id, count=count),
    )
    result: list[tuple[str, ModelT]] = []

    for message_id, payload in messages:
        result.append((message_id, model_type.model_validate_json(payload["payload"])))

    return result


def read_group_models(
    client: Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    model_type: type[ModelT],
    *,
    count: int = 100,
    block_ms: int | None = None,
) -> list[tuple[str, ModelT]]:
    response = cast(
        list[tuple[str, list[tuple[str, dict[str, str]]]]],
        client.xreadgroup(
            group_name,
            consumer_name,
            {stream_name: ">"},
            count=count,
            block=block_ms,
        ),
    )

    result: list[tuple[str, ModelT]] = []
    for _, messages in response:
        for message_id, payload in messages:
            result.append((message_id, model_type.model_validate_json(payload["payload"])))

    return result


def acknowledge_message(client: Redis, stream_name: str, group_name: str, message_id: str) -> int:
    return int(cast(int, client.xack(stream_name, group_name, message_id)))
