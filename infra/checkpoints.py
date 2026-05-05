from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.schemas import CollectorCheckpoint


class CheckpointStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save(self, checkpoint: CollectorCheckpoint) -> CollectorCheckpoint:
        normalized = checkpoint.model_copy(
            update={
                "observed_at": (
                    checkpoint.observed_at.astimezone(UTC)
                    if checkpoint.observed_at is not None
                    else None
                ),
                "updated_at": (checkpoint.updated_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO collector_checkpoints ("
                "checkpoint_key, cursor, observed_at, metadata, updated_at"
                ") VALUES ("
                ":checkpoint_key, :cursor, :observed_at, :metadata, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO collector_checkpoints ("
                "checkpoint_key, cursor, observed_at, metadata, updated_at"
                ") VALUES ("
                ":checkpoint_key, :cursor, :observed_at, :metadata, :updated_at"
                ") ON CONFLICT (checkpoint_key) DO UPDATE SET "
                "cursor = EXCLUDED.cursor, "
                "observed_at = EXCLUDED.observed_at, "
                "metadata = EXCLUDED.metadata, "
                "updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "checkpoint_key": normalized.checkpoint_key,
                    "cursor": normalized.cursor,
                    "observed_at": normalized.observed_at,
                    "metadata": json.dumps(normalized.metadata, sort_keys=True),
                    "updated_at": normalized.updated_at,
                },
            )
        return normalized

    def load(self, checkpoint_key: str) -> CollectorCheckpoint | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT checkpoint_key, cursor, observed_at, metadata, updated_at "
                    "FROM collector_checkpoints WHERE checkpoint_key = :checkpoint_key"
                ),
                {"checkpoint_key": checkpoint_key},
            ).mappings().first()

        if row is None:
            return None

        return CollectorCheckpoint(
            checkpoint_key=str(row["checkpoint_key"]),
            cursor=str(row["cursor"]),
            observed_at=_coerce_timestamp(row["observed_at"])
            if row["observed_at"] is not None
            else None,
            metadata=_json_loads_dict(row["metadata"]),
            updated_at=_coerce_timestamp(row["updated_at"]),
        )


def _json_loads_dict(payload: object) -> dict[str, object]:
    if isinstance(payload, str):
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    raise ValueError("invalid_timestamp_value")