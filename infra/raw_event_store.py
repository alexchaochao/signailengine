from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.schemas import RawEventRecord


class RawEventStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save(self, event: RawEventRecord) -> RawEventRecord:
        normalized = _normalize_raw_event(event)
        statement = (
            text(
                "INSERT OR IGNORE INTO raw_events ("
                "id, source_type, source_name, source_event_id, chain, token, observed_at, "
                "ingested_at, cursor, payload, payload_hash, replayable, schema_version, created_at"
                ") VALUES ("
                ":id, :source_type, :source_name, :source_event_id, :chain, :token, :observed_at, "
                ":ingested_at, :cursor, :payload, :payload_hash, :replayable, :schema_version, :created_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO raw_events ("
                "id, source_type, source_name, source_event_id, chain, token, observed_at, "
                "ingested_at, cursor, payload, payload_hash, replayable, schema_version, created_at"
                ") VALUES ("
                ":id, :source_type, :source_name, :source_event_id, :chain, :token, :observed_at, "
                ":ingested_at, :cursor, :payload, :payload_hash, :replayable, :schema_version, :created_at"
                ") ON CONFLICT (source_name, source_event_id) DO NOTHING"
            )
        )
        with self.engine.begin() as connection:
            result = connection.execute(statement, _raw_event_params(normalized))

        if result.rowcount and result.rowcount > 0:
            return normalized

        existing = self.load(normalized.source_name, normalized.source_event_id)
        if existing is None:
            raise RuntimeError("raw_event_persist_failed")
        return existing

    def load(self, source_name: str, source_event_id: str) -> RawEventRecord | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT id, source_type, source_name, source_event_id, chain, token, observed_at, "
                    "ingested_at, cursor, payload, payload_hash, replayable, schema_version, created_at "
                    "FROM raw_events WHERE source_name = :source_name "
                    "AND source_event_id = :source_event_id"
                ),
                {"source_name": source_name, "source_event_id": source_event_id},
            ).mappings().first()

        if row is None:
            return None

        return _row_to_raw_event(row)

    def read_events(
        self,
        *,
        source_type: str | None = None,
        chain: str | None = None,
        token: str | None = None,
        cursor_after: datetime | None = None,
        limit: int = 1000,
    ) -> list[RawEventRecord]:
        where_clauses: list[str] = []
        params: dict[str, object] = {"limit": limit}
        if source_type is not None:
            where_clauses.append("source_type = :source_type")
            params["source_type"] = source_type
        if chain is not None:
            where_clauses.append("chain = :chain")
            params["chain"] = chain
        if token is not None:
            where_clauses.append("token = :token")
            params["token"] = token
        if cursor_after is not None:
            where_clauses.append("observed_at >= :cursor_after")
            params["cursor_after"] = cursor_after.astimezone(UTC)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT id, source_type, source_name, source_event_id, chain, token, observed_at, "
                    "ingested_at, cursor, payload, payload_hash, replayable, schema_version, created_at "
                    f"FROM raw_events {where_sql} "
                    "ORDER BY observed_at ASC, ingested_at ASC, source_name ASC, source_event_id ASC "
                    "LIMIT :limit"
                ),
                params,
            ).mappings().all()

        return [_row_to_raw_event(row) for row in rows]


def _normalize_raw_event(event: RawEventRecord) -> RawEventRecord:
    return event.model_copy(
        update={
            "id": event.id or str(uuid4()),
            "observed_at": event.observed_at.astimezone(UTC),
            "ingested_at": event.ingested_at.astimezone(UTC),
            "payload_hash": event.payload_hash or _payload_hash(event.payload),
            "created_at": (event.created_at or datetime.now(UTC)).astimezone(UTC),
        }
    )


def _raw_event_params(event: RawEventRecord) -> dict[str, object]:
    return {
        "id": event.id,
        "source_type": event.source_type,
        "source_name": event.source_name,
        "source_event_id": event.source_event_id,
        "chain": event.chain,
        "token": event.token,
        "observed_at": event.observed_at,
        "ingested_at": event.ingested_at,
        "cursor": event.cursor,
        "payload": json.dumps(event.payload, sort_keys=True),
        "payload_hash": event.payload_hash,
        "replayable": event.replayable,
        "schema_version": event.schema_version,
        "created_at": event.created_at,
    }


def _row_to_raw_event(row: object) -> RawEventRecord:
    mapping = dict(row)
    return RawEventRecord(
        id=str(mapping["id"]),
        source_type=str(mapping["source_type"]),
        source_name=str(mapping["source_name"]),
        source_event_id=str(mapping["source_event_id"]),
        chain=str(mapping["chain"]) if mapping["chain"] is not None else None,
        token=str(mapping["token"]) if mapping["token"] is not None else None,
        observed_at=_coerce_timestamp(mapping["observed_at"]),
        ingested_at=_coerce_timestamp(mapping["ingested_at"]),
        cursor=str(mapping["cursor"]) if mapping["cursor"] is not None else None,
        payload=_json_loads_dict(mapping["payload"]),
        payload_hash=str(mapping["payload_hash"]),
        replayable=bool(mapping["replayable"]),
        schema_version=str(mapping["schema_version"]),
        created_at=_coerce_timestamp(mapping["created_at"]),
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


def _payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()