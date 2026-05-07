from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

from discovery.schemas import (
    AlphaCandidate,
    AlphaCandidateEvent,
    AlphaCandidateStatus,
    AlphaSnapshot,
    AlphaType,
)


class AlphaDiscoveryStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_candidate(self, candidate: AlphaCandidate) -> AlphaCandidate:
        normalized = candidate.model_copy(
            update={
                "first_seen_at": candidate.first_seen_at.astimezone(UTC),
                "last_seen_at": candidate.last_seen_at.astimezone(UTC),
                "created_at": (candidate.created_at or datetime.now(UTC)).astimezone(UTC),
                "updated_at": (candidate.updated_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO alpha_candidates ("
                "candidate_id, alpha_type, chain, token, pool_address, dex, quote_asset, status, score, "
                "first_seen_at, last_seen_at, initial_liquidity_usd, liquidity_lock_ratio, "
                "buy_notional_5m_usd, trade_count_5m, unique_wallets_5m, smart_money_wallets_5m, "
                "creator_hold_pct, reasons, metadata, created_at, updated_at"
                ") VALUES ("
                ":candidate_id, :alpha_type, :chain, :token, :pool_address, :dex, :quote_asset, :status, :score, "
                ":first_seen_at, :last_seen_at, :initial_liquidity_usd, :liquidity_lock_ratio, "
                ":buy_notional_5m_usd, :trade_count_5m, :unique_wallets_5m, :smart_money_wallets_5m, "
                ":creator_hold_pct, :reasons, :metadata, :created_at, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO alpha_candidates ("
                "candidate_id, alpha_type, chain, token, pool_address, dex, quote_asset, status, score, "
                "first_seen_at, last_seen_at, initial_liquidity_usd, liquidity_lock_ratio, "
                "buy_notional_5m_usd, trade_count_5m, unique_wallets_5m, smart_money_wallets_5m, "
                "creator_hold_pct, reasons, metadata, created_at, updated_at"
                ") VALUES ("
                ":candidate_id, :alpha_type, :chain, :token, :pool_address, :dex, :quote_asset, :status, :score, "
                ":first_seen_at, :last_seen_at, :initial_liquidity_usd, :liquidity_lock_ratio, "
                ":buy_notional_5m_usd, :trade_count_5m, :unique_wallets_5m, :smart_money_wallets_5m, "
                ":creator_hold_pct, :reasons, :metadata, :created_at, :updated_at"
                ") ON CONFLICT (chain, pool_address) DO UPDATE SET "
                "candidate_id = EXCLUDED.candidate_id, "
                "alpha_type = EXCLUDED.alpha_type, chain = EXCLUDED.chain, token = EXCLUDED.token, "
                "dex = EXCLUDED.dex, quote_asset = EXCLUDED.quote_asset, status = EXCLUDED.status, "
                "score = EXCLUDED.score, first_seen_at = LEAST(alpha_candidates.first_seen_at, EXCLUDED.first_seen_at), "
                "last_seen_at = EXCLUDED.last_seen_at, initial_liquidity_usd = EXCLUDED.initial_liquidity_usd, "
                "liquidity_lock_ratio = EXCLUDED.liquidity_lock_ratio, buy_notional_5m_usd = EXCLUDED.buy_notional_5m_usd, "
                "trade_count_5m = EXCLUDED.trade_count_5m, unique_wallets_5m = EXCLUDED.unique_wallets_5m, "
                "smart_money_wallets_5m = EXCLUDED.smart_money_wallets_5m, creator_hold_pct = EXCLUDED.creator_hold_pct, "
                "reasons = EXCLUDED.reasons, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(statement, _candidate_params(normalized))
        return normalized

    def append_event(self, event: AlphaCandidateEvent) -> AlphaCandidateEvent:
        normalized = event.model_copy(
            update={
                "created_at": (event.created_at or datetime.now(UTC)).astimezone(UTC),
                "observed_at": event.observed_at.astimezone(UTC),
                "event_id": event.event_id or str(uuid4()),
            }
        )
        statement = (
            text(
                "INSERT OR IGNORE INTO alpha_candidate_events ("
                "event_id, candidate_id, event_type, observed_at, payload, created_at"
                ") VALUES ("
                ":event_id, :candidate_id, :event_type, :observed_at, :payload, :created_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO alpha_candidate_events ("
                "event_id, candidate_id, event_type, observed_at, payload, created_at"
                ") VALUES ("
                ":event_id, :candidate_id, :event_type, :observed_at, :payload, :created_at"
                ") ON CONFLICT (event_id) DO NOTHING"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(statement, _event_params(normalized))
        return normalized

    def save_snapshot(self, snapshot: AlphaSnapshot) -> AlphaSnapshot:
        normalized = snapshot.model_copy(
            update={
                "created_at": (snapshot.created_at or datetime.now(UTC)).astimezone(UTC),
                "observed_at": snapshot.observed_at.astimezone(UTC),
            }
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO alpha_snapshots ("
                "snapshot_id, candidate_id, alpha_type, chain, token, observed_at, status, score, payload, created_at"
                ") VALUES ("
                ":snapshot_id, :candidate_id, :alpha_type, :chain, :token, :observed_at, :status, :score, :payload, :created_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO alpha_snapshots ("
                "snapshot_id, candidate_id, alpha_type, chain, token, observed_at, status, score, payload, created_at"
                ") VALUES ("
                ":snapshot_id, :candidate_id, :alpha_type, :chain, :token, :observed_at, :status, :score, :payload, :created_at"
                ") ON CONFLICT (snapshot_id) DO UPDATE SET "
                "candidate_id = EXCLUDED.candidate_id, alpha_type = EXCLUDED.alpha_type, chain = EXCLUDED.chain, token = EXCLUDED.token, "
                "observed_at = EXCLUDED.observed_at, status = EXCLUDED.status, score = EXCLUDED.score, "
                "payload = EXCLUDED.payload, created_at = EXCLUDED.created_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(statement, _snapshot_params(normalized))
        return normalized

    def load_candidate(self, candidate_id: str) -> AlphaCandidate | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT candidate_id, chain, token, pool_address, dex, quote_asset, status, score, "
                    "alpha_type, "
                    "first_seen_at, last_seen_at, initial_liquidity_usd, liquidity_lock_ratio, buy_notional_5m_usd, "
                    "trade_count_5m, unique_wallets_5m, smart_money_wallets_5m, creator_hold_pct, reasons, metadata, "
                    "created_at, updated_at FROM alpha_candidates WHERE candidate_id = :candidate_id"
                ),
                {"candidate_id": candidate_id},
            ).mappings().first()
        if row is None:
            return None
        return _row_to_candidate(row)

    def list_candidates(
        self,
        *,
        status: AlphaCandidateStatus | None = None,
        alpha_type: AlphaType | None = None,
        chain: str | None = None,
        limit: int = 100,
    ) -> list[AlphaCandidate]:
        where_clauses: list[str] = []
        params: dict[str, object] = {"limit": limit}
        if status is not None:
            where_clauses.append("status = :status")
            params["status"] = status.value
        if alpha_type is not None:
            where_clauses.append("alpha_type = :alpha_type")
            params["alpha_type"] = alpha_type.value
        if chain is not None:
            where_clauses.append("chain = :chain")
            params["chain"] = chain
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT candidate_id, chain, token, pool_address, dex, quote_asset, status, score, "
                    "alpha_type, "
                    "first_seen_at, last_seen_at, initial_liquidity_usd, liquidity_lock_ratio, buy_notional_5m_usd, "
                    "trade_count_5m, unique_wallets_5m, smart_money_wallets_5m, creator_hold_pct, reasons, metadata, "
                    "created_at, updated_at FROM alpha_candidates "
                    f"{where_sql} ORDER BY score DESC, last_seen_at DESC LIMIT :limit"
                ),
                params,
            ).mappings().all()
        return [_row_to_candidate(row) for row in rows]


def _candidate_params(candidate: AlphaCandidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "alpha_type": candidate.alpha_type.value,
        "chain": candidate.chain,
        "token": candidate.token,
        "pool_address": candidate.pool_address,
        "dex": candidate.dex,
        "quote_asset": candidate.quote_asset,
        "status": candidate.status.value,
        "score": candidate.score,
        "first_seen_at": candidate.first_seen_at,
        "last_seen_at": candidate.last_seen_at,
        "initial_liquidity_usd": candidate.initial_liquidity_usd,
        "liquidity_lock_ratio": candidate.liquidity_lock_ratio,
        "buy_notional_5m_usd": candidate.buy_notional_5m_usd,
        "trade_count_5m": candidate.trade_count_5m,
        "unique_wallets_5m": candidate.unique_wallets_5m,
        "smart_money_wallets_5m": candidate.smart_money_wallets_5m,
        "creator_hold_pct": candidate.creator_hold_pct,
        "reasons": json.dumps(candidate.reasons),
        "metadata": json.dumps(candidate.metadata, sort_keys=True),
        "created_at": candidate.created_at,
        "updated_at": candidate.updated_at,
    }


def _event_params(event: AlphaCandidateEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "candidate_id": event.candidate_id,
        "event_type": event.event_type,
        "observed_at": event.observed_at,
        "payload": json.dumps(event.payload, sort_keys=True),
        "created_at": event.created_at,
    }


def _snapshot_params(snapshot: AlphaSnapshot) -> dict[str, object]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "candidate_id": snapshot.candidate_id,
        "alpha_type": snapshot.alpha_type.value,
        "chain": snapshot.chain,
        "token": snapshot.token,
        "observed_at": snapshot.observed_at,
        "status": snapshot.status.value,
        "score": snapshot.score,
        "payload": json.dumps(snapshot.payload, sort_keys=True),
        "created_at": snapshot.created_at,
    }


def _row_to_candidate(row: object) -> AlphaCandidate:
    mapping = dict(row)
    return AlphaCandidate(
        candidate_id=str(mapping["candidate_id"]),
        alpha_type=AlphaType(str(mapping["alpha_type"])),
        chain=str(mapping["chain"]),
        token=str(mapping["token"]),
        pool_address=str(mapping["pool_address"]),
        dex=str(mapping["dex"]),
        quote_asset=str(mapping["quote_asset"]),
        status=AlphaCandidateStatus(str(mapping["status"])),
        score=float(mapping["score"]),
        first_seen_at=_coerce_timestamp(mapping["first_seen_at"]),
        last_seen_at=_coerce_timestamp(mapping["last_seen_at"]),
        initial_liquidity_usd=float(mapping["initial_liquidity_usd"]),
        liquidity_lock_ratio=(
            float(mapping["liquidity_lock_ratio"])
            if mapping["liquidity_lock_ratio"] is not None
            else None
        ),
        buy_notional_5m_usd=float(mapping["buy_notional_5m_usd"]),
        trade_count_5m=int(mapping["trade_count_5m"]),
        unique_wallets_5m=int(mapping["unique_wallets_5m"]),
        smart_money_wallets_5m=int(mapping["smart_money_wallets_5m"]),
        creator_hold_pct=(
            float(mapping["creator_hold_pct"])
            if mapping["creator_hold_pct"] is not None
            else None
        ),
        reasons=_json_loads_list(mapping["reasons"]),
        metadata=_json_loads_dict(mapping["metadata"]),
        created_at=_coerce_timestamp(mapping["created_at"]),
        updated_at=_coerce_timestamp(mapping["updated_at"]),
    )


def _json_loads_dict(payload: object) -> dict[str, object]:
    if isinstance(payload, str):
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _json_loads_list(payload: object) -> list[str]:
    if isinstance(payload, str):
        parsed = json.loads(payload)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _coerce_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    raise ValueError("invalid_timestamp_value")