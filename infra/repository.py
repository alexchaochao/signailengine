from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.schemas import (
    CollectorCheckpoint,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    FeatureQualityRecord,
    FeatureSnapshot,
    PortfolioSnapshot,
    PositionState,
    RawEventRecord,
    ReconciliationResult,
    VenueType,
)
from discovery.repository import AlphaDiscoveryStore
from infra.checkpoints import CheckpointStore
from infra.feature_store import FeatureStore
from infra.raw_event_store import RawEventStore

if TYPE_CHECKING:
    from core.pipeline import PipelineResult

from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.wallet_refresh_job import RefreshedWalletState
from sentinel.wallet_score_aggregator import WalletTokenFlow


@dataclass(frozen=True)
class RecoverableIntent:
    intent: ExecutionIntent
    status: str
    adjusted_notional_usd: float


class AuditRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_pipeline_result(self, result: "PipelineResult") -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO token_signals (token, chain, payload) "
                    "VALUES (:token, :chain, :payload)"
                ),
                {
                    "token": result.signal.token,
                    "chain": result.signal.chain,
                    "payload": result.signal.model_dump_json(),
                },
            )
            connection.execute(
                text("INSERT INTO state_transitions (token, payload) VALUES (:token, :payload)"),
                {
                    "token": result.signal.token,
                    "payload": result.transition.model_dump_json(),
                },
            )
            connection.execute(
                text("INSERT INTO route_decisions (token, payload) VALUES (:token, :payload)"),
                {
                    "token": result.signal.token,
                    "payload": result.route.model_dump_json(),
                },
            )
            connection.execute(
                text(
                    "INSERT INTO risk_decisions (intent_id, payload) "
                    "VALUES (:intent_id, :payload)"
                ),
                {
                    "intent_id": result.risk.intent_id,
                    "payload": result.risk.model_dump_json(),
                },
            )

        if result.execution is not None:
            self.save_execution_report(result.execution)

        if result.reconciliation is not None:
            self.save_reconciliation_result(result.reconciliation)

        self.append_execution_ledger(result.execution_ledger)

    def save_execution_report(self, report: ExecutionReport) -> None:
        if self._record_exists(
            "execution_reports",
            "intent_id = :intent_id AND payload = :payload",
            {"intent_id": report.intent_id, "payload": report.model_dump_json()},
        ):
            return

        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO execution_reports (intent_id, payload) "
                    "VALUES (:intent_id, :payload)"
                ),
                {
                    "intent_id": report.intent_id,
                    "payload": report.model_dump_json(),
                },
            )

    def save_reconciliation_result(self, result: ReconciliationResult) -> None:
        if self._record_exists(
            "reconciliation_results",
            "intent_id = :intent_id AND payload = :payload",
            {"intent_id": result.intent_id, "payload": result.model_dump_json()},
        ):
            return

        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO reconciliation_results (intent_id, payload) "
                    "VALUES (:intent_id, :payload)"
                ),
                {
                    "intent_id": result.intent_id,
                    "payload": result.model_dump_json(),
                },
            )

    def append_execution_ledger(self, entries: list[ExecutionLedgerEntry]) -> None:
        if not entries:
            return

        with self.engine.begin() as connection:
            for ledger_entry in entries:
                if self._record_exists(
                    "execution_ledger",
                    "intent_id = :intent_id AND stage = :stage AND status = :status "
                    "AND message = :message AND payload = :payload",
                    {
                        "intent_id": ledger_entry.intent_id,
                        "stage": ledger_entry.stage,
                        "status": ledger_entry.status,
                        "message": ledger_entry.message,
                        "payload": ledger_entry.model_dump_json(),
                    },
                    connection=connection,
                ):
                    continue

                connection.execute(
                    text(
                        "INSERT INTO execution_ledger ("
                        "intent_id, token, venue_type, venue, stage, status, "
                        "notional_usd, message, payload"
                        ") VALUES ("
                        ":intent_id, :token, :venue_type, :venue, :stage, :status, "
                        ":notional_usd, :message, :payload"
                        ")"
                    ),
                    {
                        "intent_id": ledger_entry.intent_id,
                        "token": ledger_entry.token,
                        "venue_type": ledger_entry.venue_type.value,
                        "venue": ledger_entry.venue,
                        "stage": ledger_entry.stage,
                        "status": ledger_entry.status,
                        "notional_usd": ledger_entry.notional_usd,
                        "message": ledger_entry.message,
                        "payload": ledger_entry.model_dump_json(),
                    },
                )

    def load_latest_execution_report(self, intent_id: str) -> ExecutionReport | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT payload FROM execution_reports WHERE intent_id = :intent_id "
                    "ORDER BY id DESC LIMIT 1"
                ),
                {"intent_id": intent_id},
            ).mappings().first()

        if row is None:
            return None

        return ExecutionReport.model_validate_json(str(row["payload"]))

    def has_ledger_status(self, intent_id: str, status: str) -> bool:
        return self._record_exists(
            "execution_ledger",
            "intent_id = :intent_id AND status = :status",
            {"intent_id": intent_id, "status": status},
        )

    def _record_exists(
        self,
        table_name: str,
        where_clause: str,
        params: dict[str, object],
        *,
        connection=None,
    ) -> bool:
        query = text(f"SELECT 1 FROM {table_name} WHERE {where_clause} LIMIT 1")

        if connection is not None:
            return connection.execute(query, params).first() is not None

        with self.engine.connect() as local_connection:
            return local_connection.execute(query, params).first() is not None


class OrderRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_order_from_pipeline_result(self, result: "PipelineResult") -> None:
        if result.route.intent is None:
            return

        execution_attempts = self.get_execution_attempts(result.route.intent.intent_id)
        self.upsert_order(
            result.route.intent,
            result.risk.adjusted_notional_usd,
            result.execution.status if result.execution is not None else "REJECTED",
            execution_attempts=execution_attempts,
        )

    def upsert_order(
        self,
        intent: ExecutionIntent,
        adjusted_notional_usd: float,
        status: str,
        *,
        execution_attempts: int | None = None,
    ) -> None:
        current_attempts = (
            self.get_execution_attempts(intent.intent_id)
            if execution_attempts is None
            else execution_attempts
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO orders ("
                "intent_id, token, venue_type, venue, action, state, confidence, "
                "requested_notional_usd, adjusted_notional_usd, status, "
                "execution_attempts, intent_payload"
                ") VALUES ("
                ":intent_id, :token, :venue_type, :venue, :action, :state, :confidence, "
                ":requested_notional_usd, :adjusted_notional_usd, :status, "
                ":execution_attempts, :intent_payload"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO orders ("
                "intent_id, token, venue_type, venue, action, state, confidence, "
                "requested_notional_usd, adjusted_notional_usd, status, "
                "execution_attempts, intent_payload"
                ") VALUES ("
                ":intent_id, :token, :venue_type, :venue, :action, :state, :confidence, "
                ":requested_notional_usd, :adjusted_notional_usd, :status, "
                ":execution_attempts, :intent_payload"
                ") ON CONFLICT (intent_id) DO UPDATE SET "
                "adjusted_notional_usd = EXCLUDED.adjusted_notional_usd, "
                "status = EXCLUDED.status, "
                "execution_attempts = EXCLUDED.execution_attempts, "
                "intent_payload = EXCLUDED.intent_payload"
            )
        )

        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "intent_id": intent.intent_id,
                    "token": intent.token,
                    "venue_type": intent.venue_type.value,
                    "venue": intent.venue,
                    "action": intent.action.value,
                    "state": intent.state.value,
                    "confidence": intent.confidence,
                    "requested_notional_usd": intent.target_notional_usd,
                    "adjusted_notional_usd": adjusted_notional_usd,
                    "status": status,
                    "execution_attempts": current_attempts,
                    "intent_payload": intent.model_dump_json(),
                },
            )

    def mark_order_status(self, intent_id: str, status: str) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text("UPDATE orders SET status = :status WHERE intent_id = :intent_id"),
                {"intent_id": intent_id, "status": status},
            )

    def increment_execution_attempts(self, intent_id: str) -> int:
        next_attempt = self.get_execution_attempts(intent_id) + 1
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE orders SET execution_attempts = :execution_attempts "
                    "WHERE intent_id = :intent_id"
                ),
                {
                    "intent_id": intent_id,
                    "execution_attempts": next_attempt,
                },
            )
        return next_attempt

    def get_order_status(self, intent_id: str) -> str | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT status FROM orders WHERE intent_id = :intent_id"),
                {"intent_id": intent_id},
            ).mappings().first()

        if row is None:
            return None

        return str(row["status"])

    def get_execution_attempts(self, intent_id: str) -> int:
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT execution_attempts FROM orders WHERE intent_id = :intent_id"),
                {"intent_id": intent_id},
            ).mappings().first()

        if row is None:
            return 0

        return int(row["execution_attempts"])

    def load_recoverable_intents(self) -> list[RecoverableIntent]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT intent_payload, status, adjusted_notional_usd "
                    "FROM orders WHERE status IN ('SUBMITTED', 'FILLED', 'RETRY')"
                )
            ).mappings().all()

        return [
            RecoverableIntent(
                intent=ExecutionIntent.model_validate_json(str(row["intent_payload"])),
                status=str(row["status"]),
                adjusted_notional_usd=float(row["adjusted_notional_usd"]),
            )
            for row in rows
        ]


class StateRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load_position(self, token: str) -> PositionState:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT is_open, venue_type, token_exposure, last_exit_timestamp "
                    "FROM positions WHERE token = :token"
                ),
                {"token": token},
            ).mappings().first()

        if row is None:
            return PositionState()

        return PositionState(
            is_open=bool(row["is_open"]),
            venue_type=VenueType(row["venue_type"]),
            token_exposure=float(row["token_exposure"]),
            last_exit_timestamp=row["last_exit_timestamp"],
        )

    def save_position(self, token: str, position: PositionState) -> None:
        statement = (
            text(
                "INSERT OR REPLACE INTO positions "
                "(token, is_open, venue_type, token_exposure, last_exit_timestamp) "
                "VALUES (:token, :is_open, :venue_type, :token_exposure, :last_exit_timestamp)"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO positions "
                "(token, is_open, venue_type, token_exposure, last_exit_timestamp) "
                "VALUES (:token, :is_open, :venue_type, :token_exposure, :last_exit_timestamp) "
                "ON CONFLICT (token) DO UPDATE SET "
                "is_open = EXCLUDED.is_open, "
                "venue_type = EXCLUDED.venue_type, "
                "token_exposure = EXCLUDED.token_exposure, "
                "last_exit_timestamp = EXCLUDED.last_exit_timestamp"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "token": token,
                    "is_open": position.is_open,
                    "venue_type": position.venue_type.value,
                    "token_exposure": position.token_exposure,
                    "last_exit_timestamp": position.last_exit_timestamp,
                },
            )

    def load_portfolio(self) -> PortfolioSnapshot:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT total_portfolio_usd, token_exposure, chain_exposure, "
                    "open_positions, daily_pnl_fraction "
                    "FROM portfolio_state WHERE state_key = 'global'"
                )
            ).mappings().first()

        if row is None:
            return PortfolioSnapshot()

        return PortfolioSnapshot(
            total_portfolio_usd=float(row["total_portfolio_usd"]),
            token_exposure=float(row["token_exposure"]),
            chain_exposure=float(row["chain_exposure"]),
            open_positions=int(row["open_positions"]),
            daily_pnl_fraction=float(row["daily_pnl_fraction"]),
        )

    def save_portfolio(self, portfolio: PortfolioSnapshot) -> None:
        statement = (
            text(
                "INSERT OR REPLACE INTO portfolio_state ("
                "state_key, total_portfolio_usd, token_exposure, chain_exposure, "
                "open_positions, daily_pnl_fraction"
                ") VALUES ("
                "'global', :total_portfolio_usd, :token_exposure, :chain_exposure, "
                ":open_positions, :daily_pnl_fraction"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO portfolio_state ("
                "state_key, total_portfolio_usd, token_exposure, chain_exposure, "
                "open_positions, daily_pnl_fraction"
                ") VALUES ("
                "'global', :total_portfolio_usd, :token_exposure, :chain_exposure, "
                ":open_positions, :daily_pnl_fraction"
                ") ON CONFLICT (state_key) DO UPDATE SET "
                "total_portfolio_usd = EXCLUDED.total_portfolio_usd, "
                "token_exposure = EXCLUDED.token_exposure, "
                "chain_exposure = EXCLUDED.chain_exposure, "
                "open_positions = EXCLUDED.open_positions, "
                "daily_pnl_fraction = EXCLUDED.daily_pnl_fraction"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "total_portfolio_usd": portfolio.total_portfolio_usd,
                    "token_exposure": portfolio.token_exposure,
                    "chain_exposure": portfolio.chain_exposure,
                    "open_positions": portfolio.open_positions,
                    "daily_pnl_fraction": portfolio.daily_pnl_fraction,
                },
            )


class WalletIntelligenceRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_registry_entries(self, entries: list[TrackedWalletRegistryEntry]) -> None:
        if not entries:
            return
        statement = (
            text(
                "INSERT OR REPLACE INTO tracked_wallet_registry ("
                "wallet_address, chain, wallet_class, weight, status, source, "
                "source_metadata, version, discovered_at, last_seen_at, updated_at"
                ") VALUES ("
                ":wallet_address, :chain, :wallet_class, :weight, :status, :source, "
                ":source_metadata, :version, :discovered_at, :last_seen_at, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO tracked_wallet_registry ("
                "wallet_address, chain, wallet_class, weight, status, source, "
                "source_metadata, version, discovered_at, last_seen_at, updated_at"
                ") VALUES ("
                ":wallet_address, :chain, :wallet_class, :weight, :status, :source, "
                ":source_metadata, :version, :discovered_at, :last_seen_at, :updated_at"
                ") ON CONFLICT (wallet_address, chain) DO UPDATE SET "
                "wallet_class = EXCLUDED.wallet_class, "
                "weight = EXCLUDED.weight, "
                "status = EXCLUDED.status, "
                "source = EXCLUDED.source, "
                "source_metadata = EXCLUDED.source_metadata, "
                "version = EXCLUDED.version, "
                "discovered_at = EXCLUDED.discovered_at, "
                "last_seen_at = EXCLUDED.last_seen_at, "
                "updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            for entry in entries:
                connection.execute(
                    statement,
                    {
                        "wallet_address": entry.wallet_address,
                        "chain": entry.chain,
                        "wallet_class": entry.wallet_class,
                        "weight": entry.weight,
                        "status": entry.status,
                        "source": entry.source,
                        "source_metadata": json.dumps(entry.source_metadata, sort_keys=True),
                        "version": entry.version,
                        "discovered_at": entry.discovered_at,
                        "last_seen_at": entry.last_seen_at,
                        "updated_at": entry.updated_at,
                    },
                )

    def list_active_registry_entries(self, chain: str) -> list[TrackedWalletRegistryEntry]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT wallet_address, chain, wallet_class, weight, status, source, "
                    "source_metadata, version, discovered_at, last_seen_at, updated_at "
                    "FROM tracked_wallet_registry WHERE chain = :chain AND status = 'active'"
                ),
                {"chain": chain},
            ).mappings().all()
        return [
            TrackedWalletRegistryEntry(
                wallet_address=str(row["wallet_address"]),
                chain=str(row["chain"]),
                wallet_class=str(row["wallet_class"]),
                weight=float(row["weight"]),
                status=str(row["status"]),
                source=str(row["source"]),
                source_metadata=_json_loads_dict(row["source_metadata"]),
                version=str(row["version"]),
                discovered_at=_coerce_timestamp(row["discovered_at"]),
                last_seen_at=_coerce_timestamp(row["last_seen_at"]),
                updated_at=_coerce_timestamp(row["updated_at"]),
            )
            for row in rows
        ]

    def save_refresh_states(self, states: list[RefreshedWalletState]) -> None:
        if not states:
            return
        statement = (
            text(
                "INSERT OR REPLACE INTO tracked_wallet_refresh_state ("
                "wallet_address, chain, refreshed_at, total_value_usd, realized_pnl_usd, "
                "win_rate, recent_tx_count, last_active_at, source_data"
                ") VALUES ("
                ":wallet_address, :chain, :refreshed_at, :total_value_usd, :realized_pnl_usd, "
                ":win_rate, :recent_tx_count, :last_active_at, :source_data"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO tracked_wallet_refresh_state ("
                "wallet_address, chain, refreshed_at, total_value_usd, realized_pnl_usd, "
                "win_rate, recent_tx_count, last_active_at, source_data"
                ") VALUES ("
                ":wallet_address, :chain, :refreshed_at, :total_value_usd, :realized_pnl_usd, "
                ":win_rate, :recent_tx_count, :last_active_at, :source_data"
                ") ON CONFLICT (wallet_address, chain) DO UPDATE SET "
                "refreshed_at = EXCLUDED.refreshed_at, "
                "total_value_usd = EXCLUDED.total_value_usd, "
                "realized_pnl_usd = EXCLUDED.realized_pnl_usd, "
                "win_rate = EXCLUDED.win_rate, "
                "recent_tx_count = EXCLUDED.recent_tx_count, "
                "last_active_at = EXCLUDED.last_active_at, "
                "source_data = EXCLUDED.source_data"
            )
        )
        with self.engine.begin() as connection:
            for state in states:
                connection.execute(
                    statement,
                    {
                        "wallet_address": state.wallet_address,
                        "chain": state.chain,
                        "refreshed_at": state.refreshed_at,
                        "total_value_usd": state.total_value_usd,
                        "realized_pnl_usd": state.realized_pnl_usd,
                        "win_rate": state.win_rate,
                        "recent_tx_count": state.recent_tx_count,
                        "last_active_at": state.last_active_at,
                        "source_data": json.dumps(state.source_data, sort_keys=True),
                    },
                )

    def append_wallet_flows(self, flows: list[WalletTokenFlow]) -> None:
        if not flows:
            return
        statement = (
            text(
                "INSERT OR REPLACE INTO wallet_token_flows ("
                "flow_id, chain, token, wallet_address, direction, notional_usd, observed_at, trade_count"
                ") VALUES ("
                ":flow_id, :chain, :token, :wallet_address, :direction, :notional_usd, :observed_at, :trade_count"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO wallet_token_flows ("
                "flow_id, chain, token, wallet_address, direction, notional_usd, observed_at, trade_count"
                ") VALUES ("
                ":flow_id, :chain, :token, :wallet_address, :direction, :notional_usd, :observed_at, :trade_count"
                ") ON CONFLICT (flow_id) DO UPDATE SET "
                "direction = EXCLUDED.direction, "
                "notional_usd = EXCLUDED.notional_usd, "
                "observed_at = EXCLUDED.observed_at, "
                "trade_count = EXCLUDED.trade_count"
            )
        )
        with self.engine.begin() as connection:
            for flow in flows:
                connection.execute(
                    statement,
                    {
                        "flow_id": _flow_id(flow),
                        "chain": flow.chain,
                        "token": flow.token,
                        "wallet_address": flow.wallet_address,
                        "direction": flow.direction,
                        "notional_usd": flow.notional_usd,
                        "observed_at": flow.observed_at,
                        "trade_count": flow.trade_count,
                    },
                )

    def load_wallet_flows(
        self,
        chain: str,
        token: str,
        *,
        since: datetime | None = None,
    ) -> list[WalletTokenFlow]:
        query = (
            "SELECT flow_id, chain, token, wallet_address, direction, notional_usd, observed_at, trade_count "
            "FROM wallet_token_flows WHERE chain = :chain AND token = :token"
        )
        params: dict[str, object] = {"chain": chain, "token": token}
        if since is not None:
            query += " AND observed_at >= :since"
            params["since"] = since.astimezone(UTC)
        with self.engine.connect() as connection:
            rows = connection.execute(text(query), params).mappings().all()
        return [
            WalletTokenFlow(
                chain=str(row["chain"]),
                token=str(row["token"]),
                wallet_address=str(row["wallet_address"]),
                direction=str(row["direction"]),
                notional_usd=float(row["notional_usd"]),
                observed_at=_coerce_timestamp(row["observed_at"]),
                trade_count=int(row["trade_count"]),
                flow_id=str(row["flow_id"]),
            )
            for row in rows
        ]

    def load_sync_state(self, sync_key: str) -> dict[str, object] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT sync_key, last_raw_event_id, last_synced_at, last_published_at, updated_at "
                    "FROM wallet_intelligence_sync_state WHERE sync_key = :sync_key"
                ),
                {"sync_key": sync_key},
            ).mappings().first()
        if row is None:
            return None
        return {
            "sync_key": str(row["sync_key"]),
            "last_raw_event_id": str(row["last_raw_event_id"]),
            "last_synced_at": _coerce_timestamp(row["last_synced_at"]),
            "last_published_at": (
                _coerce_timestamp(row["last_published_at"])
                if row["last_published_at"] is not None
                else None
            ),
            "updated_at": _coerce_timestamp(row["updated_at"]),
        }

    def save_sync_state(
        self,
        sync_key: str,
        *,
        last_raw_event_id: str,
        last_synced_at: datetime,
        last_published_at: datetime | None,
    ) -> None:
        updated_at = datetime.now(UTC)
        statement = (
            text(
                "INSERT OR REPLACE INTO wallet_intelligence_sync_state ("
                "sync_key, last_raw_event_id, last_synced_at, last_published_at, updated_at"
                ") VALUES ("
                ":sync_key, :last_raw_event_id, :last_synced_at, :last_published_at, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO wallet_intelligence_sync_state ("
                "sync_key, last_raw_event_id, last_synced_at, last_published_at, updated_at"
                ") VALUES ("
                ":sync_key, :last_raw_event_id, :last_synced_at, :last_published_at, :updated_at"
                ") ON CONFLICT (sync_key) DO UPDATE SET "
                "last_raw_event_id = EXCLUDED.last_raw_event_id, "
                "last_synced_at = EXCLUDED.last_synced_at, "
                "last_published_at = EXCLUDED.last_published_at, "
                "updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "sync_key": sync_key,
                    "last_raw_event_id": last_raw_event_id,
                    "last_synced_at": last_synced_at.astimezone(UTC),
                    "last_published_at": (
                        last_published_at.astimezone(UTC)
                        if last_published_at is not None
                        else None
                    ),
                    "updated_at": updated_at,
                },
            )

    def load_portfolio(self) -> PortfolioSnapshot:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT total_portfolio_usd, token_exposure, chain_exposure, "
                    "open_positions, daily_pnl_fraction "
                    "FROM portfolio_state WHERE state_key = 'global'"
                )
            ).mappings().first()

        if row is None:
            return PortfolioSnapshot()

        return PortfolioSnapshot(
            total_portfolio_usd=float(row["total_portfolio_usd"]),
            token_exposure=float(row["token_exposure"]),
            chain_exposure=float(row["chain_exposure"]),
            open_positions=int(row["open_positions"]),
            daily_pnl_fraction=float(row["daily_pnl_fraction"]),
        )

    def save_portfolio(self, portfolio: PortfolioSnapshot) -> None:
        statement = (
            text(
                "INSERT OR REPLACE INTO portfolio_state ("
                "state_key, total_portfolio_usd, token_exposure, chain_exposure, "
                "open_positions, daily_pnl_fraction"
                ") VALUES ("
                "'global', :total_portfolio_usd, :token_exposure, :chain_exposure, "
                ":open_positions, :daily_pnl_fraction"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO portfolio_state ("
                "state_key, total_portfolio_usd, token_exposure, chain_exposure, "
                "open_positions, daily_pnl_fraction"
                ") VALUES ("
                "'global', :total_portfolio_usd, :token_exposure, :chain_exposure, "
                ":open_positions, :daily_pnl_fraction"
                ") ON CONFLICT (state_key) DO UPDATE SET "
                "total_portfolio_usd = EXCLUDED.total_portfolio_usd, "
                "token_exposure = EXCLUDED.token_exposure, "
                "chain_exposure = EXCLUDED.chain_exposure, "
                "open_positions = EXCLUDED.open_positions, "
                "daily_pnl_fraction = EXCLUDED.daily_pnl_fraction"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "total_portfolio_usd": portfolio.total_portfolio_usd,
                    "token_exposure": portfolio.token_exposure,
                    "chain_exposure": portfolio.chain_exposure,
                    "open_positions": portfolio.open_positions,
                    "daily_pnl_fraction": portfolio.daily_pnl_fraction,
                },
            )


class NotificationRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def load_delivery(
        self,
        *,
        channel: str,
        destination: str,
        candidate_id: str,
        event_type: str,
    ) -> dict[str, object] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT channel, destination, candidate_id, event_type, status, payload, "
                    "remote_message_id, error_message, created_at, updated_at, delivered_at "
                    "FROM notification_deliveries WHERE channel = :channel AND destination = :destination "
                    "AND candidate_id = :candidate_id AND event_type = :event_type"
                ),
                {
                    "channel": channel,
                    "destination": destination,
                    "candidate_id": candidate_id,
                    "event_type": event_type,
                },
            ).mappings().first()
        if row is None:
            return None
        return {
            "channel": str(row["channel"]),
            "destination": str(row["destination"]),
            "candidate_id": str(row["candidate_id"]),
            "event_type": str(row["event_type"]),
            "status": str(row["status"]),
            "payload": _json_loads_dict(row["payload"]),
            "remote_message_id": (
                str(row["remote_message_id"]) if row["remote_message_id"] is not None else None
            ),
            "error_message": (
                str(row["error_message"]) if row["error_message"] is not None else None
            ),
            "created_at": _coerce_timestamp(row["created_at"]),
            "updated_at": _coerce_timestamp(row["updated_at"]),
            "delivered_at": (
                _coerce_timestamp(row["delivered_at"])
                if row["delivered_at"] is not None
                else None
            ),
        }

    def save_delivery(
        self,
        *,
        channel: str,
        destination: str,
        candidate_id: str,
        event_type: str,
        status: str,
        payload: dict[str, object],
        remote_message_id: str | None = None,
        error_message: str | None = None,
        delivered_at: datetime | None = None,
    ) -> None:
        now = datetime.now(UTC)
        statement = (
            text(
                "INSERT OR REPLACE INTO notification_deliveries ("
                "id, channel, destination, candidate_id, event_type, status, payload, "
                "remote_message_id, error_message, created_at, updated_at, delivered_at"
                ") VALUES ("
                ":id, :channel, :destination, :candidate_id, :event_type, :status, :payload, "
                ":remote_message_id, :error_message, :created_at, :updated_at, :delivered_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO notification_deliveries ("
                "id, channel, destination, candidate_id, event_type, status, payload, "
                "remote_message_id, error_message, created_at, updated_at, delivered_at"
                ") VALUES ("
                ":id, :channel, :destination, :candidate_id, :event_type, :status, :payload, "
                ":remote_message_id, :error_message, :created_at, :updated_at, :delivered_at"
                ") ON CONFLICT (channel, destination, candidate_id, event_type) DO UPDATE SET "
                "status = EXCLUDED.status, "
                "payload = EXCLUDED.payload, "
                "remote_message_id = EXCLUDED.remote_message_id, "
                "error_message = EXCLUDED.error_message, "
                "updated_at = EXCLUDED.updated_at, "
                "delivered_at = EXCLUDED.delivered_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "id": f"{channel}:{destination}:{candidate_id}:{event_type}",
                    "channel": channel,
                    "destination": destination,
                    "candidate_id": candidate_id,
                    "event_type": event_type,
                    "status": status,
                    "payload": json.dumps(payload, sort_keys=True),
                    "remote_message_id": remote_message_id,
                    "error_message": error_message,
                    "created_at": now,
                    "updated_at": now,
                    "delivered_at": delivered_at.astimezone(UTC) if delivered_at is not None else None,
                },
            )


class StorageRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.audit = AuditRepository(engine)
        self.orders = OrderRepository(engine)
        self.state = StateRepository(engine)
        self.wallet_intelligence = WalletIntelligenceRepository(engine)
        self.notifications = NotificationRepository(engine)
        self.raw_events = RawEventStore(engine)
        self.checkpoints = CheckpointStore(engine)
        self.features = FeatureStore(engine)
        self.discovery = AlphaDiscoveryStore(engine)

    def persist_pipeline_result(self, result: "PipelineResult") -> None:
        self.audit.save_pipeline_result(result)
        self.orders.save_order_from_pipeline_result(result)
        if result.reconciliation is not None:
            self.state.save_position(result.signal.token, result.reconciliation.position)
            self.state.save_portfolio(result.reconciliation.portfolio)

    def load_recoverable_intents(self) -> list[RecoverableIntent]:
        return [
            record
            for record in self.orders.load_recoverable_intents()
            if self.audit.has_ledger_status(record.intent.intent_id, "SUBMITTED")
            and not self.audit.has_ledger_status(record.intent.intent_id, "RECONCILED")
            and not self.audit.has_ledger_status(record.intent.intent_id, "REJECTED")
        ]


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
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(UTC)
    raise ValueError("invalid_timestamp_value")


def _flow_id(flow: WalletTokenFlow) -> str:
    if flow.flow_id:
        return flow.flow_id
    observed_bucket = int(flow.observed_at.astimezone(UTC).timestamp())
    return (
        f"{flow.chain}:{flow.token}:{flow.wallet_address}:{flow.direction}:"
        f"{observed_bucket}:{flow.trade_count}"
    )
