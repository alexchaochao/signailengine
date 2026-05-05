from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from core.config import AppSettings
from infra.repository import StorageRepository

if TYPE_CHECKING:
    from core.pipeline import PipelineResult


def _sqlite_iso_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(sep=" ")


def _sqlite_iso_date(value: date) -> str:
    return value.isoformat()


sqlite3.register_adapter(datetime, _sqlite_iso_datetime)
sqlite3.register_adapter(date, _sqlite_iso_date)


def get_engine(settings: AppSettings) -> Engine:
    return create_engine(
        settings.postgres.url,
        echo=settings.postgres.echo,
        pool_size=settings.postgres.pool_size,
        max_overflow=settings.postgres.max_overflow,
    )


def ping_postgres(settings: AppSettings) -> bool:
    engine = get_engine(settings)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False


def init_storage(engine: Engine) -> None:
    id_column = _id_column_sql(engine)
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS token_signals (
            id {id_column},
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS state_transitions (
            id {id_column},
            token TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS route_decisions (
            id {id_column},
            token TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS risk_decisions (
            id {id_column},
            intent_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS execution_reports (
            id {id_column},
            intent_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS reconciliation_results (
            id {id_column},
            intent_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS execution_ledger (
            id {id_column},
            intent_id TEXT NOT NULL,
            token TEXT NOT NULL,
            venue_type TEXT NOT NULL,
            venue TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            notional_usd REAL NOT NULL,
            message TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS orders (
            id {id_column},
            intent_id TEXT NOT NULL UNIQUE,
            token TEXT NOT NULL,
            venue_type TEXT NOT NULL,
            venue TEXT NOT NULL,
            action TEXT NOT NULL,
            state TEXT NOT NULL,
            confidence REAL NOT NULL,
            requested_notional_usd REAL NOT NULL,
            adjusted_notional_usd REAL NOT NULL,
            status TEXT NOT NULL,
            execution_attempts INTEGER NOT NULL,
            intent_payload TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS positions (
            token TEXT PRIMARY KEY,
            is_open BOOLEAN NOT NULL,
            venue_type TEXT NOT NULL,
            token_exposure REAL NOT NULL,
            last_exit_timestamp INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS portfolio_state (
            state_key TEXT PRIMARY KEY,
            total_portfolio_usd REAL NOT NULL,
            token_exposure REAL NOT NULL,
            chain_exposure REAL NOT NULL,
            open_positions INTEGER NOT NULL,
            daily_pnl_fraction REAL NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tracked_wallet_registry (
            wallet_address TEXT NOT NULL,
            chain TEXT NOT NULL,
            wallet_class TEXT NOT NULL,
            weight REAL NOT NULL,
            status TEXT NOT NULL,
            source TEXT NOT NULL,
            source_metadata TEXT NOT NULL,
            version TEXT NOT NULL,
            discovered_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (wallet_address, chain)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tracked_wallet_refresh_state (
            wallet_address TEXT NOT NULL,
            chain TEXT NOT NULL,
            refreshed_at TIMESTAMP NOT NULL,
            total_value_usd REAL,
            realized_pnl_usd REAL,
            win_rate REAL,
            recent_tx_count INTEGER NOT NULL,
            last_active_at TIMESTAMP,
            source_data TEXT NOT NULL,
            PRIMARY KEY (wallet_address, chain)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_token_flows (
            flow_id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            wallet_address TEXT NOT NULL,
            direction TEXT NOT NULL,
            notional_usd REAL NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            trade_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_intelligence_sync_state (
            sync_key TEXT PRIMARY KEY,
            last_raw_event_id TEXT NOT NULL,
            last_synced_at TIMESTAMP NOT NULL,
            last_published_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_events (
            id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            chain TEXT,
            token TEXT,
            observed_at TIMESTAMP NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            cursor TEXT,
            payload TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            replayable BOOLEAN NOT NULL,
            schema_version TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_events_source_dedupe
        ON raw_events (source_name, source_event_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_raw_events_type_observed
        ON raw_events (source_type, observed_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_raw_events_chain_token_observed
        ON raw_events (chain, token, observed_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS collector_checkpoints (
            checkpoint_key TEXT PRIMARY KEY,
            cursor TEXT NOT NULL,
            observed_at TIMESTAMP,
            metadata TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS feature_snapshots (
            id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            feature_name TEXT NOT NULL,
            feature_value REAL NOT NULL,
            window_name TEXT NOT NULL,
            as_of TIMESTAMP NOT NULL,
            sample_count INTEGER NOT NULL,
            freshness_seconds REAL NOT NULL,
            quality_flag TEXT NOT NULL,
            formula_version TEXT NOT NULL,
            inputs TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_feature_snapshots_unique
        ON feature_snapshots (chain, token, feature_name, window_name, as_of, formula_version)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_feature_snapshots_chain_token_asof
        ON feature_snapshots (chain, token, as_of)
        """,
        """
        CREATE TABLE IF NOT EXISTS feature_quality (
            id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            feature_name TEXT NOT NULL,
            as_of TIMESTAMP NOT NULL,
            freshness_seconds REAL NOT NULL,
            source_lag_seconds REAL NOT NULL,
            missing_sources TEXT NOT NULL,
            degraded_reason TEXT,
            created_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_feature_quality_chain_token_feature_asof
        ON feature_quality (chain, token, feature_name, as_of)
        """,
        """
        CREATE TABLE IF NOT EXISTS dex_trade_facts (
            trade_id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pool_address TEXT NOT NULL,
            wallet_address TEXT,
            side TEXT NOT NULL,
            token_amount REAL NOT NULL,
            quote_amount_usd REAL NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            source_event_id TEXT NOT NULL,
            classification_version TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_dex_trade_facts_chain_token_observed
        ON dex_trade_facts (chain, token, observed_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS token_trade_windows (
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            window_name TEXT NOT NULL,
            window_end TIMESTAMP NOT NULL,
            buy_notional_usd REAL NOT NULL,
            sell_notional_usd REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            unique_wallets INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (chain, token, window_name, window_end)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dex_quote_samples (
            quote_id TEXT PRIMARY KEY,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            quote_notional_usd REAL NOT NULL,
            expected_out_usd REAL NOT NULL,
            reference_mid_usd REAL NOT NULL,
            slippage_bps REAL NOT NULL,
            route_summary TEXT NOT NULL,
            quoted_at TIMESTAMP NOT NULL,
            source_event_id TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_dex_quote_samples_chain_token_quoted
        ON dex_quote_samples (chain, token, quoted_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS slippage_curves (
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            curve_as_of TIMESTAMP NOT NULL,
            sample_points TEXT NOT NULL,
            curve_version TEXT NOT NULL,
            freshness_seconds REAL NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (chain, token, curve_as_of, curve_version)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS alpha_candidates (
            candidate_id TEXT PRIMARY KEY,
            alpha_type TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            pool_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            quote_asset TEXT NOT NULL,
            status TEXT NOT NULL,
            score REAL NOT NULL,
            first_seen_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            initial_liquidity_usd REAL NOT NULL,
            liquidity_lock_ratio REAL,
            buy_notional_5m_usd REAL NOT NULL,
            trade_count_5m INTEGER NOT NULL,
            unique_wallets_5m INTEGER NOT NULL,
            smart_money_wallets_5m INTEGER NOT NULL,
            creator_hold_pct REAL,
            reasons TEXT NOT NULL,
            metadata TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alpha_candidates_chain_pool
        ON alpha_candidates (chain, pool_address)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_status_score
        ON alpha_candidates (alpha_type, status, score)
        """,
        """
        CREATE TABLE IF NOT EXISTS alpha_candidate_events (
            event_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_candidate_events_candidate_observed
        ON alpha_candidate_events (candidate_id, observed_at)
        """,
        """
        CREATE TABLE IF NOT EXISTS notification_deliveries (
            id TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            destination TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload TEXT NOT NULL,
            remote_message_id TEXT,
            error_message TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            delivered_at TIMESTAMP
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_deliveries_unique
        ON notification_deliveries (channel, destination, candidate_id, event_type)
        """,
        """
        CREATE TABLE IF NOT EXISTS alpha_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            alpha_type TEXT NOT NULL,
            chain TEXT NOT NULL,
            token TEXT NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            status TEXT NOT NULL,
            score REAL NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_snapshots_candidate_observed
        ON alpha_snapshots (candidate_id, observed_at)
        """,
    ]

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def persist_pipeline_result(engine: Engine, result: "PipelineResult") -> None:
    StorageRepository(engine).persist_pipeline_result(result)


def count_rows(engine: Engine, table_name: str) -> int:
    with engine.connect() as connection:
        value = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return int(value.scalar_one())


def _id_column_sql(engine: Engine) -> str:
    if engine.dialect.name == "sqlite":
        return "INTEGER PRIMARY KEY AUTOINCREMENT"
    return "INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"


def load_position_state(engine: Engine, token: str):
    return StorageRepository(engine).state.load_position(token)


def save_position_state(engine: Engine, token: str, position) -> None:
    StorageRepository(engine).state.save_position(token, position)


def load_portfolio_snapshot(engine: Engine):
    return StorageRepository(engine).state.load_portfolio()


def save_portfolio_snapshot(engine: Engine, portfolio) -> None:
    StorageRepository(engine).state.save_portfolio(portfolio)