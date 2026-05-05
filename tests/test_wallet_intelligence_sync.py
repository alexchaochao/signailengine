from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.wallet_intelligence_sync import (
    WalletIntelligenceSyncRequest,
    WalletIntelligenceSyncService,
)
from sentinel.wallet_refresh_job import RefreshedWalletState
from sentinel.onchain_listener import build_onchain_trade_event
from tests.test_event_flow import FakeRedis


class StubImporter:
    def import_wallets(self, request, observed_at=None, registry_version="okx_registry_v1"):
        timestamp = observed_at or datetime(2026, 5, 3, 0, 0, tzinfo=UTC)
        return [
            TrackedWalletRegistryEntry(
                wallet_address="wallet-1",
                chain=request.chain,
                wallet_class="smart_money",
                weight=0.8,
                status="active",
                source="okx_leaderboard",
                source_metadata={"wallet_type": request.wallet_type},
                version=registry_version,
                discovered_at=timestamp,
                last_seen_at=timestamp,
                updated_at=timestamp,
            )
        ]


class StubRefreshJob:
    def __init__(self) -> None:
        self.wallets_seen: list[str] = []

    def refresh_wallets(self, requests):
        self.wallets_seen = [request.wallet_address for request in requests]
        return [
            RefreshedWalletState(
                wallet_address=request.wallet_address,
                chain=request.chain,
                refreshed_at=datetime(2026, 5, 3, 0, 5, tzinfo=UTC),
                total_value_usd=1000.0,
                realized_pnl_usd=500.0,
                win_rate=60.0,
                recent_tx_count=3,
                last_active_at=datetime(2026, 5, 3, 0, 4, tzinfo=UTC),
                source_data={"overview": True},
            )
            for request in requests
        ]


def test_wallet_intelligence_sync_persists_and_publishes_snapshot() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    trade_event = build_onchain_trade_event(
        {
            "token": "BONK",
            "chain": "solana",
            "wallet_address": "wallet-1",
            "direction": "inflow",
            "notional_usd": 1250.0,
            "trade_count": 2,
            "observed_at": datetime(2026, 5, 3, 0, 1, tzinfo=UTC),
        }
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": trade_event.event_type,
            "payload": trade_event.model_dump_json(),
        },
    )

    service = WalletIntelligenceSyncService(
        settings,
        cast(Redis, client),
        repository,
        importer=StubImporter(),
        refresh_job=StubRefreshJob(),
    )

    result = service.run(
        WalletIntelligenceSyncRequest(
            chain="solana",
            chain_index="501",
            token="BONK",
            raw_event_count=10,
            refresh_limit=5,
        )
    )

    assert result.imported_wallets == 1
    assert result.refreshed_wallets == 1
    assert result.projected_flows == 1
    assert result.published_message_ids == ["2-0"]
    assert result.last_raw_event_id == "1-0"
    assert count_rows(engine, "tracked_wallet_registry") == 1
    assert count_rows(engine, "tracked_wallet_refresh_state") == 1
    assert count_rows(engine, "wallet_token_flows") == 1
    assert count_rows(engine, "wallet_intelligence_sync_state") == 1


def test_wallet_intelligence_sync_resumes_from_saved_cursor() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    first_trade_event = build_onchain_trade_event(
        {
            "token": "BONK",
            "chain": "solana",
            "wallet_address": "wallet-1",
            "direction": "inflow",
            "notional_usd": 50.0,
            "trade_count": 1,
            "observed_at": datetime(2026, 5, 3, 0, 1, tzinfo=UTC),
            "event_id": "trade-1",
        }
    )
    second_trade_event = build_onchain_trade_event(
        {
            "token": "BONK",
            "chain": "solana",
            "wallet_address": "wallet-1",
            "direction": "outflow",
            "notional_usd": 25.0,
            "trade_count": 1,
            "observed_at": datetime(2026, 5, 3, 0, 2, tzinfo=UTC),
            "event_id": "trade-2",
        }
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {"kind": first_trade_event.event_type, "payload": first_trade_event.model_dump_json()},
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {"kind": second_trade_event.event_type, "payload": second_trade_event.model_dump_json()},
    )

    service = WalletIntelligenceSyncService(
        settings,
        cast(Redis, client),
        repository,
        importer=StubImporter(),
        refresh_job=StubRefreshJob(),
    )

    first_result = service.run(
        WalletIntelligenceSyncRequest(
            chain="solana",
            chain_index="501",
            token="BONK",
            raw_event_count=1,
            sync_key="wallet_intelligence:solana:BONK",
        )
    )
    second_result = service.run(
        WalletIntelligenceSyncRequest(
            chain="solana",
            chain_index="501",
            token="BONK",
            raw_event_count=10,
            sync_key="wallet_intelligence:solana:BONK",
        )
    )

    assert first_result.projected_flows == 1
    assert second_result.projected_flows == 1
    assert first_result.last_raw_event_id == "1-0"
    assert second_result.last_raw_event_id == "3-0"


def test_wallet_flow_projection_uses_existing_registry_without_okx_import() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    repository.wallet_intelligence.upsert_registry_entries(
        [
            TrackedWalletRegistryEntry(
                wallet_address="wallet-1",
                chain="base",
                wallet_class="smart_money",
                weight=0.8,
                status="active",
                source="manual_registry",
                source_metadata={},
                version="manual_v1",
                discovered_at=datetime(2026, 5, 3, 0, 0, tzinfo=UTC),
                last_seen_at=datetime(2026, 5, 3, 0, 0, tzinfo=UTC),
                updated_at=datetime(2026, 5, 3, 0, 0, tzinfo=UTC),
            )
        ]
    )
    trade_event = build_onchain_trade_event(
        {
            "token": "AERO",
            "chain": "base",
            "wallet_address": "wallet-1",
            "direction": "inflow",
            "notional_usd": 2500.0,
            "trade_count": 1,
            "observed_at": datetime(2026, 5, 3, 0, 1, tzinfo=UTC),
        },
        source="evm_base_pool",
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": trade_event.event_type,
            "payload": trade_event.model_dump_json(),
        },
    )
    service = WalletIntelligenceSyncService(
        settings,
        cast(Redis, client),
        repository,
        importer=StubImporter(),
        refresh_job=StubRefreshJob(),
    )

    result = service.project_existing_registry(
        WalletIntelligenceSyncRequest(
            chain="base",
            chain_index="8453",
            token="AERO",
            raw_event_count=10,
            sync_key="wallet_intelligence:base:AERO",
        )
    )

    assert result.imported_wallets == 0
    assert result.refreshed_wallets == 0
    assert result.projected_flows == 1
    assert result.last_raw_event_id == "1-0"
    assert count_rows(engine, "wallet_token_flows") == 1


def test_wallet_intelligence_sync_prioritizes_new_imports_for_refresh() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    repository.wallet_intelligence.upsert_registry_entries(
        [
            TrackedWalletRegistryEntry(
                wallet_address="stale-wallet",
                chain="solana",
                wallet_class="smart_money",
                weight=0.2,
                status="active",
                source="manual_registry",
                source_metadata={},
                version="manual_v1",
                discovered_at=datetime(2026, 5, 2, 0, 0, tzinfo=UTC),
                last_seen_at=datetime(2026, 5, 2, 0, 0, tzinfo=UTC),
                updated_at=datetime(2026, 5, 2, 0, 0, tzinfo=UTC),
            )
        ]
    )
    refresh_job = StubRefreshJob()
    service = WalletIntelligenceSyncService(
        settings,
        cast(Redis, client),
        repository,
        importer=StubImporter(),
        refresh_job=refresh_job,
    )

    result = service.run(
        WalletIntelligenceSyncRequest(
            chain="solana",
            chain_index="501",
            token="BONK",
            raw_event_count=0,
            refresh_limit=1,
        )
    )

    assert result.imported_wallets == 1
    assert result.refreshed_wallets == 1
    assert refresh_job.wallets_seen == ["wallet-1"]