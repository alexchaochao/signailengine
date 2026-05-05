from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine

from core.config import AppSettings, FlowAlphaLiveSourceConfig
from discovery.flow_live_sources import build_flow_live_sources
from infra.postgres import init_storage
from infra.repository import StorageRepository
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.wallet_score_aggregator import WalletTokenFlow


def test_flow_live_source_builds_snapshot_from_wallet_intelligence_store() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    now = datetime.now(UTC)
    repository.wallet_intelligence.upsert_registry_entries(
        [
            TrackedWalletRegistryEntry(
                wallet_address="wallet-1",
                chain="base",
                wallet_class="smart_money",
                weight=1.0,
                status="active",
                source="okx",
                source_metadata={},
                version="okx_registry_v1",
                discovered_at=now,
                last_seen_at=now,
                updated_at=now,
            )
        ]
    )
    repository.wallet_intelligence.append_wallet_flows(
        [
            WalletTokenFlow(
                chain="base",
                token="AERO",
                wallet_address="wallet-1",
                direction="inflow",
                notional_usd=60_000.0,
                observed_at=now - timedelta(minutes=5),
            ),
            WalletTokenFlow(
                chain="base",
                token="AERO",
                wallet_address="wallet-1",
                direction="outflow",
                notional_usd=10_000.0,
                observed_at=now - timedelta(minutes=4),
            ),
        ]
    )
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": AppSettings.load().acquisition.model_copy(
                update={
                    "flow_alpha_sources": {
                        "base_aero": FlowAlphaLiveSourceConfig(
                            enabled=True,
                            source_name="flow_alpha_base_aero",
                            chain="base",
                            token="AERO",
                            min_netflow_15m_usd=25_000.0,
                            min_smart_money_inflow_usd=40_000.0,
                            min_unique_buyer_wallets_15m=1,
                        )
                    }
                }
            )
        }
    )

    sources = build_flow_live_sources(settings, repository)
    snapshots = sources[0].fetch_snapshots()

    assert len(sources) == 1
    assert len(snapshots) == 1
    assert snapshots[0].token == "AERO"
    assert snapshots[0].netflow_15m_usd == 50_000.0