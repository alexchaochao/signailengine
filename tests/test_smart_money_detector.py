"""Tests for Smart Money Inflow Detection."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from discovery.smart_money_detector import SmartMoneyDetector, SmartMoneyInflowResult
from discovery.schemas import AlphaCandidate, AlphaCandidateStatus, AlphaType
from infra.postgres import init_storage
from infra.repository import StorageRepository
from tests.test_pipeline import FakeRedis
from core.event_flow import publish_raw_events
from core.schemas import EventEnvelope
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry


def _setup_registry(repo: StorageRepository, chain: str, wallets: list[str]) -> None:
    """Populate wallet registry with test entries."""
    from datetime import UTC, datetime
    for addr in wallets:
        entry = TrackedWalletRegistryEntry(
            wallet_address=addr,
            chain=chain,
            wallet_class="okx_top_trader",
            weight=1.0,
            status="active",
            source="okx_leaderboard",
            source_metadata={"time_frame": "3", "sort_by": "1"},
            version="test_v1",
            discovered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        repo.wallet_intelligence.upsert_registry_entries([entry])


def _publish_trade_fact(client: FakeRedis, settings: AppSettings, *, chain: str, token: str, wallet: str, notional: float) -> None:
    """Publish an onchain.trade_fact event for a smart wallet buy."""
    publish_raw_events(
        cast(Redis, client), settings,
        EventEnvelope(
            event_id=f"trade:{chain}:{token}:{wallet}:{datetime.now(UTC).timestamp()}",
            event_type="onchain.trade_fact",
            source="test",
            chain=chain,
            token=token,
            observed_at=datetime.now(UTC),
            ingested_at=datetime.now(UTC),
            payload={
                "wallet_address": wallet,
                "direction": "inflow",
                "notional_usd": notional,
                "trade_count": 1,
            },
        ),
    )


def _fake_read_models(*args, **kwargs):
    """Mock read_models: return empty list (no trade events in test)."""
    return []


def test_smart_money_detector_no_registry_returns_zero(monkeypatch) -> None:
    """When no registry entries exist, check should return 0 smart buyers."""
    import discovery.smart_money_detector as smd
    monkeypatch.setattr(smd, "read_models", _fake_read_models)
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repo = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()

    detector = SmartMoneyDetector(settings, cast(Redis, client), repo)
    result = detector.check_token("solana", "TEST123")

    assert isinstance(result, SmartMoneyInflowResult)
    assert result.smart_wallet_buyers == 0
    assert result.has_smart_money is False
    assert result.confidence_score == 0.0


def test_smart_money_detector_with_registry_but_no_trades(monkeypatch) -> None:
    """Registry exists but no trade events → 0 smart buyers."""
    import discovery.smart_money_detector as smd
    monkeypatch.setattr(smd, "read_models", _fake_read_models)
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repo = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()

    _setup_registry(repo, "solana", ["wallet1", "wallet2", "wallet3"])

    detector = SmartMoneyDetector(settings, cast(Redis, client), repo)
    result = detector.check_token("solana", "TEST123")

    assert result.smart_wallet_buyers == 0
    assert result.has_smart_money is False


def test_smart_money_detector_one_smart_wallet_bought(monkeypatch) -> None:
    """One tracked wallet bought the token → score ~0.42."""
    import discovery.smart_money_detector as smd

    def _read_with_one_trade(*a, **kw):
        from datetime import UTC, datetime
        return [
            ("1-0", EventEnvelope(
                event_id="trade:1",
                event_type="onchain.trade_fact",
                source="test",
                chain="solana",
                token="NEWCOIN",
                observed_at=datetime.now(UTC),
                ingested_at=datetime.now(UTC),
                payload={
                    "wallet_address": "wallet_a",
                    "direction": "inflow",
                    "notional_usd": 5000.0,
                    "trade_count": 1,
                },
            )),
        ]

    monkeypatch.setattr(smd, "read_models", _read_with_one_trade)
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repo = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()

    _setup_registry(repo, "solana", ["wallet_a", "wallet_b", "wallet_c"])

    detector = SmartMoneyDetector(settings, cast(Redis, client), repo)
    result = detector.check_token("solana", "NEWCOIN")

    assert result.smart_wallet_buyers == 1
    assert result.has_smart_money is True
    assert 0.35 <= result.confidence_score <= 0.50


def test_smart_money_detector_multiple_smart_wallets_boost_score(monkeypatch) -> None:
    """Multiple smart wallets buying → higher confidence score."""
    import discovery.smart_money_detector as smd

    def _read_with_three_trades(*a, **kw):
        from datetime import UTC, datetime
        return [
            ("1-0", EventEnvelope(event_id="t1", event_type="onchain.trade_fact", source="test",
                                  chain="solana", token="HOTCOIN",
                                  observed_at=datetime.now(UTC), ingested_at=datetime.now(UTC),
                                  payload={"wallet_address": "wallet_0", "direction": "inflow", "notional_usd": 20000.0})),
            ("2-0", EventEnvelope(event_id="t2", event_type="onchain.trade_fact", source="test",
                                  chain="solana", token="HOTCOIN",
                                  observed_at=datetime.now(UTC), ingested_at=datetime.now(UTC),
                                  payload={"wallet_address": "wallet_1", "direction": "inflow", "notional_usd": 20000.0})),
            ("3-0", EventEnvelope(event_id="t3", event_type="onchain.trade_fact", source="test",
                                  chain="solana", token="HOTCOIN",
                                  observed_at=datetime.now(UTC), ingested_at=datetime.now(UTC),
                                  payload={"wallet_address": "wallet_2", "direction": "inflow", "notional_usd": 20000.0})),
        ]

    monkeypatch.setattr(smd, "read_models", _read_with_three_trades)
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repo = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()

    wallets = [f"wallet_{i}" for i in range(5)]
    _setup_registry(repo, "solana", wallets)

    detector = SmartMoneyDetector(settings, cast(Redis, client), repo)
    result = detector.check_token("solana", "HOTCOIN")

    assert result.smart_wallet_buyers == 3
    assert result.confidence_score > 0.7


def test_smart_money_detector_wallet_outflow_is_ignored(monkeypatch) -> None:
    """Outflow from smart wallets should not count as buyers."""
    import discovery.smart_money_detector as smd

    def _read_with_outflow(*a, **kw):
        from datetime import UTC, datetime
        return [
            ("1-0", EventEnvelope(event_id="to1", event_type="onchain.trade_fact", source="test",
                                  chain="solana", token="SELLTOKEN",
                                  observed_at=datetime.now(UTC), ingested_at=datetime.now(UTC),
                                  payload={"wallet_address": "wallet_x", "direction": "outflow", "notional_usd": 10000.0})),
        ]

    monkeypatch.setattr(smd, "read_models", _read_with_outflow)
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repo = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()

    _setup_registry(repo, "solana", ["wallet_x"])

    detector = SmartMoneyDetector(settings, cast(Redis, client), repo)
    result = detector.check_token("solana", "SELLTOKEN")

    assert result.smart_wallet_buyers == 0
    assert result.has_smart_money is False
