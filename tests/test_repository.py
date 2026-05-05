from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine

from core.pipeline import PipelineResult
from core.router import RouteDecision
from core.schemas import (
    ActionType,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    PortfolioSnapshot,
    PositionState,
    ReconciliationResult,
    RiskDecision,
    StateTransition,
    TokenSignal,
    TokenState,
    VenueType,
)
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.wallet_refresh_job import RefreshedWalletState
from sentinel.wallet_score_aggregator import WalletTokenFlow
from discovery.schemas import (
    AlphaCandidate,
    AlphaCandidateEvent,
    AlphaCandidateStatus,
    AlphaSnapshot,
    AlphaType,
)


def test_storage_repository_persists_audit_order_and_state_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    timestamp = datetime.now(UTC)
    intent = ExecutionIntent(
        intent_id="intent-1",
        token="BONK",
        chain="solana",
        venue_type=VenueType.DEX,
        venue="solana_primary",
        action=ActionType.BUY,
        confidence=0.8,
        target_notional_usd=250.0,
        max_slippage_bps=100,
        state=TokenState.NARRATIVE_EXPLOSION,
        strategy="paper_test",
    )
    result = PipelineResult(
        signal=TokenSignal(
            token="BONK",
            chain="solana",
            state_candidate=TokenState.NARRATIVE_EXPLOSION,
            sub_scores={"market_structure": 0.8},
            alpha_score=0.8,
            timestamp=int(timestamp.timestamp()),
        ),
        transition=StateTransition(
            previous_state=TokenState.UNKNOWN,
            new_state=TokenState.NARRATIVE_EXPLOSION,
            changed=True,
            reasons=["market_structure_strong"],
            timestamp=int(timestamp.timestamp()),
        ),
        route=RouteDecision(route="DEX_ENTRY", reasons=["dex_entry_conditions_met"], intent=intent),
        risk=RiskDecision(
            intent_id="intent-1",
            allowed=True,
            adjusted_notional_usd=250.0,
            timestamp=timestamp,
        ),
        execution=ExecutionReport(
            intent_id="intent-1",
            venue_type=VenueType.DEX,
            venue="solana_primary",
            status="FILLED",
            executed_notional_usd=250.0,
            message="paper_dex_execution",
            timestamp=timestamp,
        ),
        reconciliation=ReconciliationResult(
            intent_id="intent-1",
            position=PositionState(is_open=True, venue_type=VenueType.DEX, token_exposure=0.025),
            portfolio=PortfolioSnapshot(total_portfolio_usd=10_000.0, token_exposure=0.025),
            applied=True,
            reasons=["execution_reconciled"],
            timestamp=timestamp,
        ),
        execution_ledger=[
            ExecutionLedgerEntry(
                intent_id="intent-1",
                token="BONK",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="SUBMISSION",
                status="SUBMITTED",
                notional_usd=250.0,
                message="intent_created",
                timestamp=timestamp,
            )
        ],
    )

    repository.persist_pipeline_result(result)

    assert count_rows(engine, "token_signals") == 1
    assert count_rows(engine, "orders") == 1
    assert count_rows(engine, "positions") == 1
    assert count_rows(engine, "portfolio_state") == 1
    assert count_rows(engine, "execution_ledger") == 1


def test_storage_repository_loads_recoverable_intents_from_submission_ledger() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    timestamp = datetime.now(UTC)
    intent = ExecutionIntent(
        intent_id="intent-2",
        token="WIF",
        chain="solana",
        venue_type=VenueType.DEX,
        venue="solana_primary",
        action=ActionType.BUY,
        confidence=0.75,
        target_notional_usd=100.0,
        max_slippage_bps=120,
        state=TokenState.EARLY_LIQUIDITY,
        strategy="paper_test",
    )

    repository.orders.upsert_order(intent, 100.0, "SUBMITTED")
    repository.audit.append_execution_ledger(
        [
            ExecutionLedgerEntry(
                intent_id="intent-2",
                token="WIF",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="SUBMISSION",
                status="SUBMITTED",
                notional_usd=100.0,
                message="intent_created",
                timestamp=timestamp,
            )
        ]
    )

    recoverable = repository.load_recoverable_intents()

    assert len(recoverable) == 1
    assert recoverable[0].intent.intent_id == "intent-2"
    assert recoverable[0].status == "SUBMITTED"


def test_storage_repository_persists_wallet_intelligence_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    timestamp = datetime.now(UTC)

    repository.wallet_intelligence.upsert_registry_entries(
        [
            TrackedWalletRegistryEntry(
                wallet_address="wallet-1",
                chain="solana",
                wallet_class="smart_money",
                weight=0.8,
                status="active",
                source="okx_leaderboard",
                source_metadata={"sort_by": "1"},
                version="registry-v1",
                discovered_at=timestamp,
                last_seen_at=timestamp,
                updated_at=timestamp,
            )
        ]
    )
    repository.wallet_intelligence.save_refresh_states(
        [
            RefreshedWalletState(
                wallet_address="wallet-1",
                chain="solana",
                refreshed_at=timestamp,
                total_value_usd=1200.0,
                realized_pnl_usd=400.0,
                win_rate=55.0,
                recent_tx_count=2,
                last_active_at=timestamp,
                source_data={"portfolio": True},
            )
        ]
    )
    repository.wallet_intelligence.append_wallet_flows(
        [
            WalletTokenFlow(
                chain="solana",
                token="BONK",
                wallet_address="wallet-1",
                direction="inflow",
                notional_usd=500.0,
                observed_at=timestamp,
                trade_count=1,
                flow_id="flow-1",
            )
        ]
    )

    loaded_registry = repository.wallet_intelligence.list_active_registry_entries("solana")
    loaded_flows = repository.wallet_intelligence.load_wallet_flows("solana", "BONK")

    assert count_rows(engine, "tracked_wallet_registry") == 1
    assert count_rows(engine, "tracked_wallet_refresh_state") == 1
    assert count_rows(engine, "wallet_token_flows") == 1
    assert loaded_registry[0].wallet_address == "wallet-1"
    assert loaded_flows[0].flow_id == "flow-1"


def test_storage_repository_persists_launch_alpha_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    timestamp = datetime.now(UTC)

    repository.discovery.upsert_candidate(
        AlphaCandidate(
            candidate_id="solana:pool-1",
            alpha_type=AlphaType.LAUNCH,
            chain="solana",
            token="NEWTKN",
            pool_address="pool-1",
            dex="raydium",
            quote_asset="USDC",
            status=AlphaCandidateStatus.QUALIFIED,
            score=0.94,
            first_seen_at=timestamp,
            last_seen_at=timestamp,
            initial_liquidity_usd=25000.0,
            liquidity_lock_ratio=0.92,
            buy_notional_5m_usd=18000.0,
            trade_count_5m=16,
            unique_wallets_5m=11,
            smart_money_wallets_5m=3,
            creator_hold_pct=0.08,
            reasons=["initial_liquidity_ready", "buy_notional_ready"],
            metadata={"launchpad": "pumpfun"},
        )
    )
    repository.discovery.append_event(
        AlphaCandidateEvent(
            event_id="launch-1",
            candidate_id="solana:pool-1",
            event_type="launch_candidate_qualified",
            observed_at=timestamp,
            payload={"score": 0.94},
        )
    )
    repository.discovery.save_snapshot(
        AlphaSnapshot(
            snapshot_id="launch-1",
            candidate_id="solana:pool-1",
            alpha_type=AlphaType.LAUNCH,
            chain="solana",
            token="NEWTKN",
            observed_at=timestamp,
            status=AlphaCandidateStatus.QUALIFIED,
            score=0.94,
            payload={"initial_liquidity_usd": 25000.0},
        )
    )

    loaded = repository.discovery.load_candidate("solana:pool-1")

    assert count_rows(engine, "alpha_candidates") == 1
    assert count_rows(engine, "alpha_candidate_events") == 1
    assert count_rows(engine, "alpha_snapshots") == 1
    assert loaded is not None
    assert loaded.alpha_type == AlphaType.LAUNCH
    assert loaded.status == AlphaCandidateStatus.QUALIFIED