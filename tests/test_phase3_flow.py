from datetime import UTC, datetime

from core.config import AppSettings
from core.router import Router
from core.schemas import (
    PortfolioSnapshot,
    PositionState,
    TokenState,
    VenueStatus,
    VenueType,
)
from core.signal_engine import SignalEngine
from core.state_engine import StateEngine
from portfolio.risk_engine import RiskEngine
from sentinel.market_listener import build_market_event
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


def test_state_router_risk_flow_approves_dex_entry() -> None:
    settings = AppSettings.load()
    signal_engine = SignalEngine()
    state_engine = StateEngine()
    router = Router()
    risk_engine = RiskEngine()
    observed_at = datetime.now(UTC)

    signal = signal_engine.build_signal(
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
        build_market_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "cex_listing_confirmed": False,
            }
        ),
    )

    transition = state_engine.transition(
        TokenState.UNKNOWN,
        signal,
        seconds_since_last_transition=120,
    )
    route = router.route(signal, transition, PositionState(), VenueStatus())
    assert route.intent is not None

    decision = risk_engine.evaluate(
        settings,
        signal,
        route.intent,
        PositionState(),
        PortfolioSnapshot(total_portfolio_usd=10_000, token_exposure=0.02, chain_exposure=0.10),
    )

    assert transition.new_state == TokenState.NARRATIVE_EXPLOSION
    assert route.route == "DEX_ENTRY"
    assert route.intent.venue_type == VenueType.DEX
    assert route.intent.venue == "solana_primary"
    assert decision.allowed is True
    assert decision.adjusted_notional_usd > 0


def test_router_routes_base_dex_intent_to_evm_primary() -> None:
    signal_engine = SignalEngine()
    state_engine = StateEngine()
    router = Router()
    observed_at = datetime.now(UTC)

    signal = signal_engine.build_signal(
        build_onchain_event(
            {
                "chain": "base",
                "token": "AERO",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "chain": "base",
                "token": "AERO",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    )

    transition = state_engine.transition(
        TokenState.UNKNOWN,
        signal,
        seconds_since_last_transition=120,
    )
    route = router.route(signal, transition, PositionState(), VenueStatus())

    assert route.intent is not None
    assert route.intent.chain == "base"
    assert route.intent.venue_type == VenueType.DEX
    assert route.intent.venue == "evm_primary"


def test_risk_engine_rejects_low_liquidity_signal() -> None:
    settings = AppSettings.load()
    signal_engine = SignalEngine()
    state_engine = StateEngine()
    router = Router()
    risk_engine = RiskEngine()
    observed_at = datetime.now(UTC)

    signal = signal_engine.build_signal(
        build_onchain_event(
            {
                "token": "TEST",
                "observed_at": observed_at,
                "liquidity_usd": 50_000,
                "volume_5m_usd": 30_000,
                "buy_pressure": 0.90,
                "estimated_slippage_bps": 120,
            }
        ),
        build_wallet_event(
            {
                "token": "TEST",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.60,
            }
        ),
    )

    transition = state_engine.transition(
        TokenState.UNKNOWN,
        signal,
        seconds_since_last_transition=120,
    )
    route = router.route(signal, transition, PositionState(), VenueStatus())
    assert route.intent is not None

    decision = risk_engine.evaluate(
        settings,
        signal,
        route.intent,
        PositionState(),
        PortfolioSnapshot(total_portfolio_usd=10_000),
    )

    assert decision.allowed is False
    assert "liquidity_below_minimum" in decision.violations


def test_risk_engine_rejects_zero_notional_buy() -> None:
    settings = AppSettings.load()
    signal_engine = SignalEngine()
    state_engine = StateEngine()
    router = Router()
    risk_engine = RiskEngine()
    observed_at = datetime.now(UTC)

    signal = signal_engine.build_signal(
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    )

    transition = state_engine.transition(
        TokenState.UNKNOWN,
        signal,
        seconds_since_last_transition=120,
    )
    route = router.route(signal, transition, PositionState(), VenueStatus())
    assert route.intent is not None

    decision = risk_engine.evaluate(
        settings,
        signal,
        route.intent,
        PositionState(),
        PortfolioSnapshot(total_portfolio_usd=0.0, token_exposure=0.0, chain_exposure=0.0),
    )

    assert decision.allowed is False
    assert decision.adjusted_notional_usd == 0.0
    assert "notional_below_minimum" in decision.violations


def test_risk_engine_rejects_buy_during_cooldown() -> None:
    settings = AppSettings.load()
    signal_engine = SignalEngine()
    state_engine = StateEngine()
    router = Router()
    risk_engine = RiskEngine()
    observed_at = datetime.now(UTC)

    signal = signal_engine.build_signal(
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    )

    transition = state_engine.transition(
        TokenState.UNKNOWN,
        signal,
        seconds_since_last_transition=120,
    )
    route = router.route(signal, transition, PositionState(), VenueStatus())
    assert route.intent is not None

    just_exited = PositionState(
        is_open=False,
        venue_type=VenueType.NO_TRADE,
        token_exposure=0.0,
        last_exit_timestamp=int(datetime.now(UTC).timestamp()) - 60,
    )
    decision = risk_engine.evaluate(
        settings,
        signal,
        route.intent,
        just_exited,
        PortfolioSnapshot(total_portfolio_usd=10_000, token_exposure=0.0, chain_exposure=0.0),
    )

    assert decision.allowed is False
    assert "cooldown_active" in decision.violations