from datetime import UTC, datetime

from core.schemas import EventEnvelope, TokenState
from core.signal_engine import SignalEngine
from sentinel.market_listener import build_market_event
from sentinel.onchain_listener import build_onchain_event
from sentinel.social_listener import build_social_event


def test_signal_engine_builds_deterministic_signal() -> None:
    engine = SignalEngine()
    observed_at = datetime.now(UTC)

    onchain_event = build_onchain_event(
        {
            "token": "BONK",
            "observed_at": observed_at,
            "liquidity_usd": 120_000,
            "volume_5m_usd": 45_000,
            "buy_pressure": 0.78,
            "holder_growth_15m": 0.22,
            "wallet_inflow_score": 0.62,
            "estimated_slippage_bps": 55,
        }
    )
    market_event = build_market_event(
        {
            "token": "BONK",
            "observed_at": observed_at,
            "cex_listing_confirmed": False,
            "cex_rumor_score": 0.15,
        }
    )
    social_event = build_social_event(
        {
            "token": "BONK",
            "observed_at": observed_at,
            "social_sentiment": 0.45,
            "social_velocity": 0.40,
        }
    )

    signal = engine.build_signal(onchain_event, market_event, social_event)

    assert signal.token == "BONK"
    assert signal.state_candidate == TokenState.NARRATIVE_EXPLOSION
    assert 0 <= signal.alpha_score <= 1
    assert signal.sub_scores["market_structure"] >= 0.74
    assert "wallet_behavior_positive" in signal.reasons


def test_signal_engine_marks_low_liquidity_as_pre_launch() -> None:
    engine = SignalEngine()
    onchain_event = build_onchain_event(
        {
            "token": "TEST",
            "observed_at": datetime.now(UTC),
            "liquidity_usd": 2_500,
            "volume_5m_usd": 100,
            "buy_pressure": 0.2,
        }
    )

    signal = engine.build_signal(onchain_event)

    assert signal.state_candidate == TokenState.PRE_LAUNCH
    assert "liquidity_below_threshold" in signal.reasons


def test_signal_engine_scores_onchain_feature_quality() -> None:
    engine = SignalEngine()
    onchain_event = build_onchain_event(
        {
            "token": "AERO",
            "chain": "base",
            "observed_at": datetime.now(UTC),
            "liquidity_usd": 700000,
            "volume_5m_usd": 150000,
            "buy_pressure": 0.81,
            "estimated_slippage_bps": 72,
            "feature_quality": {
                "buy_pressure": "ok",
                "estimated_slippage_bps": "ok",
            },
        }
    )

    signal = engine.build_signal(onchain_event)

    assert signal.features["onchain_feature_quality"] == 1.0
    assert "feature_quality_confirmed" in signal.reasons


def test_signal_engine_promotes_qualified_launch_candidate_to_early_liquidity() -> None:
    engine = SignalEngine()
    observed_at = datetime.now(UTC)
    launch_event = EventEnvelope(
        event_id="launch-1",
        event_type="alpha.launch_candidate",
        source="launch_alpha_backfill",
        chain="solana",
        token="NEWTKN",
        observed_at=observed_at,
        ingested_at=observed_at,
        payload={
            "launch_candidate_status": "QUALIFIED",
            "launch_alpha_score": 0.92,
            "liquidity_usd": 150_000.0,
            "volume_5m_usd": 45_000.0,
            "buy_pressure": 0.82,
            "wallet_inflow_score": 0.70,
            "holder_growth_15m": 0.65,
            "estimated_slippage_bps": 80.0,
            "feature_quality": {"launch_alpha": "ok"},
        },
    )

    signal = engine.build_signal(launch_event)

    assert signal.state_candidate == TokenState.EARLY_LIQUIDITY
    assert signal.alpha_score >= 0.7
    assert "launch_alpha_candidate_qualified" in signal.reasons


def test_signal_engine_promotes_qualified_flow_candidate_to_narrative() -> None:
    engine = SignalEngine()
    observed_at = datetime.now(UTC)
    flow_event = EventEnvelope(
        event_id="flow-1",
        event_type="alpha.flow_candidate",
        source="flow_alpha_backfill",
        chain="base",
        token="AERO",
        observed_at=observed_at,
        ingested_at=observed_at,
        payload={
            "flow_candidate_status": "QUALIFIED",
            "flow_alpha_score": 0.9,
            "wallet_inflow_score": 0.82,
            "wallet_outflow_score": 0.1,
            "volume_5m_usd": 48_000.0,
            "buy_pressure": 0.84,
            "holder_growth_15m": 0.7,
            "liquidity_usd": 80_000.0,
            "estimated_slippage_bps": 95.0,
            "feature_quality": {"flow_alpha": "ok"},
        },
    )

    signal = engine.build_signal(flow_event)

    assert signal.state_candidate == TokenState.NARRATIVE_EXPLOSION
    assert signal.alpha_score >= 0.7
    assert "flow_alpha_candidate_qualified" in signal.reasons


def test_signal_engine_marks_deteriorating_onchain_snapshot_as_distribution() -> None:
    engine = SignalEngine()
    onchain_event = build_onchain_event(
        {
            "token": "AERO",
            "chain": "base",
            "observed_at": datetime.now(UTC),
            "liquidity_usd": 42000,
            "volume_5m_usd": 12000,
            "buy_pressure": 0.18,
            "estimated_slippage_bps": 260,
            "feature_quality": {
                "buy_pressure": "stale",
                "estimated_slippage_bps": "ok",
            },
        }
    )

    signal = engine.build_signal(onchain_event)

    assert signal.state_candidate == TokenState.DISTRIBUTION
    assert "execution_conditions_deteriorating" in signal.reasons
    assert "feature_quality_degraded" in signal.reasons