from __future__ import annotations

from sqlalchemy import create_engine

from core.schemas import PortfolioSnapshot, PositionState, VenueType
from infra.postgres import (
    init_storage,
    load_portfolio_snapshot,
    load_position_state,
    save_portfolio_snapshot,
    save_position_state,
)


def test_position_and_portfolio_state_round_trip() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)

    save_position_state(
        engine,
        "BONK",
        PositionState(is_open=True, venue_type=VenueType.DEX, token_exposure=0.05),
    )
    save_portfolio_snapshot(
        engine,
        PortfolioSnapshot(total_portfolio_usd=20_000.0, token_exposure=0.05, open_positions=1),
    )

    position = load_position_state(engine, "BONK")
    portfolio = load_portfolio_snapshot(engine)

    assert position.is_open is True
    assert position.venue_type == VenueType.DEX
    assert portfolio.total_portfolio_usd == 20_000.0
    assert portfolio.open_positions == 1