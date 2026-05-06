"""Portfolio exposure tracking.

Reads position and portfolio state from the repository and computes current
exposure ratios per token and per chain.  Used by the risk engine and the
allocator to enforce capital limits.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.schemas import PortfolioSnapshot, PositionState
from infra.repository import StorageRepository


@dataclass(frozen=True)
class ExposureReport:
    total_portfolio_usd: float
    token_exposure_pct: float
    chain_exposure_pct: float
    open_positions: int
    daily_pnl_fraction: float
    token_headroom_usd: float
    chain_headroom_usd: float
    position_headroom: int


class ExposureTracker:
    """Tracks portfolio exposure across tokens and chains.

    Reads live state from the repository on each call so it reflects the
    latest reconciled positions.
    """

    def __init__(
        self,
        repository: StorageRepository,
        *,
        max_token_exposure: float = 0.10,
        max_chain_exposure: float = 0.40,
        max_positions: int = 5,
    ) -> None:
        self.repository = repository
        self.max_token_exposure = max_token_exposure
        self.max_chain_exposure = max_chain_exposure
        self.max_positions = max_positions

    def compute(self) -> ExposureReport:
        portfolio: PortfolioSnapshot = self.repository.state.load_portfolio()
        return ExposureReport(
            total_portfolio_usd=portfolio.total_portfolio_usd,
            token_exposure_pct=portfolio.token_exposure,
            chain_exposure_pct=portfolio.chain_exposure,
            open_positions=portfolio.open_positions,
            daily_pnl_fraction=portfolio.daily_pnl_fraction,
            token_headroom_usd=max(
                self.max_token_exposure - portfolio.token_exposure, 0.0
            ) * portfolio.total_portfolio_usd,
            chain_headroom_usd=max(
                self.max_chain_exposure - portfolio.chain_exposure, 0.0
            ) * portfolio.total_portfolio_usd,
            position_headroom=max(self.max_positions - portfolio.open_positions, 0),
        )

    def can_open_new_position(self) -> tuple[bool, str]:
        report = self.compute()
        if report.open_positions >= self.max_positions:
            return False, "max_positions_reached"
        if report.token_exposure_pct >= self.max_token_exposure:
            return False, "token_exposure_limit_reached"
        if report.chain_exposure_pct >= self.max_chain_exposure:
            return False, "chain_exposure_limit_reached"
        return True, "ok"