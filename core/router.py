from __future__ import annotations

from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field

from core.schemas import (
    ActionType,
    ExecutionIntent,
    PositionState,
    StateTransition,
    TokenSignal,
    TokenState,
    VenueStatus,
    VenueType,
)


class RouteDecision(BaseModel):
    route: str
    reasons: list[str] = Field(default_factory=list)
    intent: ExecutionIntent | None = None


class Router:
    def route(
        self,
        signal: TokenSignal,
        transition: StateTransition,
        position: PositionState,
        venue_status: VenueStatus,
    ) -> RouteDecision:
        state = transition.new_state

        if venue_status.degraded:
            return RouteDecision(route="REJECT", reasons=["venue_degraded"])

        if state == TokenState.DISTRIBUTION and position.is_open:
            route = "DEX_EXIT" if position.venue_type == VenueType.DEX else "CEX_EXIT"
            return RouteDecision(
                route=route,
                reasons=["distribution_exit"],
                intent=_build_intent(signal, state, position.venue_type, ActionType.EXIT, 0.0),
            )

        if signal.alpha_score < 0.55:
            return RouteDecision(route="REJECT", reasons=["alpha_below_threshold"])

        if state in {
            TokenState.PRE_LAUNCH,
            TokenState.EARLY_LIQUIDITY,
            TokenState.NARRATIVE_EXPLOSION,
        }:
            if not venue_status.dex_ready:
                return RouteDecision(route="HOLD", reasons=["dex_not_ready"])

            return RouteDecision(
                route="DEX_ENTRY",
                reasons=["dex_entry_conditions_met"],
                intent=_build_intent(
                    signal,
                    state,
                    VenueType.DEX,
                    ActionType.BUY,
                    _suggest_notional(signal.alpha_score),
                ),
            )

        if state == TokenState.CEX_LISTING:
            if not venue_status.cex_ready:
                return RouteDecision(route="HOLD", reasons=["cex_not_ready"])

            return RouteDecision(
                route="CEX_ENTRY",
                reasons=["cex_entry_conditions_met"],
                intent=_build_intent(
                    signal,
                    state,
                    VenueType.CEX,
                    ActionType.BUY,
                    _suggest_notional(signal.alpha_score),
                ),
            )

        return RouteDecision(route="HOLD", reasons=["no_route"])


def _build_intent(
    signal: TokenSignal,
    state: TokenState,
    venue_type: VenueType,
    action: ActionType,
    target_notional_usd: float,
) -> ExecutionIntent:
    venue = _select_venue(signal.chain, venue_type)
    strategy = "dex_momentum_v1" if venue_type == VenueType.DEX else "cex_listing_v1"
    deterministic_key = ":".join(
        [
            signal.chain,
            signal.token,
            str(signal.timestamp),
            state.value,
            venue_type.value,
            action.value,
            venue,
            strategy,
        ]
    )
    return ExecutionIntent(
        intent_id=str(uuid5(NAMESPACE_DNS, deterministic_key)),
        token=signal.token,
        chain=signal.chain,
        venue_type=venue_type,
        venue=venue,
        action=action,
        confidence=signal.alpha_score,
        target_notional_usd=target_notional_usd,
        max_slippage_bps=int(signal.features.get("estimated_slippage_bps", 150)),
        state=state,
        strategy=strategy,
        reasons=list(signal.reasons),
    )


def _select_venue(chain: str, venue_type: VenueType) -> str:
    if venue_type == VenueType.CEX:
        return "binance_paper"
    if chain == "solana":
        return "solana_primary"
    return "evm_primary"


def _suggest_notional(alpha_score: float) -> float:
    return round(max(alpha_score - 0.5, 0.0) * 5000, 2)
