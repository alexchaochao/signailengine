from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from core.config import AppSettings
from core.schemas import (
    ActionType,
    ExecutionIntent,
    PortfolioSnapshot,
    PositionState,
    RiskDecision,
    TokenSignal,
)
from portfolio.balance_provider import BalanceSnapshot


@dataclass(frozen=True)
class RiskEvaluationContext:
    settings: AppSettings
    signal: TokenSignal
    intent: ExecutionIntent
    position: PositionState
    portfolio: PortfolioSnapshot
    balance_snapshot: BalanceSnapshot | None = None


class RiskEngine:
    def evaluate(
        self,
        settings: AppSettings,
        signal: TokenSignal,
        intent: ExecutionIntent,
        position: PositionState,
        portfolio: PortfolioSnapshot,
        balance_snapshot: BalanceSnapshot | None = None,
    ) -> RiskDecision:
        context = RiskEvaluationContext(
            settings,
            signal,
            intent,
            position,
            portfolio,
            balance_snapshot,
        )
        violations = _liquidity_policy(context)
        violations.extend(_exposure_policy(context))
        violations.extend(_operational_policy(context))
        sized_notional, warnings = _size_notional(context)
        allowed = not violations and sized_notional >= 0

        return RiskDecision(
            intent_id=intent.intent_id,
            allowed=allowed,
            adjusted_notional_usd=sized_notional if allowed else 0.0,
            violations=violations,
            warnings=warnings,
            timestamp=datetime.now(UTC),
        )


def _operational_policy(context: RiskEvaluationContext) -> list[str]:
    violations: list[str] = []

    if context.settings.live.rollout.global_kill_switch_enabled:
        violations.append("global_kill_switch_enabled")

    if (
        context.settings.runtime.environment == "live"
        and not context.settings.risk.live_trading_enabled
    ):
        violations.append("live_trading_disabled")

    if (
        context.settings.runtime.environment == "live"
        and context.settings.risk.live_trading_enabled
        and context.intent.venue_type.value == "DEX"
        and not _has_live_dex_credentials(context.settings)
    ):
        violations.append("live_dex_credentials_missing")

    if (
        context.settings.runtime.environment == "live"
        and context.settings.risk.live_trading_enabled
        and context.intent.venue_type.value == "CEX"
        and not _has_live_cex_credentials(context.settings)
    ):
        violations.append("live_cex_credentials_missing")

    if context.settings.live.rollout.enforce_position_preflight:
        if context.intent.action == ActionType.BUY and context.position.is_open:
            violations.append("position_preflight_open_position")
        if (
            context.intent.action in {ActionType.SELL, ActionType.EXIT}
            and not context.position.is_open
        ):
            violations.append("position_preflight_no_open_position")

    if (
        context.settings.runtime.environment == "live"
        and context.settings.risk.live_trading_enabled
        and context.settings.live.rollout.enforce_balance_preflight
    ):
        if context.balance_snapshot is None:
            violations.append("balance_provider_unavailable")
        elif (
            context.balance_snapshot.available_balance_usd
            <= context.settings.live.rollout.min_available_balance_usd
        ):
            violations.append("balance_below_live_buffer")

    if (
        context.portfolio.open_positions >= context.settings.risk.max_concurrent_positions
        and not context.position.is_open
    ):
        violations.append("max_positions_reached")

    if context.portfolio.daily_pnl_fraction <= -context.settings.risk.max_daily_loss:
        violations.append("daily_loss_limit_reached")

    return violations


def _liquidity_policy(context: RiskEvaluationContext) -> list[str]:
    if context.intent.action != ActionType.BUY:
        return []

    violations: list[str] = []
    liquidity_usd = float(context.signal.features.get("liquidity_usd", 0.0))
    volume_5m_usd = float(context.signal.features.get("volume_5m_usd", 0.0))
    slippage_bps = int(context.signal.features.get("estimated_slippage_bps", 0))

    if liquidity_usd < context.settings.risk.min_liquidity_usd:
        violations.append("liquidity_below_minimum")
    if volume_5m_usd < context.settings.risk.min_volume_5m_usd:
        violations.append("volume_below_minimum")
    if slippage_bps > context.settings.risk.max_slippage_bps:
        violations.append("slippage_above_limit")

    return violations


def _exposure_policy(context: RiskEvaluationContext) -> list[str]:
    if context.intent.action != ActionType.BUY:
        return []

    violations: list[str] = []

    if context.portfolio.token_exposure >= context.settings.risk.max_token_exposure:
        violations.append("token_exposure_limit_reached")
    if context.portfolio.chain_exposure >= context.settings.risk.max_chain_exposure:
        violations.append("chain_exposure_limit_reached")

    return violations


def _size_notional(context: RiskEvaluationContext) -> tuple[float, list[str]]:
    warnings: list[str] = []
    token_headroom = max(
        context.settings.risk.max_token_exposure - context.portfolio.token_exposure,
        0.0,
    )
    chain_headroom = max(
        context.settings.risk.max_chain_exposure - context.portfolio.chain_exposure,
        0.0,
    )
    portfolio_headroom = min(token_headroom, chain_headroom)
    sized_notional = min(
        context.intent.target_notional_usd,
        context.portfolio.total_portfolio_usd * portfolio_headroom,
    )

    if (
        context.settings.runtime.environment == "live"
        and context.settings.risk.live_trading_enabled
        and sized_notional > context.settings.live.rollout.capped_notional_usd
    ):
        sized_notional = context.settings.live.rollout.capped_notional_usd
        warnings.append("notional_capped_by_live_rollout")

    if (
        context.settings.runtime.environment == "live"
        and context.settings.risk.live_trading_enabled
        and context.settings.live.rollout.enforce_balance_preflight
    ):
        available_balance_usd = (
            context.balance_snapshot.available_balance_usd
            if context.balance_snapshot is not None
            else 0.0
        )
        max_balance_notional = max(
            available_balance_usd - context.settings.live.rollout.min_available_balance_usd,
            0.0,
        )
        if sized_notional > max_balance_notional:
            sized_notional = max_balance_notional
            warnings.append("notional_reduced_by_balance_buffer")

    if sized_notional < context.intent.target_notional_usd:
        warnings.append("notional_reduced_by_headroom")

    return sized_notional, warnings


def _has_live_dex_credentials(settings: AppSettings) -> bool:
    credentials = settings.live.credentials
    solana_wallet = credentials.chain_wallets.get("solana")
    okx_provider = credentials.dex_providers.get("okx")
    return bool(
        (
            _credential_field(solana_wallet, "private_key")
            and _credential_field(solana_wallet, "wallet_address")
        )
        or (
            _credential_field(okx_provider, "api_key")
            and _credential_field(okx_provider, "secret_key")
            and _credential_field(okx_provider, "api_passphrase")
            and _credential_field(okx_provider, "project_id")
        )
    )


def _has_live_cex_credentials(settings: AppSettings) -> bool:
    binance_provider = settings.live.credentials.cex_providers.get("binance")
    return bool(
        _credential_field(binance_provider, "api_key")
        and _credential_field(binance_provider, "api_secret")
    )


def _credential_field(payload: object, field_name: str) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value in {None, ""}:
        return None
    return str(value)
