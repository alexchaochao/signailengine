"""Capital allocation across concurrent alpha opportunities.

The allocator receives pipeline results (signals / intents) that passed risk
gating and decides how much capital to commit to each when multiple
opportunities overlap in time.

Current implementation uses a simple pro-rata split: when multiple intents
arrive in the same poll cycle the available headroom is divided proportional
to each candidate's alpha score.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.schemas import ExecutionIntent, TokenSignal


@dataclass(frozen=True)
class AllocatedIntent:
    intent: ExecutionIntent
    allocated_notional_usd: float
    allocation_reason: str


class Allocator:
    """Simple pro‑rata capital allocator.

    When multiple intents compete for the same pool of capital their requested
    notionals are scaled down proportionally to alpha score so that higher‑
    conviction opportunities receive a larger share.
    """

    def allocate(
        self,
        intents: list[ExecutionIntent],
        signals: list[TokenSignal],
        *,
        available_capital_usd: float,
    ) -> list[AllocatedIntent]:
        if not intents:
            return []
        if len(intents) == 1:
            return [
                AllocatedIntent(
                    intent=intents[0],
                    allocated_notional_usd=min(
                        intents[0].target_notional_usd, available_capital_usd
                    ),
                    allocation_reason="single_opportunity",
                )
            ]

        # Build score map
        score_map: dict[str, float] = {
            signal.token: signal.alpha_score for signal in signals
        }
        total_weight = sum(
            score_map.get(intent.token, 0.5) for intent in intents
        )
        if total_weight <= 0:
            total_weight = 1.0

        result: list[AllocatedIntent] = []
        remaining = available_capital_usd
        for intent in sorted(
            intents,
            key=lambda i: score_map.get(i.token, 0.0),
            reverse=True,
        ):
            weight = score_map.get(intent.token, 0.5) / total_weight
            share = min(intent.target_notional_usd, remaining * weight)
            result.append(
                AllocatedIntent(
                    intent=intent,
                    allocated_notional_usd=round(share, 2),
                    allocation_reason=(
                        "pro_rata_alpha_weighted"
                        if len(intents) > 1
                        else "single_opportunity"
                    ),
                )
            )
            remaining -= share
        return result