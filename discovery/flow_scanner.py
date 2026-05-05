from __future__ import annotations

from discovery.schemas import (
    AlphaCandidate,
    AlphaCandidateStatus,
    AlphaType,
    FlowActivitySnapshot,
)


class FlowAlphaScanner:
    def evaluate(self, snapshot: FlowActivitySnapshot) -> AlphaCandidate:
        pool_key = _flow_pool_key(snapshot)
        netflow_score = min(snapshot.netflow_15m_usd / 150_000.0, 1.0)
        smart_money_total = snapshot.smart_money_inflow_usd + snapshot.smart_money_outflow_usd
        smart_money_dominance = (
            snapshot.smart_money_inflow_usd / smart_money_total if smart_money_total > 0 else 0.0
        )
        participation_score = min(
            (
                snapshot.unique_buyer_wallets_15m
                + snapshot.whale_buy_count_15m
                + max(snapshot.unique_buyer_wallets_15m - snapshot.unique_seller_wallets_15m, 0)
            )
            / 24.0,
            1.0,
        )
        exchange_support = min(snapshot.exchange_outflow_usd / 100_000.0, 1.0)
        score = round(
            min(
                netflow_score * 0.45
                + smart_money_dominance * 0.25
                + participation_score * 0.15
                + exchange_support * 0.15,
                1.0,
            ),
            4,
        )
        status = (
            AlphaCandidateStatus.QUALIFIED
            if score >= 0.72
            and snapshot.netflow_15m_usd >= 50_000
            and snapshot.smart_money_inflow_usd > snapshot.smart_money_outflow_usd
            else AlphaCandidateStatus.OBSERVED
        )
        reasons = [
            f"flow_type:{snapshot.flow_type}",
            "netflow_strong" if snapshot.netflow_15m_usd >= 75_000 else "netflow_building",
            (
                "smart_money_dominant"
                if smart_money_dominance >= 0.65
                else "smart_money_mixed"
            ),
            (
                "exchange_outflow_supportive"
                if snapshot.exchange_outflow_usd >= 50_000
                else "exchange_outflow_light"
            ),
        ]
        return AlphaCandidate(
            candidate_id=pool_key,
            alpha_type=AlphaType.FLOW,
            chain=snapshot.chain,
            token=snapshot.token,
            pool_address=pool_key,
            dex=snapshot.venue or "flow",
            quote_asset="USD",
            status=status,
            score=score,
            first_seen_at=snapshot.observed_at,
            last_seen_at=snapshot.observed_at,
            initial_liquidity_usd=0.0,
            buy_notional_5m_usd=snapshot.smart_money_inflow_usd,
            trade_count_5m=(
                snapshot.unique_buyer_wallets_15m + snapshot.unique_seller_wallets_15m
            ),
            unique_wallets_5m=snapshot.unique_buyer_wallets_15m,
            smart_money_wallets_5m=snapshot.whale_buy_count_15m,
            reasons=reasons,
            metadata={
                "flow_type": snapshot.flow_type,
                "netflow_15m_usd": snapshot.netflow_15m_usd,
                "smart_money_inflow_usd": snapshot.smart_money_inflow_usd,
                "smart_money_outflow_usd": snapshot.smart_money_outflow_usd,
                "exchange_outflow_usd": snapshot.exchange_outflow_usd,
                **snapshot.metadata,
            },
        )


def _flow_pool_key(snapshot: FlowActivitySnapshot) -> str:
    venue_key = (snapshot.venue or snapshot.flow_type).strip() or "flow"
    return f"flow:{snapshot.chain}:{snapshot.token}:{venue_key}"