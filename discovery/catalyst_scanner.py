from __future__ import annotations

from discovery.schemas import (
    AlphaCandidate,
    AlphaCandidateStatus,
    AlphaType,
    CatalystEventSnapshot,
)


class CatalystAlphaScanner:
    def evaluate(self, snapshot: CatalystEventSnapshot) -> AlphaCandidate:
        timeliness = round(max(0.0, 1.0 - min(snapshot.lead_time_minutes / 180.0, 1.0)), 4)
        score = round(
            min(
                snapshot.impact_score * 0.5
                + snapshot.credibility_score * 0.35
                + timeliness * 0.15,
                1.0,
            ),
            4,
        )
        status = (
            AlphaCandidateStatus.QUALIFIED
            if score >= 0.72 and snapshot.credibility_score >= 0.6
            else AlphaCandidateStatus.OBSERVED
        )
        reasons = [
            f"catalyst_type:{snapshot.catalyst_type}",
            "catalyst_impact_strong" if snapshot.impact_score >= 0.7 else "catalyst_impact_moderate",
            (
                "catalyst_credibility_confirmed"
                if snapshot.credibility_score >= 0.7
                else "catalyst_credibility_watch"
            ),
        ]
        return AlphaCandidate(
            candidate_id=f"catalyst:{snapshot.chain}:{snapshot.token}:{snapshot.source_event_id}",
            alpha_type=AlphaType.CATALYST,
            chain=snapshot.chain,
            token=snapshot.token,
            pool_address=snapshot.venue or f"catalyst:{snapshot.catalyst_type}",
            dex=snapshot.venue or "catalyst",
            quote_asset="USD",
            status=status,
            score=score,
            first_seen_at=snapshot.observed_at,
            last_seen_at=snapshot.observed_at,
            initial_liquidity_usd=0.0,
            buy_notional_5m_usd=0.0,
            trade_count_5m=0,
            unique_wallets_5m=0,
            smart_money_wallets_5m=0,
            reasons=reasons,
            metadata={
                "catalyst_type": snapshot.catalyst_type,
                "headline": snapshot.headline,
                "impact_score": snapshot.impact_score,
                "credibility_score": snapshot.credibility_score,
                "lead_time_minutes": snapshot.lead_time_minutes,
                **snapshot.metadata,
            },
        )