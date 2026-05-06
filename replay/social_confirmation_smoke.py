from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from core.schemas import EventEnvelope, SocialQueryRequest
from discovery.service import SocialConfirmationSyncService
from infra.postgres import init_storage
from infra.repository import StorageRepository
from sentinel.onchain_listener import build_onchain_event
from sentinel.social_live_sources import (
    build_social_analysis_event,
    build_social_confirmation_source,
)
from sentinel.social_llm import build_social_llm_analyzer
from sentinel.wallet_tracker import build_wallet_event


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: set[tuple[str, str]] = set()
        self.acked: list[tuple[str, str, str]] = []
        self.counter = 0

    def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream_name, []).append((message_id, mapping))
        return message_id

    def xgroup_create(
        self,
        stream_name: str,
        group_name: str,
        id: str = "0-0",
        mkstream: bool = False,
    ) -> bool:
        _ = id
        if mkstream:
            self.streams.setdefault(stream_name, [])
        self.groups.add((stream_name, group_name))
        return True

    def xrange(self, stream_name: str, min: str = "0-0", count: int = 100):
        _ = min
        return self.streams.get(stream_name, [])[:count]


def _social_query_from_event(event: EventEnvelope) -> SocialQueryRequest:
    return SocialQueryRequest(
        request_id=str(event.payload["request_id"]),
        source_name=event.source,
        platform=str(event.payload.get("platform") or "x"),
        chain=event.chain,
        token=event.token,
        query=str(event.payload.get("query") or event.token),
        mode=str(event.payload.get("mode") or "confirmation"),
        requested_at=event.observed_at,
        candidate_id=str(event.payload.get("candidate_id") or f"social:{event.chain}:{event.token}"),
        fsm_context=event.payload.get("fsm_context"),
        metadata=dict(event.payload.get("metadata", {})),
    )


def _fake_x_transport(url: str, headers: dict[str, str], timeout_seconds: float):
    _ = headers, timeout_seconds
    now = datetime.now(UTC)
    return {
        "records": [
            {
                "id": "x-1",
                "author_handle": "alpha_one",
                "created_at": (now).isoformat(),
                "like_count": 220,
                "repost_count": 35,
                "reply_count": 18,
                "quote_count": 12,
                "follower_count": 50000,
                "text": "BONK listing rumor is accelerating and smart money is watching.",
                "url": "https://x.com/alpha_one/status/x-1",
            },
            {
                "id": "x-2",
                "author_handle": "alpha_two",
                "created_at": (now).isoformat(),
                "like_count": 110,
                "repost_count": 20,
                "reply_count": 9,
                "quote_count": 6,
                "follower_count": 12000,
                "text": "BONK momentum and community narrative are strengthening quickly.",
                "url": "https://x.com/alpha_two/status/x-2",
            },
        ],
        "request_url": url,
    }


def main() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "llm": {
                "enabled": False,
                "provider": "heuristic",
                "model": "gpt-5.4",
            },
            "acquisition": {
                "social_sources": {
                    "x_bonk": {
                        "enabled": True,
                        "platform": "x",
                        "provider": "x_snapshot_json",
                        "source_name": "x_bonk_watch",
                        "token": "BONK",
                        "chain": "solana",
                        "query_template": "${cashtag} OR ${token}",
                        "source_url": "https://social-bridge.example/x/search.json",
                        "min_mentions": 2,
                        "min_unique_authors": 2,
                    }
                }
            },
        }
    )
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")
    sync_service = SocialConfirmationSyncService(settings, cast(Redis, client), repository)
    analyzer = build_social_llm_analyzer(settings)

    initial_result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": datetime.now(UTC),
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": datetime.now(UTC),
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    request_events = [
        EventEnvelope.model_validate_json(message[1]["payload"])
        for message in client.streams[settings.redis.raw_events_stream]
        if message[1]["kind"] == "social.query_requested"
    ]

    analysis_payloads: list[dict[str, object]] = []
    catalyst_events: list[EventEnvelope] = []
    for request_event in request_events:
        social_query = _social_query_from_event(request_event)
        source = build_social_confirmation_source(settings, social_query, transport=_fake_x_transport)
        social_event = source.fetch_events()[0]
        llm_analysis = analyzer.analyze(social_query, social_event)
        analysis_event = build_social_analysis_event(
            social_query,
            source_name=source.config.source_name or source.config.platform,
            social_event=social_event,
            llm_analysis=llm_analysis,
        )
        sync_result = sync_service.ingest_analysis_event(
            analysis_event,
            source_name=source.config.source_name or source.config.platform,
        )
        analysis_payloads.append(
            {
                "request_id": social_query.request_id,
                "confirmation_score": analysis_event.payload["confirmation_score"],
                "llm_summary": analysis_event.payload["llm_summary"],
                "llm_relevance_score": analysis_event.payload["llm_relevance_score"],
                "candidate_status": sync_result.status.value,
                "candidate_score": sync_result.score,
            }
        )

    catalyst_events = [
        EventEnvelope.model_validate_json(message[1]["payload"])
        for message in client.streams[settings.redis.raw_events_stream]
        if message[1]["kind"] == "alpha.catalyst_candidate"
    ]
    feedback_result = worker.process_events([catalyst_events[-1]]) if catalyst_events else None

    summary = {
        "initial_transition": {
            "state": initial_result.transition.new_state.value,
            "alpha_score": initial_result.signal.alpha_score,
            "route": initial_result.route.route,
        },
        "social_requests": len(request_events),
        "social_analysis": analysis_payloads,
        "feedback_transition": (
            {
                "state": feedback_result.transition.new_state.value,
                "alpha_score": feedback_result.signal.alpha_score,
                "route": feedback_result.route.route,
                "reasons": feedback_result.signal.reasons,
            }
            if feedback_result is not None
            else None
        ),
        "raw_event_kinds": [message[1]["kind"] for message in client.streams[settings.redis.raw_events_stream]],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
