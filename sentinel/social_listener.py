from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.schemas import EventEnvelope


def build_social_event(payload: dict[str, Any], source: str = "social_listener") -> EventEnvelope:
	token = str(payload["token"])
	chain = str(payload.get("chain", "solana"))
	observed_at = _coerce_datetime(payload.get("observed_at"))

	normalized_payload = {
		"social_sentiment": _bounded(payload.get("social_sentiment", 0.0)),
		"social_velocity": _bounded(payload.get("social_velocity", 0.0)),
		"source_platform": str(payload.get("source_platform", "generic")),
		"message_count": int(payload.get("message_count", 0)),
		"unique_authors": int(payload.get("unique_authors", 0)),
		"engagement_score": _bounded(payload.get("engagement_score", 0.0)),
		"credibility_score": _bounded(payload.get("credibility_score", 0.0)),
	}

	if "query" in payload:
		normalized_payload["query"] = str(payload["query"])
	if "retrieval_mode" in payload:
		normalized_payload["retrieval_mode"] = str(payload["retrieval_mode"])
	if "evidence_texts" in payload and isinstance(payload["evidence_texts"], list):
		normalized_payload["evidence_texts"] = [str(item) for item in payload["evidence_texts"][:6]]

	if "post_id" in payload:
		normalized_payload["post_id"] = str(payload["post_id"])
	if "url" in payload:
		normalized_payload["url"] = str(payload["url"])

	return EventEnvelope(
		event_id=str(payload.get("event_id", uuid4())),
		event_type="social.signal_snapshot",
		source=source,
		chain=chain,
		token=token,
		observed_at=observed_at,
		ingested_at=datetime.now(UTC),
		payload=normalized_payload,
	)


def build_x_event(payload: dict[str, Any], source: str = "x_listener") -> EventEnvelope:
	mention_count = int(payload.get("mention_count", payload.get("post_count", 0)))
	unique_authors = int(payload.get("unique_authors", payload.get("author_count", 0)))
	engagement_score = _bounded(payload.get("engagement_score", payload.get("viral_score", 0.0)))
	credibility_score = _bounded(
		payload.get("credibility_score", payload.get("influencer_ratio", 0.0))
	)
	social_velocity = _bounded(max(payload.get("social_velocity", 0.0), mention_count / 50))
	social_sentiment = _bounded(
		payload.get(
			"social_sentiment",
			payload.get("sentiment_score", engagement_score * 0.6 + credibility_score * 0.4),
		)
	)

	return build_social_event(
		{
			**payload,
			"source_platform": "x",
			"social_sentiment": social_sentiment,
			"social_velocity": social_velocity,
			"message_count": mention_count,
			"unique_authors": unique_authors,
			"engagement_score": engagement_score,
			"credibility_score": credibility_score,
		},
		source=source,
	)


def build_reddit_event(payload: dict[str, Any], source: str = "reddit_listener") -> EventEnvelope:
	mention_count = int(payload.get("mention_count", payload.get("thread_count", 0)))
	unique_authors = int(payload.get("unique_authors", payload.get("author_count", 0)))
	comment_velocity = _bounded(payload.get("comment_velocity", 0.0))
	upvote_ratio = _bounded(payload.get("upvote_ratio", 0.0))
	engagement_score = _bounded(
		payload.get("engagement_score", comment_velocity * 0.55 + upvote_ratio * 0.45)
	)
	credibility_score = _bounded(payload.get("credibility_score", payload.get("subreddit_quality", 0.0)))
	social_velocity = _bounded(max(payload.get("social_velocity", 0.0), mention_count / 30))
	social_sentiment = _bounded(
		payload.get(
			"social_sentiment",
			payload.get("sentiment_score", upvote_ratio * 0.5 + credibility_score * 0.5),
		)
	)

	return build_social_event(
		{
			**payload,
			"source_platform": "reddit",
			"social_sentiment": social_sentiment,
			"social_velocity": social_velocity,
			"message_count": mention_count,
			"unique_authors": unique_authors,
			"engagement_score": engagement_score,
			"credibility_score": credibility_score,
		},
		source=source,
	)


def _coerce_datetime(value: object) -> datetime:
	if isinstance(value, datetime):
		return value.astimezone(UTC)
	return datetime.now(UTC)


def _bounded(value: object) -> float:
	if isinstance(value, bool):
		return float(int(value))
	if isinstance(value, (int, float)):
		return max(0.0, min(float(value), 1.0))
	return 0.0