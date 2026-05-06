from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from math import log10
from time import sleep
from typing import Any, Callable
from urllib import parse, request
from urllib.error import URLError

from core.config import AcquisitionConfig, AppSettings, SocialLiveSourceConfig
from core.schemas import EventEnvelope, SocialQueryRequest
from sentinel.social_llm import SocialLlmAnalysis
from sentinel.social_listener import build_reddit_event, build_x_event

SocialHttpTransport = Callable[[str, dict[str, str], float], dict[str, Any]]
CASHTAG_PATTERN = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})")
UPPER_TOKEN_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]{1,9}\b")
TOKEN_STOPWORDS = {
    "AND",
    "ARE",
    "FOR",
    "FROM",
    "JUST",
    "NEW",
    "NOW",
    "OR",
    "SOON",
    "THE",
    "THIS",
    "USD",
}


class RedditSnapshotSource:
    def __init__(
        self,
        settings: AppSettings,
        config: SocialLiveSourceConfig,
        *,
        transport: SocialHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or _http_json_get_transport

    def fetch_events(self) -> list[EventEnvelope]:
        if self.config.provider != "reddit_search_json":
            raise ValueError(f"unsupported_social_provider:{self.config.provider}")
        payload = self._fetch_payload()
        children = payload.get("data", {}).get("children")
        if not isinstance(children, list):
            raise ValueError("invalid_reddit_search_payload")

        posts = [entry.get("data") for entry in children if isinstance(entry, dict)]
        filtered = [post for post in posts if isinstance(post, dict) and self._is_recent(post)]
        if not filtered:
            return []

        mention_count = len(filtered)
        unique_authors = len(
            {str(post.get("author", "")).strip() for post in filtered if str(post.get("author", "")).strip()}
        )
        if mention_count < self.config.min_mentions or unique_authors < self.config.min_unique_authors:
            return []

        social_velocity = _bounded(mention_count / max(self.config.limit, 1))
        social_sentiment = _bounded(sum(_post_sentiment(post) for post in filtered) / mention_count)
        if social_sentiment < self.config.min_sentiment_score:
            return []
        if social_velocity < self.config.min_velocity_score:
            return []

        latest_post = max(filtered, key=lambda post: float(post.get("created_utc", 0.0) or 0.0))
        observed_at = datetime.fromtimestamp(float(latest_post.get("created_utc", 0.0) or 0.0), UTC)
        resolved_query = _resolve_query(self.config)
        token = _event_token(
            self.config,
            _reddit_discovery_texts(filtered),
            fallback_query=resolved_query,
        )
        if token is None:
            return []
        event = build_reddit_event(
            {
                "event_id": self._build_event_id(filtered, token=token),
                "token": token,
                "chain": self.config.chain or "unknown",
                "query": resolved_query,
                "retrieval_mode": _retrieval_mode(self.config),
                "observed_at": observed_at,
                "thread_count": mention_count,
                "author_count": unique_authors,
                "comment_velocity": _bounded(sum(_post_comment_velocity(post) for post in filtered) / mention_count),
                "upvote_ratio": _bounded(sum(_post_upvote_ratio(post) for post in filtered) / mention_count),
                "subreddit_quality": _bounded(sum(_post_subreddit_quality(post) for post in filtered) / mention_count),
                "engagement_score": _bounded(sum(_post_engagement(post) for post in filtered) / mention_count),
                "credibility_score": _bounded(sum(_post_subreddit_quality(post) for post in filtered) / mention_count),
                "evidence_texts": _reddit_evidence_texts(filtered),
                "url": _post_url(latest_post),
                "post_id": str(latest_post.get("id", "")),
            },
            source=self.config.source_name or "reddit_listener",
        )
        return [event]

    def _fetch_payload(self) -> dict[str, Any]:
        url = self._build_url()
        headers = {"User-Agent": self.config.user_agent}
        attempts = max(self.config.retry_attempts, 1)
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return self.transport(url, headers, self.config.timeout_seconds)
            except (OSError, URLError, ValueError) as error:
                last_error = error
                if attempt + 1 >= attempts:
                    break
                if self.config.retry_backoff_seconds > 0:
                    sleep(self.config.retry_backoff_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("reddit_transport_failed_without_error")

    def _build_url(self) -> str:
        base_url = self.config.source_url or f"https://www.reddit.com/r/{self.config.subreddit}/search.json"
        params = {
            self.config.query_param_name: _resolve_query(self.config),
            "restrict_sr": "1",
            "sort": self.config.sort,
            "limit": str(self.config.limit),
        }
        return _merge_url_query(base_url, params)

    def _is_recent(self, post: dict[str, Any]) -> bool:
        created_utc = float(post.get("created_utc", 0.0) or 0.0)
        if created_utc <= 0:
            return False
        age_seconds = (datetime.now(UTC) - datetime.fromtimestamp(created_utc, UTC)).total_seconds()
        return age_seconds <= self.config.max_snapshot_age_seconds

    def _build_event_id(self, posts: list[dict[str, Any]], *, token: str) -> str:
        latest_created_utc = int(max(float(post.get("created_utc", 0.0) or 0.0) for post in posts))
        latest_post_id = str(max(posts, key=lambda post: float(post.get("created_utc", 0.0) or 0.0)).get("id", "unknown"))
        return (
            f"reddit:{self.config.subreddit}:{token}:{latest_created_utc}:"
            f"{latest_post_id}:{len(posts)}"
        )


class XSnapshotSource:
    def __init__(
        self,
        settings: AppSettings,
        config: SocialLiveSourceConfig,
        *,
        transport: SocialHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or _http_json_get_transport

    def fetch_events(self) -> list[EventEnvelope]:
        if self.config.provider != "x_snapshot_json":
            raise ValueError(f"unsupported_social_provider:{self.config.provider}")
        payload = self._fetch_payload()
        records = _extract_x_records(payload)
        filtered = [record for record in records if self._is_recent(record)]
        if not filtered:
            return []

        mention_count = len(filtered)
        unique_authors = len(
            {
                str(record.get("author_handle", record.get("author", ""))).strip()
                for record in filtered
                if str(record.get("author_handle", record.get("author", ""))).strip()
            }
        )
        if mention_count < self.config.min_mentions or unique_authors < self.config.min_unique_authors:
            return []

        social_velocity = _bounded(sum(_x_velocity(record) for record in filtered) / mention_count)
        social_sentiment = _bounded(sum(_x_sentiment(record) for record in filtered) / mention_count)
        if social_sentiment < self.config.min_sentiment_score:
            return []
        if social_velocity < self.config.min_velocity_score:
            return []

        latest_record = max(filtered, key=_x_created_timestamp)
        observed_at = _coerce_social_datetime(latest_record.get("created_at"))
        resolved_query = _resolve_query(self.config)
        token = _event_token(
            self.config,
            _x_discovery_texts(filtered),
            fallback_query=resolved_query,
        )
        if token is None:
            return []
        event = build_x_event(
            {
                "event_id": self._build_event_id(filtered, token=token),
                "token": token,
                "chain": self.config.chain or "unknown",
                "query": resolved_query,
                "retrieval_mode": _retrieval_mode(self.config),
                "observed_at": observed_at,
                "mention_count": mention_count,
                "unique_authors": unique_authors,
                "viral_score": _bounded(sum(_x_viral_score(record) for record in filtered) / mention_count),
                "influencer_ratio": _bounded(sum(_x_credibility(record) for record in filtered) / mention_count),
                "engagement_score": _bounded(sum(_x_engagement(record) for record in filtered) / mention_count),
                "credibility_score": _bounded(sum(_x_credibility(record) for record in filtered) / mention_count),
                "sentiment_score": social_sentiment,
                "social_velocity": social_velocity,
                "evidence_texts": _x_evidence_texts(filtered),
                "url": str(latest_record.get("url", "")).strip(),
                "post_id": str(latest_record.get("id", "")),
            },
            source=self.config.source_name or "x_listener",
        )
        return [event]

    def _fetch_payload(self) -> dict[str, Any]:
        url = self._build_url()
        if not url:
            raise ValueError("x_source_url_missing")
        headers = {"User-Agent": self.config.user_agent}
        attempts = max(self.config.retry_attempts, 1)
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return self.transport(url, headers, self.config.timeout_seconds)
            except (OSError, URLError, ValueError) as error:
                last_error = error
                if attempt + 1 >= attempts:
                    break
                if self.config.retry_backoff_seconds > 0:
                    sleep(self.config.retry_backoff_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("x_transport_failed_without_error")

    def _build_url(self) -> str:
        base_url = (self.config.source_url or "").strip()
        if not base_url:
            raise ValueError("x_source_url_missing")
        resolved_query = _resolve_query(self.config)
        return _merge_url_query(
            _replace_query_placeholders(base_url, resolved_query),
            {self.config.query_param_name: resolved_query},
            skip_keys_if_present={self.config.query_param_name},
        )

    def _is_recent(self, record: dict[str, Any]) -> bool:
        observed_at = _coerce_social_datetime(record.get("created_at"))
        age_seconds = (datetime.now(UTC) - observed_at).total_seconds()
        return age_seconds <= self.config.max_snapshot_age_seconds

    def _build_event_id(self, records: list[dict[str, Any]], *, token: str) -> str:
        latest_record = max(records, key=_x_created_timestamp)
        latest_timestamp = int(_x_created_timestamp(latest_record))
        latest_post_id = str(latest_record.get("id", "unknown"))
        query_key = _resolve_query(self.config)
        return f"x:{query_key}:{token}:{latest_timestamp}:{latest_post_id}:{len(records)}"


def build_social_live_sources(settings: AppSettings) -> list[RedditSnapshotSource | XSnapshotSource]:
    """Build social discovery sources from configured social_sources.

    Each enabled source with a valid provider is instantiated as a polling
    snapshot source.  These run in the ``--social-live`` worker and emit
    ``social.signal_snapshot`` events into the raw-event stream.

    Note: discovery-mode sources do not carry a specific token — the query,
    subreddit or platform-level config determines what is polled.  Token-
    specific queries belong to confirmation mode (see
    :func:`build_social_confirmation_source`).
    """
    acquisition = AcquisitionConfig.model_validate(settings.acquisition)
    sources: list[RedditSnapshotSource | XSnapshotSource] = []
    for source_key, source_config in sorted(acquisition.social_sources.items()):
        config = source_config.model_copy(
            update={"source_name": source_config.source_name or f"social_{source_key}"}
        )
        if not config.enabled:
            continue
        if config.provider == "reddit_search_json":
            sources.append(RedditSnapshotSource(settings, config))
        elif config.provider == "x_snapshot_json":
            sources.append(XSnapshotSource(settings, config))
        else:
            raise ValueError(f"unsupported_social_provider:{config.provider}")
    return sources


def build_social_confirmation_source(
    settings: AppSettings,
    social_query: SocialQueryRequest,
    *,
    transport: SocialHttpTransport | None = None,
) -> RedditSnapshotSource | XSnapshotSource:
    acquisition = AcquisitionConfig.model_validate(settings.acquisition)
    source_key, source_config = _resolve_social_source_config(acquisition, social_query)
    config = source_config.model_copy(
        update={
            "source_name": source_config.source_name or f"social_{source_key}_confirmation",
            "platform": social_query.platform or source_config.platform,
            "chain": social_query.chain,
            "token": social_query.token,
            "query": social_query.query,
        }
    )
    if config.platform == "reddit":
        return RedditSnapshotSource(settings, config, transport=transport)
    if config.platform == "x":
        return XSnapshotSource(settings, config, transport=transport)
    raise ValueError(f"unsupported_social_platform:{config.platform}")


def build_social_query_requested_event(
    social_query: SocialQueryRequest,
    *,
    source_name: str,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"social-query:{source_name}:{social_query.request_id}",
        event_type="social.query_requested",
        source=source_name,
        chain=social_query.chain,
        token=social_query.token,
        observed_at=social_query.requested_at,
        ingested_at=datetime.now(UTC),
        payload={
            "request_id": social_query.request_id,
            "query": social_query.query,
            "platform": social_query.platform,
            "mode": social_query.mode,
            "candidate_id": social_query.candidate_id,
            "fsm_context": (
                social_query.fsm_context.model_dump(mode="json")
                if social_query.fsm_context is not None
                else None
            ),
            "metadata": social_query.metadata,
        },
    )


def build_social_analysis_event(
    social_query: SocialQueryRequest,
    *,
    source_name: str,
    social_event: EventEnvelope | None,
    llm_analysis: SocialLlmAnalysis | None = None,
) -> EventEnvelope:
    event_payload = social_event.payload if social_event is not None else {}
    message_count = int(event_payload.get("message_count", 0))
    unique_authors = int(event_payload.get("unique_authors", 0))
    engagement_score = _bounded(event_payload.get("engagement_score", 0.0))
    credibility_score = _bounded(event_payload.get("credibility_score", 0.0))
    social_sentiment = _bounded(event_payload.get("social_sentiment", 0.0))
    social_velocity = _bounded(event_payload.get("social_velocity", 0.0))
    base_confirmation_score = _bounded(
        social_sentiment * 0.35
        + social_velocity * 0.25
        + engagement_score * 0.2
        + credibility_score * 0.2
    )
    llm_relevance = llm_analysis.relevance_score if llm_analysis is not None else 0.0
    llm_narrative = llm_analysis.narrative_strength if llm_analysis is not None else 0.0
    llm_credibility = llm_analysis.credibility_score if llm_analysis is not None else credibility_score
    llm_noise = llm_analysis.noise_score if llm_analysis is not None else 0.0
    confirmation_score = _bounded(
        base_confirmation_score * 0.65
        + llm_relevance * 0.15
        + llm_narrative * 0.1
        + llm_credibility * 0.1
        - llm_noise * 0.1
    )
    analysis_status = "matched" if social_event is not None else "no_recent_social_match"
    observed_at = social_event.observed_at if social_event is not None else social_query.requested_at
    return EventEnvelope(
        event_id=f"social-analysis:{source_name}:{social_query.request_id}",
        event_type="social.analysis_completed",
        source=source_name,
        chain=social_query.chain,
        token=social_query.token,
        observed_at=observed_at,
        ingested_at=datetime.now(UTC),
        payload={
            "request_id": social_query.request_id,
            "query": social_query.query,
            "mode": social_query.mode,
            "analysis_status": analysis_status,
            "candidate_id": social_query.candidate_id,
            "platform": social_event.payload.get("source_platform") if social_event is not None else social_query.platform,
            "message_count": message_count,
            "unique_authors": unique_authors,
            "engagement_score": engagement_score,
            "credibility_score": credibility_score,
            "social_sentiment": social_sentiment,
            "social_velocity": social_velocity,
            "base_confirmation_score": base_confirmation_score,
            "confirmation_score": confirmation_score,
            "snapshot_event_id": social_event.event_id if social_event is not None else None,
            "llm_provider": llm_analysis.provider if llm_analysis is not None else None,
            "llm_model": llm_analysis.model if llm_analysis is not None else None,
            "llm_relevance_score": llm_relevance,
            "llm_entity_confidence": llm_analysis.entity_confidence if llm_analysis is not None else 0.0,
            "llm_narrative_strength": llm_narrative,
            "llm_credibility_score": llm_credibility,
            "llm_noise_score": llm_noise,
            "llm_risk_flags": llm_analysis.risk_flags if llm_analysis is not None else [],
            "llm_summary": llm_analysis.summary if llm_analysis is not None else None,
            "llm_catalyst_type": llm_analysis.catalyst_type if llm_analysis is not None else None,
            "fsm_context": (
                social_query.fsm_context.model_dump(mode="json")
                if social_query.fsm_context is not None
                else None
            ),
        },
    )


def _http_json_get_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
    http_request = request.Request(url, headers=headers)
    with request.urlopen(http_request, timeout=timeout_seconds) as response:  # noqa: S310
        body = response.read().decode("utf-8")
    parsed = json.loads(body)
    if isinstance(parsed, dict):
        return parsed
    raise ValueError("invalid_social_http_payload")


def _post_sentiment(post: dict[str, Any]) -> float:
    upvote_ratio = _post_upvote_ratio(post)
    engagement = _post_engagement(post)
    return _bounded(upvote_ratio * 0.6 + engagement * 0.4)


def _post_comment_velocity(post: dict[str, Any]) -> float:
    comments = max(int(post.get("num_comments", 0) or 0), 0)
    return _bounded(comments / 50)


def _post_upvote_ratio(post: dict[str, Any]) -> float:
    value = post.get("upvote_ratio", 0.0)
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return _bounded(value)
    return 0.0


def _post_engagement(post: dict[str, Any]) -> float:
    score = max(int(post.get("score", 0) or 0), 0)
    comments = max(int(post.get("num_comments", 0) or 0), 0)
    return _bounded((log10(score + 1) / 3) * 0.6 + (comments / 50) * 0.4)


def _post_subreddit_quality(post: dict[str, Any]) -> float:
    subscribers = max(int(post.get("subreddit_subscribers", 0) or 0), 0)
    return _bounded(log10(subscribers + 1) / 6)


def _post_url(post: dict[str, Any]) -> str:
    permalink = str(post.get("permalink", "")).strip()
    if permalink.startswith("/"):
        return f"https://www.reddit.com{permalink}"
    return permalink


def _bounded(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _reddit_evidence_texts(posts: list[dict[str, Any]]) -> list[str]:
    texts: list[str] = []
    for post in posts[:6]:
        title = str(post.get("title", "")).strip()
        body = str(post.get("selftext", post.get("body", ""))).strip()
        combined = " ".join(part for part in [title, body] if part).strip()
        if combined:
            texts.append(combined)
    return texts


def _x_evidence_texts(records: list[dict[str, Any]]) -> list[str]:
    texts: list[str] = []
    for record in records[:6]:
        text = str(record.get("text", record.get("full_text", ""))).strip()
        if text:
            texts.append(text)
    return texts


def _resolve_social_source_config(
    acquisition: AcquisitionConfig,
    social_query: SocialQueryRequest,
) -> tuple[str, SocialLiveSourceConfig]:
    if social_query.source_name:
        direct = acquisition.social_sources.get(social_query.source_name)
        if direct is not None:
            return social_query.source_name, direct
        for source_key, source_config in acquisition.social_sources.items():
            if source_config.source_name == social_query.source_name:
                return source_key, source_config
    if social_query.platform:
        for source_key, source_config in acquisition.social_sources.items():
            if source_config.platform == social_query.platform:
                return source_key, source_config
    raise ValueError("unknown_social_confirmation_source")


def _resolve_query(config: SocialLiveSourceConfig) -> str:
    template = config.query or config.query_template
    if not template:
        raise ValueError("social_query_missing")
    replacements = {
        "token": config.token,
        "chain": config.chain,
        "query": config.query or config.token,
        "cashtag": f"${config.token}" if config.token else None,
    }
    rendered = str(template)
    for key, value in replacements.items():
        if value is None:
            continue
        rendered = rendered.replace(f"${{{key}}}", value)
        rendered = rendered.replace(f"{{{key}}}", value)
    return rendered


def _event_token(
    config: SocialLiveSourceConfig,
    texts: list[str],
    *,
    fallback_query: str,
) -> str | None:
    if config.token:
        return config.token
    candidates = _extract_candidate_tokens([*texts, fallback_query])
    if not candidates:
        return None
    return candidates[0]


def _retrieval_mode(config: SocialLiveSourceConfig) -> str:
    return "discovery" if not config.token else "confirmation"


def _extract_candidate_tokens(texts: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    for text in texts:
        for match in CASHTAG_PATTERN.findall(text):
            token = match.upper()
            counts[token] = counts.get(token, 0) + 2
        for match in UPPER_TOKEN_PATTERN.findall(text):
            token = match.upper()
            if token in TOKEN_STOPWORDS:
                continue
            counts[token] = counts.get(token, 0) + 1
    return [token for token, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def _reddit_discovery_texts(posts: list[dict[str, Any]]) -> list[str]:
    texts: list[str] = []
    for post in posts:
        for key in ("title", "selftext", "body", "text"):
            value = str(post.get(key, "")).strip()
            if value:
                texts.append(value)
    return texts


def _x_discovery_texts(records: list[dict[str, Any]]) -> list[str]:
    texts: list[str] = []
    for record in records:
        for key in ("text", "full_text", "body"):
            value = str(record.get(key, "")).strip()
            if value:
                texts.append(value)
    return texts


def _required_token(config: SocialLiveSourceConfig) -> str:
    if not config.token:
        raise ValueError("social_token_missing")
    return config.token


def _required_chain(config: SocialLiveSourceConfig) -> str:
    if not config.chain:
        raise ValueError("social_chain_missing")
    return config.chain


def _replace_query_placeholders(url: str, resolved_query: str) -> str:
    encoded_query = parse.quote_plus(resolved_query)
    return (
        url.replace("{query}", encoded_query)
        .replace("${query}", encoded_query)
        .replace("{query_raw}", resolved_query)
        .replace("${query_raw}", resolved_query)
    )


def _merge_url_query(
    base_url: str,
    params: dict[str, str],
    *,
    skip_keys_if_present: set[str] | None = None,
) -> str:
    split = parse.urlsplit(base_url)
    query_pairs = dict(parse.parse_qsl(split.query, keep_blank_values=True))
    for key, value in params.items():
        if not value:
            continue
        if skip_keys_if_present is not None and key in skip_keys_if_present and key in query_pairs:
            continue
        query_pairs[key] = value
    query = parse.urlencode(query_pairs)
    return parse.urlunsplit((split.scheme, split.netloc, split.path, query, split.fragment))


def _extract_x_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = payload.get("records")
    if isinstance(records, list):
        return [record for record in records if isinstance(record, dict)]
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return [record for record in data["records"] if isinstance(record, dict)]
    raise ValueError("invalid_x_snapshot_payload")


def _coerce_social_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC)
    raise ValueError("invalid_social_datetime")


def _x_created_timestamp(record: dict[str, Any]) -> float:
    return _coerce_social_datetime(record.get("created_at")).timestamp()


def _x_engagement(record: dict[str, Any]) -> float:
    if isinstance(record.get("engagement_score"), (int, float)):
        return _bounded(float(record["engagement_score"]))
    likes = max(int(record.get("like_count", 0) or 0), 0)
    reposts = max(int(record.get("repost_count", 0) or 0), 0)
    replies = max(int(record.get("reply_count", 0) or 0), 0)
    quotes = max(int(record.get("quote_count", 0) or 0), 0)
    return _bounded((log10(likes + reposts * 2 + replies + quotes * 2 + 1) / 4))


def _x_credibility(record: dict[str, Any]) -> float:
    if isinstance(record.get("credibility_score"), (int, float)):
        return _bounded(float(record["credibility_score"]))
    followers = max(int(record.get("follower_count", 0) or 0), 0)
    verified_bonus = 0.15 if bool(record.get("is_verified", False)) else 0.0
    return _bounded((log10(followers + 1) / 6) + verified_bonus)


def _x_viral_score(record: dict[str, Any]) -> float:
    if isinstance(record.get("viral_score"), (int, float)):
        return _bounded(float(record["viral_score"]))
    reposts = max(int(record.get("repost_count", 0) or 0), 0)
    quotes = max(int(record.get("quote_count", 0) or 0), 0)
    return _bounded((reposts + quotes * 2) / 100)


def _x_velocity(record: dict[str, Any]) -> float:
    if isinstance(record.get("social_velocity"), (int, float)):
        return _bounded(float(record["social_velocity"]))
    return _bounded((_x_engagement(record) * 0.6) + (_x_viral_score(record) * 0.4))


def _x_sentiment(record: dict[str, Any]) -> float:
    if isinstance(record.get("sentiment_score"), (int, float)):
        return _bounded(float(record["sentiment_score"]))
    return _bounded((_x_engagement(record) * 0.55) + (_x_credibility(record) * 0.45))