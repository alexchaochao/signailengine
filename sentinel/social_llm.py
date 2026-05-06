from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx

from core.config import AppSettings, LlmConfig
from core.schemas import EventEnvelope, SocialQueryRequest


@dataclass(frozen=True)
class SocialLlmAnalysis:
    relevance_score: float
    entity_confidence: float
    narrative_strength: float
    credibility_score: float
    noise_score: float
    risk_flags: list[str]
    summary: str
    catalyst_type: str
    provider: str
    model: str


class SocialLlmAnalyzer:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings

    def analyze(
        self,
        social_query: SocialQueryRequest,
        social_event: EventEnvelope | None,
    ) -> SocialLlmAnalysis:
        heuristic = self._heuristic_analysis(social_query, social_event)
        llm_config = LlmConfig.model_validate(self.settings.llm)
        if not llm_config.enabled or llm_config.provider == "heuristic":
            return heuristic
        if not llm_config.base_url or not llm_config.api_key:
            return heuristic
        try:
            return self._remote_analysis(social_query, social_event, heuristic)
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return heuristic

    def _heuristic_analysis(
        self,
        social_query: SocialQueryRequest,
        social_event: EventEnvelope | None,
    ) -> SocialLlmAnalysis:
        llm_config = LlmConfig.model_validate(self.settings.llm)
        payload = social_event.payload if social_event is not None else {}
        query = social_query.query or social_query.token
        evidence_texts = _evidence_texts(payload, limit=llm_config.max_evidence_texts)
        message_count = max(int(payload.get("message_count", 0) or 0), 0)
        unique_authors = max(int(payload.get("unique_authors", 0) or 0), 0)
        engagement_score = _bounded(payload.get("engagement_score", 0.0))
        credibility_score = _bounded(payload.get("credibility_score", 0.0))
        sentiment_score = _bounded(payload.get("social_sentiment", 0.0))
        velocity_score = _bounded(payload.get("social_velocity", 0.0))
        query_terms = _query_terms(query)
        evidence_blob = " ".join(evidence_texts).lower()
        matched_terms = sum(1 for term in query_terms if term in evidence_blob)
        relevance = _bounded(0.25 + matched_terms / max(len(query_terms), 1) * 0.75) if evidence_texts else _bounded(0.35 + sentiment_score * 0.2)
        entity_confidence = _bounded(max(relevance, 0.25 + min(unique_authors / 5.0, 0.75)))
        narrative_strength = _bounded(velocity_score * 0.45 + engagement_score * 0.35 + sentiment_score * 0.2)
        noise_score = _bounded(max(0.0, 0.6 - credibility_score) * 0.6 + (0.3 if message_count <= 1 else 0.0))
        risk_flags: list[str] = []
        if "scam" in evidence_blob or "rug" in evidence_blob:
            risk_flags.append("scam_or_rug_language")
        if unique_authors <= 1 and message_count > 1:
            risk_flags.append("author_concentration")
        summary = _truncate_summary(
            "heuristic social analysis"
            f" query={query}"
            f" mentions={message_count}"
            f" authors={unique_authors}"
            f" relevance={relevance:.2f}"
            f" narrative={narrative_strength:.2f}"
            f" credibility={credibility_score:.2f}",
            llm_config.max_summary_chars,
        )
        return SocialLlmAnalysis(
            relevance_score=relevance,
            entity_confidence=entity_confidence,
            narrative_strength=narrative_strength,
            credibility_score=credibility_score,
            noise_score=noise_score,
            risk_flags=risk_flags,
            summary=summary,
            catalyst_type="social_confirmation",
            provider="heuristic",
            model="heuristic-v1",
        )

    def _remote_analysis(
        self,
        social_query: SocialQueryRequest,
        social_event: EventEnvelope | None,
        fallback: SocialLlmAnalysis,
    ) -> SocialLlmAnalysis:
        llm_config = LlmConfig.model_validate(self.settings.llm)
        payload = social_event.payload if social_event is not None else {}
        request_payload = {
            "query": social_query.query or social_query.token,
            "token": social_query.token,
            "chain": social_query.chain,
            "platform": payload.get("source_platform", social_query.platform),
            "message_count": payload.get("message_count", 0),
            "unique_authors": payload.get("unique_authors", 0),
            "engagement_score": payload.get("engagement_score", 0.0),
            "credibility_score": payload.get("credibility_score", 0.0),
            "social_sentiment": payload.get("social_sentiment", 0.0),
            "social_velocity": payload.get("social_velocity", 0.0),
            "evidence_texts": _evidence_texts(payload, limit=llm_config.max_evidence_texts),
            "fsm_context": social_query.fsm_context.model_dump(mode="json") if social_query.fsm_context is not None else None,
        }
        system_prompt = (
            "You are a crypto social analyst. Return strict JSON with keys "
            "relevance_score, entity_confidence, narrative_strength, credibility_score, noise_score, risk_flags, summary, catalyst_type."
        )
        user_prompt = json.dumps(request_payload, ensure_ascii=True)
        endpoint = llm_config.base_url.rstrip("/") + "/chat/completions"
        with httpx.Client(timeout=llm_config.timeout_seconds) as client:
            response = client.post(
                endpoint,
                headers={
                    "Authorization": f"Bearer {llm_config.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": llm_config.model,
                    "temperature": llm_config.temperature,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        return SocialLlmAnalysis(
            relevance_score=_bounded(parsed.get("relevance_score", fallback.relevance_score)),
            entity_confidence=_bounded(parsed.get("entity_confidence", fallback.entity_confidence)),
            narrative_strength=_bounded(parsed.get("narrative_strength", fallback.narrative_strength)),
            credibility_score=_bounded(parsed.get("credibility_score", fallback.credibility_score)),
            noise_score=_bounded(parsed.get("noise_score", fallback.noise_score)),
            risk_flags=[str(flag) for flag in parsed.get("risk_flags", fallback.risk_flags)],
            summary=_truncate_summary(str(parsed.get("summary", fallback.summary)), llm_config.max_summary_chars),
            catalyst_type=str(parsed.get("catalyst_type", fallback.catalyst_type)),
            provider=llm_config.provider,
            model=llm_config.model,
        )


def build_social_llm_analyzer(settings: AppSettings) -> SocialLlmAnalyzer:
    return SocialLlmAnalyzer(settings)


def _evidence_texts(payload: dict[str, Any], *, limit: int) -> list[str]:
    texts = payload.get("evidence_texts")
    if not isinstance(texts, list):
        return []
    result: list[str] = []
    for item in texts[:limit]:
        if not isinstance(item, str):
            continue
        stripped = item.strip()
        if stripped:
            result.append(stripped)
    return result


def _query_terms(query: str) -> list[str]:
    cleaned = query.replace("$", " ").replace("OR", " ")
    return [part.strip().lower() for part in cleaned.split() if part.strip()]


def _truncate_summary(value: str, max_chars: int) -> str:
    stripped = value.strip()
    if len(stripped) <= max_chars:
        return stripped
    return stripped[: max_chars - 3].rstrip() + "..."


def _bounded(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return max(0.0, min(float(value), 1.0))
    return 0.0