from __future__ import annotations

import json
import re
from dataclasses import dataclass

import httpx

from core.config import AppSettings, CatalystAlphaLiveSourceConfig


@dataclass(frozen=True)
class ExtractedCatalystEntity:
    token: str
    chain: str
    project_name: str | None
    catalyst_type: str
    confidence: float


class CatalystEntityExtractor:
    def __init__(self, settings: AppSettings, config: CatalystAlphaLiveSourceConfig) -> None:
        self.settings = settings
        self.config = config

    def extract(self, *, headline: str, summary: str) -> list[ExtractedCatalystEntity]:
        heuristic_entities = self._heuristic_extract(headline=headline, summary=summary)
        if self.settings.llm.enabled and self.settings.llm.provider != "heuristic":
            remote_entities = self._remote_extract(headline=headline, summary=summary)
            if remote_entities:
                return remote_entities[: self.config.extraction_max_entities]
        return heuristic_entities[: self.config.extraction_max_entities]

    def _heuristic_extract(self, *, headline: str, summary: str) -> list[ExtractedCatalystEntity]:
        text = f"{headline} {summary}".strip()
        token_matches = list(dict.fromkeys(re.findall(r"\(([A-Z0-9]{2,10})\)", headline)))
        if not token_matches:
            token_matches = list(
                dict.fromkeys(
                    re.findall(r"(?<![A-Za-z0-9])\$([A-Z0-9]{2,10})(?![A-Za-z0-9])", text)
                )
            )
        entities: list[ExtractedCatalystEntity] = []
        for token in token_matches:
            entities.append(
                ExtractedCatalystEntity(
                    token=token,
                    chain=self._infer_chain(text),
                    project_name=self._project_name_from_headline(headline, token),
                    catalyst_type=self._infer_catalyst_type(text),
                    confidence=0.65,
                )
            )
        return entities

    def _remote_extract(self, *, headline: str, summary: str) -> list[ExtractedCatalystEntity]:
        llm = self.settings.llm
        if not llm.api_key or not llm.base_url:
            return []
        prompt = (
            "Extract crypto catalyst entities from the announcement. Return JSON only with the shape "
            '{"entities":[{"token":"AERO","chain":"base","project_name":"Aerodrome",'
            '"catalyst_type":"cex_listing_announcement","confidence":0.9}]}. '
            "Use lowercase chains. Omit entities if token symbol is not clear.\n\n"
            f"headline: {headline}\nsummary: {summary}"
        )
        payload = {
            "model": llm.model,
            "temperature": llm.temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": "You extract structured crypto catalyst entities from announcements.",
                },
                {"role": "user", "content": prompt},
            ],
        }
        url = llm.base_url.rstrip("/") + "/chat/completions"
        try:
            with httpx.Client(timeout=llm.timeout_seconds) as client:
                response = client.post(
                    url,
                    headers={"Authorization": f"Bearer {llm.api_key}"},
                    json=payload,
                )
                response.raise_for_status()
        except httpx.HTTPError:
            return []
        try:
            content = response.json()["choices"][0]["message"]["content"]
            document = json.loads(content)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            return []

        entities: list[ExtractedCatalystEntity] = []
        for item in document.get("entities", []):
            if not isinstance(item, dict):
                continue
            token = str(item.get("token", "")).strip().upper()
            if not token:
                continue
            entities.append(
                ExtractedCatalystEntity(
                    token=token,
                    chain=str(item.get("chain") or self.config.default_chain).strip().lower() or self.config.default_chain,
                    project_name=str(item.get("project_name", "")).strip() or None,
                    catalyst_type=str(item.get("catalyst_type") or self.config.default_catalyst_type).strip(),
                    confidence=max(0.0, min(float(item.get("confidence", 0.0) or 0.0), 1.0)),
                )
            )
        return entities

    def _infer_chain(self, text: str) -> str:
        lowered = text.lower()
        if "arbitrum" in lowered:
            return "arbitrum"
        if "base" in lowered:
            return "base"
        if "solana" in lowered:
            return "solana"
        if "ethereum" in lowered or "erc-20" in lowered:
            return "ethereum"
        return self.config.default_chain

    def _infer_catalyst_type(self, text: str) -> str:
        lowered = text.lower()
        if "roadmap" in lowered:
            return "listing_roadmap_update"
        if "perpetual" in lowered or "futures" in lowered:
            return "derivatives_listing_announcement"
        if "launch" in lowered:
            return "product_launch_announcement"
        return self.config.default_catalyst_type

    def _project_name_from_headline(self, headline: str, token: str) -> str | None:
        match = re.search(rf"(?P<name>.+?)\s*\({re.escape(token)}\)", headline)
        if match is None:
            return None
        project_name = match.group("name")
        project_name = re.sub(r"^(binance|coinbase|exchange)\s+", "", project_name, flags=re.IGNORECASE)
        project_name = re.sub(
            r"\b(will list|will add|adds|list|listing|roadmap|launch|launches)\b",
            "",
            project_name,
            flags=re.IGNORECASE,
        )
        return project_name.strip(" :-") or None
