from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import URLError

import httpx

from core.config import AppSettings, NativeAssetPriceSourceConfig


@dataclass(frozen=True)
class PriceSnapshot:
    asset: str
    quote_currency: str
    price: float
    source: str


PriceTransport = Callable[[str, float], dict[str, Any]]


class PriceProvider:
    def get_native_token_price_usd(self, chain: str) -> PriceSnapshot | None:
        raise NotImplementedError


class NullPriceProvider(PriceProvider):
    def get_native_token_price_usd(self, chain: str) -> PriceSnapshot | None:
        del chain
        return None


class HttpPriceProvider(PriceProvider):
    def __init__(
        self,
        settings: AppSettings,
        transport: PriceTransport | None = None,
    ) -> None:
        self.settings = settings
        self.transport = transport or _http_json_get_transport

    def get_native_token_price_usd(self, chain: str) -> PriceSnapshot | None:
        raw_source_config = self.settings.live.pricing.native_asset_sources.get(chain)
        if raw_source_config is None:
            return None
        source_config = self._coerce_source_config(raw_source_config)

        payload = self._dispatch(
            source_config.url,
            self.settings.live.pricing.timeout_seconds,
            self.settings.live.pricing.max_retries,
        )
        return self._parse_price_payload(source_config, payload)

    def _coerce_source_config(
        self,
        source_config: NativeAssetPriceSourceConfig | dict[str, Any],
    ) -> NativeAssetPriceSourceConfig:
        if isinstance(source_config, NativeAssetPriceSourceConfig):
            return source_config
        if isinstance(source_config, dict):
            return NativeAssetPriceSourceConfig.model_validate(source_config)
        raise ValueError("invalid_live_price_source_config")

    def _dispatch(self, url: str, timeout_seconds: float, max_retries: int) -> dict[str, Any]:
        last_error: Exception | None = None
        for _ in range(max_retries + 1):
            try:
                payload = self.transport(url, timeout_seconds)
                if not isinstance(payload, dict):
                    raise ValueError("invalid_live_price_response")
                return payload
            except (TimeoutError, URLError, OSError, ValueError) as error:
                last_error = error
        if last_error is not None:
            raise ValueError("live_price_provider_error") from last_error
        raise ValueError("live_price_provider_error")

    def _parse_price_payload(
        self,
        source_config: NativeAssetPriceSourceConfig,
        payload: dict[str, Any],
    ) -> PriceSnapshot:
        if source_config.provider == "coingecko_simple_price":
            return self._parse_coingecko_price(source_config, payload)
        if source_config.provider == "binance_ticker_price":
            return self._parse_binance_ticker_price(source_config, payload)
        raise ValueError("unsupported_live_price_provider")

    def _parse_coingecko_price(
        self,
        source_config: NativeAssetPriceSourceConfig,
        payload: dict[str, Any],
    ) -> PriceSnapshot:
        asset_payload = payload.get(source_config.lookup_key)
        if not isinstance(asset_payload, dict):
            raise ValueError("invalid_live_price_response")
        quote_key = source_config.quote_currency.lower()
        price = asset_payload.get(quote_key)
        if not isinstance(price, (float, int)):
            raise ValueError("invalid_live_price_response")
        return PriceSnapshot(
            asset=source_config.asset,
            quote_currency=source_config.quote_currency,
            price=float(price),
            source=source_config.provider,
        )

    def _parse_binance_ticker_price(
        self,
        source_config: NativeAssetPriceSourceConfig,
        payload: dict[str, Any],
    ) -> PriceSnapshot:
        symbol = payload.get("symbol")
        price = payload.get("price")
        if symbol != source_config.lookup_key or not isinstance(price, str):
            raise ValueError("invalid_live_price_response")
        try:
            numeric_price = float(price)
        except ValueError as error:
            raise ValueError("invalid_live_price_response") from error
        return PriceSnapshot(
            asset=source_config.asset,
            quote_currency=source_config.quote_currency,
            price=numeric_price,
            source=source_config.provider,
        )


def build_price_provider(settings: AppSettings) -> PriceProvider:
    if (
        settings.runtime.environment != "live"
        or not settings.live.rollout.enforce_balance_preflight
    ):
        return NullPriceProvider()
    return HttpPriceProvider(settings)


def _http_json_get_transport(url: str, timeout_seconds: float) -> dict[str, Any]:
    last_error: Exception | None = None
    for http2_enabled in (True, False):
        for attempt in range(3):
            try:
                with httpx.Client(http2=http2_enabled, timeout=timeout_seconds) as client:
                    response = client.get(url)
                    response.raise_for_status()
                    payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("invalid_live_price_response")
                return payload
            except httpx.HTTPStatusError as error:
                body_preview = error.response.text[:200]
                raise ValueError(
                    f"invalid_live_price_response_status:{error.response.status_code}:{body_preview}"
                ) from error
            except (httpx.TimeoutException, httpx.TransportError, json.JSONDecodeError) as error:
                last_error = error
                if attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                break
    if last_error is not None:
        raise URLError(str(last_error)) from last_error
    raise URLError("live_price_transport_error")