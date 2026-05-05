from __future__ import annotations

from typing import Any
from urllib.error import URLError

import pytest

from core.config import AppSettings
from portfolio.price_provider import HttpPriceProvider


def _transport(payload: dict[str, Any] | Exception) -> Any:
    def transport(url: str, timeout_seconds: float) -> dict[str, Any]:
        _ = url, timeout_seconds
        if isinstance(payload, Exception):
            raise payload
        return payload

    return transport


def test_http_price_provider_returns_solana_usd_price() -> None:
    provider = HttpPriceProvider(
        AppSettings.load(),
        transport=_transport({"solana": {"usd": 145.25}}),
    )

    snapshot = provider.get_native_token_price_usd("solana")

    assert snapshot is not None
    assert snapshot.asset == "SOL"
    assert snapshot.quote_currency == "USD"
    assert snapshot.price == 145.25
    assert snapshot.source == "coingecko_simple_price"


def test_http_price_provider_supports_exchange_ticker_source() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "pricing": AppSettings.load().live.pricing.model_copy(
                        update={
                            "native_asset_sources": {
                                "ethereum": {
                                    "provider": "binance_ticker_price",
                                    "asset": "ETH",
                                    "quote_currency": "USD",
                                    "lookup_key": "ETHUSDT",
                                    "url": "https://prices.example/eth-usd",
                                }
                            }
                        }
                    )
                }
            )
        }
    )
    provider = HttpPriceProvider(
        settings,
        transport=_transport({"symbol": "ETHUSDT", "price": "3125.55"}),
    )

    snapshot = provider.get_native_token_price_usd("ethereum")

    assert snapshot is not None
    assert snapshot.asset == "ETH"
    assert snapshot.quote_currency == "USD"
    assert snapshot.price == 3125.55
    assert snapshot.source == "binance_ticker_price"


def test_http_price_provider_retries_and_raises_provider_error() -> None:
    provider = HttpPriceProvider(
        AppSettings.load().model_copy(
            update={
                "live": AppSettings.load().live.model_copy(
                    update={
                        "pricing": AppSettings.load().live.pricing.model_copy(
                            update={"max_retries": 0}
                        )
                    }
                )
            }
        ),
        transport=_transport(URLError("down")),
    )

    with pytest.raises(ValueError, match="live_price_provider_error"):
        provider.get_native_token_price_usd("solana")


def test_http_price_provider_rejects_malformed_payload() -> None:
    provider = HttpPriceProvider(
        AppSettings.load(),
        transport=_transport({"solana": {"usd": "bad"}}),
    )

    with pytest.raises(ValueError, match="invalid_live_price_response"):
        provider.get_native_token_price_usd("solana")


def test_http_price_provider_returns_none_for_unconfigured_chain() -> None:
    provider = HttpPriceProvider(
        AppSettings.load(),
        transport=_transport({"solana": {"usd": 145.25}}),
    )

    assert provider.get_native_token_price_usd("base") is None