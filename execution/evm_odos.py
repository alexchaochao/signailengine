from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from core.config import AppSettings, EvmLiveSourceConfig
from sentinel.onchain_live_sources import (
    EvmQuoteHttpTransport,
    _evm_quote_headers,
    _extract_evm_quote_price,
    _http_json_get_transport,
    _http_json_request_transport,
)

PriceHttpTransport = Callable[[str, dict[str, str], float], dict[str, Any]]


@dataclass(frozen=True)
class EvmOdosQuoteState:
    path_id: str
    out_amount_atomic: int
    raw_quote: dict[str, Any]


@dataclass(frozen=True)
class EvmAssembledTransaction:
    transaction: dict[str, Any]
    raw_response: dict[str, Any]


class OdosSwapClient:
    def __init__(
        self,
        settings: AppSettings,
        route: EvmLiveSourceConfig,
        *,
        quote_transport: EvmQuoteHttpTransport | None = None,
        price_transport: PriceHttpTransport | None = None,
        assemble_transport: EvmQuoteHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.route = route
        self.quote_transport = quote_transport or _http_json_request_transport
        self.price_transport = price_transport or _http_json_get_transport
        self.assemble_transport = assemble_transport or _http_json_request_transport

    def quote(
        self,
        *,
        wallet_address: str,
        sell_token: str,
        buy_token: str,
        sell_amount_atomic: int,
        slippage_bps: int,
    ) -> EvmOdosQuoteState:
        payload = self.quote_transport(
            self.route.quote_api_url,
            _evm_quote_headers(self.settings, self.route.api_provider),
            self.settings.live.pricing.timeout_seconds,
            "POST",
            {
                "chainId": self.route.chain_id,
                "inputTokens": [
                    {
                        "tokenAddress": sell_token,
                        "amount": str(sell_amount_atomic),
                    }
                ],
                "outputTokens": [
                    {
                        "tokenAddress": buy_token,
                        "proportion": 1,
                    }
                ],
                "slippageLimitPercent": slippage_bps / 100.0,
                "userAddr": wallet_address,
                "disableRFQs": True,
                "compact": True,
            },
        )
        path_id = payload.get("pathId")
        out_amounts = payload.get("outAmounts")
        if not isinstance(path_id, str):
            raise ValueError("invalid_odos_quote_path_id")
        if not isinstance(out_amounts, list) or not out_amounts or not isinstance(out_amounts[0], str):
            raise ValueError("invalid_odos_quote_out_amounts")
        return EvmOdosQuoteState(
            path_id=path_id,
            out_amount_atomic=int(out_amounts[0]),
            raw_quote=payload,
        )

    def assemble(self, *, path_id: str, wallet_address: str) -> EvmAssembledTransaction:
        payload = self.assemble_transport(
            _assemble_url(self.route.quote_api_url),
            _evm_quote_headers(self.settings, self.route.api_provider),
            self.settings.live.pricing.timeout_seconds,
            "POST",
            {
                "userAddr": wallet_address,
                "pathId": path_id,
                "simulate": False,
            },
        )
        transaction = payload.get("transaction")
        if not isinstance(transaction, dict):
            raise ValueError("invalid_odos_assemble_transaction")
        return EvmAssembledTransaction(transaction=transaction, raw_response=payload)

    def token_price_usd(self, token_contract: str) -> float:
        payload = self.price_transport(
            f"{self.route.price_url}/{token_contract}",
            {},
            self.settings.live.pricing.timeout_seconds,
        )
        return _extract_evm_quote_price(
            payload,
            provider=self.route.api_provider,
            chain=self.route.chain,
            token_contract=token_contract,
        )


def _assemble_url(quote_url: str) -> str:
    if "/quote" in quote_url:
        return quote_url.rsplit("/quote", 1)[0] + "/assemble"
    return quote_url.rstrip("/") + "/assemble"