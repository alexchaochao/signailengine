from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

import httpx

from core.config import JupiterQuoteSourceConfig


JupiterTransport = Callable[
    [str, str, dict[str, Any] | None, dict[str, Any] | None, float],
    dict[str, Any],
]


@dataclass(frozen=True)
class JupiterQuoteState:
    input_mint: str
    output_mint: str
    in_amount_atomic: int
    out_amount_atomic: int
    price_impact_pct: float
    raw_quote: dict[str, Any]


class JupiterSwapClient:
    def __init__(
        self,
        config: JupiterQuoteSourceConfig,
        transport: JupiterTransport | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.config = config
        self.transport = transport or _http_json_transport
        self.timeout_seconds = timeout_seconds

    def quote(
        self,
        *,
        input_mint: str,
        output_mint: str,
        amount_atomic: int,
        slippage_bps: int,
    ) -> JupiterQuoteState:
        payload = self.transport(
            "GET",
            self.config.quote_url,
            {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_atomic),
                "slippageBps": str(slippage_bps),
                "swapMode": "ExactIn",
            },
            None,
            self.timeout_seconds,
        )
        in_amount = _require_int(payload.get("inAmount"), "invalid_jupiter_quote_in_amount")
        out_amount = _require_int(payload.get("outAmount"), "invalid_jupiter_quote_out_amount")
        return JupiterQuoteState(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount_atomic=in_amount,
            out_amount_atomic=out_amount,
            price_impact_pct=_require_float(
                payload.get("priceImpactPct", 0.0),
                "invalid_jupiter_quote_price_impact",
            ),
            raw_quote=payload,
        )

    def swap_transaction(
        self,
        *,
        quote_response: dict[str, Any],
        user_public_key: str,
    ) -> str:
        payload = self.transport(
            "POST",
            self.config.swap_url,
            None,
            {
                "userPublicKey": user_public_key,
                "wrapAndUnwrapSol": True,
                "dynamicComputeUnitLimit": True,
                "prioritizationFeeLamports": "auto",
                "quoteResponse": quote_response,
            },
            self.timeout_seconds,
        )
        swap_transaction = payload.get("swapTransaction")
        if not isinstance(swap_transaction, str) or not swap_transaction:
            raise ValueError("invalid_jupiter_swap_transaction")
        return swap_transaction

    def price_usd(self, mint: str) -> float:
        payload = self.transport(
            "GET",
            self.config.price_url,
            {"ids": mint},
            None,
            self.timeout_seconds,
        )
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("invalid_jupiter_price_response")
        entry = data.get(mint)
        if not isinstance(entry, dict):
            raise ValueError("invalid_jupiter_price_response")
        return _require_float(entry.get("price"), "invalid_jupiter_price_response")


def _require_int(value: Any, error: str) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(error)


def _require_float(value: Any, error: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(error) from exc
    raise ValueError(error)


def _http_json_transport(
    method: str,
    url: str,
    params: dict[str, Any] | None,
    json_body: dict[str, Any] | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for http2_enabled in (True, False):
        for attempt in range(3):
            try:
                with httpx.Client(http2=http2_enabled, timeout=timeout_seconds) as client:
                    response = client.request(method, url, params=params, json=json_body)
                    response.raise_for_status()
                    payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("invalid_jupiter_response")
                return payload
            except httpx.HTTPStatusError as exc:
                body_preview = exc.response.text[:200]
                raise ValueError(
                    f"jupiter_http_error:{exc.response.status_code}:{body_preview}"
                ) from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                break
    raise ValueError("jupiter_transport_error") from last_error