from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from urllib import parse

import httpx
from redis import Redis

from core.config import (
    AcquisitionConfig,
    AppSettings,
    EvmLiveSourceConfig,
    resolve_evm_route_config,
    resolve_evm_routes,
    EvmTransferTradeSourceConfig,
    JupiterQuoteSourceConfig,
    LiveConfig,
    SolanaWalletTradeSourceConfig,
    VenueConfig,
)
from core.schemas import EventEnvelope, MeasurementProfile
from infra.redis_stream import acknowledge_message, ensure_consumer_group, read_group_models
from execution.evm_rpc import _http_json_rpc_transport as _http_evm_json_rpc_transport
from execution.solana_rpc import SolanaHttpTransportResponse, _http_json_rpc_transport

JupiterHttpTransport = Callable[[str, dict[str, str], float], dict[str, Any]]
SolanaRpcTransport = Callable[[str, dict[str, Any], float], SolanaHttpTransportResponse]
EvmQuoteHttpTransport = Callable[
    [str, dict[str, str], float, str, dict[str, Any] | None],
    dict[str, Any],
]
TRANSFER_EVENT_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55aeb0b5a2b88"
)
SWAP_EVENT_TOPIC_UNISWAP_V2 = (
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
)
SWAP_EVENT_TOPIC_UNISWAP_V3 = (
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
)
SUPPORTED_V2_SWAP_PROTOCOLS = frozenset({"uniswap_v2", "aerodrome", "velodrome"})
SUPPORTED_V3_SWAP_PROTOCOLS = frozenset({"uniswap_v3", "aerodrome_cl", "velodrome_cl"})


@dataclass(frozen=True)
class EvmQuoteRequest:
    method: str
    url: str
    body: dict[str, Any] | None
    provider_label: str


@dataclass(frozen=True)
class SourceTradeRecord:
    cursor: str
    observed_at: datetime
    payload: dict[str, Any]


class JupiterQuoteSource:
    def __init__(
        self,
        settings: AppSettings,
        config: JupiterQuoteSourceConfig,
        *,
        quote_transport: JupiterHttpTransport | None = None,
        price_transport: JupiterHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.quote_transport = quote_transport or _http_json_get_transport
        self.price_transport = price_transport or _http_json_get_transport

    def fetch_quotes(self) -> list[dict[str, Any]]:
        if not self.config.input_mint or not self.config.output_mint:
            raise ValueError("missing_jupiter_source_mints")
        quoted_at = datetime.now(UTC)
        amount = int(round(self.config.quote_notional_usd * (10**self.config.input_decimals)))
        query = parse.urlencode(
            {
                "inputMint": self.config.input_mint,
                "outputMint": self.config.output_mint,
                "amount": str(amount),
                "slippageBps": str(self.config.slippage_bps),
            }
        )
        headers = _jupiter_headers(self.settings)
        quote_payload = self.quote_transport(
            f"{self.config.quote_url}?{query}",
            headers,
            self.settings.live.pricing.timeout_seconds,
        )
        price_payload = self.price_transport(
            f"{self.config.price_url}?{parse.urlencode({'ids': self.config.output_mint})}",
            headers,
            self.settings.live.pricing.timeout_seconds,
        )
        out_amount = float(_require_string_field(quote_payload, "outAmount"))
        out_tokens = out_amount / float(10**self.config.output_decimals)
        token_price_usd = _extract_jupiter_price(price_payload, self.config.output_mint)
        context_slot = int(quote_payload.get("contextSlot", 0))
        route_plan = quote_payload.get("routePlan", [])
        return [
            {
                "chain": self.config.chain,
                "token": self.config.token,
                "quote_request_id": (
                    f"jupiter:{self.config.output_mint}:{context_slot}:{quoted_at.isoformat()}"
                ),
                "quote_notional_usd": self.config.quote_notional_usd,
                "expected_out_usd": round(out_tokens * token_price_usd, 6),
                "reference_mid_usd": self.config.quote_notional_usd,
                "route_summary": {
                    "provider": "jupiter",
                    "hops": len(route_plan) if isinstance(route_plan, list) else 0,
                    "context_slot": context_slot,
                },
                "quoted_at": quoted_at,
            }
        ]


class SolanaWalletTradeSource:
    def __init__(
        self,
        settings: AppSettings,
        config: SolanaWalletTradeSourceConfig,
        *,
        transport: SolanaRpcTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or _http_json_rpc_transport

    def fetch_trades(self, last_cursor: str | None = None) -> list[SourceTradeRecord]:
        if not self.signature_address or not self.config.token_mint or not self.config.quote_mint:
            raise ValueError("missing_solana_wallet_trade_source_config")
        signatures = self._fetch_signatures(last_cursor)
        records: list[SourceTradeRecord] = []
        for signature in reversed(signatures):
            transaction_payload = self._fetch_transaction(signature)
            normalized = self._normalize_transaction(signature, transaction_payload)
            if normalized is not None:
                records.append(normalized)
        return records

    def _fetch_signatures(self, last_cursor: str | None) -> list[str]:
        params: dict[str, Any] = {
            "limit": self.config.poll_limit,
            "commitment": "confirmed",
        }
        if last_cursor:
            params["until"] = last_cursor
        payload = self._dispatch(
            method="getSignaturesForAddress",
            params=[self.signature_address, params],
            request_id="solana-wallet-signatures",
        )
        if not isinstance(payload, list):
            raise ValueError("invalid_solana_wallet_signature_response")
        return [
            str(item["signature"])
            for item in payload
            if isinstance(item, dict) and isinstance(item.get("signature"), str)
        ]

    def _fetch_transaction(self, signature: str) -> dict[str, Any]:
        payload = self._dispatch(
            method="getTransaction",
            params=[
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
            request_id=f"solana-wallet-tx:{signature}",
        )
        if not isinstance(payload, dict):
            raise ValueError("invalid_solana_wallet_transaction_response")
        return payload

    def _normalize_transaction(
        self,
        signature: str,
        payload: dict[str, Any],
    ) -> SourceTradeRecord | None:
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            return None
        owner = self.owner_address
        if owner is not None:
            token_delta = _owner_mint_delta(
                meta.get("preTokenBalances"),
                meta.get("postTokenBalances"),
                owner,
                str(self.config.token_mint),
            )
            quote_delta = _owner_mint_delta(
                meta.get("preTokenBalances"),
                meta.get("postTokenBalances"),
                owner,
                str(self.config.quote_mint),
            )
        else:
            token_delta = _mint_delta(
                meta.get("preTokenBalances"),
                meta.get("postTokenBalances"),
                str(self.config.token_mint),
            )
            quote_delta = _mint_delta(
                meta.get("preTokenBalances"),
                meta.get("postTokenBalances"),
                str(self.config.quote_mint),
            )
        if token_delta == 0.0 or quote_delta == 0.0:
            return None
        if token_delta > 0 and quote_delta < 0:
            side = "buy"
        elif token_delta < 0 and quote_delta > 0:
            side = "sell"
        else:
            return None
        block_time = payload.get("blockTime")
        observed_at = (
            datetime.fromtimestamp(int(block_time), tz=UTC)
            if isinstance(block_time, int)
            else datetime.now(UTC)
        )
        trade_payload = {
            "chain": self.config.chain,
            "tx_hash": signature,
            "log_index": 0,
            "slot": int(payload.get("slot", 0)),
            "pool_address": self.config.pool_address or self.signature_address,
            "wallet_address": owner
            or _infer_primary_owner(
                meta.get("preTokenBalances"),
                meta.get("postTokenBalances"),
                str(self.config.token_mint),
            ),
            "token": self.config.token,
            "quote_asset": self.config.quote_asset,
            "token_amount": round(abs(token_delta), 12),
            "quote_amount": round(abs(quote_delta), 12),
            "quote_amount_usd": round(abs(quote_delta) * self.config.quote_asset_usd_rate, 6),
            "side": side,
            "route_hint": f"solana_rpc_{self.config.source_kind}_watch",
            "observed_at": observed_at,
        }
        return SourceTradeRecord(cursor=signature, observed_at=observed_at, payload=trade_payload)

    @property
    def signature_address(self) -> str | None:
        return self.config.signature_address or self.config.wallet_address

    @property
    def owner_address(self) -> str | None:
        return self.config.owner_address or self.config.wallet_address

    def _dispatch(self, *, method: str, params: list[Any], request_id: str) -> Any:
        response = self.transport(
            self.settings.venues.solana_rpc_url,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
            self.settings.venues.solana_rpc_timeout_seconds,
        )
        payload = response.payload
        if payload.get("id") != request_id:
            raise ValueError("invalid_solana_wallet_response_id")
        if "error" in payload:
            raise ValueError("solana_wallet_source_rpc_error")
        if "result" not in payload:
            raise ValueError("invalid_solana_wallet_rpc_response")
        return payload["result"]

class EvmTransferTradeSource:
    def __init__(
        self,
        settings: AppSettings,
        config: EvmTransferTradeSourceConfig | EvmLiveSourceConfig,
        *,
        transport: SolanaRpcTransport | None = None,
    ) -> None:
        self.settings = settings
        if isinstance(config, EvmLiveSourceConfig):
            acquisition = AcquisitionConfig.model_validate(settings.acquisition)
            self.config = resolve_evm_route_config(acquisition, config)
        else:
            self.config = config
        self.transport = transport or _http_evm_json_rpc_transport
        venue_settings = VenueConfig.model_validate(settings.venues)
        raw_chain_config = venue_settings.native_asset_rpc.get(self.config.chain)
        if raw_chain_config is None:
            raise ValueError(f"unsupported_evm_rpc_chain:{self.config.chain}")
        self.rpc_url = raw_chain_config.url if hasattr(raw_chain_config, "url") else raw_chain_config["url"]
        self.timeout_seconds = (
            raw_chain_config.timeout_seconds
            if hasattr(raw_chain_config, "timeout_seconds")
            else raw_chain_config.get("timeout_seconds", 5.0)
        )

    def fetch_trades(self, last_cursor: str | None = None) -> list[SourceTradeRecord]:
        if (
            not self.config.wallet_address
            or not self.config.token_contract
            or not self.config.quote_contract
        ):
            raise ValueError("missing_evm_transfer_trade_source_config")
        latest_block = self._block_number()
        start_block = max(0, latest_block - self.config.initial_lookback_blocks + 1)
        if last_cursor is not None:
            start_block = int(last_cursor) + 1
        if start_block > latest_block:
            return []
        token_logs = self._get_transfer_logs(
            self.config.token_contract,
            start_block=start_block,
            end_block=latest_block,
        )
        quote_logs = self._get_transfer_logs(
            self.config.quote_contract,
            start_block=start_block,
            end_block=latest_block,
        )
        logs_by_tx: dict[str, dict[str, Any]] = {}
        for log in token_logs:
            tx_hash = str(log.get("transactionHash", "")).lower()
            if not tx_hash:
                continue
            entry = logs_by_tx.setdefault(tx_hash, {"token": [], "quote": [], "block_number": 0})
            entry["token"].append(log)
            entry["block_number"] = max(entry["block_number"], _hex_to_int(log.get("blockNumber")))
        for log in quote_logs:
            tx_hash = str(log.get("transactionHash", "")).lower()
            if not tx_hash:
                continue
            entry = logs_by_tx.setdefault(tx_hash, {"token": [], "quote": [], "block_number": 0})
            entry["quote"].append(log)
            entry["block_number"] = max(entry["block_number"], _hex_to_int(log.get("blockNumber")))

        timestamps: dict[int, datetime] = {}
        records: list[SourceTradeRecord] = []
        for tx_hash, grouped in sorted(logs_by_tx.items(), key=lambda item: item[1]["block_number"]):
            token_delta = _evm_logs_delta(
                grouped["token"],
                wallet_address=str(self.config.wallet_address),
                decimals=self.config.token_decimals,
            )
            quote_delta = _evm_logs_delta(
                grouped["quote"],
                wallet_address=str(self.config.wallet_address),
                decimals=self.config.quote_decimals,
            )
            if token_delta == 0.0 or quote_delta == 0.0:
                continue
            if token_delta > 0 and quote_delta < 0:
                side = "buy"
            elif token_delta < 0 and quote_delta > 0:
                side = "sell"
            else:
                continue
            block_number = int(grouped["block_number"])
            observed_at = timestamps.get(block_number)
            if observed_at is None:
                observed_at = self._block_timestamp(block_number)
                timestamps[block_number] = observed_at
            records.append(
                SourceTradeRecord(
                    cursor=str(block_number),
                    observed_at=observed_at,
                    payload={
                        "chain": self.config.chain,
                        "tx_hash": tx_hash,
                        "log_index": 0,
                        "slot": block_number,
                        "pool_address": self.config.pool_address,
                        "wallet_address": self.config.wallet_address,
                        "token": self.config.token,
                        "quote_asset": self.config.quote_asset,
                        "token_amount": round(abs(token_delta), 12),
                        "quote_amount": round(abs(quote_delta), 12),
                        "quote_amount_usd": round(
                            abs(quote_delta) * self.config.quote_asset_usd_rate,
                            6,
                        ),
                        "side": side,
                        "route_hint": "evm_rpc_transfer_watch",
                        "observed_at": observed_at,
                    },
                )
            )
        return records

    def _block_number(self) -> int:
        result = self._dispatch("eth_blockNumber", [], "evm-block-number")
        return _hex_to_int(result)

    def _block_timestamp(self, block_number: int) -> datetime:
        payload = self._dispatch(
            "eth_getBlockByNumber",
            [_to_hex(block_number), False],
            f"evm-block:{block_number}",
        )
        if not isinstance(payload, dict):
            raise ValueError("invalid_evm_block_response")
        return datetime.fromtimestamp(_hex_to_int(payload.get("timestamp")), tz=UTC)

    def _get_transfer_logs(
        self,
        contract_address: str,
        *,
        start_block: int,
        end_block: int,
    ) -> list[dict[str, Any]]:
        payload = self._dispatch(
            "eth_getLogs",
            [
                {
                    "fromBlock": _to_hex(start_block),
                    "toBlock": _to_hex(end_block),
                    "address": contract_address,
                    "topics": [TRANSFER_EVENT_TOPIC],
                }
            ],
            f"evm-logs:{contract_address}:{start_block}:{end_block}",
        )
        if not isinstance(payload, list):
            raise ValueError("invalid_evm_logs_response")
        return [entry for entry in payload if isinstance(entry, dict)]

    def _dispatch(self, method: str, params: list[Any], request_id: str) -> Any:
        response = self.transport(
            self.rpc_url,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
            self.timeout_seconds,
        )
        payload = response.payload
        if payload.get("id") != request_id:
            raise ValueError("invalid_evm_source_response_id")
        if "error" in payload:
            raise ValueError("evm_source_rpc_error")
        if "result" not in payload:
            raise ValueError("invalid_evm_source_response")
        return payload["result"]


class EvmPoolSwapTradeSource:
    def __init__(
        self,
        settings: AppSettings,
        config: EvmLiveSourceConfig,
        *,
        transport: SolanaRpcTransport | None = None,
    ) -> None:
        self.settings = settings
        acquisition = AcquisitionConfig.model_validate(settings.acquisition)
        self.config = resolve_evm_route_config(acquisition, config)
        self.transport = transport or _http_evm_json_rpc_transport
        venue_settings = VenueConfig.model_validate(settings.venues)
        raw_chain_config = venue_settings.native_asset_rpc.get(self.config.chain)
        if raw_chain_config is None:
            raise ValueError(f"unsupported_evm_rpc_chain:{self.config.chain}")
        self.rpc_url = raw_chain_config.url
        self.timeout_seconds = raw_chain_config.timeout_seconds

    def fetch_trades(self, last_cursor: str | None = None) -> list[SourceTradeRecord]:
        if not self.config.pool_address:
            raise ValueError("missing_evm_pool_swap_source_config")
        latest_block = self._block_number()
        start_block = max(0, latest_block - self.config.initial_lookback_blocks + 1)
        if last_cursor is not None:
            start_block = int(last_cursor) + 1
        if start_block > latest_block:
            return []
        logs = self._get_swap_logs(start_block=start_block, end_block=latest_block)
        timestamps: dict[int, datetime] = {}
        records: list[SourceTradeRecord] = []
        for log in logs:
            block_number = _hex_to_int(log.get("blockNumber"))
            observed_at = timestamps.get(block_number)
            if observed_at is None:
                observed_at = self._block_timestamp(block_number)
                timestamps[block_number] = observed_at
            normalized = self._normalize_swap_log(log, observed_at)
            if normalized is not None:
                records.append(normalized)
        return records

    def _normalize_swap_log(
        self,
        log: dict[str, Any],
        observed_at: datetime,
    ) -> SourceTradeRecord | None:
        diagnostics: dict[str, Any] = {}
        normalized: tuple[str, int, int] | None
        if self.config.pool_protocol in SUPPORTED_V2_SWAP_PROTOCOLS:
            normalized = self._normalize_v2_swap_log(log)
        elif self.config.pool_protocol in SUPPORTED_V3_SWAP_PROTOCOLS:
            normalized, diagnostics = self._normalize_v3_swap_log(log)
        else:
            normalized = None
        if normalized is None:
            return None
        side, token_amount_raw, quote_amount_raw = normalized
        token_amount = token_amount_raw / float(10**self.config.token_decimals)
        quote_amount = quote_amount_raw / float(10**self.config.quote_decimals)
        tx_hash = str(log.get("transactionHash", "")).lower()
        log_index = _hex_to_int(log.get("logIndex", "0x0"))
        return SourceTradeRecord(
            cursor=str(_hex_to_int(log.get("blockNumber"))),
            observed_at=observed_at,
            payload={
                "chain": self.config.chain,
                "tx_hash": tx_hash,
                "log_index": log_index,
                "slot": _hex_to_int(log.get("blockNumber")),
                "pool_address": str(self.config.pool_address),
                "wallet_address": None,
                "token": self.config.token,
                "quote_asset": self.config.quote_asset,
                "token_amount": round(token_amount, 12),
                "quote_amount": round(quote_amount, 12),
                "quote_amount_usd": round(quote_amount * self.config.quote_asset_usd_rate, 6),
                "side": side,
                "route_hint": f"evm_{self.config.pool_protocol}_swap_watch",
                "route_diagnostics": diagnostics,
                "observed_at": observed_at,
            },
        )

    def _normalize_v2_swap_log(self, log: dict[str, Any]) -> tuple[str, int, int] | None:
        values = _decode_uint256_words(str(log.get("data", "")), expected_words=4)
        if values is None:
            return None
        amount0_in, amount1_in, amount0_out, amount1_out = values
        if self.config.token_is_token0:
            token_in, token_out = amount0_in, amount0_out
            quote_in, quote_out = amount1_in, amount1_out
        else:
            token_in, token_out = amount1_in, amount1_out
            quote_in, quote_out = amount0_in, amount0_out
        if token_out > 0 and quote_in > 0:
            return ("buy", token_out, quote_in)
        if token_in > 0 and quote_out > 0:
            return ("sell", token_in, quote_out)
        return None

    def _normalize_v3_swap_log(
        self, log: dict[str, Any]
    ) -> tuple[tuple[str, int, int] | None, dict[str, Any]]:
        values = _decode_signed_int256_words(str(log.get("data", "")), expected_words=5)
        if values is None:
            return None, {}
        amount0, amount1 = values[0], values[1]
        sqrt_price_x96, liquidity, tick = values[2], values[3], values[4]
        token_pool_delta = amount0 if self.config.token_is_token0 else amount1
        quote_pool_delta = amount1 if self.config.token_is_token0 else amount0
        diagnostics = {
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": liquidity,
            "tick": tick,
        }
        if token_pool_delta < 0 and quote_pool_delta > 0:
            return ("buy", abs(token_pool_delta), abs(quote_pool_delta)), diagnostics
        if token_pool_delta > 0 and quote_pool_delta < 0:
            return ("sell", abs(token_pool_delta), abs(quote_pool_delta)), diagnostics
        return None, diagnostics

    def _block_number(self) -> int:
        result = self._dispatch("eth_blockNumber", [], "evm-swap-block-number")
        return _hex_to_int(result)

    def _block_timestamp(self, block_number: int) -> datetime:
        payload = self._dispatch(
            "eth_getBlockByNumber",
            [_to_hex(block_number), False],
            f"evm-swap-block:{block_number}",
        )
        if not isinstance(payload, dict):
            raise ValueError("invalid_evm_block_response")
        return datetime.fromtimestamp(_hex_to_int(payload.get("timestamp")), tz=UTC)

    def _get_swap_logs(self, *, start_block: int, end_block: int) -> list[dict[str, Any]]:
        topic = _swap_event_topic(self.config.pool_protocol)
        if topic is None:
            raise ValueError("unsupported_evm_pool_protocol")
        payload = self._dispatch(
            "eth_getLogs",
            [
                {
                    "fromBlock": _to_hex(start_block),
                    "toBlock": _to_hex(end_block),
                    "address": self.config.pool_address,
                    "topics": [topic],
                }
            ],
            f"evm-swap-logs:{self.config.pool_address}:{start_block}:{end_block}",
        )
        if not isinstance(payload, list):
            raise ValueError("invalid_evm_swap_logs_response")
        return [entry for entry in payload if isinstance(entry, dict)]

    def _dispatch(self, method: str, params: list[Any], request_id: str) -> Any:
        response = self.transport(
            self.rpc_url,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
            self.timeout_seconds,
        )
        payload = response.payload
        if payload.get("id") != request_id:
            raise ValueError("invalid_evm_source_response_id")
        if "error" in payload:
            raise ValueError("evm_source_rpc_error")
        if "result" not in payload:
            raise ValueError("invalid_evm_source_response")
        return payload["result"]


class EvmQuoteSource:
    def __init__(
        self,
        settings: AppSettings,
        config: EvmLiveSourceConfig,
        *,
        quote_transport: EvmQuoteHttpTransport | None = None,
        price_transport: JupiterHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        acquisition = AcquisitionConfig.model_validate(settings.acquisition)
        self.config = resolve_evm_route_config(acquisition, config)
        self.quote_transport = quote_transport or _http_json_request_transport
        self.price_transport = price_transport or _http_json_get_transport

    def fetch_quotes(self) -> list[dict[str, Any]]:
        if not self.config.token_contract or not self.config.quote_contract:
            raise ValueError("missing_evm_quote_source_config")
        live_settings = LiveConfig.model_validate(self.settings.live)
        quoted_at = datetime.now(UTC)
        quote_request = _build_evm_quote_request(self.config)
        headers = _evm_quote_headers(self.settings, self.config.api_provider)
        quote_payload = self.quote_transport(
            quote_request.url,
            headers,
            live_settings.pricing.timeout_seconds,
            quote_request.method,
            quote_request.body,
        )
        price_payload = self.price_transport(
            f"{self.config.price_url}/{self.config.token_contract}",
            {},
            live_settings.pricing.timeout_seconds,
        )
        buy_amount, route_summary = _parse_evm_quote_response(
            quote_payload,
            self.config,
            provider_label=quote_request.provider_label,
        )
        route_summary = _merge_market_context(
            route_summary,
            _extract_dexscreener_market_context(
                price_payload,
                chain=self.config.chain,
                token_contract=str(self.config.token_contract),
                quote_contract=str(self.config.quote_contract),
            ),
        )
        token_amount = buy_amount / float(10**self.config.token_decimals)
        token_price_usd = _extract_evm_quote_price(
            price_payload,
            provider=self.config.api_provider,
            chain=self.config.chain,
            token_contract=str(self.config.token_contract),
        )
        return [
            {
                "chain": self.config.chain,
                "token": self.config.token,
                "quote_request_id": (
                    f"{self.config.api_provider}:{self.config.token_contract}:{self.config.chain_id}:{quoted_at.isoformat()}"
                ),
                "quote_notional_usd": self.config.quote_notional_usd,
                "expected_out_usd": round(token_amount * token_price_usd, 6),
                "reference_mid_usd": self.config.quote_notional_usd,
                "route_summary": route_summary,
                "quoted_at": quoted_at,
            }
        ]



class MeasurementProfileRegistry:
    """Registry of temporary on-chain measurement profiles.

    Profiles are registered from discovery events (launch/catalyst candidates)
    and expire after *ttl_seconds*.  When a *redis_client* is provided, profiles
    are persisted to Redis so they survive worker restarts and are shared across
    workers.  Without Redis the registry operates purely in memory (test mode).
    """

    REDIS_KEY_PREFIX = "measurement:profile:"

    def __init__(
        self,
        default_ttl_seconds: float = 3600.0,
        *,
        redis_client: Redis | None = None,
        redis_key_prefix: str = REDIS_KEY_PREFIX,
    ) -> None:
        self._profiles: dict[str, MeasurementProfile] = {}
        self._default_ttl = default_ttl_seconds
        self._redis = redis_client
        self._key_prefix = redis_key_prefix
        self._loaded_from_redis = False

    # ── public API ────────────────────────────────────────────────────────

    def register(self, profile: MeasurementProfile) -> None:
        key = f"{profile.chain}:{profile.token}"
        existing = self._profiles.get(key)
        if existing is not None and existing.profile_id == profile.profile_id:
            return  # already registered
        self._profiles[key] = profile
        if self._redis is not None:
            self._redis.setex(
                self._redis_key(key),
                int(profile.ttl_seconds),
                profile.model_dump_json(),
            )

    def get(self, chain: str, token: str) -> MeasurementProfile | None:
        self._lazy_load_from_redis()
        profile = self._profiles.get(f"{chain}:{token}")
        if profile is None:
            return None
        if self._is_stale(profile):
            self._remove(chain, token)
            return None
        return profile

    def active_profiles(self) -> list[MeasurementProfile]:
        self._lazy_load_from_redis()
        now = datetime.now(UTC)
        active: list[MeasurementProfile] = []
        stale_keys: list[str] = []
        for key, profile in self._profiles.items():
            if (now - profile.registered_at).total_seconds() > profile.ttl_seconds:
                stale_keys.append(key)
            else:
                active.append(profile)
        for raw_key in stale_keys:
            chain, token = raw_key.split(":", 1)
            self._remove(chain, token)
        return active

    def cleanup(self) -> None:
        self.active_profiles()  # side-effect: removes stale entries

    def count(self) -> int:
        return len(self.active_profiles())

    # ── Redis persistence ─────────────────────────────────────────────────

    def _lazy_load_from_redis(self) -> None:
        if self._redis is None or self._loaded_from_redis:
            return
        self._loaded_from_redis = True
        cursor = 0
        pattern = f"{self._key_prefix}*"
        stale_redis_keys: list[str] = []
        while True:
            cursor, keys = self._redis.scan(cursor, match=pattern, count=100)
            for redis_key in keys:
                raw = self._redis.get(redis_key)
                if raw is None:
                    continue
                try:
                    data = json.loads(raw)
                    profile = MeasurementProfile.model_validate(data)
                    map_key = f"{profile.chain}:{profile.token}"
                    if self._is_stale(profile):
                        stale_redis_keys.append(redis_key)
                    elif map_key not in self._profiles:
                        self._profiles[map_key] = profile
                except (json.JSONDecodeError, Exception):
                    stale_redis_keys.append(redis_key)
            if cursor == 0:
                break
        for stale_key in stale_redis_keys:
            self._redis.delete(stale_key)

    def _redis_key(self, profile_key: str) -> str:
        return f"{self._key_prefix}{profile_key}"

    def _remove(self, chain: str, token: str) -> None:
        map_key = f"{chain}:{token}"
        self._profiles.pop(map_key, None)
        if self._redis is not None:
            self._redis.delete(self._redis_key(map_key))

    @staticmethod
    def _is_stale(profile: MeasurementProfile) -> bool:
        return (datetime.now(UTC) - profile.registered_at).total_seconds() > profile.ttl_seconds


DISCOVERY_EVENTS_FOR_MEASUREMENT = frozenset(
    {"alpha.launch_candidate", "alpha.catalyst_candidate"}
)
MEASUREMENT_CONSUMER_GROUP = "onchain-measurement"
MEASUREMENT_CONSUMER_NAME = "onchain-measurement-1"


def resolve_token_addresses_for_discovery(
    chain: str,
    token: str,
    *,
    http_get: Callable[[str, dict[str, str], float], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve on-chain addresses for a token symbol using DexScreener search.

    Returns a dict with keys that can be merged into a ``MeasurementProfile``:
    ``pool_address``, ``token_mint``/``token_contract``, ``quote_mint``/``quote_contract``,
    ``dex``, ``chain_type``.  Returns an empty dict when resolution fails.
    """
    transport = http_get or _http_json_get_transport
    try:
        payload = transport(
            f"https://api.dexscreener.com/latest/dex/search?q={parse.quote(token)}",
            {},
            5.0,
        )
    except Exception:
        return {}

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list) or not pairs:
        return {}

    normalized_chain = chain.lower()
    best: dict[str, Any] | None = None
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        pair_chain = str(pair.get("chainId", "")).lower()
        if pair_chain != normalized_chain:
            continue
        base = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
        base_symbol = str(base.get("symbol", "")).upper()
        if base_symbol != token.upper():
            continue
        best = pair
        break

    if best is None:
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            if str(pair.get("chainId", "")).lower() != normalized_chain:
                continue
            best = pair
            break

    if best is None:
        return {}

    base = best.get("baseToken") if isinstance(best.get("baseToken"), dict) else {}
    quote = best.get("quoteToken") if isinstance(best.get("quoteToken"), dict) else {}
    base_address = str(base.get("address", "")) if base else ""
    quote_address = str(quote.get("address", "")) if quote else ""

    result: dict[str, Any] = {
        "pool_address": best.get("pairAddress"),
        "dex": best.get("dexId"),
    }

    if normalized_chain == "solana":
        result["token_mint"] = base_address or None
        result["quote_mint"] = quote_address or None
        result["chain_type"] = "solana"
    else:
        result["token_contract"] = base_address or None
        result["quote_contract"] = quote_address or None
        result["chain_type"] = "evm"

    return result


def _build_source_from_profile(
    settings: AppSettings,
    profile: MeasurementProfile,
) -> object | None:
    """Build a single live source object from a resolved measurement profile.

    Returns ``None`` when the profile lacks the addresses required for the
    detected chain type.
    """
    if profile.chain_type == "solana":
        if not profile.token_mint or not profile.quote_mint:
            return None
        wallet_config = SolanaWalletTradeSourceConfig(
            enabled=True,
            chain="solana",
            token=profile.token,
            token_mint=profile.token_mint,
            quote_mint=profile.quote_mint,
            pool_address=profile.pool_address or "",
            source_name=f"profile_solana_wallet_{profile.token.lower()}",
            checkpoint_key=f"acquisition:profile:solana:{profile.chain}:{profile.token}",
        )
        return SolanaWalletTradeSource(settings, wallet_config)

    if profile.chain_type == "evm":
        if not profile.token_contract or not profile.quote_contract:
            return None
        route_config = EvmLiveSourceConfig(
            enabled=True,
            chain=profile.chain,
            token=profile.token,
            token_contract=profile.token_contract,
            quote_contract=profile.quote_contract,
            pool_address=profile.pool_address or "",
            source_name=f"profile_evm_pool_{profile.token.lower()}",
            checkpoint_key=f"acquisition:profile:evm:{profile.chain}:{profile.token}",
            source_type="pool_swap_trade",
        )
        return EvmPoolSwapTradeSource(settings, route_config)

    return None


def consume_discovery_events_for_measurement(
    redis_client: Redis,
    settings: AppSettings,
    registry: MeasurementProfileRegistry,
    *,
    count: int = 20,
    resolution_ttl_seconds: float = 3600.0,
    http_get: Callable[[str, dict[str, str], float], dict[str, Any]] | None = None,
) -> int:
    """Read discovery events from the raw-events stream and register profiles.

    Returns the number of newly registered profiles.
    """
    from infra.logging import get_logger

    logger = get_logger("signalengine.onchain_measurement_consumer")

    ensure_consumer_group(
        redis_client,
        settings.redis.raw_events_stream,
        MEASUREMENT_CONSUMER_GROUP,
    )
    messages = read_group_models(
        redis_client,
        settings.redis.raw_events_stream,
        MEASUREMENT_CONSUMER_GROUP,
        MEASUREMENT_CONSUMER_NAME,
        EventEnvelope,
        count=count,
        block_ms=100,
    )
    if not messages:
        return 0

    newly_registered = 0
    for message_id, event in messages:
        try:
            if event.event_type not in DISCOVERY_EVENTS_FOR_MEASUREMENT:
                continue

            existing = registry.get(event.chain, event.token)
            if existing is not None:
                continue  # already have an active profile for this token

            resolved = resolve_token_addresses_for_discovery(
                event.chain,
                event.token,
                http_get=http_get,
            )
            if not resolved:
                logger.info(
                    "measurement_profile_resolution_failed",
                    extra={
                        "service": "onchain_measurement_consumer",
                        "outcome": f"unresolved:{event.chain}:{event.token}",
                        "event_type": event.event_type,
                    },
                )
                continue

            profile = MeasurementProfile(
                profile_id=f"disc:{event.event_id}",
                chain=event.chain,
                token=event.token,
                discovery_event_type=event.event_type,
                discovery_event_id=event.event_id,
                registered_at=datetime.now(UTC),
                ttl_seconds=resolution_ttl_seconds,
                pool_address=resolved.get("pool_address"),
                token_mint=resolved.get("token_mint"),
                quote_mint=resolved.get("quote_mint"),
                token_contract=resolved.get("token_contract"),
                quote_contract=resolved.get("quote_contract"),
                dex=resolved.get("dex"),
                chain_type=resolved.get("chain_type"),
            )
            registry.register(profile)
            newly_registered += 1

            logger.info(
                "measurement_profile_registered",
                extra={
                    "service": "onchain_measurement_consumer",
                    "outcome": f"registered:{profile.chain}:{profile.token}",
                    "profile_id": profile.profile_id,
                    "chain_type": profile.chain_type,
                },
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "measurement_consumer_event_failed",
                extra={
                    "service": "onchain_measurement_consumer",
                    "outcome": event.event_id,
                },
            )
        finally:
            acknowledge_message(
                redis_client,
                settings.redis.raw_events_stream,
                MEASUREMENT_CONSUMER_GROUP,
                message_id,
            )

    return newly_registered


def build_live_sources(
    settings: AppSettings,
    *,
    registry: MeasurementProfileRegistry | None = None,
) -> list[object]:
    acquisition = AcquisitionConfig.model_validate(settings.acquisition)
    sources: list[object] = []

    # 1. Registry-based profiles (event-driven measurement)
    if registry is not None:
        for profile in registry.active_profiles():
            source = _build_source_from_profile(settings, profile)
            if source is not None:
                sources.append(source)

    # 2. Static acquisition config (explicitly configured measurement routes)
    if acquisition.solana_wallet_trade.enabled and _is_complete_solana_trade_source(
        acquisition.solana_wallet_trade
    ):
        sources.append(SolanaWalletTradeSource(settings, acquisition.solana_wallet_trade))
    if acquisition.jupiter_quote.enabled and _is_complete_jupiter_quote_source(
        acquisition.jupiter_quote
    ):
        sources.append(JupiterQuoteSource(settings, acquisition.jupiter_quote))
    for source_key, source_config in sorted(resolve_evm_routes(acquisition).items()):
        config = source_config.model_copy(
            update={
                "source_name": source_config.source_name or f"evm_{source_key}",
                "checkpoint_key": source_config.checkpoint_key or f"acquisition:evm_sources:{source_key}",
            }
        )
        if not config.enabled or not _is_complete_evm_source(config):
            continue
        if config.source_type == "transfer_trade":
            sources.append(EvmTransferTradeSource(settings, config))
        elif config.source_type == "pool_swap_trade":
            sources.append(EvmPoolSwapTradeSource(settings, config))
        elif config.source_type == "quote":
            sources.append(EvmQuoteSource(settings, config))
        else:
            raise ValueError(f"unsupported_evm_source_type:{config.source_type}")
    return sources


def _has_value(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_complete_solana_trade_source(config: SolanaWalletTradeSourceConfig) -> bool:
    has_identity = any(
        _has_value(value)
        for value in (config.signature_address, config.owner_address, config.wallet_address)
    )
    return (
        has_identity
        and _has_value(config.token)
        and _has_value(config.token_mint)
        and _has_value(config.quote_mint)
    )


def _is_complete_jupiter_quote_source(config: JupiterQuoteSourceConfig) -> bool:
    return (
        _has_value(config.token)
        and _has_value(config.input_mint)
        and _has_value(config.output_mint)
    )


def _is_complete_evm_source(config: EvmLiveSourceConfig) -> bool:
    if not _has_value(config.token):
        return False
    if config.source_type == "transfer_trade":
        return (
            _has_value(config.wallet_address)
            and _has_value(config.token_contract)
            and _has_value(config.quote_contract)
        )
    if config.source_type == "pool_swap_trade":
        return (
            _has_value(config.pool_address)
            and _has_value(config.token_contract)
            and _has_value(config.quote_contract)
        )
    if config.source_type == "quote":
        return _has_value(config.token_contract) and _has_value(config.quote_contract)
    return False


def _jupiter_headers(settings: AppSettings) -> dict[str, str]:
    credentials = settings.live.credentials.dex_providers.get("jupiter")
    if credentials is None or not credentials.api_key:
        return {}
    return {"x-api-key": credentials.api_key}


def _evm_quote_headers(settings: AppSettings, provider: str) -> dict[str, str]:
    live_settings = LiveConfig.model_validate(settings.live)
    credentials = live_settings.credentials.dex_providers.get(provider)
    headers: dict[str, str] = {}
    if provider == "zeroex":
        headers["0x-version"] = "v2"
        if credentials is not None and credentials.api_key:
            headers["0x-api-key"] = credentials.api_key
        return headers
    if credentials is not None and credentials.api_key:
        headers["Authorization"] = f"Bearer {credentials.api_key}"
    return headers


def _build_evm_quote_request(config: EvmLiveSourceConfig) -> EvmQuoteRequest:
    sell_amount = int(round(config.quote_notional_usd * (10**config.quote_decimals)))
    if config.api_provider == "zeroex":
        query = parse.urlencode(
            {
                "chainId": str(config.chain_id),
                "sellToken": str(config.quote_contract),
                "buyToken": str(config.token_contract),
                "sellAmount": str(sell_amount),
            }
        )
        return EvmQuoteRequest(
            method="GET",
            url=f"{config.quote_api_url}?{query}",
            body=None,
            provider_label="0x",
        )
    if config.api_provider == "odos":
        return EvmQuoteRequest(
            method="POST",
            url=config.quote_api_url,
            body={
                "chainId": config.chain_id,
                "inputTokens": [
                    {
                        "tokenAddress": str(config.quote_contract),
                        "amount": str(sell_amount),
                    }
                ],
                "outputTokens": [
                    {
                        "tokenAddress": str(config.token_contract),
                        "proportion": 1,
                    }
                ],
                "slippageLimitPercent": config.quote_slippage_bps / 100.0,
                "userAddr": config.wallet_address or "0x0000000000000000000000000000000000000000",
                "disableRFQs": True,
                "compact": True,
            },
            provider_label="odos",
        )
    raise ValueError("unsupported_evm_quote_provider")


def _parse_evm_quote_response(
    payload: dict[str, Any],
    config: EvmLiveSourceConfig,
    *,
    provider_label: str,
) -> tuple[float, dict[str, Any]]:
    if config.api_provider == "zeroex":
        buy_amount = float(_require_string_field(payload, "buyAmount"))
        route_fills = payload.get("route", {}).get("fills", [])
        return (
            buy_amount,
            {
                "provider": provider_label,
                "chain_id": config.chain_id,
                "fills": len(route_fills) if isinstance(route_fills, list) else 0,
            },
        )
    if config.api_provider == "odos":
        out_amounts = payload.get("outAmounts")
        if not isinstance(out_amounts, list) or not out_amounts or not isinstance(out_amounts[0], str):
            raise ValueError("invalid_odos_quote_out_amounts")
        path_viz = payload.get("pathViz")
        input_tokens = payload.get("inTokens")
        output_tokens = payload.get("outTokens")
        return (
            float(out_amounts[0]),
            {
                "provider": provider_label,
                "chain_id": config.chain_id,
                "path_id": payload.get("pathId") if isinstance(payload.get("pathId"), str) else None,
                "gas_estimate": payload.get("gasEstimate") if isinstance(payload.get("gasEstimate"), (int, float)) else None,
                "path_segments": len(path_viz) if isinstance(path_viz, list) else 0,
                "input_count": len(input_tokens) if isinstance(input_tokens, list) else 0,
                "output_count": len(output_tokens) if isinstance(output_tokens, list) else 0,
            },
        )
    raise ValueError("unsupported_evm_quote_provider")


def _swap_event_topic(pool_protocol: str) -> str | None:
    if pool_protocol in SUPPORTED_V2_SWAP_PROTOCOLS:
        return SWAP_EVENT_TOPIC_UNISWAP_V2
    if pool_protocol in SUPPORTED_V3_SWAP_PROTOCOLS:
        return SWAP_EVENT_TOPIC_UNISWAP_V3
    return None


def _extract_jupiter_price(payload: dict[str, Any], mint: str) -> float:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("invalid_jupiter_price_response")
    entry = data.get(mint)
    if not isinstance(entry, dict):
        raise ValueError("invalid_jupiter_price_response")
    price = entry.get("price")
    if isinstance(price, (int, float)):
        return float(price)
    if isinstance(price, str):
        return float(price)
    raise ValueError("invalid_jupiter_price_response")


def _extract_dexscreener_price(payload: dict[str, Any], chain: str) -> float:
    pairs = payload.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        raise ValueError("invalid_dexscreener_price_response")
    for pair in pairs:
        if isinstance(pair, dict) and str(pair.get("chainId", "")).lower() == chain.lower():
            price = pair.get("priceUsd")
            if isinstance(price, str):
                return float(price)
    first = pairs[0]
    if not isinstance(first, dict) or not isinstance(first.get("priceUsd"), str):
        raise ValueError("invalid_dexscreener_price_response")
    return float(first["priceUsd"])


def _extract_dexscreener_market_context(
    payload: dict[str, Any],
    *,
    chain: str,
    token_contract: str,
    quote_contract: str,
) -> dict[str, Any]:
    pairs = payload.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        return {}
    normalized_chain = chain.lower()
    normalized_token = token_contract.lower()
    normalized_quote = quote_contract.lower()
    best_pair: dict[str, Any] | None = None
    best_score: tuple[float, int, float, int] | None = None
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if str(pair.get("chainId", "")).lower() != normalized_chain:
            continue
        base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
        quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}
        addresses = {
            str(base_token.get("address", "")).lower(),
            str(quote_token.get("address", "")).lower(),
        }
        if normalized_token not in addresses:
            continue
        volume = pair.get("volume") if isinstance(pair.get("volume"), dict) else {}
        txns = pair.get("txns") if isinstance(pair.get("txns"), dict) else {}
        txns_m5 = txns.get("m5") if isinstance(txns.get("m5"), dict) else {}
        buys = int(txns_m5.get("buys", 0) or 0)
        sells = int(txns_m5.get("sells", 0) or 0)
        total_txns = buys + sells
        liquidity = pair.get("liquidity") if isinstance(pair.get("liquidity"), dict) else {}
        volume_m5 = float(volume.get("m5", 0.0) or 0.0)
        liquidity_usd = float(liquidity.get("usd", 0.0) or 0.0)
        quote_match = 1 if normalized_quote in addresses else 0
        score = (volume_m5, total_txns, liquidity_usd, quote_match)
        if best_score is None or score > best_score:
            best_pair = pair
            best_score = score
    if best_pair is None:
        return {}
    volume = best_pair.get("volume") if isinstance(best_pair.get("volume"), dict) else {}
    txns = best_pair.get("txns") if isinstance(best_pair.get("txns"), dict) else {}
    txns_m5 = txns.get("m5") if isinstance(txns.get("m5"), dict) else {}
    buys = int(txns_m5.get("buys", 0) or 0)
    sells = int(txns_m5.get("sells", 0) or 0)
    total_txns = buys + sells
    buy_pressure = (buys / total_txns) if total_txns > 0 else 0.0
    return {
        "market_source": "dexscreener",
        "market_pair_address": best_pair.get("pairAddress"),
        "market_dex_id": best_pair.get("dexId"),
        "volume_5m_usd": float(volume.get("m5", 0.0) or 0.0),
        "buy_pressure": round(buy_pressure, 6),
        "buy_txn_count_5m": buys,
        "sell_txn_count_5m": sells,
    }


def _merge_market_context(route_summary: dict[str, Any], market_context: dict[str, Any]) -> dict[str, Any]:
    merged = dict(route_summary)
    merged.update({key: value for key, value in market_context.items() if value is not None})
    return merged


def _extract_evm_quote_price(
    payload: dict[str, Any],
    *,
    provider: str,
    chain: str,
    token_contract: str,
) -> float:
    if provider == "odos":
        token_prices = payload.get("tokenPrices")
        if isinstance(token_prices, dict):
            token_price = token_prices.get(token_contract.lower()) or token_prices.get(token_contract)
            if isinstance(token_price, str):
                return float(token_price)
            if isinstance(token_price, (int, float)):
                return float(token_price)
        price = payload.get("price")
        if isinstance(price, dict):
            usd_value = price.get("usd")
            if isinstance(usd_value, str):
                return float(usd_value)
            if isinstance(usd_value, (int, float)):
                return float(usd_value)
    return _extract_dexscreener_price(payload, chain)


def _require_string_field(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"invalid_jupiter_quote_{field_name}")
    return value


def _owner_mint_delta(
    pre_balances: Any,
    post_balances: Any,
    owner: str,
    mint: str,
) -> float:
    return _owner_mint_balance(post_balances, owner, mint) - _owner_mint_balance(
        pre_balances,
        owner,
        mint,
    )


def _mint_delta(
    pre_balances: Any,
    post_balances: Any,
    mint: str,
) -> float:
    return _mint_balance(post_balances, mint) - _mint_balance(pre_balances, mint)


def _owner_mint_balance(balances: Any, owner: str, mint: str) -> float:
    if not isinstance(balances, list):
        return 0.0
    total = 0.0
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        if entry.get("owner") != owner or entry.get("mint") != mint:
            continue
        ui_token_amount = entry.get("uiTokenAmount")
        if not isinstance(ui_token_amount, dict):
            continue
        ui_amount = ui_token_amount.get("uiAmount")
        if isinstance(ui_amount, (int, float)):
            total += float(ui_amount)
            continue
        amount = ui_token_amount.get("amount")
        decimals = ui_token_amount.get("decimals")
        if isinstance(amount, str) and isinstance(decimals, int):
            total += float(amount) / float(10**decimals)
    return total


def _mint_balance(balances: Any, mint: str) -> float:
    if not isinstance(balances, list):
        return 0.0
    total = 0.0
    for entry in balances:
        if not isinstance(entry, dict) or entry.get("mint") != mint:
            continue
        ui_token_amount = entry.get("uiTokenAmount")
        if not isinstance(ui_token_amount, dict):
            continue
        ui_amount = ui_token_amount.get("uiAmount")
        if isinstance(ui_amount, (int, float)):
            total += float(ui_amount)
            continue
        amount = ui_token_amount.get("amount")
        decimals = ui_token_amount.get("decimals")
        if isinstance(amount, str) and isinstance(decimals, int):
            total += float(amount) / float(10**decimals)
    return total


def _infer_primary_owner(
    pre_balances: Any,
    post_balances: Any,
    mint: str,
) -> str | None:
    owners: set[str] = set()
    for balances in (pre_balances, post_balances):
        if not isinstance(balances, list):
            continue
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            if entry.get("mint") != mint:
                continue
            owner = entry.get("owner")
            if isinstance(owner, str) and owner:
                owners.add(owner)
    if not owners:
        return None
    ranked = sorted(
        owners,
        key=lambda owner: abs(_owner_mint_delta(pre_balances, post_balances, owner, mint)),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _evm_logs_delta(logs: list[dict[str, Any]], *, wallet_address: str, decimals: int) -> float:
    normalized_wallet = wallet_address.lower()
    delta = 0.0
    for log in logs:
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) < 3:
            continue
        from_address = _topic_address(topics[1])
        to_address = _topic_address(topics[2])
        value = _hex_to_int(log.get("data")) / float(10**decimals)
        if to_address == normalized_wallet:
            delta += value
        if from_address == normalized_wallet:
            delta -= value
    return delta


def _topic_address(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    raw = value.lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    return f"0x{raw[-40:]}"


def _hex_to_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.startswith("0x"):
        return int(value, 16)
    raise ValueError("invalid_hex_value")


def _to_hex(value: int) -> str:
    return hex(value)


def _decode_uint256_words(data: str, *, expected_words: int) -> list[int] | None:
    raw = data[2:] if data.startswith("0x") else data
    if len(raw) != expected_words * 64:
        return None
    return [int(raw[index:index + 64], 16) for index in range(0, len(raw), 64)]


def _decode_signed_int256_words(data: str, *, expected_words: int) -> list[int] | None:
    raw = data[2:] if data.startswith("0x") else data
    if len(raw) != expected_words * 64:
        return None
    values: list[int] = []
    for index in range(0, len(raw), 64):
        word = int(raw[index:index + 64], 16)
        if word >= 2**255:
            word -= 2**256
        values.append(word)
    return values


def _http_json_get_transport(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
) -> dict[str, Any]:
    return _http_json_transport(
        url=url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        method="GET",
        body=None,
    )


def _http_json_request_transport(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    method: str,
    body: dict[str, Any] | None,
) -> dict[str, Any]:
    return _http_json_transport(
        url=url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        method=method,
        body=body,
    )


def _http_json_transport(
    *,
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    method: str,
    body: dict[str, Any] | None,
) -> dict[str, Any]:
    request_headers = dict(headers)
    if body is not None:
        request_headers.setdefault("Content-Type", "application/json")

    last_error: Exception | None = None
    for http2_enabled in (True, False):
        for attempt in range(3):
            try:
                with httpx.Client(http2=http2_enabled, timeout=timeout_seconds) as client:
                    response = client.request(
                        method,
                        url,
                        headers=request_headers,
                        json=body,
                    )
                    response.raise_for_status()
                    payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("invalid_http_json_response")
                return payload
            except httpx.HTTPStatusError as exc:
                body_preview = exc.response.text[:200]
                raise ValueError(
                    f"invalid_http_json_status:{exc.response.status_code}:{body_preview}"
                ) from exc
            except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
                last_error = exc
                if isinstance(exc, ValueError) and str(exc) == "invalid_http_json_response":
                    raise
                if attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                break
    if last_error is not None:
        raise ValueError("http_json_transport_error") from last_error
    raise ValueError("http_json_transport_error")