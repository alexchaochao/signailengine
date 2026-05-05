from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.error import URLError

import httpx

from core.config import NativeAssetRpcConfig, VenueConfig
from execution.solana_rpc import SolanaHttpTransportResponse

MAX_EVM_RPC_RESPONSE_BYTES = 1_000_000
WEI_PER_ETH = 1_000_000_000_000_000_000

EvmRpcTransport = Callable[[str, dict[str, Any], float], SolanaHttpTransportResponse]


@dataclass(frozen=True)
class EvmJsonRpcRequest:
    method: str
    params: list[Any]
    request_id: str
    jsonrpc: str = "2.0"

    def to_payload(self) -> dict[str, Any]:
        return {
            "jsonrpc": self.jsonrpc,
            "id": self.request_id,
            "method": self.method,
            "params": self.params,
        }


@dataclass(frozen=True)
class EvmBalanceState:
    wallet_address: str
    wei_balance: int


@dataclass(frozen=True)
class EvmSubmissionResult:
    external_order_id: str
    transaction_hash: str
    submitted: bool
    transport: str


class EvmRpcClient:
    def __init__(
        self,
        config: VenueConfig,
        chain: str,
        transport: EvmRpcTransport | None = None,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        self.config = config
        self.chain = chain
        self.chain_config = self._resolve_chain_config(chain)
        self.transport = transport or _http_json_rpc_transport
        self.timeout_seconds = timeout_seconds or self.chain_config.timeout_seconds
        self.max_retries = (
            max_retries if max_retries is not None else self.chain_config.max_retries
        )

    def build_balance_request(self, wallet_address: str) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=f"evm-balance:{wallet_address}",
            method="eth_getBalance",
            params=[wallet_address, "latest"],
        )

    def build_transaction_count_request(self, wallet_address: str) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=f"evm-nonce:{wallet_address}",
            method="eth_getTransactionCount",
            params=[wallet_address, "pending"],
        )

    def build_send_raw_transaction_request(self, signed_transaction: str) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id="evm-submit",
            method="eth_sendRawTransaction",
            params=[_normalize_hex_data(signed_transaction)],
        )

    def build_block_request(self, block_tag: str = "latest") -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=f"evm-block:{block_tag}",
            method="eth_getBlockByNumber",
            params=[block_tag, False],
        )

    def build_call_request(
        self,
        call_object: dict[str, Any],
        block_tag: str = "latest",
        *,
        request_id: str = "evm-call",
    ) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=request_id,
            method="eth_call",
            params=[call_object, block_tag],
        )

    def build_estimate_gas_request(
        self,
        call_object: dict[str, Any],
        *,
        request_id: str = "evm-estimate-gas",
    ) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=request_id,
            method="eth_estimateGas",
            params=[call_object],
        )

    def build_transaction_receipt_request(self, transaction_hash: str) -> EvmJsonRpcRequest:
        return EvmJsonRpcRequest(
            request_id=f"evm-receipt:{transaction_hash}",
            method="eth_getTransactionReceipt",
            params=[transaction_hash],
        )

    def wallet_balance(self, wallet_address: str) -> EvmBalanceState:
        request_message = self.build_balance_request(wallet_address)
        payload = self._dispatch(request_message)
        return self.parse_balance_response(wallet_address, payload)

    def transaction_count(self, wallet_address: str) -> int:
        request_message = self.build_transaction_count_request(wallet_address)
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("invalid_evm_transaction_count_response")
        try:
            return int(result, 16)
        except ValueError as error:
            raise ValueError("invalid_evm_transaction_count_response") from error

    def send_raw_transaction(self, signed_transaction: str) -> EvmSubmissionResult:
        request_message = self.build_send_raw_transaction_request(signed_transaction)
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("invalid_evm_submission_response")
        return EvmSubmissionResult(
            external_order_id=f"evm-submit:{result}",
            transaction_hash=result,
            submitted=True,
            transport="rpc",
        )

    def latest_base_fee_per_gas(self) -> int | None:
        request_message = self.build_block_request("latest")
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if not isinstance(result, dict):
            raise ValueError("invalid_evm_block_response")
        base_fee = result.get("baseFeePerGas")
        if base_fee is None:
            return None
        if not isinstance(base_fee, str) or not base_fee.startswith("0x"):
            raise ValueError("invalid_evm_block_response")
        try:
            return int(base_fee, 16)
        except ValueError as error:
            raise ValueError("invalid_evm_block_response") from error

    def call(self, call_object: dict[str, Any], block_tag: str = "latest") -> str:
        request_message = self.build_call_request(call_object, block_tag)
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if not isinstance(result, str):
            raise ValueError("invalid_evm_call_response")
        return result

    def estimate_gas(self, call_object: dict[str, Any]) -> int:
        request_message = self.build_estimate_gas_request(call_object)
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("invalid_evm_estimate_gas_response")
        try:
            return int(result, 16)
        except ValueError as error:
            raise ValueError("invalid_evm_estimate_gas_response") from error

    def transaction_receipt(self, transaction_hash: str) -> dict[str, Any] | None:
        request_message = self.build_transaction_receipt_request(transaction_hash)
        payload = self._dispatch(request_message)
        result = self._extract_result(payload)
        if result is None:
            return None
        if not isinstance(result, dict):
            raise ValueError("invalid_evm_transaction_receipt")
        return result

    def confirm_submission(self, transaction_hash: str) -> bool:
        receipt = self.transaction_receipt(transaction_hash)
        if receipt is None:
            return False
        status = receipt.get("status")
        if not isinstance(status, str) or not status.startswith("0x"):
            raise ValueError("invalid_evm_transaction_receipt")
        return int(status, 16) == 1

    def parse_balance_response(
        self,
        wallet_address: str,
        payload: dict[str, Any],
    ) -> EvmBalanceState:
        result = self._extract_result(payload)
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("invalid_evm_balance_response")
        try:
            wei_balance = int(result, 16)
        except ValueError as error:
            raise ValueError("invalid_evm_balance_response") from error
        return EvmBalanceState(wallet_address=wallet_address, wei_balance=wei_balance)

    def _dispatch(self, request_message: EvmJsonRpcRequest) -> dict[str, Any]:
        payload = request_message.to_payload()
        last_error: Exception | None = None
        for _ in range(self.max_retries + 1):
            try:
                response = self.transport(self.chain_config.url, payload, self.timeout_seconds)
                self._validate_http_response(response)
                self._validate_rpc_envelope(request_message, response.payload)
                return response.payload
            except (TimeoutError, URLError, OSError) as error:
                last_error = error
        if last_error is not None:
            raise ValueError("evm_rpc_transport_error") from last_error
        raise ValueError("evm_rpc_transport_error")

    def _validate_http_response(self, response: SolanaHttpTransportResponse) -> None:
        if response.status_code < 200 or response.status_code >= 300:
            raise ValueError(f"invalid_evm_rpc_http_status:{response.status_code}")
        if response.content_type is None:
            raise ValueError("invalid_evm_rpc_content_type")
        if response.body_size_bytes > MAX_EVM_RPC_RESPONSE_BYTES:
            raise ValueError("invalid_evm_rpc_response_too_large")
        normalized_content_type = response.content_type.split(";", 1)[0].strip().lower()
        if normalized_content_type != "application/json":
            raise ValueError("invalid_evm_rpc_content_type")

    def _validate_rpc_envelope(
        self,
        request_message: EvmJsonRpcRequest,
        payload: dict[str, Any],
    ) -> None:
        if payload.get("jsonrpc") != request_message.jsonrpc:
            raise ValueError("invalid_evm_rpc_jsonrpc")
        if payload.get("id") != request_message.request_id:
            raise ValueError("invalid_evm_rpc_response_id")

    def _extract_result(self, payload: dict[str, Any]) -> Any:
        if "error" in payload:
            error = payload["error"]
            if isinstance(error, dict) and "code" in error:
                raise ValueError(f"evm_rpc_error:{error['code']}")
            raise ValueError("evm_rpc_error")
        if "result" not in payload:
            raise ValueError("invalid_evm_rpc_response")
        return payload["result"]

    def _resolve_chain_config(self, chain: str) -> NativeAssetRpcConfig:
        raw_config = self.config.native_asset_rpc.get(chain)
        if raw_config is None:
            raise ValueError(f"unsupported_evm_rpc_chain:{chain}")
        if isinstance(raw_config, NativeAssetRpcConfig):
            return raw_config
        if isinstance(raw_config, dict):
            return NativeAssetRpcConfig.model_validate(raw_config)
        raise ValueError(f"invalid_evm_rpc_chain_config:{chain}")


def _http_json_rpc_transport(
    rpc_url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> SolanaHttpTransportResponse:
    last_error: Exception | None = None
    for http2_enabled in (True, False):
        for attempt in range(3):
            try:
                with httpx.Client(http2=http2_enabled, timeout=timeout_seconds) as client:
                    response = client.post(
                        rpc_url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                    )
                status_code = response.status_code
                content_type = response.headers.get("Content-Type")
                raw_body = response.content
                break
            except (httpx.TimeoutException, httpx.TransportError) as error:
                last_error = error
                if attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                break
        else:
            continue
        if last_error is None or 'raw_body' in locals():
            break
    else:
        raise URLError("evm_rpc_transport_error") from last_error

    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("invalid_evm_rpc_response") from error
    if not isinstance(parsed, dict):
        raise ValueError("invalid_evm_rpc_response")
    return SolanaHttpTransportResponse(
        status_code=status_code,
        content_type=content_type,
        payload=parsed,
        body_size_bytes=len(raw_body),
    )


def _normalize_hex_data(value: str) -> str:
    if not value:
        raise ValueError("invalid_evm_hex_data")
    if value.startswith(("0x", "0X")):
        return "0x" + value[2:]
    return f"0x{value}"
