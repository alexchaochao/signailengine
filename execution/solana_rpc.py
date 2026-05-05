from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request
from urllib.error import URLError

from core.config import VenueConfig

MAX_SOLANA_RPC_RESPONSE_BYTES = 1_000_000


@dataclass(frozen=True)
class SolanaHttpTransportResponse:
    status_code: int
    content_type: str | None
    payload: dict[str, Any]
    body_size_bytes: int = 0


RpcTransport = Callable[[str, dict[str, Any], float], SolanaHttpTransportResponse]


@dataclass(frozen=True)
class SolanaJsonRpcRequest:
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
class SolanaBlockhashState:
    blockhash: str
    slot: int


@dataclass(frozen=True)
class SolanaQuoteContext:
    rpc_url: str
    slippage_bps: int
    jito_enabled: bool
    latest_blockhash: str
    rpc_method: str


@dataclass(frozen=True)
class SolanaSubmissionResult:
    external_order_id: str
    signature: str
    submitted: bool
    transport: str


@dataclass(frozen=True)
class SolanaSubmissionState:
    signature: str
    rpc_method: str


@dataclass(frozen=True)
class SolanaBalanceState:
    lamports: int
    wallet_address: str


@dataclass(frozen=True)
class SolanaConfirmationState:
    signature: str
    confirmed: bool
    confirmation_status: str | None


class SolanaRpcClient:
    def __init__(
        self,
        config: VenueConfig,
        transport: RpcTransport | None = None,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or _http_json_rpc_transport
        self.timeout_seconds = timeout_seconds or config.solana_rpc_timeout_seconds
        self.max_retries = max_retries if max_retries is not None else config.solana_rpc_max_retries

    def build_quote_request(self) -> SolanaJsonRpcRequest:
        return SolanaJsonRpcRequest(
            request_id="solana-quote-context",
            method="getLatestBlockhash",
            params=[{"commitment": "processed"}],
        )

    def build_submit_request(
        self,
        intent_id: str,
        *,
        signed_transaction: str | None = None,
    ) -> SolanaJsonRpcRequest:
        return SolanaJsonRpcRequest(
            request_id=f"solana-submit:{intent_id}",
            method="sendTransaction",
            params=[
                signed_transaction or f"stub-signed:{intent_id}",
                {
                    "encoding": "base64",
                    "skipPreflight": False,
                    "maxRetries": 3,
                },
            ],
        )

    def build_signature_status_request(self, signature: str) -> SolanaJsonRpcRequest:
        return SolanaJsonRpcRequest(
            request_id=f"solana-confirm:{signature}",
            method="getSignatureStatuses",
            params=[[signature], {"searchTransactionHistory": True}],
        )

    def build_balance_request(self, wallet_address: str) -> SolanaJsonRpcRequest:
        return SolanaJsonRpcRequest(
            request_id=f"solana-balance:{wallet_address}",
            method="getBalance",
            params=[wallet_address, {"commitment": "processed"}],
        )

    def quote_context(self) -> SolanaQuoteContext:
        request = self.build_quote_request()
        response = self._dispatch(request)
        blockhash_state = self.parse_quote_response(response)
        return SolanaQuoteContext(
            rpc_url=self.config.solana_rpc_url,
            slippage_bps=self.config.solana_quote_slippage_bps,
            jito_enabled=self.config.solana_jito_enabled,
            latest_blockhash=blockhash_state.blockhash,
            rpc_method=request.method,
        )

    def submit_order(
        self,
        intent_id: str,
        *,
        signed_transaction: str | None = None,
    ) -> SolanaSubmissionResult:
        request = self.build_submit_request(intent_id, signed_transaction=signed_transaction)
        response = self._dispatch(request)
        submission_state = self.parse_submit_response(response)
        transport = "jito" if self.config.solana_jito_enabled else "rpc"
        return SolanaSubmissionResult(
            external_order_id=f"solana-submit:{submission_state.signature}",
            signature=submission_state.signature,
            submitted=True,
            transport=transport,
        )

    def confirm_submission(self, signature: str, *, confirmation_checks: int | None = None) -> bool:
        attempts = confirmation_checks if confirmation_checks is not None else max(1, self.max_retries + 1)
        for attempt in range(attempts):
            request = self.build_signature_status_request(signature)
            response = self._dispatch(request)
            confirmation = self.parse_signature_status_response(signature, response)
            if confirmation.confirmed:
                return True
            if attempt < attempts - 1:
                time.sleep(min(self.timeout_seconds / 10.0, 0.5))
        return False

    def wallet_balance(self, wallet_address: str) -> SolanaBalanceState:
        request = self.build_balance_request(wallet_address)
        response = self._dispatch(request)
        return self.parse_balance_response(wallet_address, response)

    def parse_quote_response(self, payload: dict[str, Any]) -> SolanaBlockhashState:
        result = self._extract_result(payload)
        context = self._require_mapping(result, "context")
        value = self._require_mapping(result, "value")
        blockhash = value.get("blockhash")
        slot = context.get("slot")
        if not isinstance(blockhash, str) or not isinstance(slot, int):
            raise ValueError("invalid_solana_quote_response")
        return SolanaBlockhashState(blockhash=blockhash, slot=slot)

    def parse_submit_response(self, payload: dict[str, Any]) -> SolanaSubmissionState:
        result = self._extract_result(payload)
        if not isinstance(result, str):
            raise ValueError("invalid_solana_submit_response")
        return SolanaSubmissionState(signature=result, rpc_method="sendTransaction")

    def parse_balance_response(
        self,
        wallet_address: str,
        payload: dict[str, Any],
    ) -> SolanaBalanceState:
        result = self._extract_result(payload)
        if not isinstance(result, dict):
            raise ValueError("invalid_solana_balance_response")
        lamports = result.get("value")
        if not isinstance(lamports, int):
            raise ValueError("invalid_solana_balance_response")
        return SolanaBalanceState(lamports=lamports, wallet_address=wallet_address)

    def parse_signature_status_response(
        self,
        signature: str,
        payload: dict[str, Any],
    ) -> SolanaConfirmationState:
        result = self._extract_result(payload)
        if not isinstance(result, dict):
            raise ValueError("invalid_solana_signature_status_response")
        value = result.get("value")
        if not isinstance(value, list) or not value:
            raise ValueError("invalid_solana_signature_status_response")
        entry = value[0]
        if entry is None:
            return SolanaConfirmationState(
                signature=signature,
                confirmed=False,
                confirmation_status=None,
            )
        if not isinstance(entry, dict):
            raise ValueError("invalid_solana_signature_status_response")
        if entry.get("err") is not None:
            raise ValueError("solana_submission_failed")
        confirmation_status = entry.get("confirmationStatus")
        if confirmation_status is not None and not isinstance(confirmation_status, str):
            raise ValueError("invalid_solana_signature_status_response")
        return SolanaConfirmationState(
            signature=signature,
            confirmed=confirmation_status in {"confirmed", "finalized"},
            confirmation_status=confirmation_status,
        )

    def _dispatch(self, request: SolanaJsonRpcRequest) -> dict[str, Any]:
        payload = request.to_payload()
        last_error: Exception | None = None
        for _ in range(self.max_retries + 1):
            try:
                response = self.transport(self.config.solana_rpc_url, payload, self.timeout_seconds)
                self._validate_http_response(response)
                self._validate_rpc_envelope(request, response.payload)
                return response.payload
            except (TimeoutError, URLError, OSError) as exc:
                last_error = exc
        if last_error is not None:
            raise ValueError("solana_rpc_transport_error") from last_error
        raise ValueError("solana_rpc_transport_error")

    def _validate_http_response(self, response: SolanaHttpTransportResponse) -> None:
        if response.status_code < 200 or response.status_code >= 300:
            raise ValueError(f"invalid_solana_rpc_http_status:{response.status_code}")
        if response.content_type is None:
            raise ValueError("invalid_solana_rpc_content_type")
        if response.body_size_bytes > MAX_SOLANA_RPC_RESPONSE_BYTES:
            raise ValueError("invalid_solana_rpc_response_too_large")
        normalized_content_type = response.content_type.split(";", 1)[0].strip().lower()
        if normalized_content_type != "application/json":
            raise ValueError("invalid_solana_rpc_content_type")

    def _validate_rpc_envelope(
        self,
        request_message: SolanaJsonRpcRequest,
        payload: dict[str, Any],
    ) -> None:
        if payload.get("jsonrpc") != request_message.jsonrpc:
            raise ValueError("invalid_solana_rpc_jsonrpc")
        if payload.get("id") != request_message.request_id:
            raise ValueError("invalid_solana_rpc_response_id")

    def _extract_result(self, payload: dict[str, Any]) -> Any:
        if "error" in payload:
            error = payload["error"]
            if isinstance(error, dict) and "code" in error:
                raise ValueError(f"solana_rpc_error:{error['code']}")
            raise ValueError("solana_rpc_error")
        if "result" not in payload:
            raise ValueError("invalid_solana_rpc_response")
        return payload["result"]

    def _require_mapping(self, payload: Any, field_name: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError(f"invalid_solana_rpc_{field_name}")
        value = payload.get(field_name)
        if not isinstance(value, dict):
            raise ValueError(f"invalid_solana_rpc_{field_name}")
        return value


def _http_json_rpc_transport(
    rpc_url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> SolanaHttpTransportResponse:
    body = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        rpc_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=timeout_seconds) as response:
        status_code = response.status
        content_type = response.headers.get("Content-Type")
        raw_body = response.read()
    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_solana_rpc_response") from exc
    if not isinstance(parsed, dict):
        raise ValueError("invalid_solana_rpc_response")
    return SolanaHttpTransportResponse(
        status_code=status_code,
        content_type=content_type,
        payload=parsed,
        body_size_bytes=len(raw_body),
    )